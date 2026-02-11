package com.hrm.fs.core

import com.hrm.fs.api.FsEntry
import com.hrm.fs.api.FsError
import com.hrm.fs.api.FsMeta
import com.hrm.fs.api.FsPermissions
import com.hrm.fs.api.FsType
import com.hrm.fs.api.PathUtils
import com.hrm.fs.api.log.FLog
import com.hrm.fs.core.persistence.SnapshotNode
import com.hrm.fs.core.persistence.SnapshotPermissions
import com.hrm.fs.core.persistence.WalEntry
import kotlin.time.Clock

/**
 * 纯内存虚拟文件树。
 *
 * 负责内存中 [VfsNode] 树结构的增删改查、快照序列化 / 反序列化、WAL 回放。
 * **无线程安全保证**，外部需自行加锁。
 */
internal class VfsTree {
    companion object {
        private const val TAG = "VfsTree"

        /** 符号链接解析的最大深度，防止循环引用。 */
        private const val MAX_SYMLINK_DEPTH = 40

        fun splitParent(path: String): Pair<String, String> {
            val normalized = PathUtils.normalize(path)
            val idx = normalized.lastIndexOf('/')
            return if (idx <= 0) "/" to normalized.removePrefix("/")
            else normalized.take(idx) to normalized.substring(idx + 1)
        }

        fun nowMillis(): Long = Clock.System.now().toEpochMilliseconds()
    }

    var root: DirNode = DirNode("/", nowMillis(), nowMillis(), FsPermissions.FULL)
        private set

    /** 增量维护的已用字节数缓存，避免每次都遍历整棵树。 */
    private var _usedBytes: Long = 0L

    // ── 节点解析 ────────────────────────────────────────────────

    /**
     * 内联路径段迭代器，避免 split("/") 分配列表。
     * 对 [path] 中每个路径段调用 [action](segment, isLast)。
     */
    private inline fun forEachSegment(path: String, action: (segment: String, index: Int, isLast: Boolean) -> Unit) {
        val str = if (path.startsWith("/")) path else "/$path"
        var start = 1 // 跳过开头的 '/'
        var segIndex = 0
        while (start < str.length) {
            val end = str.indexOf('/', start)
            if (end < 0) {
                // 最后一段
                val seg = str.substring(start)
                if (seg.isNotEmpty()) action(seg, segIndex, true)
                return
            }
            val seg = str.substring(start, end)
            if (seg.isNotEmpty()) {
                // 预判是否为最后一段：看后面是否还有非空内容
                val remaining = str.substring(end + 1)
                val isLast = remaining.isBlank() || remaining.all { it == '/' }
                action(seg, segIndex, isLast)
                segIndex++
            }
            start = end + 1
        }
    }

    /** 计算路径段数（不分配列表）。 */
    private fun segmentCount(path: String): Int {
        val str = if (path.startsWith("/")) path else "/$path"
        var count = 0
        var start = 1
        while (start < str.length) {
            val end = str.indexOf('/', start)
            if (end < 0) {
                if (start < str.length) count++
                break
            }
            if (end > start) count++
            start = end + 1
        }
        return count
    }

    /**
     * 解析路径到节点，默认跟随符号链接。
     *
     * @param path 要解析的路径
     * @param followSymlinks 是否跟随符号链接（默认 true）
     * @return 解析到的节点，未找到返回 null
     */
    fun resolveNode(path: String, followSymlinks: Boolean = true): VfsNode? {
        return resolveNodeInternal(path, followSymlinks, 0)
    }

    /**
     * 内部递归解析，带深度计数防循环。
     */
    private fun resolveNodeInternal(path: String, followSymlinks: Boolean, depth: Int): VfsNode? {
        if (depth > MAX_SYMLINK_DEPTH) return null
        if (path == "/") return root
        val totalSegments = segmentCount(path)
        var current: VfsNode = root
        var currentPath = ""
        forEachSegment(path) { part, index, _ ->
            // 如果当前节点是 symlink 且需要跟随，先解析 symlink
            val cur = current
            if (cur is SymlinkNode && followSymlinks) {
                val resolved = resolveSymlinkTarget(cur, currentPath, depth + 1) ?: return null
                current = resolved
            }
            val dir = current as? DirNode ?: return null
            val child = dir.children[part] ?: return null
            currentPath = if (currentPath.isEmpty()) "/$part" else "$currentPath/$part"
            // 对于中间路径段的 symlink，总是跟随
            if (child is SymlinkNode && followSymlinks && index < totalSegments - 1) {
                val resolved = resolveSymlinkTarget(child, currentPath, depth + 1) ?: return null
                current = resolved
            } else if (child is SymlinkNode && followSymlinks && index == totalSegments - 1) {
                // 最后一段也跟随
                val resolved = resolveSymlinkTarget(child, currentPath, depth + 1) ?: return null
                current = resolved
            } else {
                current = child
            }
        }
        return current
    }

    /**
     * 解析 symlink 节点指向的目标。
     *
     * @param symlink 符号链接节点
     * @param symlinkPath 符号链接自身的绝对路径
     * @param depth 当前递归深度
     */
    private fun resolveSymlinkTarget(symlink: SymlinkNode, symlinkPath: String, depth: Int): VfsNode? {
        if (depth > MAX_SYMLINK_DEPTH) return null
        val target = symlink.targetPath
        val absoluteTarget = if (target.startsWith("/")) {
            PathUtils.normalize(target)
        } else {
            // 相对路径：基于 symlink 所在目录解析
            val parentDir = symlinkPath.substringBeforeLast('/', "/")
            PathUtils.normalize("$parentDir/$target")
        }
        return resolveNodeInternal(absoluteTarget, followSymlinks = true, depth = depth)
    }

    /**
     * 解析路径并返回解析后的绝对路径（跟随 symlink）。
     * 用于需要知道最终路径的场景（如 stat 返回的路径）。
     */
    fun resolvePathFollowingSymlinks(path: String): String? {
        return resolvePathInternal(path, 0)
    }

    private fun resolvePathInternal(path: String, depth: Int): String? {
        if (depth > MAX_SYMLINK_DEPTH) return null
        if (path == "/") return "/"
        // resolvePathInternal 需要 parts.drop() 拼接剩余路径，仍使用 split，
        // 但此方法不在热路径上（仅 stat 的 symlink 分支调用），影响可忽略。
        val parts = path.removePrefix("/").split("/").filter { it.isNotBlank() }
        var current: VfsNode = root
        var currentPath = ""
        for ((index, part) in parts.withIndex()) {
            if (current is SymlinkNode) {
                val target = current.targetPath
                val absoluteTarget = if (target.startsWith("/")) {
                    PathUtils.normalize(target)
                } else {
                    val parentDir = currentPath.substringBeforeLast('/', "/")
                    PathUtils.normalize("$parentDir/$target")
                }
                val resolvedPath = resolvePathInternal(absoluteTarget, depth + 1) ?: return null
                val remaining = parts.drop(index).joinToString("/")
                return resolvePathInternal("$resolvedPath/$remaining", depth + 1)
            }
            if (current !is DirNode) return null
            val child = current.children[part] ?: return null
            currentPath = if (currentPath.isEmpty()) "/$part" else "$currentPath/$part"
            if (child is SymlinkNode && index < parts.size - 1) {
                // 中间 symlink：解析并拼接剩余路径
                val target = child.targetPath
                val absoluteTarget = if (target.startsWith("/")) {
                    PathUtils.normalize(target)
                } else {
                    val parentDir = currentPath.substringBeforeLast('/', "/")
                    PathUtils.normalize("$parentDir/$target")
                }
                val resolvedPath = resolvePathInternal(absoluteTarget, depth + 1) ?: return null
                val remaining = parts.drop(index + 1).joinToString("/")
                return resolvePathInternal("$resolvedPath/$remaining", depth + 1)
            }
            current = child
        }
        // 最后一个节点如果是 symlink，不解析（返回 symlink 自身路径）
        return currentPath.ifEmpty { "/" }
    }

    fun resolveNodeOrError(path: String): Result<VfsNode> {
        val node = resolveNode(path) ?: return Result.failure(FsError.NotFound(path))
        return Result.success(node)
    }

    /**
     * 解析节点，不跟随最后一段的 symlink。返回错误时区分循环引用。
     */
    fun resolveNodeNoFollowOrError(path: String): Result<VfsNode> {
        val node = resolveNodeNoFollow(path) ?: return Result.failure(FsError.NotFound(path))
        return Result.success(node)
    }

    /**
     * 解析路径到节点，不跟随最后一段的符号链接（但中间路径段的 symlink 仍会跟随）。
     * 用于 readLink / delete symlink 等场景。
     */
    fun resolveNodeNoFollow(path: String): VfsNode? {
        if (path == "/") return root
        val totalSegments = segmentCount(path)
        var current: VfsNode = root
        var currentPath = ""
        forEachSegment(path) { part, index, _ ->
            val cur = current
            if (cur is SymlinkNode) {
                val resolved = resolveSymlinkTarget(cur, currentPath, 1) ?: return null
                current = resolved
            }
            val dir = current as? DirNode ?: return null
            val child = dir.children[part] ?: return null
            currentPath = if (currentPath.isEmpty()) "/$part" else "$currentPath/$part"
            if (index < totalSegments - 1 && child is SymlinkNode) {
                // 中间路径段：跟随 symlink
                val resolved = resolveSymlinkTarget(child, currentPath, 1) ?: return null
                current = resolved
            } else {
                current = child
            }
        }
        return current
    }

    fun resolveDirOrError(path: String): Result<DirNode> {
        val node = resolveNode(path) ?: return Result.failure(FsError.NotFound(path))
        if (node !is DirNode) return Result.failure(FsError.NotDirectory(path))
        return Result.success(node)
    }

    // ── 创建 ──────────────────────────────────────────────────

    fun createFile(normalized: String): Result<Unit> {
        val (parent, name) = splitParent(normalized)
        val parentNode = resolveDirOrError(parent).getOrElse { return Result.failure(it) }
        if (!parentNode.permissions.canWrite()) return Result.failure(
            FsError.PermissionDenied(
                parent
            )
        )
        if (parentNode.children.containsKey(name)) return Result.failure(
            FsError.AlreadyExists(
                normalized
            )
        )
        val now = nowMillis()
        parentNode.children[name] = FileNode(name, now, now, FsPermissions.FULL)
        parentNode.modifiedAtMillis = now
        return Result.success(Unit)
    }

    fun createDir(normalized: String): Result<Unit> {
        if (normalized == "/") return Result.success(Unit)
        val (parent, name) = splitParent(normalized)
        val parentNode = resolveDirOrError(parent).getOrElse { return Result.failure(it) }
        if (!parentNode.permissions.canWrite()) return Result.failure(
            FsError.PermissionDenied(
                parent
            )
        )
        if (parentNode.children.containsKey(name)) return Result.failure(
            FsError.AlreadyExists(
                normalized
            )
        )
        val now = nowMillis()
        parentNode.children[name] = DirNode(name, now, now, FsPermissions.FULL)
        parentNode.modifiedAtMillis = now
        return Result.success(Unit)
    }

    fun createSymlink(normalized: String, targetPath: String): Result<Unit> {
        val (parent, name) = splitParent(normalized)
        val parentNode = resolveDirOrError(parent).getOrElse { return Result.failure(it) }
        if (!parentNode.permissions.canWrite()) return Result.failure(
            FsError.PermissionDenied(parent)
        )
        if (parentNode.children.containsKey(name)) return Result.failure(
            FsError.AlreadyExists(normalized)
        )
        val now = nowMillis()
        parentNode.children[name] = SymlinkNode(name, now, now, FsPermissions.FULL, targetPath)
        parentNode.modifiedAtMillis = now
        return Result.success(Unit)
    }

    fun readLink(normalized: String): Result<String> {
        val node = resolveNodeNoFollow(normalized)
            ?: return Result.failure(FsError.NotFound(normalized))
        if (node !is SymlinkNode) return Result.failure(FsError.InvalidPath("非符号链接: $normalized"))
        return Result.success(node.targetPath)
    }

    // ── 删除 ──────────────────────────────────────────────────
    fun delete(normalized: String): Result<Unit> {
        if (normalized == "/") return Result.failure(FsError.PermissionDenied("/"))
        val (parent, name) = splitParent(normalized)
        val parentNode = resolveDirOrError(parent).getOrElse { return Result.failure(it) }
        if (!parentNode.permissions.canWrite()) return Result.failure(
            FsError.PermissionDenied(
                parent
            )
        )
        val node = parentNode.children[name] ?: return Result.failure(FsError.NotFound(normalized))
        if (node is DirNode && node.children.isNotEmpty()) return Result.failure(
            FsError.PermissionDenied(
                normalized
            )
        )
        if (node is FileNode) {
            _usedBytes -= node.size.toLong()
        }
        parentNode.children.remove(name)
        parentNode.modifiedAtMillis = nowMillis()
        return Result.success(Unit)
    }

    // ── 读取 ──────────────────────────────────────────────────
    fun readDir(normalized: String): Result<List<FsEntry>> {
        val dir = resolveDirOrError(normalized).getOrElse { return Result.failure(it) }
        if (!dir.permissions.canRead()) return Result.failure(FsError.PermissionDenied(normalized))
        return Result.success(dir.children.values.map { FsEntry(it.name, it.type) })
    }

    fun stat(normalized: String): Result<FsMeta> {
        val node = resolveNodeOrError(normalized).getOrElse { return Result.failure(it) }
        if (!node.permissions.canRead()) return Result.failure(FsError.PermissionDenied(normalized))
        val size = if (node is FileNode) node.size.toLong() else 0L
        val target = if (node is SymlinkNode) node.targetPath else null
        return Result.success(
            FsMeta(
                path = normalized, type = node.type, size = size,
                createdAtMillis = node.createdAtMillis,
                modifiedAtMillis = node.modifiedAtMillis,
                permissions = node.permissions,
                target = target
            )
        )
    }

    /**
     * stat 不跟随 symlink（lstat 语义）。
     * 当路径本身是 symlink 时，返回 symlink 自身的元数据。
     */
    fun lstat(normalized: String): Result<FsMeta> {
        val node = resolveNodeNoFollow(normalized)
            ?: return Result.failure(FsError.NotFound(normalized))
        if (!node.permissions.canRead()) return Result.failure(FsError.PermissionDenied(normalized))
        val size = if (node is FileNode) node.size.toLong() else 0L
        val target = if (node is SymlinkNode) node.targetPath else null
        return Result.success(
            FsMeta(
                path = normalized, type = node.type, size = size,
                createdAtMillis = node.createdAtMillis,
                modifiedAtMillis = node.modifiedAtMillis,
                permissions = node.permissions,
                target = target
            )
        )
    }

    // ── 权限 ──────────────────────────────────────────────────

    fun setPermissions(normalized: String, permissions: FsPermissions): Result<Unit> {
        val node = resolveNodeOrError(normalized).getOrElse { return Result.failure(it) }
        node.permissions = permissions
        node.modifiedAtMillis = nowMillis()
        return Result.success(Unit)
    }

    // ── 扩展属性（xattr） ──────────────────────────────────────

    fun setXattr(normalized: String, name: String, value: ByteArray): Result<Unit> {
        val node = resolveNodeOrError(normalized).getOrElse { return Result.failure(it) }
        if (!node.permissions.canWrite()) return Result.failure(FsError.PermissionDenied(normalized))
        node.xattrs[name] = value.copyOf()
        node.modifiedAtMillis = nowMillis()
        return Result.success(Unit)
    }

    fun getXattr(normalized: String, name: String): Result<ByteArray> {
        val node = resolveNodeOrError(normalized).getOrElse { return Result.failure(it) }
        if (!node.permissions.canRead()) return Result.failure(FsError.PermissionDenied(normalized))
        val value = node.xattrs[name] ?: return Result.failure(FsError.NotFound("xattr '$name' on $normalized"))
        return Result.success(value.copyOf())
    }

    fun removeXattr(normalized: String, name: String): Result<Unit> {
        val node = resolveNodeOrError(normalized).getOrElse { return Result.failure(it) }
        if (!node.permissions.canWrite()) return Result.failure(FsError.PermissionDenied(normalized))
        if (node.xattrs.remove(name) == null) {
            return Result.failure(FsError.NotFound("xattr '$name' on $normalized"))
        }
        node.modifiedAtMillis = nowMillis()
        return Result.success(Unit)
    }

    fun listXattrs(normalized: String): Result<List<String>> {
        val node = resolveNodeOrError(normalized).getOrElse { return Result.failure(it) }
        if (!node.permissions.canRead()) return Result.failure(FsError.PermissionDenied(normalized))
        return Result.success(node.xattrs.keys.toList())
    }

    // ── 文件读写（内存） ──────────────────────────────────────
    fun readAt(node: FileNode, offset: Long, length: Int): Result<ByteArray> {
        if (!node.permissions.canRead()) return Result.failure(FsError.PermissionDenied(node.name))
        if (offset < 0 || length < 0) return Result.failure(FsError.InvalidPath("offset/length"))
        if (offset >= node.size) return Result.success(ByteArray(0))
        return Result.success(node.blocks.read(offset.toInt(), length))
    }

    fun writeAt(node: FileNode, offset: Long, data: ByteArray): Result<Unit> {
        if (!node.permissions.canWrite()) return Result.failure(FsError.PermissionDenied(node.name))
        if (offset < 0) return Result.failure(FsError.InvalidPath("offset"))
        val oldSize = node.size.toLong()
        node.blocks.write(offset.toInt(), data)
        _usedBytes += node.size.toLong() - oldSize
        node.modifiedAtMillis = nowMillis()
        return Result.success(Unit)
    }

    // ── 空间统计 ──────────────────────────────────────────────

    /** 返回增量维护的已用字节数（O(1)）。 */
    fun totalUsedBytes(): Long = _usedBytes

    // ── 搜索 / 查找（内存树部分） ────────────────────────────

    /**
     * 在内存文件树中搜索匹配条件的节点。
     *
     * DFS 遍历，对每个节点调用 [matcher] 判断是否匹配。
     * 不进入挂载点目录（挂载点由外部另行处理）。
     *
     * @param rootPath 搜索起始路径
     * @param maxDepth 最大递归深度（-1 无限制）
     * @param mountPoints 活跃挂载点集合，遍历时跳过挂载点子树
     * @param matcher 匹配回调：(path, node) -> 是否匹配
     * @return 匹配的 (path, node) 列表
     */
    fun find(
        rootPath: String,
        maxDepth: Int,
        mountPoints: Set<String>,
        matcher: (String, VfsNode) -> Boolean
    ): List<Pair<String, VfsNode>> {
        val startNode = resolveNode(rootPath) ?: return emptyList()
        val results = mutableListOf<Pair<String, VfsNode>>()

        fun walk(node: VfsNode, path: String, depth: Int) {
            // 跳过挂载点子树（由外部处理）
            if (path != rootPath && mountPoints.contains(path)) return

            if (matcher(path, node)) {
                results.add(path to node)
            }

            if (node is DirNode && (maxDepth < 0 || depth < maxDepth)) {
                for ((childName, child) in node.children) {
                    val childPath = if (path == "/") "/$childName" else "$path/$childName"
                    walk(child, childPath, depth + 1)
                }
            }
        }

        // 根节点本身不算深度，从 depth=0 开始
        if (startNode is DirNode) {
            // 对于目录，先检查根本身，再遍历子节点
            if (matcher(rootPath, startNode)) {
                results.add(rootPath to startNode)
            }
            if (maxDepth != 0) {
                for ((childName, child) in startNode.children) {
                    val childPath = if (rootPath == "/") "/$childName" else "$rootPath/$childName"
                    walk(child, childPath, 1)
                }
            }
        } else {
            // 搜索单个文件/symlink
            if (matcher(rootPath, startNode)) {
                results.add(rootPath to startNode)
            }
        }

        return results
    }

    // ── 确保挂载点目录存在 ────────────────────────────────────
    fun ensureDirPath(mountPath: String) {
        val parts = mountPath.removePrefix("/").split("/").filter { it.isNotBlank() }
        var current: DirNode = root
        for (part in parts) {
            val child = current.children[part]
            if (child is DirNode) {
                current = child
            } else {
                val now = nowMillis()
                val newDir = DirNode(part, now, now, FsPermissions.FULL)
                current.children[part] = newDir
                current = newDir
            }
        }
    }

    // ── 搜索节点路径（用于 WAL 记录） ────────────────────────
    fun pathForNode(node: FileNode): String {
        val result = StringBuilder()
        fun dfs(current: VfsNode, target: FileNode, prefix: String): Boolean {
            if (current == target) {
                result.append(prefix); return true
            }
            if (current is DirNode) {
                for ((name, child) in current.children) {
                    val next = if (prefix == "/") "/$name" else "$prefix/$name"
                    if (dfs(child, target, next)) return true
                }
            }
            return false
        }
        dfs(root, node, "/")
        return result.toString().ifBlank { "/" }
    }

    // ── 快照 ─────────────────────────────────────────────────

    fun toSnapshot(): SnapshotNode = snapshotFromNode(root)

    fun restoreFromSnapshot(snapshot: SnapshotNode) {
        FLog.d(TAG, "restoreFromSnapshot: root=${snapshot.name}")
        root = buildFromSnapshot(snapshot)
        _usedBytes = recalcUsedBytes()
    }

    // ── WAL 回放（静默，不抛异常） ───────────────────────────

    fun replayWal(entries: List<WalEntry>) {
        FLog.d(TAG, "replayWal: ${entries.size} entries")
        entries.forEach { entry ->
            when (entry) {
                is WalEntry.CreateFile -> createFileInternal(entry.path)
                is WalEntry.CreateDir -> createDirInternal(entry.path)
                is WalEntry.CreateSymlink -> createSymlinkInternal(entry.path, entry.target)
                is WalEntry.Delete -> deleteInternal(entry.path)
                is WalEntry.Write -> writeInternal(entry.path, entry.offset, entry.data)
                is WalEntry.SetPermissions -> setPermissionsInternal(
                    entry.path,
                    entry.permissions.toFsPermissions()
                )

                is WalEntry.SetXattr -> setXattrInternal(entry.path, entry.name, entry.value)
                is WalEntry.RemoveXattr -> removeXattrInternal(entry.path, entry.name)
                is WalEntry.Copy -> copyInternal(entry.src, entry.dst)
                is WalEntry.Move -> {
                    copyInternal(entry.src, entry.dst)
                    deleteRecursiveInternal(entry.src)
                }
                is WalEntry.MoveToTrash -> { deleteRecursiveInternal(entry.path) }
                is WalEntry.RestoreFromTrash -> { /* tree 变更由 restore 后的操作完成 */ }
            }
        }
    }

    // ── internal helpers ─────────────────────────────────────

    /** 遍历整棵树计算已用字节数，仅在快照恢复时调用。 */
    private fun recalcUsedBytes(): Long {
        var total = 0L
        fun walk(node: VfsNode) {
            when (node) {
                is FileNode -> total += node.size
                is DirNode -> node.children.values.forEach { walk(it) }
                is SymlinkNode -> {}
            }
        }
        walk(root)
        return total
    }

    private fun snapshotFromNode(node: VfsNode): SnapshotNode {
        val xattrs = if (node.xattrs.isEmpty()) null
        else node.xattrs.mapValues { it.value.copyOf() }
        return when (node) {
            is DirNode -> SnapshotNode(
                name = node.name, type = node.type.name,
                createdAtMillis = node.createdAtMillis, modifiedAtMillis = node.modifiedAtMillis,
                permissions = SnapshotPermissions.from(node.permissions),
                children = node.children.values.map { snapshotFromNode(it) },
                xattrs = xattrs
            )

            is FileNode -> {
                // 直接引用 blocks 的底层数据，避免额外拷贝导致内存翻倍。
                // SnapshotNode 在序列化后即被 GC，期间 tree 持有锁不会被修改。
                val contentRef = node.blocks.toByteArray()
                SnapshotNode(
                    name = node.name, type = node.type.name,
                    createdAtMillis = node.createdAtMillis, modifiedAtMillis = node.modifiedAtMillis,
                    permissions = SnapshotPermissions.from(node.permissions),
                    content = contentRef,
                    xattrs = xattrs
                )
            }

            is SymlinkNode -> SnapshotNode(
                name = node.name, type = node.type.name,
                createdAtMillis = node.createdAtMillis, modifiedAtMillis = node.modifiedAtMillis,
                permissions = SnapshotPermissions.from(node.permissions),
                target = node.targetPath,
                xattrs = xattrs
            )
        }
    }

    private fun buildFromSnapshot(snapshot: SnapshotNode): DirNode {
        fun restoreXattrs(vfsNode: VfsNode, snapshotNode: SnapshotNode) {
            snapshotNode.xattrs?.forEach { (k, v) -> vfsNode.xattrs[k] = v }
        }

        fun build(node: SnapshotNode): VfsNode {
            val perms = node.permissions.toFsPermissions()
            return when (node.fsType()) {
                FsType.DIRECTORY -> {
                    val dir = DirNode(node.name, node.createdAtMillis, node.modifiedAtMillis, perms)
                    node.children.orEmpty().forEach { child ->
                        val childNode = build(child)
                        dir.children[childNode.name] = childNode
                    }
                    restoreXattrs(dir, node)
                    dir
                }
                FsType.SYMLINK -> {
                    val sym = SymlinkNode(
                        node.name, node.createdAtMillis, node.modifiedAtMillis, perms,
                        node.target ?: ""
                    )
                    restoreXattrs(sym, node)
                    sym
                }
                FsType.FILE -> {
                    val file = FileNode(node.name, node.createdAtMillis, node.modifiedAtMillis, perms)
                    val content = node.content ?: ByteArray(0)
                    if (content.isNotEmpty()) {
                        file.blocks.write(0, content)
                    }
                    restoreXattrs(file, node)
                    file
                }
            }
        }

        val built = build(snapshot)
        return if (built is DirNode) built else DirNode(
            "/",
            nowMillis(),
            nowMillis(),
            FsPermissions.FULL
        )
    }

    /** WAL 回放用：递归拷贝节点（静默，不抛异常）。 */
    private fun copyInternal(src: String, dst: String) {
        val srcNorm = PathUtils.normalize(src)
        val dstNorm = PathUtils.normalize(dst)
        val srcNode = resolveNode(srcNorm) ?: return
        copyNodeRecursive(srcNode, dstNorm)
    }

    private fun copyNodeRecursive(node: VfsNode, dstPath: String) {
        when (node) {
            is FileNode -> {
                createFileInternal(dstPath)
                val created = resolveNode(dstPath) as? FileNode ?: return
                val data = node.blocks.toByteArray()
                if (data.isNotEmpty()) {
                    val oldSize = created.size.toLong()
                    created.blocks.write(0, data)
                    _usedBytes += created.size.toLong() - oldSize
                    created.modifiedAtMillis = nowMillis()
                }
            }
            is DirNode -> {
                createDirInternal(dstPath)
                for ((name, child) in node.children) {
                    copyNodeRecursive(child, "$dstPath/$name")
                }
            }
            is SymlinkNode -> {
                createSymlinkInternal(dstPath, node.targetPath)
            }
        }
    }

    private fun createFileInternal(path: String) {
        val normalized = PathUtils.normalize(path)
        val (parent, name) = splitParent(normalized)
        val parentNode = resolveNode(parent) as? DirNode ?: return
        if (parentNode.children.containsKey(name)) return
        val now = nowMillis()
        parentNode.children[name] = FileNode(name, now, now, FsPermissions.FULL)
        parentNode.modifiedAtMillis = now
    }

    private fun createDirInternal(path: String) {
        val normalized = PathUtils.normalize(path)
        if (normalized == "/") return
        val (parent, name) = splitParent(normalized)
        val parentNode = resolveNode(parent) as? DirNode ?: return
        if (parentNode.children.containsKey(name)) return
        val now = nowMillis()
        parentNode.children[name] = DirNode(name, now, now, FsPermissions.FULL)
        parentNode.modifiedAtMillis = now
    }

    private fun createSymlinkInternal(path: String, target: String) {
        val normalized = PathUtils.normalize(path)
        val (parent, name) = splitParent(normalized)
        val parentNode = resolveNode(parent) as? DirNode ?: return
        if (parentNode.children.containsKey(name)) return
        val now = nowMillis()
        parentNode.children[name] = SymlinkNode(name, now, now, FsPermissions.FULL, target)
        parentNode.modifiedAtMillis = now
    }

    private fun deleteInternal(path: String) {
        val normalized = PathUtils.normalize(path)
        if (normalized == "/") return
        val (parent, name) = splitParent(normalized)
        val parentNode = resolveNode(parent) as? DirNode ?: return
        val node = parentNode.children[name] ?: return
        if (node is DirNode && node.children.isNotEmpty()) return
        if (node is FileNode) {
            _usedBytes -= node.size.toLong()
        }
        parentNode.children.remove(name)
        parentNode.modifiedAtMillis = nowMillis()
    }

    private fun deleteRecursiveInternal(path: String) {
        val normalized = PathUtils.normalize(path)
        if (normalized == "/") return
        val (parent, name) = splitParent(normalized)
        val parentNode = resolveNode(parent) as? DirNode ?: return
        val node = parentNode.children[name] ?: return
        if (node is DirNode) {
            subtractUsedBytes(node)
        } else if (node is FileNode) {
            _usedBytes -= node.size.toLong()
        }
        parentNode.children.remove(name)
        parentNode.modifiedAtMillis = nowMillis()
    }

    private fun subtractUsedBytes(dir: DirNode) {
        for (child in dir.children.values) {
            when (child) {
                is FileNode -> _usedBytes -= child.size.toLong()
                is DirNode -> subtractUsedBytes(child)
                is SymlinkNode -> {}
            }
        }
    }

    private fun writeInternal(path: String, offset: Long, data: ByteArray) {
        val normalized = PathUtils.normalize(path)
        val node = resolveNode(normalized)
        if (node !is FileNode) return
        val oldSize = node.size.toLong()
        node.blocks.write(offset.toInt(), data)
        _usedBytes += node.size.toLong() - oldSize
        node.modifiedAtMillis = nowMillis()
    }

    private fun setPermissionsInternal(path: String, permissions: FsPermissions) {
        val normalized = PathUtils.normalize(path)
        val node = resolveNode(normalized) ?: return
        node.permissions = permissions
        node.modifiedAtMillis = nowMillis()
    }

    private fun setXattrInternal(path: String, name: String, value: ByteArray) {
        val normalized = PathUtils.normalize(path)
        val node = resolveNode(normalized) ?: return
        node.xattrs[name] = value.copyOf()
        node.modifiedAtMillis = nowMillis()
    }

    private fun removeXattrInternal(path: String, name: String) {
        val normalized = PathUtils.normalize(path)
        val node = resolveNode(normalized) ?: return
        node.xattrs.remove(name)
        node.modifiedAtMillis = nowMillis()
    }
}
