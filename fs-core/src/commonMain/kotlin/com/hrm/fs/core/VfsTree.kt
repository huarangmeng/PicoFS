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

    // ── 节点解析 ────────────────────────────────────────────────

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
        val parts = path.removePrefix("/").split("/").filter { it.isNotBlank() }
        var current: VfsNode = root
        var currentPath = ""
        for ((index, part) in parts.withIndex()) {
            // 如果当前节点是 symlink 且需要跟随，先解析 symlink
            if (current is SymlinkNode && followSymlinks) {
                val resolved = resolveSymlinkTarget(current, currentPath, depth + 1) ?: return null
                current = resolved
            }
            if (current !is DirNode) return null
            val child = current.children[part] ?: return null
            currentPath = if (currentPath.isEmpty()) "/$part" else "$currentPath/$part"
            // 对于中间路径段的 symlink，总是跟随
            if (child is SymlinkNode && followSymlinks && index < parts.size - 1) {
                val resolved = resolveSymlinkTarget(child, currentPath, depth + 1) ?: return null
                current = resolved
            } else if (child is SymlinkNode && followSymlinks && index == parts.size - 1) {
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
        val parts = path.removePrefix("/").split("/").filter { it.isNotBlank() }
        var current: VfsNode = root
        var currentPath = ""
        for ((index, part) in parts.withIndex()) {
            if (current is SymlinkNode) {
                val resolved = resolveSymlinkTarget(current, currentPath, 1) ?: return null
                current = resolved
            }
            if (current !is DirNode) return null
            val child = current.children[part] ?: return null
            currentPath = if (currentPath.isEmpty()) "/$part" else "$currentPath/$part"
            if (index < parts.size - 1 && child is SymlinkNode) {
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
        node.blocks.write(offset.toInt(), data)
        node.modifiedAtMillis = nowMillis()
        return Result.success(Unit)
    }

    // ── 空间统计 ──────────────────────────────────────────────

    /** 计算内存文件树中所有文件的有效字节数总和。 */
    fun totalUsedBytes(): Long {
        var total = 0L
        fun walk(node: VfsNode) {
            when (node) {
                is FileNode -> total += node.size
                is DirNode -> node.children.values.forEach { walk(it) }
                is SymlinkNode -> { /* symlink 不占用文件内容空间 */ }
            }
        }
        walk(root)
        return total
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
            }
        }
    }

    // ── internal helpers ─────────────────────────────────────

    private fun snapshotFromNode(node: VfsNode): SnapshotNode = when (node) {
        is DirNode -> SnapshotNode(
            name = node.name, type = node.type.name,
            createdAtMillis = node.createdAtMillis, modifiedAtMillis = node.modifiedAtMillis,
            permissions = SnapshotPermissions.from(node.permissions),
            children = node.children.values.map { snapshotFromNode(it) }
        )

        is FileNode -> SnapshotNode(
            name = node.name, type = node.type.name,
            createdAtMillis = node.createdAtMillis, modifiedAtMillis = node.modifiedAtMillis,
            permissions = SnapshotPermissions.from(node.permissions),
            content = node.blocks.toByteArray()
        )

        is SymlinkNode -> SnapshotNode(
            name = node.name, type = node.type.name,
            createdAtMillis = node.createdAtMillis, modifiedAtMillis = node.modifiedAtMillis,
            permissions = SnapshotPermissions.from(node.permissions),
            target = node.targetPath
        )
    }

    private fun buildFromSnapshot(snapshot: SnapshotNode): DirNode {
        fun build(node: SnapshotNode): VfsNode {
            val perms = node.permissions.toFsPermissions()
            return when (node.fsType()) {
                FsType.DIRECTORY -> {
                    val dir = DirNode(node.name, node.createdAtMillis, node.modifiedAtMillis, perms)
                    node.children.orEmpty().forEach { child ->
                        val childNode = build(child)
                        dir.children[childNode.name] = childNode
                    }
                    dir
                }
                FsType.SYMLINK -> {
                    SymlinkNode(
                        node.name, node.createdAtMillis, node.modifiedAtMillis, perms,
                        node.target ?: ""
                    )
                }
                FsType.FILE -> {
                    val file = FileNode(node.name, node.createdAtMillis, node.modifiedAtMillis, perms)
                    val content = node.content ?: ByteArray(0)
                    if (content.isNotEmpty()) {
                        file.blocks.write(0, content)
                    }
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
        parentNode.children.remove(name)
        parentNode.modifiedAtMillis = nowMillis()
    }

    private fun writeInternal(path: String, offset: Long, data: ByteArray) {
        val normalized = PathUtils.normalize(path)
        val node = resolveNode(normalized)
        if (node !is FileNode) return
        node.blocks.write(offset.toInt(), data)
        node.modifiedAtMillis = nowMillis()
    }

    private fun setPermissionsInternal(path: String, permissions: FsPermissions) {
        val normalized = PathUtils.normalize(path)
        val node = resolveNode(normalized) ?: return
        node.permissions = permissions
        node.modifiedAtMillis = nowMillis()
    }
}
