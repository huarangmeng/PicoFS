package com.hrm.fs.core

import com.hrm.fs.api.FsEntry
import com.hrm.fs.api.FsError
import com.hrm.fs.api.FsMeta
import com.hrm.fs.api.FsPermissions
import com.hrm.fs.api.FsType
import com.hrm.fs.api.PathUtils
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

    var root: DirNode = DirNode("/", nowMillis(), nowMillis(), FsPermissions.FULL)
        private set

    // ── 节点解析 ────────────────────────────────────────────────

    fun resolveNode(path: String): VfsNode? {
        if (path == "/") return root
        val parts = path.removePrefix("/").split("/").filter { it.isNotBlank() }
        var current: VfsNode = root
        for (part in parts) {
            if (current !is DirNode) return null
            current = current.children[part] ?: return null
        }
        return current
    }

    fun resolveNodeOrError(path: String): Result<VfsNode> {
        val node = resolveNode(path) ?: return Result.failure(FsError.NotFound(path))
        return Result.success(node)
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
        if (!parentNode.permissions.canWrite()) return Result.failure(FsError.PermissionDenied(parent))
        if (parentNode.children.containsKey(name)) return Result.failure(FsError.AlreadyExists(normalized))
        val now = nowMillis()
        parentNode.children[name] = FileNode(name, now, now, FsPermissions.FULL)
        parentNode.modifiedAtMillis = now
        return Result.success(Unit)
    }

    fun createDir(normalized: String): Result<Unit> {
        if (normalized == "/") return Result.success(Unit)
        val (parent, name) = splitParent(normalized)
        val parentNode = resolveDirOrError(parent).getOrElse { return Result.failure(it) }
        if (!parentNode.permissions.canWrite()) return Result.failure(FsError.PermissionDenied(parent))
        if (parentNode.children.containsKey(name)) return Result.failure(FsError.AlreadyExists(normalized))
        val now = nowMillis()
        parentNode.children[name] = DirNode(name, now, now, FsPermissions.FULL)
        parentNode.modifiedAtMillis = now
        return Result.success(Unit)
    }

    // ── 删除 ──────────────────────────────────────────────────

    fun delete(normalized: String): Result<Unit> {
        if (normalized == "/") return Result.failure(FsError.PermissionDenied("/"))
        val (parent, name) = splitParent(normalized)
        val parentNode = resolveDirOrError(parent).getOrElse { return Result.failure(it) }
        if (!parentNode.permissions.canWrite()) return Result.failure(FsError.PermissionDenied(parent))
        val node = parentNode.children[name] ?: return Result.failure(FsError.NotFound(normalized))
        if (node is DirNode && node.children.isNotEmpty()) return Result.failure(FsError.PermissionDenied(normalized))
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
        return Result.success(
            FsMeta(
                path = normalized, type = node.type, size = size,
                createdAtMillis = node.createdAtMillis,
                modifiedAtMillis = node.modifiedAtMillis,
                permissions = node.permissions
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
        val available = node.size - offset.toInt()
        val readLen = minOf(available, length)
        val out = ByteArray(readLen)
        node.content.copyInto(out, 0, offset.toInt(), offset.toInt() + readLen)
        return Result.success(out)
    }

    fun writeAt(node: FileNode, offset: Long, data: ByteArray): Result<Unit> {
        if (!node.permissions.canWrite()) return Result.failure(FsError.PermissionDenied(node.name))
        if (offset < 0) return Result.failure(FsError.InvalidPath("offset"))
        val end = offset.toInt() + data.size
        ensureCapacity(node, end)
        data.copyInto(node.content, destinationOffset = offset.toInt())
        if (end > node.size) node.size = end
        node.modifiedAtMillis = nowMillis()
        return Result.success(Unit)
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
            if (current == target) { result.append(prefix); return true }
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
        root = buildFromSnapshot(snapshot)
    }

    // ── WAL 回放（静默，不抛异常） ───────────────────────────

    fun replayWal(entries: List<WalEntry>) {
        entries.forEach { entry ->
            when (entry) {
                is WalEntry.CreateFile -> createFileInternal(entry.path)
                is WalEntry.CreateDir -> createDirInternal(entry.path)
                is WalEntry.Delete -> deleteInternal(entry.path)
                is WalEntry.Write -> writeInternal(entry.path, entry.offset, entry.data)
                is WalEntry.SetPermissions -> setPermissionsInternal(entry.path, entry.permissions.toFsPermissions())
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
            content = node.content.copyOf(node.size)
        )
    }

    private fun buildFromSnapshot(snapshot: SnapshotNode): DirNode {
        fun build(node: SnapshotNode): VfsNode {
            val perms = node.permissions.toFsPermissions()
            return if (node.fsType() == FsType.DIRECTORY) {
                val dir = DirNode(node.name, node.createdAtMillis, node.modifiedAtMillis, perms)
                node.children.orEmpty().forEach { child ->
                    val childNode = build(child)
                    dir.children[childNode.name] = childNode
                }
                dir
            } else {
                val file = FileNode(node.name, node.createdAtMillis, node.modifiedAtMillis, perms)
                val content = node.content ?: ByteArray(0)
                file.content = content.copyOf(content.size)
                file.size = content.size
                file
            }
        }
        val built = build(snapshot)
        return if (built is DirNode) built else DirNode("/", nowMillis(), nowMillis(), FsPermissions.FULL)
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
        val end = offset.toInt() + data.size
        ensureCapacity(node, end)
        data.copyInto(node.content, destinationOffset = offset.toInt())
        if (end > node.size) node.size = end
        node.modifiedAtMillis = nowMillis()
    }

    private fun setPermissionsInternal(path: String, permissions: FsPermissions) {
        val normalized = PathUtils.normalize(path)
        val node = resolveNode(normalized) ?: return
        node.permissions = permissions
        node.modifiedAtMillis = nowMillis()
    }

    private fun ensureCapacity(node: FileNode, size: Int) {
        if (size <= node.content.size) return
        val newSize = maxOf(size, node.content.size * 2 + 1)
        val newBuffer = ByteArray(newSize)
        node.content.copyInto(newBuffer, endIndex = node.size)
        node.content = newBuffer
    }

    companion object {
        fun splitParent(path: String): Pair<String, String> {
            val normalized = PathUtils.normalize(path)
            val idx = normalized.lastIndexOf('/')
            return if (idx <= 0) "/" to normalized.removePrefix("/")
            else normalized.take(idx) to normalized.substring(idx + 1)
        }

        fun nowMillis(): Long = Clock.System.now().toEpochMilliseconds()
    }
}
