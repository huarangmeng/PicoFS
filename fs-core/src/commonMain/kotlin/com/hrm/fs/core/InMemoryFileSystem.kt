package com.hrm.fs.core

import com.hrm.fs.api.FileHandle
import com.hrm.fs.api.FileSystem
import com.hrm.fs.api.FsEntry
import com.hrm.fs.api.FsError
import com.hrm.fs.api.FsMeta
import com.hrm.fs.api.FsPermissions
import com.hrm.fs.api.FsStorage
import com.hrm.fs.api.OpenMode
import com.hrm.fs.api.PathUtils
import com.hrm.fs.core.persistence.PersistenceConfig
import com.hrm.fs.core.persistence.SnapshotNode
import com.hrm.fs.core.persistence.SnapshotPermissions
import com.hrm.fs.core.persistence.VfsPersistenceCodec
import com.hrm.fs.core.persistence.WalEntry
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlin.time.Clock

internal class InMemoryFileSystem(
    private val storage: FsStorage? = null,
    private val persistenceConfig: PersistenceConfig = PersistenceConfig()
) : FileSystem {
    private val mutex = Mutex()
    private var root = DirNode("/", nowMillis(), nowMillis(), FsPermissions.FULL)
    private val walEntries: MutableList<WalEntry> = mutableListOf()
    private var opsSinceSnapshot: Int = 0

    init {
        if (storage != null) {
            locked {
                loadFromStorage()
            }
        }
    }

    override fun createFile(path: String): Result<Unit> = locked {
        val normalized = PathUtils.normalize(path)
        val (parent, name) = splitParent(normalized)
        val parentNode = resolveDirOrError(parent).getOrElse { return@locked Result.failure(it) }
        if (!parentNode.permissions.canWrite()) {
            return@locked Result.failure(FsError.PermissionDenied(parent))
        }
        if (parentNode.children.containsKey(name)) {
            return@locked Result.failure(FsError.AlreadyExists(normalized))
        }
        val now = nowMillis()
        parentNode.children[name] = FileNode(name, now, now, FsPermissions.FULL)
        parentNode.modifiedAtMillis = now
        appendWal(WalEntry.CreateFile(normalized))
        Result.success(Unit)
    }

    override fun createDir(path: String): Result<Unit> = locked {
        val normalized = PathUtils.normalize(path)
        if (normalized == "/") return@locked Result.success(Unit)
        val (parent, name) = splitParent(normalized)
        val parentNode = resolveDirOrError(parent).getOrElse { return@locked Result.failure(it) }
        if (!parentNode.permissions.canWrite()) {
            return@locked Result.failure(FsError.PermissionDenied(parent))
        }
        if (parentNode.children.containsKey(name)) {
            return@locked Result.failure(FsError.AlreadyExists(normalized))
        }
        val now = nowMillis()
        parentNode.children[name] = DirNode(name, now, now, FsPermissions.FULL)
        parentNode.modifiedAtMillis = now
        appendWal(WalEntry.CreateDir(normalized))
        Result.success(Unit)
    }

    override fun open(path: String, mode: OpenMode): Result<FileHandle> = locked {
        val normalized = PathUtils.normalize(path)
        val node = resolveNodeOrError(normalized).getOrElse { return@locked Result.failure(it) }
        if (node !is FileNode) return@locked Result.failure(FsError.NotFile(normalized))
        if (!hasAccess(node.permissions, mode)) {
            return@locked Result.failure(FsError.PermissionDenied(normalized))
        }
        Result.success(InMemoryFileHandle(this, node, mode))
    }

    override fun readDir(path: String): Result<List<FsEntry>> = locked {
        val normalized = PathUtils.normalize(path)
        val dir = resolveDirOrError(normalized).getOrElse { return@locked Result.failure(it) }
        if (!dir.permissions.canRead()) {
            return@locked Result.failure(FsError.PermissionDenied(normalized))
        }
        val items = dir.children.values.map { FsEntry(it.name, it.type) }
        Result.success(items)
    }

    override fun stat(path: String): Result<FsMeta> = locked {
        val normalized = PathUtils.normalize(path)
        val node = resolveNodeOrError(normalized).getOrElse { return@locked Result.failure(it) }
        if (!node.permissions.canRead()) {
            return@locked Result.failure(FsError.PermissionDenied(normalized))
        }
        val size = if (node is FileNode) node.size.toLong() else 0L
        Result.success(
            FsMeta(
                path = normalized,
                type = node.type,
                size = size,
                createdAtMillis = node.createdAtMillis,
                modifiedAtMillis = node.modifiedAtMillis,
                permissions = node.permissions
            )
        )
    }

    override fun delete(path: String): Result<Unit> = locked {
        val normalized = PathUtils.normalize(path)
        if (normalized == "/") return@locked Result.failure(FsError.PermissionDenied("/"))
        val (parent, name) = splitParent(normalized)
        val parentNode = resolveDirOrError(parent).getOrElse { return@locked Result.failure(it) }
        if (!parentNode.permissions.canWrite()) {
            return@locked Result.failure(FsError.PermissionDenied(parent))
        }
        val node =
            parentNode.children[name] ?: return@locked Result.failure(FsError.NotFound(normalized))
        if (node is DirNode && node.children.isNotEmpty()) {
            return@locked Result.failure(FsError.PermissionDenied(normalized))
        }
        parentNode.children.remove(name)
        parentNode.modifiedAtMillis = nowMillis()
        appendWal(WalEntry.Delete(normalized))
        Result.success(Unit)
    }

    override fun setPermissions(path: String, permissions: FsPermissions): Result<Unit> = locked {
        val normalized = PathUtils.normalize(path)
        val node = resolveNodeOrError(normalized).getOrElse { return@locked Result.failure(it) }
        node.permissions = permissions
        node.modifiedAtMillis = nowMillis()
        appendWal(WalEntry.SetPermissions(normalized, SnapshotPermissions.from(permissions)))
        Result.success(Unit)
    }

    internal fun readAt(node: FileNode, offset: Long, length: Int): Result<ByteArray> = locked {
        if (!node.permissions.canRead()) return@locked Result.failure(FsError.PermissionDenied(node.name))
        if (offset < 0 || length < 0) return@locked Result.failure(FsError.InvalidPath("offset/length"))
        if (offset >= node.size) return@locked Result.success(ByteArray(0))
        val available = node.size - offset.toInt()
        val readLen = minOf(available, length)
        val out = ByteArray(readLen)
        node.content.copyInto(
            out,
            destinationOffset = 0,
            startIndex = offset.toInt(),
            endIndex = offset.toInt() + readLen
        )
        Result.success(out)
    }

    internal fun writeAt(node: FileNode, offset: Long, data: ByteArray): Result<Unit> = locked {
        if (!node.permissions.canWrite()) return@locked Result.failure(FsError.PermissionDenied(node.name))
        if (offset < 0) return@locked Result.failure(FsError.InvalidPath("offset"))
        val end = offset.toInt() + data.size
        ensureCapacity(node, end)
        data.copyInto(node.content, destinationOffset = offset.toInt())
        if (end > node.size) node.size = end
        node.modifiedAtMillis = nowMillis()
        appendWal(WalEntry.Write(pathForNode(node), offset, data))
        Result.success(Unit)
    }

    private fun ensureCapacity(node: FileNode, size: Int) {
        if (size <= node.content.size) return
        val newSize = maxOf(size, node.content.size * 2 + 1)
        val newBuffer = ByteArray(newSize)
        node.content.copyInto(newBuffer, endIndex = node.size)
        node.content = newBuffer
    }

    private fun resolveNodeOrError(path: String): Result<VfsNode> {
        val node = resolveNode(path) ?: return Result.failure(FsError.NotFound(path))
        return Result.success(node)
    }

    private fun resolveDirOrError(path: String): Result<DirNode> {
        val node = resolveNode(path) ?: return Result.failure(FsError.NotFound(path))
        if (node !is DirNode) return Result.failure(FsError.NotDirectory(path))
        return Result.success(node)
    }

    private fun resolveNode(path: String): VfsNode? {
        if (path == "/") return root
        val parts = path.removePrefix("/").split("/").filter { it.isNotBlank() }
        var current: VfsNode = root
        for (part in parts) {
            if (current !is DirNode) return null
            current = current.children[part] ?: return null
        }
        return current
    }

    private fun splitParent(path: String): Pair<String, String> {
        val normalized = PathUtils.normalize(path)
        val idx = normalized.lastIndexOf('/')
        return if (idx <= 0) "/" to normalized.removePrefix("/")
        else normalized.take(idx) to normalized.substring(idx + 1)
    }

    private fun nowMillis(): Long = Clock.System.now().toEpochMilliseconds()

    private fun <T> locked(block: () -> T): T = runBlocking {
        mutex.withLock { block() }
    }

    private fun hasAccess(perms: FsPermissions, mode: OpenMode): Boolean = when (mode) {
        OpenMode.READ -> perms.canRead()
        OpenMode.WRITE -> perms.canWrite()
        OpenMode.READ_WRITE -> perms.canRead() && perms.canWrite()
    }

    private fun pathForNode(node: FileNode): String {
        val result = StringBuilder()
        fun dfs(current: VfsNode, target: FileNode, prefix: String): Boolean {
            if (current == target) {
                result.append(prefix)
                return true
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

    private fun loadFromStorage() {
        val snapshotBytes = storage?.read(persistenceConfig.snapshotKey)?.getOrNull()
        if (snapshotBytes != null) {
            val snapshot = VfsPersistenceCodec.decodeSnapshot(snapshotBytes)
            root = buildFromSnapshot(snapshot)
        }
        val walBytes = storage?.read(persistenceConfig.walKey)?.getOrNull()
        if (walBytes != null) {
            walEntries.clear()
            walEntries.addAll(VfsPersistenceCodec.decodeWal(walBytes))
            applyWalEntries(walEntries)
        }
    }

    private fun applyWalEntries(entries: List<WalEntry>) {
        entries.forEach { entry ->
            when (entry) {
                is WalEntry.CreateFile -> createFileInternal(entry.path)
                is WalEntry.CreateDir -> createDirInternal(entry.path)
                is WalEntry.Delete -> deleteInternal(entry.path)
                is WalEntry.Write -> writeInternal(entry.path, entry.offset, entry.data)
                is WalEntry.SetPermissions -> setPermissionsInternal(
                    entry.path,
                    entry.permissions.toFsPermissions()
                )
            }
        }
    }

    private fun appendWal(entry: WalEntry) {
        if (storage == null) return
        walEntries.add(entry)
        opsSinceSnapshot++
        storage.write(persistenceConfig.walKey, VfsPersistenceCodec.encodeWal(walEntries))
        if (opsSinceSnapshot >= persistenceConfig.autoSnapshotEvery) {
            saveSnapshot()
        }
    }

    private fun saveSnapshot() {
        if (storage == null) return
        val snapshot = snapshotFromNode(root)
        storage.write(persistenceConfig.snapshotKey, VfsPersistenceCodec.encodeSnapshot(snapshot))
        walEntries.clear()
        storage.write(persistenceConfig.walKey, VfsPersistenceCodec.encodeWal(walEntries))
        opsSinceSnapshot = 0
    }

    private fun snapshotFromNode(node: VfsNode): SnapshotNode {
        return when (node) {
            is DirNode -> SnapshotNode(
                name = node.name,
                type = node.type.name,
                createdAtMillis = node.createdAtMillis,
                modifiedAtMillis = node.modifiedAtMillis,
                permissions = SnapshotPermissions.from(node.permissions),
                children = node.children.values.map { snapshotFromNode(it) }
            )

            is FileNode -> SnapshotNode(
                name = node.name,
                type = node.type.name,
                createdAtMillis = node.createdAtMillis,
                modifiedAtMillis = node.modifiedAtMillis,
                permissions = SnapshotPermissions.from(node.permissions),
                content = node.content.copyOf(node.size)
            )
        }
    }

    private fun buildFromSnapshot(snapshot: SnapshotNode): DirNode {
        fun build(node: SnapshotNode): VfsNode {
            val perms = node.permissions.toFsPermissions()
            return if (node.fsType() == com.hrm.fs.api.FsType.DIRECTORY) {
                val dir = DirNode(node.name, node.createdAtMillis, node.modifiedAtMillis, perms)
                node.children.orEmpty().forEach { child ->
                    val built = build(child)
                    dir.children[built.name] = built
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
        val parentNode = resolveDir(parent) ?: return
        if (parentNode.children.containsKey(name)) return
        val now = nowMillis()
        parentNode.children[name] = FileNode(name, now, now, FsPermissions.FULL)
        parentNode.modifiedAtMillis = now
    }

    private fun createDirInternal(path: String) {
        val normalized = PathUtils.normalize(path)
        if (normalized == "/") return
        val (parent, name) = splitParent(normalized)
        val parentNode = resolveDir(parent) ?: return
        if (parentNode.children.containsKey(name)) return
        val now = nowMillis()
        parentNode.children[name] = DirNode(name, now, now, FsPermissions.FULL)
        parentNode.modifiedAtMillis = now
    }

    private fun deleteInternal(path: String) {
        val normalized = PathUtils.normalize(path)
        if (normalized == "/") return
        val (parent, name) = splitParent(normalized)
        val parentNode = resolveDir(parent) ?: return
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

    private fun resolveDir(path: String): DirNode? = resolveNode(path) as? DirNode
}
