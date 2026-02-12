package com.hrm.fs.core

import com.hrm.fs.api.ArchiveCodec
import com.hrm.fs.api.ArchiveEntry
import com.hrm.fs.api.ArchiveFormat
import com.hrm.fs.api.ChecksumAlgorithm
import com.hrm.fs.api.DiskFileOperations
import com.hrm.fs.api.DiskFileWatcher
import com.hrm.fs.api.FileHandle
import com.hrm.fs.api.FileSystem
import com.hrm.fs.api.FileVersion
import com.hrm.fs.api.FsArchive
import com.hrm.fs.api.FsChecksum
import com.hrm.fs.api.FsEntry
import com.hrm.fs.api.FsError
import com.hrm.fs.api.FsEvent
import com.hrm.fs.api.FsEventKind
import com.hrm.fs.api.FsMeta
import com.hrm.fs.api.FsMetrics
import com.hrm.fs.api.FsMounts
import com.hrm.fs.api.FsObserve
import com.hrm.fs.api.FsPermissions
import com.hrm.fs.api.FsSearch
import com.hrm.fs.api.FsStorage
import com.hrm.fs.api.FsStreams
import com.hrm.fs.api.FsSymlinks
import com.hrm.fs.api.FsTrash
import com.hrm.fs.api.FsType
import com.hrm.fs.api.FsVersions
import com.hrm.fs.api.FsXattr
import com.hrm.fs.api.MatchedLine
import com.hrm.fs.api.MountOptions
import com.hrm.fs.api.OpenMode
import com.hrm.fs.api.PathUtils
import com.hrm.fs.api.PendingMount
import com.hrm.fs.api.QuotaInfo
import com.hrm.fs.api.SearchQuery
import com.hrm.fs.api.SearchResult
import com.hrm.fs.api.TrashItem
import com.hrm.fs.api.log.FLog
import com.hrm.fs.core.VfsMetricsCollector.Op
import com.hrm.fs.core.persistence.PersistenceConfig
import com.hrm.fs.core.persistence.SnapshotPermissions
import com.hrm.fs.core.persistence.SnapshotTrashData
import com.hrm.fs.core.persistence.WalEntry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch

/**
 * 虚拟文件系统实现（门面层）。
 *
 * 组合内部组件完成所有功能：
 * - [VfsTree]               — 纯内存文件树的增删改查 / 快照 / WAL
 * - [MountTable]            — 挂载表管理 + 最长前缀匹配
 * - [VfsEventBus]           — 事件发布 / 订阅
 * - [VfsPersistenceManager] — WAL / Snapshot / Mount 持久化
 * - [VfsMetricsCollector]   — IO 统计与可观测性
 *
 * 核心 CRUD 方法直接在本类上调用，扩展能力通过子接口属性访问：
 * ```kotlin
 * fs.createFile("/a.txt")
 * fs.mounts.mount("/disk", diskOps)
 * fs.xattr.set("/a.txt", "tag", value)
 * ```
 */
internal class InMemoryFileSystem(
    storage: FsStorage? = null,
    persistenceConfig: PersistenceConfig = PersistenceConfig(),
    watcherScope: CoroutineScope? = null,
    private val quotaBytes: Long = -1
) : FileSystem {

    companion object {
        private const val TAG = "InMemoryFS"
    }

    private val rwLock = CoroutineReadWriteMutex()
    internal val tree = VfsTree()
    internal val mountTable = MountTable()
    internal val eventBus = VfsEventBus()
    internal val persistence = VfsPersistenceManager(storage, persistenceConfig)
    internal val mc = VfsMetricsCollector()
    internal val versionManager = VfsVersionManager()
    internal val trashManager = VfsTrashManager()
    internal val fileLockManager = VfsFileLockManager()

    internal val statCache = VfsCache<String, FsMeta>(256)
    internal val readDirCache = VfsCache<String, List<FsEntry>>(128)

    /** 挂载点文件的 xattr overlay（挂载点路径 -> name -> value） */
    internal val mountXattrs = LinkedHashMap<String, MutableMap<String, ByteArray>>()

    /** 快照后 xattr overlay 中被修改过的路径（脏标记），仅这些需要重新写入 WAL */
    private val dirtyXattrPaths = mutableSetOf<String>()

    internal val watchScope = watcherScope ?: CoroutineScope(SupervisorJob())
    internal val watcherJobs = LinkedHashMap<String, Job>()

    // ═══════════════════════════════════════════════════════════
    // 扩展能力子接口
    // ═══════════════════════════════════════════════════════════

    override val mounts: FsMounts = MountsImpl()
    override val versions: FsVersions = VersionsImpl()
    override val search: FsSearch = SearchImpl()
    override val observe: FsObserve = ObserveImpl()
    override val streams: FsStreams = StreamsImpl()
    override val checksum: FsChecksum = ChecksumImpl()
    override val xattr: FsXattr = XattrImpl()
    override val symlinks: FsSymlinks = SymlinksImpl()
    override val archive: FsArchive = ArchiveImpl()
    override val trash: FsTrash = TrashImpl()

    // ═══════════════════════════════════════════════════════════
    // 基础 CRUD
    // ═══════════════════════════════════════════════════════════

    override suspend fun createFile(path: String): Result<Unit> {
        FLog.d(TAG, "createFile: path=$path")
        writeLocked { ensureLoaded() }
        val mark = mc.begin()
        val result = writeLocked {
            createFileNoLock(PathUtils.normalize(path))
        }
        mc.end(Op.CREATE_FILE, mark, result)
        return result
    }

    override suspend fun createDir(path: String): Result<Unit> {
        FLog.d(TAG, "createDir: path=$path")
        writeLocked { ensureLoaded() }
        val mark = mc.begin()
        val result = writeLocked {
            createDirNoLock(PathUtils.normalize(path))
        }
        mc.end(Op.CREATE_DIR, mark, result)
        return result
    }

    override suspend fun open(path: String, mode: OpenMode): Result<FileHandle> {
        FLog.d(TAG, "open: path=$path, mode=$mode")
        writeLocked { ensureLoaded() }
        val mark = mc.begin()
        val result = readLocked {
            openNoLock(PathUtils.normalize(path), mode)
        }
        mc.end(Op.OPEN, mark, result)
        return result
    }

    override suspend fun readDir(path: String): Result<List<FsEntry>> {
        writeLocked { ensureLoaded() }
        val mark = mc.begin()
        val result = readLocked {
            readDirNoLock(PathUtils.normalize(path))
        }
        mc.end(Op.READ_DIR, mark, result)
        return result
    }

    override suspend fun stat(path: String): Result<FsMeta> {
        writeLocked { ensureLoaded() }
        val mark = mc.begin()
        val result = readLocked {
            statNoLock(PathUtils.normalize(path))
        }
        mc.end(Op.STAT, mark, result)
        return result
    }

    override suspend fun delete(path: String): Result<Unit> {
        FLog.d(TAG, "delete: path=$path")
        writeLocked { ensureLoaded() }
        val mark = mc.begin()
        val result = writeLocked {
            deleteNoLock(PathUtils.normalize(path))
        }
        mc.end(Op.DELETE, mark, result)
        return result
    }

    override suspend fun setPermissions(path: String, permissions: FsPermissions): Result<Unit> {
        writeLocked { ensureLoaded() }
        val mark = mc.begin()
        val result = writeLocked {
            val normalized = PathUtils.normalize(path)
            if (mountTable.findMount(normalized) != null) return@writeLocked Result.success(Unit)
            tree.setPermissions(normalized, permissions).also { r ->
                if (r.isSuccess) {
                    walAppend(WalEntry.SetPermissions(normalized, SnapshotPermissions.from(permissions)))
                }
            }
        }
        mc.end(Op.SET_PERMISSIONS, mark, result)
        return result
    }

    // ═══════════════════════════════════════════════════════════
    // 递归操作（原子：单次 writeLocked 内完成）
    // ═══════════════════════════════════════════════════════════

    override suspend fun createDirRecursive(path: String): Result<Unit> {
        return writeLocked {
            ensureLoaded()
            createDirRecursiveNoLock(PathUtils.normalize(path))
        }
    }

    override suspend fun deleteRecursive(path: String): Result<Unit> {
        return writeLocked {
            ensureLoaded()
            deleteRecursiveNoLock(PathUtils.normalize(path))
        }
    }

    // ═══════════════════════════════════════════════════════════
    // 便捷读写
    // ═══════════════════════════════════════════════════════════

    override suspend fun readAll(path: String): Result<ByteArray> {
        writeLocked { ensureLoaded() }
        val mark = mc.begin()
        val result = readLocked {
            val normalized = PathUtils.normalize(path)
            val data = readAllBytes(normalized)
            if (data.isSuccess) mc.addBytesRead(data.getOrThrow().size.toLong())
            data
        }
        mc.end(Op.READ_ALL, mark, result)
        return result
    }

    override suspend fun writeAll(path: String, data: ByteArray): Result<Unit> {
        FLog.d(TAG, "writeAll: path=$path, size=${data.size}")
        writeLocked { ensureLoaded() }
        val mark = mc.begin()
        val result = writeLocked {
            writeAllNoLock(PathUtils.normalize(path), data)
        }
        mc.end(Op.WRITE_ALL, mark, result)
        return result
    }

    // ═══════════════════════════════════════════════════════════
    // copy / move（原子：单次 writeLocked 内完成）
    // ═══════════════════════════════════════════════════════════

    override suspend fun copy(srcPath: String, dstPath: String): Result<Unit> {
        FLog.d(TAG, "copy: src=$srcPath, dst=$dstPath")
        writeLocked { ensureLoaded() }
        val mark = mc.begin()
        val result = writeLocked {
            val src = PathUtils.normalize(srcPath)
            val dst = PathUtils.normalize(dstPath)
            copyNoLock(src, dst).also { r ->
                if (r.isSuccess) walAppend(WalEntry.Copy(src, dst))
            }
        }
        if (result.isFailure) FLog.w(TAG, "copy failed: src=$srcPath, dst=$dstPath, error=${result.exceptionOrNull()}")
        mc.end(Op.COPY, mark, result)
        return result
    }

    override suspend fun move(srcPath: String, dstPath: String): Result<Unit> {
        FLog.d(TAG, "move: src=$srcPath, dst=$dstPath")
        writeLocked { ensureLoaded() }
        val mark = mc.begin()
        val result = writeLocked {
            val src = PathUtils.normalize(srcPath)
            val dst = PathUtils.normalize(dstPath)
            copyNoLock(src, dst).getOrElse { return@writeLocked Result.failure(it) }
            deleteRecursiveNoLock(src).also { r ->
                if (r.isSuccess) walAppend(WalEntry.Move(src, dst))
            }
        }
        if (result.isFailure) FLog.w(TAG, "move failed: src=$srcPath, dst=$dstPath, error=${result.exceptionOrNull()}")
        mc.end(Op.MOVE, mark, result)
        return result
    }

    // ═══════════════════════════════════════════════════════════
    // 无锁内部方法（在 locked {} 内调用）
    // ═══════════════════════════════════════════════════════════

    private suspend fun createFileNoLock(normalized: String): Result<Unit> {
        val match = mountTable.findMount(normalized)
        if (match != null) {
            if (match.options.readOnly) return readOnlyError(normalized)
            val r = match.diskOps.createFile(match.relativePath)
            if (r.isSuccess) {
                invalidateCache(normalized)
                eventBus.emit(normalized, FsEventKind.CREATED)
            } else {
                FLog.w(TAG, "createFile failed (disk): $normalized, error=${r.exceptionOrNull()}")
            }
            return r
        }
        return tree.createFile(normalized).also { r ->
            if (r.isSuccess) {
                walAppend(WalEntry.CreateFile(normalized))
                eventBus.emit(normalized, FsEventKind.CREATED)
            } else {
                FLog.w(TAG, "createFile failed: $normalized, error=${r.exceptionOrNull()}")
            }
        }
    }

    private suspend fun createDirNoLock(normalized: String): Result<Unit> {
        if (normalized == "/") return Result.success(Unit)
        val match = mountTable.findMount(normalized)
        if (match != null) {
            if (match.options.readOnly) return readOnlyError(normalized)
            val r = match.diskOps.createDir(match.relativePath)
            if (r.isSuccess) {
                invalidateCache(normalized)
                eventBus.emit(normalized, FsEventKind.CREATED)
            } else {
                FLog.w(TAG, "createDir failed (disk): $normalized, error=${r.exceptionOrNull()}")
            }
            return r
        }
        return tree.createDir(normalized).also { r ->
            if (r.isSuccess) {
                walAppend(WalEntry.CreateDir(normalized))
                eventBus.emit(normalized, FsEventKind.CREATED)
            } else {
                FLog.w(TAG, "createDir failed: $normalized, error=${r.exceptionOrNull()}")
            }
        }
    }

    private suspend fun openNoLock(normalized: String, mode: OpenMode): Result<FileHandle> {
        val match = mountTable.findMount(normalized)
        if (match != null) {
            if (match.options.readOnly && mode != OpenMode.READ) {
                FLog.w(TAG, "open failed: read-only mount $normalized with mode=$mode")
                return Result.failure(FsError.PermissionDenied("挂载点只读: $normalized"))
            }
            return Result.success(
                DiskFileHandle(match.diskOps, match.relativePath, mode, normalized, fileLockManager)
            )
        }
        val node = tree.resolveNodeOrError(normalized).getOrElse {
            FLog.w(TAG, "open failed: $normalized not found")
            return Result.failure(it)
        }
        if (node !is FileNode) {
            FLog.w(TAG, "open failed: $normalized is not a file")
            return Result.failure(FsError.NotFile(normalized))
        }
        if (!hasAccess(node.permissions, mode)) {
            FLog.w(TAG, "open failed: permission denied for $normalized")
            return Result.failure(FsError.PermissionDenied(normalized))
        }
        return Result.success(InMemoryFileHandle(this, node, mode, normalized, fileLockManager))
    }

    private suspend fun readDirNoLock(normalized: String): Result<List<FsEntry>> {
        val match = mountTable.findMount(normalized)
        if (match != null) {
            readDirCache.get(normalized)?.let { return Result.success(it) }
            val r = match.diskOps.list(match.relativePath)
            if (r.isSuccess) readDirCache.put(normalized, r.getOrThrow())
            return r
        }
        return tree.readDir(normalized)
    }

    private suspend fun statNoLock(normalized: String): Result<FsMeta> {
        val match = mountTable.findMount(normalized)
        if (match != null) {
            statCache.get(normalized)?.let { return Result.success(it) }
            val r = match.diskOps.stat(match.relativePath).map { it.copy(path = normalized) }
            if (r.isSuccess) statCache.put(normalized, r.getOrThrow())
            return r
        }
        return tree.stat(normalized)
    }

    private suspend fun deleteNoLock(normalized: String): Result<Unit> {
        if (normalized == "/") {
            FLog.w(TAG, "delete failed: cannot delete root")
            return Result.failure(FsError.PermissionDenied("/"))
        }
        if (mountTable.isMountPoint(normalized)) {
            FLog.w(TAG, "delete failed: cannot delete mount point $normalized")
            return Result.failure(FsError.PermissionDenied("不能删除挂载点: $normalized"))
        }
        if (fileLockManager.isLocked(normalized)) {
            FLog.w(TAG, "delete failed: file locked $normalized")
            return Result.failure(FsError.Locked(normalized))
        }
        val match = mountTable.findMount(normalized)
        if (match != null) {
            if (match.options.readOnly) return readOnlyError(normalized)
            val r = match.diskOps.delete(match.relativePath)
            if (r.isSuccess) {
                invalidateCache(normalized)
                eventBus.emit(normalized, FsEventKind.DELETED)
            } else {
                FLog.w(TAG, "delete failed (disk): $normalized, error=${r.exceptionOrNull()}")
            }
            return r
        }
        return tree.delete(normalized).also { r ->
            if (r.isSuccess) {
                walAppend(WalEntry.Delete(normalized))
                eventBus.emit(normalized, FsEventKind.DELETED)
            } else {
                FLog.w(TAG, "delete failed: $normalized, error=${r.exceptionOrNull()}")
            }
        }
    }

    private suspend fun createDirRecursiveNoLock(normalized: String): Result<Unit> {
        if (normalized == "/") return Result.success(Unit)
        val parts = normalized.removePrefix("/").split("/")
        var current = ""
        for (part in parts) {
            current = "$current/$part"
            val result = createDirNoLock(current)
            if (result.isFailure) {
                val err = result.exceptionOrNull()
                if (err is FsError.AlreadyExists) continue
                return result
            }
        }
        return Result.success(Unit)
    }

    private suspend fun deleteRecursiveNoLock(normalized: String): Result<Unit> {
        if (normalized == "/") return Result.failure(FsError.PermissionDenied("/"))
        if (mountTable.isMountPoint(normalized)) {
            return Result.failure(FsError.PermissionDenied("不能删除挂载点: $normalized"))
        }
        val meta = statNoLock(normalized).getOrElse { return Result.failure(it) }
        val lmeta = tree.lstat(normalized).getOrNull()
        if (lmeta != null && lmeta.type == FsType.SYMLINK) return deleteNoLock(normalized)
        if (meta.type == FsType.FILE) return deleteNoLock(normalized)
        val entries = readDirNoLock(normalized).getOrElse { return Result.failure(it) }
        for (entry in entries) {
            val childPath = if (normalized == "/") "/${entry.name}" else "$normalized/${entry.name}"
            deleteRecursiveNoLock(childPath).getOrElse { return Result.failure(it) }
        }
        return deleteNoLock(normalized)
    }

    private suspend fun writeAllNoLock(normalized: String, data: ByteArray): Result<Unit> {
        val parentPath = normalized.substringBeforeLast('/', "/")
        if (parentPath != "/") {
            createDirRecursiveNoLock(parentPath).getOrElse { return Result.failure(it) }
        }
        if (statNoLock(normalized).isFailure) {
            createFileNoLock(normalized).getOrElse { return Result.failure(it) }
        }

        val match = mountTable.findMount(normalized)
        if (match != null) {
            val currentData = readAllBytes(normalized).getOrNull()
            if (currentData != null && currentData.isNotEmpty()) {
                versionManager.saveVersion(normalized, currentData)
            }
            // 挂载点写入：通过 diskOps 直接写
            val r = match.diskOps.writeFile(match.relativePath, 0, data)
            if (r.isSuccess) {
                mc.addBytesWritten(data.size.toLong())
                invalidateCache(normalized)
                eventBus.emit(normalized, FsEventKind.MODIFIED)
            }
            return r
        }

        // VFS 内存文件写入
        val node = tree.resolveNodeOrError(normalized).getOrElse { return Result.failure(it) }
        if (node !is FileNode) return Result.failure(FsError.NotFile(normalized))
        if (!node.permissions.canWrite()) return Result.failure(FsError.PermissionDenied(normalized))
        val end = data.size.toLong()
        val growth = maxOf(0L, end - node.size.toLong())
        checkQuota(growth)?.let { return it }
        if (node.size > 0) {
            val oldData = node.blocks.toByteArray()
            versionManager.saveVersion(normalized, oldData)
        }
        tree.writeAt(node, 0, data).also { result ->
            if (result.isSuccess) {
                walAppend(WalEntry.Write(normalized, 0, data))
                mc.addBytesWritten(data.size.toLong())
                invalidateCache(normalized)
                eventBus.emit(normalized, FsEventKind.MODIFIED)
            }
        }
        return Result.success(Unit)
    }

    private suspend fun copyNoLock(src: String, dst: String): Result<Unit> {
        val meta = statNoLock(src).getOrElse { return Result.failure(it) }
        if (meta.type == FsType.FILE) {
            val data = readAllBytes(src).getOrElse { return Result.failure(it) }
            return writeAllNoLock(dst, data)
        }
        createDirRecursiveNoLock(dst).getOrElse { return Result.failure(it) }
        val entries = readDirNoLock(src).getOrElse { return Result.failure(it) }
        for (entry in entries) {
            copyNoLock("$src/${entry.name}", "$dst/${entry.name}")
                .getOrElse { return Result.failure(it) }
        }
        return Result.success(Unit)
    }

    // ═══════════════════════════════════════════════════════════
    // InMemoryFileHandle 的 readAt / writeAt（内部调用）
    // ═══════════════════════════════════════════════════════════

    internal suspend fun readAt(node: FileNode, offset: Long, length: Int): Result<ByteArray> =
        readLocked { tree.readAt(node, offset, length) }

    internal suspend fun writeAt(node: FileNode, offset: Long, data: ByteArray, path: String): Result<Unit> =
        writeLocked {
            val end = offset.toInt() + data.size
            val growth = maxOf(0L, end.toLong() - node.size.toLong())
            checkQuota(growth)?.let {
                FLog.w(TAG, "writeAt failed: quota exceeded, growth=$growth")
                return@writeLocked it
            }
            if (node.size > 0) {
                val oldData = node.blocks.toByteArray()
                val contentChanged = if (offset == 0L && data.size == oldData.size) {
                    !data.contentEquals(oldData)
                } else true
                if (contentChanged) versionManager.saveVersion(path, oldData)
            }
            tree.writeAt(node, offset, data).also { result ->
                if (result.isSuccess) walAppend(WalEntry.Write(path, offset, data))
            }
        }

    // ═══════════════════════════════════════════════════════════
    // MountsImpl
    // ═══════════════════════════════════════════════════════════

    private inner class MountsImpl : FsMounts {
        override suspend fun mount(
            virtualPath: String,
            diskOps: DiskFileOperations,
            options: MountOptions
        ): Result<Unit> {
            FLog.i(TAG, "mount: virtualPath=$virtualPath, rootPath=${diskOps.rootPath}, readOnly=${options.readOnly}")
            val mark = mc.begin()
            val result = writeLocked {
                val normalized = PathUtils.normalize(virtualPath)
                mountTable.mount(normalized, diskOps, options).getOrElse {
                    FLog.w(TAG, "mount failed: $virtualPath, error=$it")
                    return@writeLocked Result.failure(it)
                }
                tree.ensureDirPath(normalized)
                persistence.persistMounts(mountTable.toMountInfoList())
                startDiskWatcherIfSupported(normalized, diskOps)
                FLog.d(TAG, "mount success: $virtualPath")
                Result.success(Unit)
            }
            mc.end(Op.MOUNT, mark, result)
            return result
        }

        override suspend fun unmount(virtualPath: String): Result<Unit> {
            FLog.i(TAG, "unmount: virtualPath=$virtualPath")
            val mark = mc.begin()
            val result = writeLocked {
                val normalized = PathUtils.normalize(virtualPath)
                stopDiskWatcher(normalized)
                mountTable.unmount(normalized).getOrElse {
                    FLog.w(TAG, "unmount failed: $virtualPath, error=$it")
                    return@writeLocked Result.failure(it)
                }
                statCache.removeByPrefix(normalized)
                readDirCache.removeByPrefix(normalized)
                persistence.persistMounts(mountTable.toMountInfoList())
                FLog.d(TAG, "unmount success: $virtualPath")
                Result.success(Unit)
            }
            mc.end(Op.UNMOUNT, mark, result)
            return result
        }

        override suspend fun list(): List<String> = readLocked { mountTable.listMounts() }

        override suspend fun pending(): List<PendingMount> {
            writeLocked { ensureLoaded() }
            return readLocked {
                mountTable.pendingMounts().map { info ->
                    PendingMount(info.virtualPath, info.rootPath, info.readOnly)
                }
            }
        }

        override suspend fun sync(path: String): Result<List<FsEvent>> {
            FLog.d(TAG, "sync: path=$path")
            writeLocked { ensureLoaded() }
            val mark = mc.begin()
            val result = writeLocked {
                val normalized = PathUtils.normalize(path)
                val match = mountTable.findMount(normalized)
                    ?: return@writeLocked Result.failure(FsError.NotMounted(normalized).also {
                        FLog.w(TAG, "sync failed: not mounted path=$normalized")
                    })
                val mountPoint = match.mountPoint
                val diskOps = match.diskOps
                val events = mutableListOf<FsEvent>()
                val diskEntries = mutableMapOf<String, FsType>()
                suspend fun scanDisk(diskPath: String, prefix: String) {
                    val list = diskOps.list(diskPath).getOrNull() ?: return
                    for (entry in list) {
                        val rel = "$prefix/${entry.name}"
                        diskEntries[rel] = entry.type
                        if (entry.type == FsType.DIRECTORY) scanDisk("$diskPath/${entry.name}", rel)
                    }
                }
                scanDisk(match.relativePath, "")
                for ((rel, type) in diskEntries) {
                    val vfsPath = "$mountPoint$rel"
                    if (type == FsType.FILE) {
                        try {
                            val diskPath = if (match.relativePath == "/") rel else "${match.relativePath}$rel"
                            val meta = diskOps.stat(diskPath).getOrNull()
                            if (meta != null && meta.type == FsType.FILE) {
                                val data = diskOps.readFile(diskPath, 0, meta.size.toInt()).getOrNull()
                                if (data != null && data.isNotEmpty()) versionManager.saveVersion(vfsPath, data)
                            }
                        } catch (e: Exception) {
                            FLog.w(TAG, "sync: failed to save version for $vfsPath: ${e.message}")
                        }
                    }
                    invalidateCache(vfsPath)
                    events.add(FsEvent(vfsPath, FsEventKind.MODIFIED))
                    eventBus.emit(vfsPath, FsEventKind.MODIFIED)
                }
                FLog.d(TAG, "sync completed: path=$path, events=${events.size}")
                Result.success(events)
            }
            mc.end(Op.SYNC, mark, result)
            return result
        }
    }

    // ═══════════════════════════════════════════════════════════
    // VersionsImpl
    // ═══════════════════════════════════════════════════════════

    private inner class VersionsImpl : FsVersions {
        override suspend fun list(path: String): Result<List<FileVersion>> {
            writeLocked { ensureLoaded() }
            return readLocked {
                val normalized = PathUtils.normalize(path)
                val meta = statInternal(normalized).getOrElse { return@readLocked Result.failure(it) }
                if (meta.type != FsType.FILE) return@readLocked Result.failure(FsError.NotFile(normalized))
                Result.success(versionManager.fileVersions(normalized))
            }
        }

        override suspend fun read(path: String, versionId: String): Result<ByteArray> {
            writeLocked { ensureLoaded() }
            return readLocked {
                val normalized = PathUtils.normalize(path)
                versionManager.readVersion(normalized, versionId)
            }
        }

        override suspend fun restore(path: String, versionId: String): Result<Unit> = writeLocked {
            ensureLoaded()
            val normalized = PathUtils.normalize(path)
            FLog.d(TAG, "restoreVersion: path=$normalized, versionId=$versionId")
            val currentData = readAllBytes(normalized).getOrElse {
                FLog.w(TAG, "restoreVersion failed: cannot read current $normalized: $it")
                return@writeLocked Result.failure(it)
            }
            val historicalData = versionManager.restoreVersion(normalized, versionId, currentData)
                .getOrElse {
                    FLog.w(TAG, "restoreVersion failed: version not found $normalized/$versionId")
                    return@writeLocked Result.failure(it)
                }
            val match = mountTable.findMount(normalized)
            if (match != null) {
                match.diskOps.writeFile(match.relativePath, 0, historicalData).getOrElse {
                    FLog.e(TAG, "restoreVersion failed: cannot write back to disk $normalized", it)
                    return@writeLocked Result.failure(it)
                }
                invalidateCache(normalized)
            } else {
                val node = tree.resolveNode(normalized) as? FileNode
                    ?: return@writeLocked Result.failure(FsError.NotFound(normalized).also {
                        FLog.w(TAG, "restoreVersion failed: node not found $normalized")
                    })
                node.blocks.clear()
                if (historicalData.isNotEmpty()) node.blocks.write(0, historicalData)
                node.modifiedAtMillis = VfsTree.nowMillis()
                walAppend(WalEntry.Write(normalized, 0, historicalData))
            }
            eventBus.emit(normalized, FsEventKind.MODIFIED)
            FLog.i(TAG, "restoreVersion success: path=$normalized, versionId=$versionId")
            Result.success(Unit)
        }
    }

    // ═══════════════════════════════════════════════════════════
    // SearchImpl
    // ═══════════════════════════════════════════════════════════

    private inner class SearchImpl : FsSearch {
        override suspend fun find(query: SearchQuery): Result<List<SearchResult>> {
            writeLocked { ensureLoaded() }
            return readLocked {
            val rootPath = PathUtils.normalize(query.rootPath)
            FLog.d(TAG, "find: rootPath=$rootPath, namePattern=${query.namePattern}, contentPattern=${query.contentPattern}")
            val results = mutableListOf<SearchResult>()
            val activeMountPoints = mountTable.listMounts().toSet()

            val nameMatcher = query.namePattern?.let { buildGlobMatcher(it, query.caseSensitive) }
            val contentNeedle = query.contentPattern?.let {
                if (query.caseSensitive) it else it.lowercase()
            }

            val memoryMatches = tree.find(rootPath, query.maxDepth, activeMountPoints) { path, node ->
                if (query.typeFilter != null && node.type != query.typeFilter) return@find false
                if (nameMatcher != null) {
                    val name = path.substringAfterLast('/')
                    if (!nameMatcher(name)) return@find false
                }
                if (contentNeedle != null && node !is FileNode) return@find false
                true
            }

            for ((path, node) in memoryMatches) {
                if (contentNeedle != null && node is FileNode) {
                    if (!node.permissions.canRead()) continue
                    val data = node.blocks.toByteArray()
                    val matchedLines = grepContent(data, contentNeedle, query.caseSensitive)
                    if (matchedLines.isEmpty()) continue
                    results.add(SearchResult(path, node.type, node.size.toLong(), matchedLines))
                } else {
                    val size = if (node is FileNode) node.size.toLong() else 0L
                    results.add(SearchResult(path, node.type, size))
                }
            }

            for (mountPoint in activeMountPoints) {
                if (!mountPoint.startsWith(rootPath) && rootPath != "/") continue
                if (rootPath != "/" && !rootPath.startsWith(mountPoint) && !mountPoint.startsWith(rootPath)) continue
                val match = mountTable.findMount(mountPoint) ?: continue
                val diskSearchRoot = if (rootPath.startsWith("$mountPoint/")) {
                    rootPath.removePrefix(mountPoint)
                } else "/"
                val depthOffset = if (rootPath == "/" || mountPoint.startsWith("$rootPath/")) {
                    mountPoint.removePrefix(rootPath).count { it == '/' }
                } else 0
                val adjustedMaxDepth = if (query.maxDepth < 0) -1 else maxOf(0, query.maxDepth - depthOffset)
                if (query.maxDepth >= 0 && adjustedMaxDepth <= 0 && diskSearchRoot == "/") continue
                searchMountPoint(
                    mountPoint, match.diskOps, diskSearchRoot, adjustedMaxDepth,
                    nameMatcher, contentNeedle, query.typeFilter, query.caseSensitive, results
                )
            }

            FLog.d(TAG, "find completed: ${results.size} results")
            Result.success(results.toList())
            }
        }

        private suspend fun searchMountPoint(
            mountPoint: String, diskOps: DiskFileOperations, diskPath: String,
            maxDepth: Int, nameMatcher: ((String) -> Boolean)?, contentNeedle: String?,
            typeFilter: FsType?, caseSensitive: Boolean, results: MutableList<SearchResult>
        ) {
            suspend fun scan(relPath: String, depth: Int) {
                val entries = diskOps.list(relPath).getOrNull() ?: return
                for (entry in entries) {
                    val childRelPath = if (relPath == "/") "/${entry.name}" else "$relPath/${entry.name}"
                    val virtualPath = "$mountPoint${childRelPath}"
                    if (typeFilter != null && entry.type != typeFilter) {
                        if (entry.type == FsType.DIRECTORY && (maxDepth < 0 || depth + 1 <= maxDepth)) scan(childRelPath, depth + 1)
                        continue
                    }
                    val nameMatched = nameMatcher == null || nameMatcher(entry.name)
                    if (nameMatched) {
                        if (contentNeedle != null && entry.type == FsType.FILE) {
                            val meta = diskOps.stat(childRelPath).getOrNull()
                            if (meta != null && meta.type == FsType.FILE && meta.size > 0) {
                                val data = diskOps.readFile(childRelPath, 0, meta.size.toInt()).getOrNull()
                                if (data != null) {
                                    val matchedLines = grepContent(data, contentNeedle, caseSensitive)
                                    if (matchedLines.isNotEmpty()) results.add(SearchResult(virtualPath, entry.type, meta.size, matchedLines))
                                }
                            }
                        } else if (contentNeedle != null && entry.type != FsType.FILE) {
                            // skip
                        } else {
                            val meta = diskOps.stat(childRelPath).getOrNull()
                            results.add(SearchResult(virtualPath, entry.type, meta?.size ?: 0L))
                        }
                    }
                    if (entry.type == FsType.DIRECTORY && (maxDepth < 0 || depth + 1 <= maxDepth)) scan(childRelPath, depth + 1)
                }
            }
            scan(diskPath, 0)
        }
    }

    // ═══════════════════════════════════════════════════════════
    // ObserveImpl
    // ═══════════════════════════════════════════════════════════

    private inner class ObserveImpl : FsObserve {
        override fun watch(path: String): Flow<FsEvent> = eventBus.watch(path)
        override fun metrics(): FsMetrics = mc.snapshot()
        override fun resetMetrics() = mc.reset()
        override fun quotaInfo(): QuotaInfo = QuotaInfo(quotaBytes = quotaBytes, usedBytes = tree.totalUsedBytes())
    }

    // ═══════════════════════════════════════════════════════════
    // StreamsImpl
    // ═══════════════════════════════════════════════════════════

    private inner class StreamsImpl : FsStreams {
        override fun read(path: String, chunkSize: Int): Flow<ByteArray> = flow {
            val normalized = PathUtils.normalize(path)
            val handle = open(normalized, OpenMode.READ).getOrThrow()
            try {
                var offset = 0L
                while (true) {
                    val chunk = handle.readAt(offset, chunkSize).getOrThrow()
                    if (chunk.isEmpty()) break
                    emit(chunk)
                    offset += chunk.size
                }
            } finally {
                handle.close()
            }
        }

        override suspend fun write(path: String, dataFlow: Flow<ByteArray>): Result<Unit> {
            FLog.d(TAG, "writeStream: path=$path")
            val normalized = PathUtils.normalize(path)
            val parentPath = normalized.substringBeforeLast('/', "/")
            if (parentPath != "/") {
                createDirRecursive(parentPath).getOrElse { return Result.failure(it) }
            }
            if (stat(normalized).isFailure) {
                createFile(normalized).getOrElse { return Result.failure(it) }
            }
            val match = readLocked { mountTable.findMount(normalized) }
            if (match != null) {
                writeLocked {
                    val currentData = readAllBytes(normalized).getOrNull()
                    if (currentData != null && currentData.isNotEmpty()) versionManager.saveVersion(normalized, currentData)
                }
            }
            val handle = open(normalized, OpenMode.WRITE).getOrElse { return Result.failure(it) }
            return try {
                var offset = 0L
                dataFlow.collect { chunk ->
                    handle.writeAt(offset, chunk).getOrThrow()
                    offset += chunk.size
                }
                eventBus.emit(normalized, FsEventKind.MODIFIED)
                Result.success(Unit)
            } catch (e: Exception) {
                FLog.e(TAG, "writeStream failed: path=$path", e)
                Result.failure(e)
            } finally {
                handle.close()
            }
        }
    }

    // ═══════════════════════════════════════════════════════════
    // ChecksumImpl
    // ═══════════════════════════════════════════════════════════

    private inner class ChecksumImpl : FsChecksum {
        override suspend fun compute(path: String, algorithm: ChecksumAlgorithm): Result<String> {
            writeLocked { ensureLoaded() }
            return readLocked {
                val normalized = PathUtils.normalize(path)
                FLog.d(TAG, "checksum: path=$normalized, algorithm=$algorithm")
                val data = readAllBytes(normalized).getOrElse {
                    FLog.w(TAG, "checksum failed: cannot read $normalized: $it")
                    return@readLocked Result.failure(it)
                }
                val hash = when (algorithm) {
                    ChecksumAlgorithm.CRC32 -> VfsChecksum.crc32(data)
                    ChecksumAlgorithm.SHA256 -> VfsChecksum.sha256(data)
                }
                Result.success(hash)
            }
        }
    }

    // ═══════════════════════════════════════════════════════════
    // XattrImpl
    // ═══════════════════════════════════════════════════════════

    private inner class XattrImpl : FsXattr {
        /**
         * 挂载点 xattr 策略：
         *   1. 先探测平台 diskOps 是否支持原生 xattr（结果缓存）
         *   2. 支持原生 → 直接调用 diskOps（iOS POSIX xattr）
         *   3. 不支持 → fallback 到内存 overlay + WAL 持久化（JVM/Android）
         */

        /** 缓存 diskOps 实例是否支持原生 xattr：null=未探测，true=支持，false=不支持 */
        private val nativeXattrSupport = HashMap<DiskFileOperations, Boolean>()

        /** 探测 diskOps 是否支持原生 xattr（首次调用时触发，结果缓存）。 */
        private suspend fun supportsNativeXattr(diskOps: DiskFileOperations, testPath: String): Boolean {
            nativeXattrSupport[diskOps]?.let { return it }
            // 用一次 listXattrs 探测：如果返回 PermissionDenied("xattr not supported") 则不支持
            val probeResult = diskOps.listXattrs(testPath)
            val supported = probeResult.isSuccess ||
                probeResult.exceptionOrNull() !is FsError.PermissionDenied
            nativeXattrSupport[diskOps] = supported
            return supported
        }

        override suspend fun set(path: String, name: String, value: ByteArray): Result<Unit> = writeLocked {
            ensureLoaded()
            val normalized = PathUtils.normalize(path)
            val match = mountTable.findMount(normalized)
            if (match != null) {
                match.diskOps.stat(match.relativePath).getOrElse { return@writeLocked Result.failure(it) }
                if (supportsNativeXattr(match.diskOps, match.relativePath)) {
                    return@writeLocked match.diskOps.setXattr(match.relativePath, name, value)
                }
                // fallback: overlay + WAL
                mountXattrs.getOrPut(normalized) { LinkedHashMap() }[name] = value.copyOf()
                dirtyXattrPaths.add(normalized)
                walAppend(WalEntry.SetXattr(normalized, name, value))
                return@writeLocked Result.success(Unit)
            }
            tree.setXattr(normalized, name, value).also { r ->
                if (r.isSuccess) walAppend(WalEntry.SetXattr(normalized, name, value))
            }
        }

        override suspend fun get(path: String, name: String): Result<ByteArray> {
            writeLocked { ensureLoaded() }
            return readLocked {
                val normalized = PathUtils.normalize(path)
                val match = mountTable.findMount(normalized)
                if (match != null) {
                    if (supportsNativeXattr(match.diskOps, match.relativePath)) {
                        return@readLocked match.diskOps.getXattr(match.relativePath, name)
                    }
                    // fallback: overlay
                    val attrs = mountXattrs[normalized]
                    val value = attrs?.get(name)
                        ?: return@readLocked Result.failure(FsError.NotFound("xattr '$name' on $normalized"))
                    return@readLocked Result.success(value.copyOf())
                }
                tree.getXattr(normalized, name)
            }
        }

        override suspend fun remove(path: String, name: String): Result<Unit> = writeLocked {
            ensureLoaded()
            val normalized = PathUtils.normalize(path)
            val match = mountTable.findMount(normalized)
            if (match != null) {
                if (supportsNativeXattr(match.diskOps, match.relativePath)) {
                    return@writeLocked match.diskOps.removeXattr(match.relativePath, name)
                }
                // fallback: overlay + WAL
                val attrs = mountXattrs[normalized]
                if (attrs == null || attrs.remove(name) == null) {
                    return@writeLocked Result.failure(FsError.NotFound("xattr '$name' on $normalized"))
                }
                if (attrs.isEmpty()) mountXattrs.remove(normalized)
                dirtyXattrPaths.add(normalized)
                walAppend(WalEntry.RemoveXattr(normalized, name))
                return@writeLocked Result.success(Unit)
            }
            tree.removeXattr(normalized, name).also { r ->
                if (r.isSuccess) walAppend(WalEntry.RemoveXattr(normalized, name))
            }
        }

        override suspend fun list(path: String): Result<List<String>> {
            writeLocked { ensureLoaded() }
            return readLocked {
                val normalized = PathUtils.normalize(path)
                val match = mountTable.findMount(normalized)
                if (match != null) {
                    if (supportsNativeXattr(match.diskOps, match.relativePath)) {
                        return@readLocked match.diskOps.listXattrs(match.relativePath)
                    }
                    // fallback: overlay
                    return@readLocked Result.success(mountXattrs[normalized]?.keys?.toList() ?: emptyList())
                }
                tree.listXattrs(normalized)
            }
        }
    }

    // ═══════════════════════════════════════════════════════════
    // SymlinksImpl
    // ═══════════════════════════════════════════════════════════

    private inner class SymlinksImpl : FsSymlinks {
        override suspend fun create(linkPath: String, targetPath: String): Result<Unit> {
            FLog.d(TAG, "createSymlink: linkPath=$linkPath, targetPath=$targetPath")
            return writeLocked {
                ensureLoaded()
                val normalized = PathUtils.normalize(linkPath)
                val match = mountTable.findMount(normalized)
                if (match != null) {
                    FLog.w(TAG, "createSymlink failed: cannot create symlink in mount point $normalized")
                    return@writeLocked Result.failure(FsError.PermissionDenied("挂载点内不支持符号链接: $normalized"))
                }
                tree.createSymlink(normalized, targetPath).also { r ->
                    if (r.isSuccess) {
                        walAppend(WalEntry.CreateSymlink(normalized, targetPath))
                        eventBus.emit(normalized, FsEventKind.CREATED)
                    } else {
                        FLog.w(TAG, "createSymlink failed: $normalized -> $targetPath, error=${r.exceptionOrNull()}")
                    }
                }
            }
        }

        override suspend fun readLink(path: String): Result<String> {
            FLog.d(TAG, "readLink: path=$path")
            writeLocked { ensureLoaded() }
            return readLocked {
                val normalized = PathUtils.normalize(path)
                tree.readLink(normalized)
            }
        }
    }

    // ═══════════════════════════════════════════════════════════
    // ArchiveImpl
    // ═══════════════════════════════════════════════════════════

    private inner class ArchiveImpl : FsArchive {
        override suspend fun compress(
            sourcePaths: List<String>,
            archivePath: String,
            format: ArchiveFormat
        ): Result<Unit> {
            FLog.d(TAG, "archive.compress: sources=$sourcePaths, archive=$archivePath, format=$format")
            return try {
                val normalizedArchive = PathUtils.normalize(archivePath)
                // 挂载点委托：若归档目标和所有源路径都在同一挂载点下，委托给 diskOps
                val archiveMatch = readLocked { mountTable.findMount(normalizedArchive) }
                if (archiveMatch != null) {
                    val allInSameMount = sourcePaths.all { src ->
                        val n = PathUtils.normalize(src)
                        val m = mountTable.findMount(n)
                        m != null && m.mountPoint == archiveMatch.mountPoint
                    }
                    if (allInSameMount) {
                        val diskSources = sourcePaths.map { src ->
                            val n = PathUtils.normalize(src)
                            mountTable.findMount(n)!!.relativePath
                        }
                        return archiveMatch.diskOps.compress(diskSources, archiveMatch.relativePath, format)
                    }
                }
                val items = mutableListOf<ArchiveCodec.ArchiveItem>()
                for (srcPath in sourcePaths) {
                    val normalized = PathUtils.normalize(srcPath)
                    collectItems(normalized, "", items)
                }
                val archiveData = when (format) {
                    ArchiveFormat.ZIP -> ArchiveCodec.zipEncode(items)
                    ArchiveFormat.TAR -> ArchiveCodec.tarEncode(items)
                }
                writeAll(normalizedArchive, archiveData)
            } catch (e: Exception) {
                FLog.e(TAG, "archive.compress failed", e)
                Result.failure(FsError.Unknown("归档失败: ${e.message}"))
            }
        }

        override suspend fun extract(
            archivePath: String,
            targetDir: String,
            format: ArchiveFormat?
        ): Result<Unit> {
            FLog.d(TAG, "archive.extract: archive=$archivePath, targetDir=$targetDir, format=$format")
            return try {
                val normalizedArchive = PathUtils.normalize(archivePath)
                val normalizedTarget = PathUtils.normalize(targetDir)
                // 挂载点委托：若归档和目标都在同一挂载点下
                val archiveMatch = readLocked { mountTable.findMount(normalizedArchive) }
                val targetMatch = readLocked { mountTable.findMount(normalizedTarget) }
                if (archiveMatch != null && targetMatch != null &&
                    archiveMatch.mountPoint == targetMatch.mountPoint) {
                    return archiveMatch.diskOps.extract(archiveMatch.relativePath, targetMatch.relativePath, format)
                }
                val archiveData = readAll(normalizedArchive).getOrElse { return Result.failure(it) }
                val resolvedFormat = format ?: ArchiveCodec.detectFormat(archiveData)
                    ?: return Result.failure(FsError.Unknown("无法检测归档格式"))
                createDirRecursive(normalizedTarget).getOrElse { return Result.failure(it) }

                when (resolvedFormat) {
                    ArchiveFormat.ZIP -> {
                        for (entry in ArchiveCodec.zipDecode(archiveData)) {
                            val path = PathUtils.normalize("$normalizedTarget/${entry.path}")
                            if (path != normalizedTarget && !path.startsWith("$normalizedTarget/")) {
                                return Result.failure(FsError.PermissionDenied("Zip Slip: ${entry.path}"))
                            }
                            if (entry.isDirectory) {
                                createDirRecursive(path).getOrElse { return Result.failure(it) }
                            } else {
                                writeAll(path, entry.data).getOrElse { return Result.failure(it) }
                            }
                        }
                    }
                    ArchiveFormat.TAR -> {
                        for (entry in ArchiveCodec.tarDecode(archiveData)) {
                            val path = PathUtils.normalize("$normalizedTarget/${entry.path}")
                            if (path != normalizedTarget && !path.startsWith("$normalizedTarget/")) {
                                return Result.failure(FsError.PermissionDenied("Zip Slip (TAR): ${entry.path}"))
                            }
                            if (entry.isDirectory) {
                                createDirRecursive(path).getOrElse { return Result.failure(it) }
                            } else {
                                writeAll(path, entry.data).getOrElse { return Result.failure(it) }
                            }
                        }
                    }
                }
                FLog.d(TAG, "archive.extract success")
                Result.success(Unit)
            } catch (e: Exception) {
                FLog.e(TAG, "archive.extract failed", e)
                Result.failure(FsError.Unknown("解压失败: ${e.message}"))
            }
        }

        override suspend fun list(
            archivePath: String,
            format: ArchiveFormat?
        ): Result<List<ArchiveEntry>> {
            FLog.d(TAG, "archive.list: archive=$archivePath, format=$format")
            return try {
                val normalized = PathUtils.normalize(archivePath)
                // 挂载点委托
                val match = readLocked { mountTable.findMount(normalized) }
                if (match != null) {
                    return match.diskOps.listArchive(match.relativePath, format)
                }
                val archiveData = readAll(normalized).getOrElse { return Result.failure(it) }
                val resolvedFormat = format ?: ArchiveCodec.detectFormat(archiveData)
                    ?: return Result.failure(FsError.Unknown("无法检测归档格式"))
                val entries = when (resolvedFormat) {
                    ArchiveFormat.ZIP -> ArchiveCodec.zipList(archiveData)
                    ArchiveFormat.TAR -> ArchiveCodec.tarList(archiveData)
                }
                Result.success(entries)
            } catch (e: Exception) {
                FLog.e(TAG, "archive.list failed", e)
                Result.failure(FsError.Unknown("列出归档内容失败: ${e.message}"))
            }
        }

        private suspend fun collectItems(
            fsPath: String,
            relativePath: String,
            items: MutableList<ArchiveCodec.ArchiveItem>
        ) {
            val meta = stat(fsPath).getOrThrow()
            val entryPath = if (relativePath.isEmpty()) fsPath.substringAfterLast('/') else relativePath
            when (meta.type) {
                FsType.FILE -> {
                    val data = readAll(fsPath).getOrThrow()
                    items.add(
                        ArchiveCodec.ArchiveItem(
                            path = entryPath,
                            type = FsType.FILE,
                            data = data,
                            modifiedAtMillis = meta.modifiedAtMillis
                        )
                    )
                }
                FsType.DIRECTORY -> {
                    items.add(
                        ArchiveCodec.ArchiveItem(
                            path = entryPath,
                            type = FsType.DIRECTORY,
                            data = ByteArray(0),
                            modifiedAtMillis = meta.modifiedAtMillis
                        )
                    )
                    val children = readDir(fsPath).getOrThrow()
                    for (child in children) {
                        val childFsPath = "$fsPath/${child.name}"
                        val childRelPath = "$entryPath/${child.name}"
                        collectItems(childFsPath, childRelPath, items)
                    }
                }
                FsType.SYMLINK -> {
                    val data = readAll(fsPath).getOrElse { ByteArray(0) }
                    items.add(
                        ArchiveCodec.ArchiveItem(
                            path = entryPath,
                            type = FsType.FILE,
                            data = data,
                            modifiedAtMillis = meta.modifiedAtMillis
                        )
                    )
                }
            }
        }
    }

    // ═══════════════════════════════════════════════════════════
    // TrashImpl
    // ═══════════════════════════════════════════════════════════

    private inner class TrashImpl : FsTrash {
        override suspend fun moveToTrash(path: String): Result<String> {
            FLog.d(TAG, "trash.moveToTrash: path=$path")
            return writeLocked {
                ensureLoaded()
                val normalized = PathUtils.normalize(path)
                if (normalized == "/") {
                    return@writeLocked Result.failure(FsError.PermissionDenied("不能删除根目录"))
                }
                if (mountTable.isMountPoint(normalized)) {
                    return@writeLocked Result.failure(FsError.PermissionDenied("不能删除挂载点: $normalized"))
                }
                if (fileLockManager.isLocked(normalized)) {
                    return@writeLocked Result.failure(FsError.Locked(normalized))
                }

                val match = mountTable.findMount(normalized)
                if (match != null) {
                    // 挂载点文件：委托给 diskOps
                    if (match.options.readOnly) return@writeLocked readOnlyError(normalized)
                    val meta = match.diskOps.stat(match.relativePath).getOrNull()
                    val type = meta?.type ?: FsType.FILE
                    val diskResult = match.diskOps.moveToTrash(match.relativePath)
                    if (diskResult.isSuccess) {
                        val trashId = diskResult.getOrThrow()
                        trashManager.recordMountedTrash(trashId, normalized, type)
                        invalidateCache(normalized)
                        eventBus.emit(normalized, FsEventKind.DELETED)
                        walAppend(WalEntry.MoveToTrash(normalized, trashId))
                        persistTrash()
                    }
                    return@writeLocked diskResult
                }

                // VFS 内存文件
                // 使用 lstat 判断是否为符号链接（不跟随）
                val lmeta = tree.lstat(normalized).getOrElse {
                    return@writeLocked Result.failure(it)
                }
                val trashId: String
                when (lmeta.type) {
                    FsType.FILE -> {
                        val content = readAllBytes(normalized).getOrElse { ByteArray(0) }
                        trashId = trashManager.moveToTrash(normalized, FsType.FILE, content)
                    }
                    FsType.DIRECTORY -> {
                        val children = collectChildrenForTrash(normalized)
                        trashId = trashManager.moveToTrash(normalized, FsType.DIRECTORY, children = children)
                    }
                    FsType.SYMLINK -> {
                        val target = tree.readLink(normalized).getOrNull()
                        val content = target?.encodeToByteArray()
                        trashId = trashManager.moveToTrash(normalized, FsType.SYMLINK, content)
                    }
                }
                // 从文件树中删除
                deleteInternalRecursive(normalized)
                walAppend(WalEntry.MoveToTrash(normalized, trashId))
                persistTrash()
                eventBus.emit(normalized, FsEventKind.DELETED)
                FLog.i(TAG, "trash.moveToTrash success: path=$normalized, trashId=$trashId")
                Result.success(trashId)
            }
        }

        override suspend fun restore(trashId: String): Result<Unit> {
            FLog.d(TAG, "trash.restore: trashId=$trashId")
            return writeLocked {
                ensureLoaded()
                val entry = trashManager.getEntry(trashId)
                    ?: return@writeLocked Result.failure(FsError.NotFound("trash entry: $trashId"))
                val originalPath = entry.originalPath

                // 检查原始路径是否已存在（lstat 检查包括符号链接）
                val existsByStat = statInternal(originalPath).isSuccess
                val existsByLstat = tree.lstat(originalPath).isSuccess
                if (existsByStat || existsByLstat) {
                    return@writeLocked Result.failure(FsError.AlreadyExists(originalPath))
                }

                if (entry.isMounted) {
                    // 挂载点文件：委托给 diskOps
                    val match = mountTable.findMount(originalPath)
                        ?: return@writeLocked Result.failure(FsError.NotMounted(originalPath))
                    val diskResult = match.diskOps.restoreFromTrash(trashId, match.relativePath)
                    if (diskResult.isSuccess) {
                        trashManager.remove(trashId)
                        invalidateCache(originalPath)
                        eventBus.emit(originalPath, FsEventKind.CREATED)
                        walAppend(WalEntry.RestoreFromTrash(trashId, originalPath))
                        persistTrash()
                    }
                    return@writeLocked diskResult
                }

                // VFS 内存文件恢复
                val parentPath = originalPath.substringBeforeLast('/', "/")
                if (parentPath != "/") {
                    tree.ensureDirPath(parentPath)
                }

                when (entry.type) {
                    FsType.FILE -> {
                        tree.createFile(originalPath).getOrElse { return@writeLocked Result.failure(it) }
                        if (entry.content != null && entry.content.isNotEmpty()) {
                            val node = tree.resolveNode(originalPath) as? FileNode
                            if (node != null) {
                                node.blocks.write(0, entry.content)
                                node.modifiedAtMillis = VfsTree.nowMillis()
                            }
                        }
                    }
                    FsType.DIRECTORY -> {
                        tree.createDir(originalPath).getOrElse { return@writeLocked Result.failure(it) }
                        if (entry.children != null) {
                            restoreChildrenFromTrash(originalPath, entry.children)
                        }
                    }
                    FsType.SYMLINK -> {
                        val target = entry.content?.decodeToString() ?: ""
                        tree.createSymlink(originalPath, target).getOrElse { return@writeLocked Result.failure(it) }
                    }
                }

                trashManager.remove(trashId)
                walAppend(WalEntry.RestoreFromTrash(trashId, originalPath))
                persistTrash()
                eventBus.emit(originalPath, FsEventKind.CREATED)
                FLog.i(TAG, "trash.restore success: trashId=$trashId, path=$originalPath")
                Result.success(Unit)
            }
        }

        override suspend fun list(): Result<List<TrashItem>> {
            writeLocked { ensureLoaded() }
            return readLocked {
                Result.success(trashManager.listItems())
            }
        }

        override suspend fun purge(trashId: String): Result<Unit> {
            FLog.d(TAG, "trash.purge: trashId=$trashId")
            return writeLocked {
                ensureLoaded()
                val entry = trashManager.getEntry(trashId)
                    ?: return@writeLocked Result.failure(FsError.NotFound("trash entry: $trashId"))
                if (entry.isMounted) {
                    val match = mountTable.findMount(entry.originalPath)
                    match?.diskOps?.purgeTrash(trashId)
                }
                trashManager.remove(trashId)
                FLog.d(TAG, "trash.purge success: trashId=$trashId")
                Result.success(Unit)
            }
        }

        override suspend fun purgeAll(): Result<Unit> {
            FLog.d(TAG, "trash.purgeAll")
            return writeLocked {
                ensureLoaded()
                val all = trashManager.clear()
                for (entry in all) {
                    if (entry.isMounted) {
                        val match = mountTable.findMount(entry.originalPath)
                        match?.diskOps?.purgeTrash(entry.trashId)
                    }
                }
                FLog.d(TAG, "trash.purgeAll success: purged ${all.size} items")
                Result.success(Unit)
            }
        }

        private fun collectChildrenForTrash(dirPath: String): List<VfsTrashManager.TrashChildEntry> {
            val result = mutableListOf<VfsTrashManager.TrashChildEntry>()
            val node = tree.resolveNode(dirPath) as? DirNode ?: return result
            for ((name, child) in node.children) {
                val childPath = "$dirPath/$name"
                when (child) {
                    is FileNode -> {
                        result.add(
                            VfsTrashManager.TrashChildEntry(
                                relativePath = name,
                                type = FsType.FILE,
                                content = child.blocks.toByteArray()
                            )
                        )
                    }
                    is DirNode -> {
                        val grandChildren = collectChildrenForTrash(childPath)
                        result.add(
                            VfsTrashManager.TrashChildEntry(
                                relativePath = name,
                                type = FsType.DIRECTORY,
                                children = grandChildren
                            )
                        )
                    }
                    is SymlinkNode -> {
                        result.add(
                            VfsTrashManager.TrashChildEntry(
                                relativePath = name,
                                type = FsType.SYMLINK,
                                content = child.targetPath.encodeToByteArray()
                            )
                        )
                    }
                }
            }
            return result
        }

        private fun restoreChildrenFromTrash(parentPath: String, children: List<VfsTrashManager.TrashChildEntry>) {
            for (child in children) {
                val childPath = "$parentPath/${child.relativePath}"
                when (child.type) {
                    FsType.FILE -> {
                        tree.createFile(childPath)
                        if (child.content != null && child.content.isNotEmpty()) {
                            val node = tree.resolveNode(childPath) as? FileNode
                            if (node != null) {
                                node.blocks.write(0, child.content)
                                node.modifiedAtMillis = VfsTree.nowMillis()
                            }
                        }
                    }
                    FsType.DIRECTORY -> {
                        tree.createDir(childPath)
                        if (child.children != null) {
                            restoreChildrenFromTrash(childPath, child.children)
                        }
                    }
                    FsType.SYMLINK -> {
                        val target = child.content?.decodeToString() ?: ""
                        tree.createSymlink(childPath, target)
                    }
                }
            }
        }

        /** 递归删除内存节点（不走 WAL、不走 event）。 */
        private fun deleteInternalRecursive(path: String) {
            val node = tree.resolveNode(path) ?: return
            if (node is DirNode) {
                val childNames = node.children.keys.toList()
                for (name in childNames) {
                    deleteInternalRecursive("$path/$name")
                }
            }
            tree.delete(path)
        }
    }

    // ═══════════════════════════════════════════════════════════
    // Disk Watcher 管理
    // ═══════════════════════════════════════════════════════════

    private fun startDiskWatcherIfSupported(mountPoint: String, diskOps: DiskFileOperations) {
        val watcher = diskOps as? DiskFileWatcher ?: return
        FLog.d(TAG, "startDiskWatcher: mountPoint=$mountPoint")
        val job = watchScope.launch {
            watcher.watchDisk(this).collect { diskEvent ->
                val virtualPath = if (diskEvent.relativePath == "/") mountPoint
                else "$mountPoint${diskEvent.relativePath}"
                FLog.v(TAG, "diskEvent: $virtualPath, kind=${diskEvent.kind}")
                if (diskEvent.kind == FsEventKind.MODIFIED) {
                    saveExternalChangeVersion(virtualPath, diskOps, diskEvent.relativePath)
                }
                invalidateCache(virtualPath)
                eventBus.emit(virtualPath, diskEvent.kind)
            }
        }
        watcherJobs[mountPoint] = job
    }

    private suspend fun saveExternalChangeVersion(
        virtualPath: String, diskOps: DiskFileOperations, relativePath: String
    ) {
        writeLocked {
            try {
                val meta = diskOps.stat(relativePath).getOrNull() ?: return@writeLocked
                if (meta.type != FsType.FILE) return@writeLocked
                val data = diskOps.readFile(relativePath, 0, meta.size.toInt()).getOrNull() ?: return@writeLocked
                if (data.isNotEmpty()) versionManager.saveVersion(virtualPath, data)
            } catch (e: Exception) {
                FLog.w(TAG, "saveExternalChangeVersion: failed for $virtualPath: ${e.message}")
            }
        }
    }

    private fun stopDiskWatcher(mountPoint: String) {
        watcherJobs.remove(mountPoint)?.cancel()
    }

    // ═══════════════════════════════════════════════════════════
    // 私有辅助
    // ═══════════════════════════════════════════════════════════

    internal suspend fun ensureLoaded() {
        val loadResult = persistence.ensureLoaded() ?: return
        FLog.i(TAG, "ensureLoaded: restoring from persistence")
        loadResult.snapshot?.let {
            tree.restoreFromSnapshot(it)
            FLog.d(TAG, "ensureLoaded: snapshot restored")
        }
        if (loadResult.walEntries.isNotEmpty()) {
            tree.replayWal(loadResult.walEntries)
            // 恢复挂载点路径的 xattr overlay
            for (entry in loadResult.walEntries) {
                when (entry) {
                    is WalEntry.SetXattr -> {
                        if (mountTable.findMount(entry.path) != null) {
                            mountXattrs.getOrPut(entry.path) { LinkedHashMap() }[entry.name] = entry.value.copyOf()
                        }
                    }
                    is WalEntry.RemoveXattr -> {
                        if (mountTable.findMount(entry.path) != null) {
                            val attrs = mountXattrs[entry.path]
                            attrs?.remove(entry.name)
                            if (attrs != null && attrs.isEmpty()) mountXattrs.remove(entry.path)
                        }
                    }
                    else -> {}
                }
            }
            FLog.d(TAG, "ensureLoaded: replayed ${loadResult.walEntries.size} WAL entries")
        }
        if (loadResult.mountInfos.isNotEmpty()) {
            mountTable.restoreFromPersistence(loadResult.mountInfos)
            FLog.d(TAG, "ensureLoaded: restored ${loadResult.mountInfos.size} mount infos")
        }
        loadResult.versionData?.let {
            versionManager.restoreFromSnapshot(it.entries)
            FLog.d(TAG, "ensureLoaded: restored version data for ${it.entries.size} files")
        }
        loadResult.trashData?.let {
            trashManager.restoreFromSnapshot(it.entries)
            FLog.d(TAG, "ensureLoaded: restored trash data with ${it.entries.size} entries")
        }
    }

    internal suspend fun walAppend(entry: WalEntry) {
        persistence.appendWal(
            entry,
            snapshotProvider = { tree.toSnapshot() },
            versionDataProvider = { versionManager.toSnapshotData() },
            trashDataProvider = { SnapshotTrashData(trashManager.toSnapshotEntries()) },
            postSnapshotWalEntries = { mountXattrsToWalEntries() }
        )
    }

    /**
     * 将 mountXattrs overlay 转为 WAL 条目。
     *
     * 快照保存后 WAL 被清空，overlay 条目必须全量重新写入 WAL，
     * 否则下次恢复时会丢失这些 xattr。
     *
     * 优化：如果 overlay 为空或自上次快照以来无 xattr 变更（dirtyXattrPaths 为空），
     * 且 overlay 中所有条目都已经在上次快照前写入了 WAL，则可以跳过回写。
     * 但由于快照会清空 WAL，这里始终需要全量回写非空的 overlay。
     */
    private fun mountXattrsToWalEntries(): List<WalEntry> {
        dirtyXattrPaths.clear()
        if (mountXattrs.isEmpty()) return emptyList()
        val entries = mutableListOf<WalEntry>()
        for ((path, attrs) in mountXattrs) {
            for ((name, value) in attrs) {
                entries.add(WalEntry.SetXattr(path, name, value))
            }
        }
        return entries
    }

    private suspend fun persistTrash() {
        persistence.persistTrash(SnapshotTrashData(trashManager.toSnapshotEntries()))
    }

    internal suspend fun <T> readLocked(block: suspend () -> T): T = rwLock.withReadLock { block() }
    internal suspend fun <T> writeLocked(block: suspend () -> T): T = rwLock.withWriteLock { block() }

    internal fun invalidateCache(path: String) {
        statCache.remove(path)
        readDirCache.remove(path)
        val parent = path.substringBeforeLast('/', "/")
        readDirCache.remove(parent)
    }

    /** stat 内部实现（不走 metrics），支持内存和挂载。 */
    internal suspend fun statInternal(normalized: String): Result<FsMeta> {
        val match = mountTable.findMount(normalized)
        if (match != null) return match.diskOps.stat(match.relativePath).map { it.copy(path = normalized) }
        return tree.stat(normalized)
    }

    /** 读取文件全部内容（内部使用，不经过 metrics/handle）。 */
    internal suspend fun readAllBytes(normalized: String): Result<ByteArray> {
        val match = mountTable.findMount(normalized)
        if (match != null) {
            val meta = match.diskOps.stat(match.relativePath).getOrElse { return Result.failure(it) }
            if (meta.type != FsType.FILE) return Result.failure(FsError.NotFile(normalized))
            return match.diskOps.readFile(match.relativePath, 0, meta.size.toInt())
        }
        val node = tree.resolveNodeOrError(normalized).getOrElse { return Result.failure(it) }
        if (node !is FileNode) return Result.failure(FsError.NotFile(normalized))
        if (!node.permissions.canRead()) return Result.failure(FsError.PermissionDenied(normalized))
        return Result.success(node.blocks.toByteArray())
    }

    private fun hasAccess(perms: FsPermissions, mode: OpenMode): Boolean = when (mode) {
        OpenMode.READ -> perms.canRead()
        OpenMode.WRITE -> perms.canWrite()
        OpenMode.READ_WRITE -> perms.canRead() && perms.canWrite()
    }

    private fun readOnlyError(path: String): Result<Nothing> =
        Result.failure(FsError.PermissionDenied("挂载点只读: $path"))

    private fun checkQuota(additionalBytes: Long): Result<Unit>? {
        if (quotaBytes < 0) return null
        val used = tree.totalUsedBytes()
        if (used + additionalBytes > quotaBytes) {
            return Result.failure(
                FsError.QuotaExceeded("配额不足: 已用 $used / 限额 $quotaBytes，需要额外 $additionalBytes 字节")
            )
        }
        return null
    }

    private fun grepContent(data: ByteArray, needle: String, caseSensitive: Boolean): List<MatchedLine> {
        if (data.isEmpty()) return emptyList()
        val text = data.decodeToString()
        val lines = text.split('\n')
        val matched = mutableListOf<MatchedLine>()
        for ((index, line) in lines.withIndex()) {
            val haystack = if (caseSensitive) line else line.lowercase()
            if (haystack.contains(needle)) matched.add(MatchedLine(index + 1, line))
        }
        return matched
    }

    private fun buildGlobMatcher(pattern: String, caseSensitive: Boolean): (String) -> Boolean {
        val regexStr = buildString {
            append("^")
            for (ch in pattern) {
                when (ch) {
                    '*' -> append(".*")
                    '?' -> append(".")
                    '.' -> append("\\.")
                    '\\' -> append("\\\\")
                    '[', ']', '(', ')', '{', '}', '^', '$', '|', '+' -> { append("\\"); append(ch) }
                    else -> append(ch)
                }
            }
            append("$")
        }
        val options = if (caseSensitive) emptySet() else setOf(RegexOption.IGNORE_CASE)
        val regex = Regex(regexStr, options)
        return { name -> regex.matches(name) }
    }
}
