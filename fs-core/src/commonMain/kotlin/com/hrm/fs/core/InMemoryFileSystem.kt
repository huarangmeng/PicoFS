package com.hrm.fs.core

import com.hrm.fs.api.ChecksumAlgorithm
import com.hrm.fs.api.DiskFileOperations
import com.hrm.fs.api.DiskFileWatcher
import com.hrm.fs.api.FileHandle
import com.hrm.fs.api.FileSystem
import com.hrm.fs.api.FileVersion
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
import com.hrm.fs.api.log.FLog
import com.hrm.fs.core.VfsMetricsCollector.Op
import com.hrm.fs.core.persistence.PersistenceConfig
import com.hrm.fs.core.persistence.SnapshotPermissions
import com.hrm.fs.core.persistence.WalEntry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

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

    private val mutex = Mutex()
    internal val tree = VfsTree()
    internal val mountTable = MountTable()
    internal val eventBus = VfsEventBus()
    internal val persistence = VfsPersistenceManager(storage, persistenceConfig)
    internal val mc = VfsMetricsCollector()
    internal val versionManager = VfsVersionManager()
    internal val fileLockManager = VfsFileLockManager()

    internal val statCache = VfsCache<String, FsMeta>(256)
    internal val readDirCache = VfsCache<String, List<FsEntry>>(128)

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

    // ═══════════════════════════════════════════════════════════
    // 基础 CRUD
    // ═══════════════════════════════════════════════════════════

    override suspend fun createFile(path: String): Result<Unit> {
        FLog.d(TAG, "createFile: path=$path")
        val mark = mc.begin()
        val result = locked {
            ensureLoaded()
            val normalized = PathUtils.normalize(path)
            val match = mountTable.findMount(normalized)
            if (match != null) {
                if (match.options.readOnly) return@locked readOnlyError(normalized)
                val r = match.diskOps.createFile(match.relativePath)
                if (r.isSuccess) {
                    invalidateCache(normalized)
                    eventBus.emit(normalized, FsEventKind.CREATED)
                } else {
                    FLog.w(TAG, "createFile failed (disk): $normalized, error=${r.exceptionOrNull()}")
                }
                return@locked r
            }
            tree.createFile(normalized).also { r ->
                if (r.isSuccess) {
                    walAppend(WalEntry.CreateFile(normalized))
                    eventBus.emit(normalized, FsEventKind.CREATED)
                } else {
                    FLog.w(TAG, "createFile failed: $normalized, error=${r.exceptionOrNull()}")
                }
            }
        }
        mc.end(Op.CREATE_FILE, mark, result)
        return result
    }

    override suspend fun createDir(path: String): Result<Unit> {
        FLog.d(TAG, "createDir: path=$path")
        val mark = mc.begin()
        val result = locked {
            ensureLoaded()
            val normalized = PathUtils.normalize(path)
            if (normalized == "/") return@locked Result.success(Unit)
            val match = mountTable.findMount(normalized)
            if (match != null) {
                if (match.options.readOnly) return@locked readOnlyError(normalized)
                val r = match.diskOps.createDir(match.relativePath)
                if (r.isSuccess) {
                    invalidateCache(normalized)
                    eventBus.emit(normalized, FsEventKind.CREATED)
                } else {
                    FLog.w(TAG, "createDir failed (disk): $normalized, error=${r.exceptionOrNull()}")
                }
                return@locked r
            }
            tree.createDir(normalized).also { r ->
                if (r.isSuccess) {
                    walAppend(WalEntry.CreateDir(normalized))
                    eventBus.emit(normalized, FsEventKind.CREATED)
                } else {
                    FLog.w(TAG, "createDir failed: $normalized, error=${r.exceptionOrNull()}")
                }
            }
        }
        mc.end(Op.CREATE_DIR, mark, result)
        return result
    }

    override suspend fun open(path: String, mode: OpenMode): Result<FileHandle> {
        FLog.d(TAG, "open: path=$path, mode=$mode")
        val mark = mc.begin()
        val result = locked {
            ensureLoaded()
            val normalized = PathUtils.normalize(path)
            val match = mountTable.findMount(normalized)
            if (match != null) {
                if (match.options.readOnly && mode != OpenMode.READ) {
                    FLog.w(TAG, "open failed: read-only mount $normalized with mode=$mode")
                    return@locked Result.failure(FsError.PermissionDenied("挂载点只读: $normalized"))
                }
                return@locked Result.success(
                    DiskFileHandle(match.diskOps, match.relativePath, mode, normalized, fileLockManager)
                )
            }
            val node = tree.resolveNodeOrError(normalized).getOrElse {
                FLog.w(TAG, "open failed: $normalized not found")
                return@locked Result.failure(it)
            }
            if (node !is FileNode) {
                FLog.w(TAG, "open failed: $normalized is not a file")
                return@locked Result.failure(FsError.NotFile(normalized))
            }
            if (!hasAccess(node.permissions, mode)) {
                FLog.w(TAG, "open failed: permission denied for $normalized")
                return@locked Result.failure(FsError.PermissionDenied(normalized))
            }
            Result.success(InMemoryFileHandle(this, node, mode, normalized, fileLockManager))
        }
        mc.end(Op.OPEN, mark, result)
        return result
    }

    override suspend fun readDir(path: String): Result<List<FsEntry>> {
        val mark = mc.begin()
        val result = locked {
            ensureLoaded()
            val normalized = PathUtils.normalize(path)
            val match = mountTable.findMount(normalized)
            if (match != null) {
                readDirCache.get(normalized)?.let { return@locked Result.success(it) }
                val r = match.diskOps.list(match.relativePath)
                if (r.isSuccess) readDirCache.put(normalized, r.getOrThrow())
                return@locked r
            }
            tree.readDir(normalized)
        }
        mc.end(Op.READ_DIR, mark, result)
        return result
    }

    override suspend fun stat(path: String): Result<FsMeta> {
        val mark = mc.begin()
        val result = locked {
            ensureLoaded()
            val normalized = PathUtils.normalize(path)
            val match = mountTable.findMount(normalized)
            if (match != null) {
                statCache.get(normalized)?.let { return@locked Result.success(it) }
                val r = match.diskOps.stat(match.relativePath).map { it.copy(path = normalized) }
                if (r.isSuccess) statCache.put(normalized, r.getOrThrow())
                return@locked r
            }
            tree.stat(normalized)
        }
        mc.end(Op.STAT, mark, result)
        return result
    }

    override suspend fun delete(path: String): Result<Unit> {
        FLog.d(TAG, "delete: path=$path")
        val mark = mc.begin()
        val result = locked {
            ensureLoaded()
            val normalized = PathUtils.normalize(path)
            if (normalized == "/") {
                FLog.w(TAG, "delete failed: cannot delete root")
                return@locked Result.failure(FsError.PermissionDenied("/"))
            }
            if (mountTable.isMountPoint(normalized)) {
                FLog.w(TAG, "delete failed: cannot delete mount point $normalized")
                return@locked Result.failure(FsError.PermissionDenied("不能删除挂载点: $normalized"))
            }
            if (fileLockManager.isLocked(normalized)) {
                FLog.w(TAG, "delete failed: file locked $normalized")
                return@locked Result.failure(FsError.Locked(normalized))
            }
            val match = mountTable.findMount(normalized)
            if (match != null) {
                if (match.options.readOnly) return@locked readOnlyError(normalized)
                val r = match.diskOps.delete(match.relativePath)
                if (r.isSuccess) {
                    invalidateCache(normalized)
                    eventBus.emit(normalized, FsEventKind.DELETED)
                } else {
                    FLog.w(TAG, "delete failed (disk): $normalized, error=${r.exceptionOrNull()}")
                }
                return@locked r
            }
            tree.delete(normalized).also { r ->
                if (r.isSuccess) {
                    walAppend(WalEntry.Delete(normalized))
                    eventBus.emit(normalized, FsEventKind.DELETED)
                } else {
                    FLog.w(TAG, "delete failed: $normalized, error=${r.exceptionOrNull()}")
                }
            }
        }
        mc.end(Op.DELETE, mark, result)
        return result
    }

    override suspend fun setPermissions(path: String, permissions: FsPermissions): Result<Unit> {
        val mark = mc.begin()
        val result = locked {
            ensureLoaded()
            val normalized = PathUtils.normalize(path)
            if (mountTable.findMount(normalized) != null) return@locked Result.success(Unit)
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
    // 递归操作
    // ═══════════════════════════════════════════════════════════

    override suspend fun createDirRecursive(path: String): Result<Unit> {
        val normalized = PathUtils.normalize(path)
        if (normalized == "/") return Result.success(Unit)
        val parts = normalized.removePrefix("/").split("/")
        var current = ""
        for (part in parts) {
            current = "$current/$part"
            val result = createDir(current)
            if (result.isFailure) {
                val err = result.exceptionOrNull()
                if (err is FsError.AlreadyExists) continue
                return result
            }
        }
        return Result.success(Unit)
    }

    override suspend fun deleteRecursive(path: String): Result<Unit> {
        val normalized = PathUtils.normalize(path)
        if (normalized == "/") return Result.failure(FsError.PermissionDenied("/"))
        if (mountTable.isMountPoint(normalized)) {
            return Result.failure(FsError.PermissionDenied("不能删除挂载点: $normalized"))
        }
        val meta = stat(normalized).getOrElse { return Result.failure(it) }
        val lmeta = locked { tree.lstat(normalized) }.getOrNull()
        if (lmeta != null && lmeta.type == FsType.SYMLINK) return delete(normalized)
        if (meta.type == FsType.FILE) return delete(normalized)
        val entries = readDir(normalized).getOrElse { return Result.failure(it) }
        for (entry in entries) {
            val childPath = if (normalized == "/") "/${entry.name}" else "$normalized/${entry.name}"
            deleteRecursive(childPath).getOrElse { return Result.failure(it) }
        }
        return delete(normalized)
    }

    // ═══════════════════════════════════════════════════════════
    // 便捷读写
    // ═══════════════════════════════════════════════════════════

    override suspend fun readAll(path: String): Result<ByteArray> {
        val mark = mc.begin()
        val handle = open(path, OpenMode.READ).getOrElse {
            val fail = Result.failure<ByteArray>(it)
            mc.end(Op.READ_ALL, mark, fail)
            return fail
        }
        val result = try {
            val meta = stat(path).getOrElse { return Result.failure(it) }
            val r = handle.readAt(0, meta.size.toInt())
            if (r.isSuccess) mc.addBytesRead(r.getOrThrow().size.toLong())
            r
        } finally {
            handle.close()
        }
        mc.end(Op.READ_ALL, mark, result)
        return result
    }

    override suspend fun writeAll(path: String, data: ByteArray): Result<Unit> {
        FLog.d(TAG, "writeAll: path=$path, size=${data.size}")
        val mark = mc.begin()
        val normalized = PathUtils.normalize(path)
        val parentPath = normalized.substringBeforeLast('/', "/")
        if (parentPath != "/") {
            createDirRecursive(parentPath).getOrElse {
                val fail = Result.failure<Unit>(it)
                mc.end(Op.WRITE_ALL, mark, fail)
                return fail
            }
        }
        if (stat(normalized).isFailure) {
            createFile(normalized).getOrElse {
                val fail = Result.failure<Unit>(it)
                mc.end(Op.WRITE_ALL, mark, fail)
                return fail
            }
        }

        val match = locked { mountTable.findMount(normalized) }
        if (match != null) {
            locked {
                val currentData = readAllBytes(normalized).getOrNull()
                if (currentData != null && currentData.isNotEmpty()) {
                    versionManager.saveVersion(normalized, currentData)
                }
            }
        }

        val handle = open(normalized, OpenMode.WRITE).getOrElse {
            val fail = Result.failure<Unit>(it)
            mc.end(Op.WRITE_ALL, mark, fail)
            return fail
        }
        val result = try {
            val r = handle.writeAt(0, data)
            if (r.isSuccess) {
                mc.addBytesWritten(data.size.toLong())
                invalidateCache(normalized)
                eventBus.emit(normalized, FsEventKind.MODIFIED)
            }
            r
        } finally {
            handle.close()
        }
        mc.end(Op.WRITE_ALL, mark, result)
        return result
    }

    // ═══════════════════════════════════════════════════════════
    // copy / move
    // ═══════════════════════════════════════════════════════════

    override suspend fun copy(srcPath: String, dstPath: String): Result<Unit> {
        FLog.d(TAG, "copy: src=$srcPath, dst=$dstPath")
        val mark = mc.begin()
        val result = copyInternal(srcPath, dstPath)
        if (result.isFailure) FLog.w(TAG, "copy failed: src=$srcPath, dst=$dstPath, error=${result.exceptionOrNull()}")
        mc.end(Op.COPY, mark, result)
        return result
    }

    private suspend fun copyInternal(srcPath: String, dstPath: String): Result<Unit> {
        val src = PathUtils.normalize(srcPath)
        val dst = PathUtils.normalize(dstPath)
        val meta = stat(src).getOrElse { return Result.failure(it) }
        if (meta.type == FsType.FILE) {
            val data = readAll(src).getOrElse { return Result.failure(it) }
            return writeAll(dst, data)
        }
        createDirRecursive(dst).getOrElse { return Result.failure(it) }
        val entries = readDir(src).getOrElse { return Result.failure(it) }
        for (entry in entries) {
            copyInternal("$src/${entry.name}", "$dst/${entry.name}")
                .getOrElse { return Result.failure(it) }
        }
        return Result.success(Unit)
    }

    override suspend fun move(srcPath: String, dstPath: String): Result<Unit> {
        FLog.d(TAG, "move: src=$srcPath, dst=$dstPath")
        val mark = mc.begin()
        val result = run {
            copyInternal(srcPath, dstPath).getOrElse { return@run Result.failure(it) }
            deleteRecursive(srcPath)
        }
        if (result.isFailure) FLog.w(TAG, "move failed: src=$srcPath, dst=$dstPath, error=${result.exceptionOrNull()}")
        mc.end(Op.MOVE, mark, result)
        return result
    }

    // ═══════════════════════════════════════════════════════════
    // InMemoryFileHandle 的 readAt / writeAt（内部调用）
    // ═══════════════════════════════════════════════════════════

    internal suspend fun readAt(node: FileNode, offset: Long, length: Int): Result<ByteArray> =
        locked { tree.readAt(node, offset, length) }

    internal suspend fun writeAt(node: FileNode, offset: Long, data: ByteArray, path: String): Result<Unit> =
        locked {
            val end = offset.toInt() + data.size
            val growth = maxOf(0L, end.toLong() - node.size.toLong())
            checkQuota(growth)?.let {
                FLog.w(TAG, "writeAt failed: quota exceeded, growth=$growth")
                return@locked it
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
            val result = locked {
                val normalized = PathUtils.normalize(virtualPath)
                mountTable.mount(normalized, diskOps, options).getOrElse {
                    FLog.w(TAG, "mount failed: $virtualPath, error=$it")
                    return@locked Result.failure(it)
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
            val result = locked {
                val normalized = PathUtils.normalize(virtualPath)
                stopDiskWatcher(normalized)
                mountTable.unmount(normalized).getOrElse {
                    FLog.w(TAG, "unmount failed: $virtualPath, error=$it")
                    return@locked Result.failure(it)
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

        override suspend fun list(): List<String> = locked { mountTable.listMounts() }

        override suspend fun pending(): List<PendingMount> = locked {
            ensureLoaded()
            mountTable.pendingMounts().map { info ->
                PendingMount(info.virtualPath, info.rootPath, info.readOnly)
            }
        }

        override suspend fun sync(path: String): Result<List<FsEvent>> {
            FLog.d(TAG, "sync: path=$path")
            val mark = mc.begin()
            val result = locked {
                ensureLoaded()
                val normalized = PathUtils.normalize(path)
                val match = mountTable.findMount(normalized)
                    ?: return@locked Result.failure(FsError.NotMounted(normalized).also {
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
        override suspend fun list(path: String): Result<List<FileVersion>> = locked {
            ensureLoaded()
            val normalized = PathUtils.normalize(path)
            val meta = statInternal(normalized).getOrElse { return@locked Result.failure(it) }
            if (meta.type != FsType.FILE) return@locked Result.failure(FsError.NotFile(normalized))
            Result.success(versionManager.fileVersions(normalized))
        }

        override suspend fun read(path: String, versionId: String): Result<ByteArray> = locked {
            ensureLoaded()
            val normalized = PathUtils.normalize(path)
            versionManager.readVersion(normalized, versionId)
        }

        override suspend fun restore(path: String, versionId: String): Result<Unit> = locked {
            ensureLoaded()
            val normalized = PathUtils.normalize(path)
            FLog.d(TAG, "restoreVersion: path=$normalized, versionId=$versionId")
            val currentData = readAllBytes(normalized).getOrElse {
                FLog.w(TAG, "restoreVersion failed: cannot read current $normalized: $it")
                return@locked Result.failure(it)
            }
            val historicalData = versionManager.restoreVersion(normalized, versionId, currentData)
                .getOrElse {
                    FLog.w(TAG, "restoreVersion failed: version not found $normalized/$versionId")
                    return@locked Result.failure(it)
                }
            val match = mountTable.findMount(normalized)
            if (match != null) {
                match.diskOps.writeFile(match.relativePath, 0, historicalData).getOrElse {
                    FLog.e(TAG, "restoreVersion failed: cannot write back to disk $normalized", it)
                    return@locked Result.failure(it)
                }
                invalidateCache(normalized)
            } else {
                val node = tree.resolveNode(normalized) as? FileNode
                    ?: return@locked Result.failure(FsError.NotFound(normalized).also {
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
        override suspend fun find(query: SearchQuery): Result<List<SearchResult>> = locked {
            ensureLoaded()
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
            val match = locked { mountTable.findMount(normalized) }
            if (match != null) {
                locked {
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
        override suspend fun compute(path: String, algorithm: ChecksumAlgorithm): Result<String> = locked {
            ensureLoaded()
            val normalized = PathUtils.normalize(path)
            FLog.d(TAG, "checksum: path=$normalized, algorithm=$algorithm")
            val data = readAllBytes(normalized).getOrElse {
                FLog.w(TAG, "checksum failed: cannot read $normalized: $it")
                return@locked Result.failure(it)
            }
            val hash = when (algorithm) {
                ChecksumAlgorithm.CRC32 -> VfsChecksum.crc32(data)
                ChecksumAlgorithm.SHA256 -> VfsChecksum.sha256(data)
            }
            Result.success(hash)
        }
    }

    // ═══════════════════════════════════════════════════════════
    // XattrImpl
    // ═══════════════════════════════════════════════════════════

    private inner class XattrImpl : FsXattr {
        override suspend fun set(path: String, name: String, value: ByteArray): Result<Unit> = locked {
            ensureLoaded()
            val normalized = PathUtils.normalize(path)
            val match = mountTable.findMount(normalized)
            if (match != null) return@locked match.diskOps.setXattr(match.relativePath, name, value)
            tree.setXattr(normalized, name, value).also { r ->
                if (r.isSuccess) walAppend(WalEntry.SetXattr(normalized, name, value))
            }
        }

        override suspend fun get(path: String, name: String): Result<ByteArray> = locked {
            ensureLoaded()
            val normalized = PathUtils.normalize(path)
            val match = mountTable.findMount(normalized)
            if (match != null) return@locked match.diskOps.getXattr(match.relativePath, name)
            tree.getXattr(normalized, name)
        }

        override suspend fun remove(path: String, name: String): Result<Unit> = locked {
            ensureLoaded()
            val normalized = PathUtils.normalize(path)
            val match = mountTable.findMount(normalized)
            if (match != null) return@locked match.diskOps.removeXattr(match.relativePath, name)
            tree.removeXattr(normalized, name).also { r ->
                if (r.isSuccess) walAppend(WalEntry.RemoveXattr(normalized, name))
            }
        }

        override suspend fun list(path: String): Result<List<String>> = locked {
            ensureLoaded()
            val normalized = PathUtils.normalize(path)
            val match = mountTable.findMount(normalized)
            if (match != null) return@locked match.diskOps.listXattrs(match.relativePath)
            tree.listXattrs(normalized)
        }
    }

    // ═══════════════════════════════════════════════════════════
    // SymlinksImpl
    // ═══════════════════════════════════════════════════════════

    private inner class SymlinksImpl : FsSymlinks {
        override suspend fun create(linkPath: String, targetPath: String): Result<Unit> {
            FLog.d(TAG, "createSymlink: linkPath=$linkPath, targetPath=$targetPath")
            return locked {
                ensureLoaded()
                val normalized = PathUtils.normalize(linkPath)
                val match = mountTable.findMount(normalized)
                if (match != null) {
                    FLog.w(TAG, "createSymlink failed: cannot create symlink in mount point $normalized")
                    return@locked Result.failure(FsError.PermissionDenied("挂载点内不支持符号链接: $normalized"))
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
            return locked {
                ensureLoaded()
                val normalized = PathUtils.normalize(path)
                tree.readLink(normalized)
            }
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
        locked {
            try {
                val meta = diskOps.stat(relativePath).getOrNull() ?: return@locked
                if (meta.type != FsType.FILE) return@locked
                val data = diskOps.readFile(relativePath, 0, meta.size.toInt()).getOrNull() ?: return@locked
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
    }

    internal suspend fun walAppend(entry: WalEntry) {
        persistence.appendWal(
            entry,
            snapshotProvider = { tree.toSnapshot() },
            versionDataProvider = { versionManager.toSnapshotData() }
        )
    }

    internal suspend fun <T> locked(block: suspend () -> T): T = mutex.withLock { block() }

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
