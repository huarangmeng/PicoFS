package com.hrm.fs.core

import com.hrm.fs.api.DiskFileOperations
import com.hrm.fs.api.DiskFileWatcher
import com.hrm.fs.api.FileHandle
import com.hrm.fs.api.FileSystem
import com.hrm.fs.api.FsEntry
import com.hrm.fs.api.FsError
import com.hrm.fs.api.FsEvent
import com.hrm.fs.api.FsEventKind
import com.hrm.fs.api.FsMeta
import com.hrm.fs.api.FsPermissions
import com.hrm.fs.api.FsStorage
import com.hrm.fs.api.FsType
import com.hrm.fs.api.MountOptions
import com.hrm.fs.api.OpenMode
import com.hrm.fs.api.PathUtils
import com.hrm.fs.api.PendingMount
import com.hrm.fs.core.persistence.PersistenceConfig
import com.hrm.fs.core.persistence.SnapshotPermissions
import com.hrm.fs.core.persistence.WalEntry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * 虚拟文件系统实现（门面层）。
 *
 * 组合四个内部组件完成所有功能：
 * - [VfsTree]               — 纯内存文件树的增删改查 / 快照 / WAL
 * - [MountTable]            — 挂载表管理 + 最长前缀匹配
 * - [VfsEventBus]           — 事件发布 / 订阅
 * - [VfsPersistenceManager] — WAL / Snapshot / Mount 持久化
 */
internal class InMemoryFileSystem(
    storage: FsStorage? = null,
    persistenceConfig: PersistenceConfig = PersistenceConfig(),
    watcherScope: CoroutineScope? = null
) : FileSystem {

    private val mutex = Mutex()
    private val tree = VfsTree()
    private val mountTable = MountTable()
    private val eventBus = VfsEventBus()
    private val persistence = VfsPersistenceManager(storage, persistenceConfig)

    /** 用于管理所有 disk watcher 协程的父 scope。 */
    private val watchScope = watcherScope ?: CoroutineScope(SupervisorJob())

    /** 每个挂载点对应的 watcher job，用于 unmount 时取消。 */
    private val watcherJobs = LinkedHashMap<String, Job>()

    // ═══════════════════════════════════════════════════════════
    // mount / unmount
    // ═══════════════════════════════════════════════════════════

    override suspend fun mount(
        virtualPath: String,
        diskOps: DiskFileOperations,
        options: MountOptions
    ): Result<Unit> = locked {
        val normalized = PathUtils.normalize(virtualPath)
        mountTable.mount(normalized, diskOps, options).getOrElse { return@locked Result.failure(it) }
        tree.ensureDirPath(normalized)
        persistence.persistMounts(mountTable.toMountInfoList())

        // 如果 DiskFileOperations 同时实现了 DiskFileWatcher，自动启动监听
        startDiskWatcherIfSupported(normalized, diskOps)

        Result.success(Unit)
    }

    override suspend fun unmount(virtualPath: String): Result<Unit> = locked {
        val normalized = PathUtils.normalize(virtualPath)

        // 停止该挂载点的 disk watcher
        stopDiskWatcher(normalized)

        mountTable.unmount(normalized).getOrElse { return@locked Result.failure(it) }
        persistence.persistMounts(mountTable.toMountInfoList())
        Result.success(Unit)
    }

    override suspend fun listMounts(): List<String> = locked {
        mountTable.listMounts()
    }

    override suspend fun pendingMounts(): List<PendingMount> = locked {
        ensureLoaded()
        mountTable.pendingMounts().map { info ->
            PendingMount(info.virtualPath, info.rootPath, info.readOnly)
        }
    }

    // ═══════════════════════════════════════════════════════════
    // sync（手动同步挂载点与磁盘状态）
    // ═══════════════════════════════════════════════════════════

    override suspend fun sync(path: String): Result<List<FsEvent>> = locked {
        ensureLoaded()
        val normalized = PathUtils.normalize(path)
        val match = mountTable.findMount(normalized)
            ?: return@locked Result.failure(FsError.NotMounted(normalized))

        val mountPoint = match.mountPoint
        val diskOps = match.diskOps
        val events = mutableListOf<FsEvent>()

        // 递归扫描磁盘目录，构建当前磁盘快照
        val diskEntries = mutableMapOf<String, FsType>() // relativePath -> FsType
        suspend fun scanDisk(diskPath: String, prefix: String) {
            val list = diskOps.list(diskPath).getOrNull() ?: return
            for (entry in list) {
                val rel = "$prefix/${entry.name}"
                diskEntries[rel] = entry.type
                if (entry.type == FsType.DIRECTORY) {
                    scanDisk("$diskPath/${entry.name}", rel)
                }
            }
        }
        scanDisk(match.relativePath, "")

        // 递归扫描 VFS 中该挂载点下已知的条目
        val vfsEntries = mutableMapOf<String, FsType>()
        suspend fun scanVfs(vfsPath: String, prefix: String) {
            val list = tree.readDir(vfsPath).getOrNull() ?: return
            for (entry in list) {
                val rel = "$prefix/${entry.name}"
                vfsEntries[rel] = entry.type
                if (entry.type == FsType.DIRECTORY) {
                    scanVfs("$vfsPath/${entry.name}", rel)
                }
            }
        }
        // VFS tree 中挂载点目录下的虚拟条目（非磁盘条目）可能为空，这里只用磁盘做对比

        // 如果挂载点 readDir 走的是 disk，就直接对比上次 readDir 缓存和当前磁盘
        // 简化方案：所有磁盘上的条目都发出事件，让 watcher 去重
        for ((rel, type) in diskEntries) {
            val vfsPath = "$mountPoint$rel"
            events.add(FsEvent(vfsPath, FsEventKind.MODIFIED))
            eventBus.emit(vfsPath, FsEventKind.MODIFIED)
        }

        Result.success(events)
    }

    // ═══════════════════════════════════════════════════════════
    // 基础 CRUD
    // ═══════════════════════════════════════════════════════════

    override suspend fun createFile(path: String): Result<Unit> = locked {
        ensureLoaded()
        val normalized = PathUtils.normalize(path)
        val match = mountTable.findMount(normalized)
        if (match != null) {
            if (match.options.readOnly) return@locked readOnlyError(normalized)
            val result = match.diskOps.createFile(match.relativePath)
            if (result.isSuccess) eventBus.emit(normalized, FsEventKind.CREATED)
            return@locked result
        }
        tree.createFile(normalized).also { result ->
            if (result.isSuccess) {
                walAppend(WalEntry.CreateFile(normalized))
                eventBus.emit(normalized, FsEventKind.CREATED)
            }
        }
    }

    override suspend fun createDir(path: String): Result<Unit> = locked {
        ensureLoaded()
        val normalized = PathUtils.normalize(path)
        if (normalized == "/") return@locked Result.success(Unit)
        val match = mountTable.findMount(normalized)
        if (match != null) {
            if (match.options.readOnly) return@locked readOnlyError(normalized)
            val result = match.diskOps.createDir(match.relativePath)
            if (result.isSuccess) eventBus.emit(normalized, FsEventKind.CREATED)
            return@locked result
        }
        tree.createDir(normalized).also { result ->
            if (result.isSuccess) {
                walAppend(WalEntry.CreateDir(normalized))
                eventBus.emit(normalized, FsEventKind.CREATED)
            }
        }
    }

    override suspend fun open(path: String, mode: OpenMode): Result<FileHandle> = locked {
        ensureLoaded()
        val normalized = PathUtils.normalize(path)
        val match = mountTable.findMount(normalized)
        if (match != null) {
            if (match.options.readOnly && mode != OpenMode.READ) {
                return@locked Result.failure(FsError.PermissionDenied("挂载点只读: $normalized"))
            }
            return@locked Result.success(DiskFileHandle(match.diskOps, match.relativePath, mode))
        }
        val node = tree.resolveNodeOrError(normalized).getOrElse { return@locked Result.failure(it) }
        if (node !is FileNode) return@locked Result.failure(FsError.NotFile(normalized))
        if (!hasAccess(node.permissions, mode)) {
            return@locked Result.failure(FsError.PermissionDenied(normalized))
        }
        Result.success(InMemoryFileHandle(this, node, mode))
    }

    override suspend fun readDir(path: String): Result<List<FsEntry>> = locked {
        ensureLoaded()
        val normalized = PathUtils.normalize(path)
        val match = mountTable.findMount(normalized)
        if (match != null) return@locked match.diskOps.list(match.relativePath)
        tree.readDir(normalized)
    }

    override suspend fun stat(path: String): Result<FsMeta> = locked {
        ensureLoaded()
        val normalized = PathUtils.normalize(path)
        val match = mountTable.findMount(normalized)
        if (match != null) {
            return@locked match.diskOps.stat(match.relativePath).map { it.copy(path = normalized) }
        }
        tree.stat(normalized)
    }

    override suspend fun delete(path: String): Result<Unit> = locked {
        ensureLoaded()
        val normalized = PathUtils.normalize(path)
        if (normalized == "/") return@locked Result.failure(FsError.PermissionDenied("/"))
        if (mountTable.isMountPoint(normalized)) {
            return@locked Result.failure(FsError.PermissionDenied("不能删除挂载点: $normalized"))
        }
        val match = mountTable.findMount(normalized)
        if (match != null) {
            if (match.options.readOnly) return@locked readOnlyError(normalized)
            val result = match.diskOps.delete(match.relativePath)
            if (result.isSuccess) eventBus.emit(normalized, FsEventKind.DELETED)
            return@locked result
        }
        tree.delete(normalized).also { result ->
            if (result.isSuccess) {
                walAppend(WalEntry.Delete(normalized))
                eventBus.emit(normalized, FsEventKind.DELETED)
            }
        }
    }

    override suspend fun setPermissions(path: String, permissions: FsPermissions): Result<Unit> = locked {
        ensureLoaded()
        val normalized = PathUtils.normalize(path)
        if (mountTable.findMount(normalized) != null) return@locked Result.success(Unit)
        tree.setPermissions(normalized, permissions).also { result ->
            if (result.isSuccess) {
                walAppend(WalEntry.SetPermissions(normalized, SnapshotPermissions.from(permissions)))
            }
        }
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
        val handle = open(path, OpenMode.READ).getOrElse { return Result.failure(it) }
        return try {
            val meta = stat(path).getOrElse { return Result.failure(it) }
            handle.readAt(0, meta.size.toInt())
        } finally {
            handle.close()
        }
    }

    override suspend fun writeAll(path: String, data: ByteArray): Result<Unit> {
        val normalized = PathUtils.normalize(path)
        val parentPath = normalized.substringBeforeLast('/', "/")
        if (parentPath != "/") {
            createDirRecursive(parentPath).getOrElse { return Result.failure(it) }
        }
        if (stat(normalized).isFailure) {
            createFile(normalized).getOrElse { return Result.failure(it) }
        }
        val handle = open(normalized, OpenMode.WRITE).getOrElse { return Result.failure(it) }
        return try {
            val result = handle.writeAt(0, data)
            if (result.isSuccess) eventBus.emit(normalized, FsEventKind.MODIFIED)
            result
        } finally {
            handle.close()
        }
    }

    // ═══════════════════════════════════════════════════════════
    // copy / move
    // ═══════════════════════════════════════════════════════════

    override suspend fun copy(srcPath: String, dstPath: String): Result<Unit> {
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
            copy("$src/${entry.name}", "$dst/${entry.name}").getOrElse { return Result.failure(it) }
        }
        return Result.success(Unit)
    }

    override suspend fun move(srcPath: String, dstPath: String): Result<Unit> {
        copy(srcPath, dstPath).getOrElse { return Result.failure(it) }
        return deleteRecursive(srcPath)
    }

    // ═══════════════════════════════════════════════════════════
    // 监听
    // ═══════════════════════════════════════════════════════════

    override fun watch(path: String): Flow<FsEvent> = eventBus.watch(path)

    // ═══════════════════════════════════════════════════════════
    // 流式读写
    // ═══════════════════════════════════════════════════════════

    override fun readStream(path: String, chunkSize: Int): Flow<ByteArray> = flow {
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

    override suspend fun writeStream(path: String, dataFlow: Flow<ByteArray>): Result<Unit> {
        val normalized = PathUtils.normalize(path)
        val parentPath = normalized.substringBeforeLast('/', "/")
        if (parentPath != "/") {
            createDirRecursive(parentPath).getOrElse { return Result.failure(it) }
        }
        if (stat(normalized).isFailure) {
            createFile(normalized).getOrElse { return Result.failure(it) }
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
            Result.failure(e)
        } finally {
            handle.close()
        }
    }

    // ═══════════════════════════════════════════════════════════
    // InMemoryFileHandle 的 readAt / writeAt（内部调用）
    // ═══════════════════════════════════════════════════════════

    internal suspend fun readAt(node: FileNode, offset: Long, length: Int): Result<ByteArray> = locked {
        tree.readAt(node, offset, length)
    }

    internal suspend fun writeAt(node: FileNode, offset: Long, data: ByteArray): Result<Unit> = locked {
        tree.writeAt(node, offset, data).also { result ->
            if (result.isSuccess) {
                walAppend(WalEntry.Write(tree.pathForNode(node), offset, data))
            }
        }
    }

    // ═══════════════════════════════════════════════════════════
    // Disk Watcher 管理
    // ═══════════════════════════════════════════════════════════

    /**
     * 如果 [diskOps] 同时实现了 [DiskFileWatcher]，则启动监听，
     * 将磁盘事件的相对路径转为虚拟路径后转发给 [VfsEventBus]。
     */
    private fun startDiskWatcherIfSupported(mountPoint: String, diskOps: DiskFileOperations) {
        val watcher = diskOps as? DiskFileWatcher ?: return

        val job = watchScope.launch {
            watcher.watchDisk(this).collect { diskEvent ->
                // 磁盘相对路径 -> 虚拟路径
                val virtualPath = if (diskEvent.relativePath == "/") mountPoint
                else "$mountPoint${diskEvent.relativePath}"
                eventBus.emit(virtualPath, diskEvent.kind)
            }
        }
        watcherJobs[mountPoint] = job
    }

    /**
     * 停止指定挂载点的 disk watcher。
     */
    private fun stopDiskWatcher(mountPoint: String) {
        watcherJobs.remove(mountPoint)?.cancel()
    }

    // ═══════════════════════════════════════════════════════════
    // 私有辅助
    // ═══════════════════════════════════════════════════════════

    private suspend fun ensureLoaded() {
        val loadResult = persistence.ensureLoaded() ?: return
        loadResult.snapshot?.let { tree.restoreFromSnapshot(it) }
        if (loadResult.walEntries.isNotEmpty()) tree.replayWal(loadResult.walEntries)
        if (loadResult.mountInfos.isNotEmpty()) mountTable.restoreFromPersistence(loadResult.mountInfos)
    }

    private suspend fun walAppend(entry: WalEntry) {
        persistence.appendWal(entry) { tree.toSnapshot() }
    }

    private suspend fun <T> locked(block: suspend () -> T): T = mutex.withLock { block() }

    private fun hasAccess(perms: FsPermissions, mode: OpenMode): Boolean = when (mode) {
        OpenMode.READ -> perms.canRead()
        OpenMode.WRITE -> perms.canWrite()
        OpenMode.READ_WRITE -> perms.canRead() && perms.canWrite()
    }

    private fun readOnlyError(path: String): Result<Nothing> =
        Result.failure(FsError.PermissionDenied("挂载点只读: $path"))
}
