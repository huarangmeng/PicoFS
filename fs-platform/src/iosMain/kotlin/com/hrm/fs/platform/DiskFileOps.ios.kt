package com.hrm.fs.platform

import com.hrm.fs.api.DiskFileEvent
import com.hrm.fs.api.DiskFileOperations
import com.hrm.fs.api.DiskFileWatcher
import com.hrm.fs.api.FsEntry
import com.hrm.fs.api.FsError
import com.hrm.fs.api.FsEventKind
import com.hrm.fs.api.FsMeta
import com.hrm.fs.api.FsPermissions
import com.hrm.fs.api.FsType
import kotlinx.cinterop.BetaInteropApi
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.usePinned
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import platform.Foundation.NSData
import platform.Foundation.NSFileHandle
import platform.Foundation.NSFileManager
import platform.Foundation.NSString
import platform.Foundation.closeFile
import platform.Foundation.create
import platform.Foundation.fileHandleForReadingAtPath
import platform.Foundation.fileHandleForWritingAtPath
import platform.Foundation.readDataOfLength
import platform.Foundation.seekToFileOffset
import platform.Foundation.stringByAppendingPathComponent
import platform.Foundation.stringByDeletingLastPathComponent
import platform.Foundation.timeIntervalSince1970
import platform.Foundation.writeData
import platform.posix.memcpy
import kotlin.concurrent.Volatile

actual fun createDiskFileOperations(rootPath: String): DiskFileOperations = IosDiskFileOperations(rootPath)

internal class IosDiskFileOperations(override val rootPath: String) : DiskFileOperations, DiskFileWatcher {
    private val fm = NSFileManager.defaultManager

    private fun resolve(path: String): String {
        val rel = path.removePrefix("/")
        return if (rel.isEmpty()) rootPath
        else (rootPath as NSString).stringByAppendingPathComponent(rel)
    }

    // ═══════════════════════════════════════════════════════════
    // DiskFileOperations
    // ═══════════════════════════════════════════════════════════

    @OptIn(ExperimentalForeignApi::class)
    override suspend fun createFile(path: String): Result<Unit> = runCatching {
        val full = resolve(path)
        val parent = (full as NSString).stringByDeletingLastPathComponent()
        fm.createDirectoryAtPath(parent, withIntermediateDirectories = true, attributes = null, error = null)
        if (!fm.createFileAtPath(full, contents = null, attributes = null)) {
            if (fm.fileExistsAtPath(full)) throw FsError.AlreadyExists(path)
            else throw FsError.Unknown("创建文件失败: $path")
        }
    }

    @OptIn(ExperimentalForeignApi::class)
    override suspend fun createDir(path: String): Result<Unit> = runCatching {
        val full = resolve(path)
        val ok = fm.createDirectoryAtPath(full, withIntermediateDirectories = true, attributes = null, error = null)
        if (!ok) throw FsError.Unknown("创建目录失败: $path")
    }

    @OptIn(ExperimentalForeignApi::class)
    override suspend fun readFile(path: String, offset: Long, length: Int): Result<ByteArray> = runCatching {
        val full = resolve(path)
        if (!fm.fileExistsAtPath(full)) throw FsError.NotFound(path)
        val handle = NSFileHandle.fileHandleForReadingAtPath(full) ?: throw FsError.NotFound(path)
        handle.seekToFileOffset(offset.toULong())
        val data = handle.readDataOfLength(length.toULong())
        handle.closeFile()
        data.toByteArray()
    }

    @OptIn(ExperimentalForeignApi::class, BetaInteropApi::class)
    override suspend fun writeFile(path: String, offset: Long, data: ByteArray): Result<Unit> = runCatching {
        val full = resolve(path)
        if (!fm.fileExistsAtPath(full)) throw FsError.NotFound(path)
        val handle = NSFileHandle.fileHandleForWritingAtPath(full) ?: throw FsError.NotFound(path)
        handle.seekToFileOffset(offset.toULong())
        val nsData = data.usePinned { pinned ->
            NSData.create(bytes = pinned.addressOf(0), length = data.size.toULong())
        }
        handle.writeData(nsData)
        handle.closeFile()
    }

    @OptIn(ExperimentalForeignApi::class)
    override suspend fun delete(path: String): Result<Unit> = runCatching {
        val full = resolve(path)
        if (!fm.fileExistsAtPath(full)) throw FsError.NotFound(path)
        val ok = fm.removeItemAtPath(full, error = null)
        if (!ok) throw FsError.Unknown("删除失败: $path")
    }

    @OptIn(ExperimentalForeignApi::class)
    override suspend fun list(path: String): Result<List<FsEntry>> = runCatching {
        val full = resolve(path)
        val contents = fm.contentsOfDirectoryAtPath(full, error = null)
            ?: throw FsError.NotFound(path)
        @Suppress("UNCHECKED_CAST")
        val names = contents as List<String>
        names.map { name ->
            val childPath = (full as NSString).stringByAppendingPathComponent(name)
            FsEntry(
                name = name,
                type = if (fm.fileExistsAtPath(childPath) && isDirectory(childPath)) FsType.DIRECTORY else FsType.FILE
            )
        }
    }

    @OptIn(ExperimentalForeignApi::class)
    override suspend fun stat(path: String): Result<FsMeta> = runCatching {
        val full = resolve(path)
        if (!fm.fileExistsAtPath(full)) throw FsError.NotFound(path)
        val attrs = fm.attributesOfItemAtPath(full, error = null) ?: emptyMap<Any?, Any?>()
        val isDir = isDirectory(full)
        val size = (attrs["NSFileSize"] as? Long) ?: 0L
        val modified = (attrs["NSFileModificationDate"] as? platform.Foundation.NSDate)
            ?.timeIntervalSince1970?.toLong()?.times(1000) ?: 0L
        FsMeta(
            path = path,
            type = if (isDir) FsType.DIRECTORY else FsType.FILE,
            size = if (isDir) 0L else size,
            createdAtMillis = modified,
            modifiedAtMillis = modified,
            permissions = FsPermissions.FULL
        )
    }

    override suspend fun exists(path: String): Boolean {
        return fm.fileExistsAtPath(resolve(path))
    }

    @OptIn(ExperimentalForeignApi::class)
    private fun isDirectory(fullPath: String): Boolean {
        val attrs = fm.attributesOfItemAtPath(fullPath, error = null) ?: return false
        return attrs["NSFileType"] == "NSFileTypeDirectory"
    }

    @OptIn(ExperimentalForeignApi::class)
    private fun NSData.toByteArray(): ByteArray {
        val size = this.length.toInt()
        if (size == 0) return ByteArray(0)
        val bytes = ByteArray(size)
        bytes.usePinned { pinned ->
            memcpy(pinned.addressOf(0), this.bytes, this.length)
        }
        return bytes
    }

    // ═══════════════════════════════════════════════════════════
    // DiskFileWatcher — 基于轮询对比快照
    //
    // iOS 的 DispatchSource.makeFileSystemObjectSource 只能监听
    // 单个目录的 vnode 事件，不支持递归子目录，且需要 open(2) fd。
    // 为简单可靠，采用定时轮询 + 快照对比方案。
    // ═══════════════════════════════════════════════════════════

    @Volatile
    private var watching = false

    @OptIn(ExperimentalForeignApi::class)
    override fun watchDisk(scope: CoroutineScope): Flow<DiskFileEvent> = callbackFlow {
        watching = true

        // 快照: relativePath -> (isDir, lastModifiedMillis)
        fun snapshot(): Map<String, Pair<Boolean, Long>> {
            val result = mutableMapOf<String, Pair<Boolean, Long>>()
            fun scan(dirPath: String, prefix: String) {
                val contents = fm.contentsOfDirectoryAtPath(dirPath, error = null) ?: return
                @Suppress("UNCHECKED_CAST")
                val names = contents as List<String>
                for (name in names) {
                    val fullChild = (dirPath as NSString).stringByAppendingPathComponent(name)
                    val relChild = "$prefix/$name"
                    val isDir = isDirectory(fullChild)
                    val attrs = fm.attributesOfItemAtPath(fullChild, error = null)
                    val modified = (attrs?.get("NSFileModificationDate") as? platform.Foundation.NSDate)
                        ?.timeIntervalSince1970?.toLong()?.times(1000) ?: 0L
                    result[relChild] = isDir to modified
                    if (isDir) scan(fullChild, relChild)
                }
            }
            scan(rootPath, "")
            return result
        }

        var prevSnapshot = snapshot()

        val pollJob = scope.launch {
            while (isActive && watching) {
                delay(1000) // 每秒轮询一次
                val currentSnapshot = snapshot()

                // 检测新增和修改
                for ((path, info) in currentSnapshot) {
                    val prev = prevSnapshot[path]
                    if (prev == null) {
                        trySend(DiskFileEvent(path, FsEventKind.CREATED))
                    } else if (prev.second != info.second) {
                        trySend(DiskFileEvent(path, FsEventKind.MODIFIED))
                    }
                }
                // 检测删除
                for (path in prevSnapshot.keys) {
                    if (path !in currentSnapshot) {
                        trySend(DiskFileEvent(path, FsEventKind.DELETED))
                    }
                }

                prevSnapshot = currentSnapshot
            }
        }

        awaitClose {
            watching = false
            pollJob.cancel()
        }
    }

    override fun stopWatching() {
        watching = false
    }
}
