package com.hrm.fs.platform

import com.hrm.fs.api.ArchiveCodec
import com.hrm.fs.api.ArchiveEntry
import com.hrm.fs.api.ArchiveFormat
import com.hrm.fs.api.DiskFileEvent
import com.hrm.fs.api.DiskFileOperations
import com.hrm.fs.api.DiskFileWatcher
import com.hrm.fs.api.FsEntry
import com.hrm.fs.api.FsError
import com.hrm.fs.api.FsEventKind
import com.hrm.fs.api.FsMeta
import com.hrm.fs.api.FsPermissions
import com.hrm.fs.api.FsType
import com.hrm.fs.api.log.FLog
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
import platform.Foundation.readDataToEndOfFile
import platform.Foundation.seekToFileOffset
import platform.Foundation.stringByAppendingPathComponent
import platform.Foundation.stringByDeletingLastPathComponent
import platform.Foundation.timeIntervalSince1970
import platform.Foundation.writeData
import platform.posix.memcpy
import kotlin.concurrent.Volatile

actual fun createDiskFileOperations(rootPath: String): DiskFileOperations = IosDiskFileOperations(rootPath)

internal class IosDiskFileOperations(override val rootPath: String) : DiskFileOperations, DiskFileWatcher {

    companion object {
        private const val TAG = "IosDiskOps"
    }

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

    // ═══════════════════════════════════════════════════════════
    // xattr — 基于 cinterop 绑定 <sys/xattr.h>
    // ═══════════════════════════════════════════════════════════

    @OptIn(ExperimentalForeignApi::class)
    override suspend fun setXattr(path: String, name: String, value: ByteArray): Result<Unit> = runCatching {
        val full = resolve(path)
        if (!fm.fileExistsAtPath(full)) throw FsError.NotFound(path)
        val result = if (value.isEmpty()) {
            xattr.setxattr(full, name, null, 0u, 0u, 0)
        } else {
            value.usePinned { pinned ->
                xattr.setxattr(full, name, pinned.addressOf(0), value.size.toULong(), 0u, 0)
            }
        }
        if (result != 0) throw FsError.PermissionDenied("setxattr failed for '$name' on $path (errno=${platform.posix.errno})")
    }

    @OptIn(ExperimentalForeignApi::class)
    override suspend fun getXattr(path: String, name: String): Result<ByteArray> = runCatching {
        val full = resolve(path)
        if (!fm.fileExistsAtPath(full)) throw FsError.NotFound(path)
        val size = xattr.getxattr(full, name, null, 0u, 0u, 0)
        if (size < 0) throw FsError.NotFound("xattr '$name' on $path")
        if (size == 0L) return@runCatching ByteArray(0)
        val bytes = ByteArray(size.toInt())
        val read = bytes.usePinned { pinned ->
            xattr.getxattr(full, name, pinned.addressOf(0), size.toULong(), 0u, 0)
        }
        if (read < 0) throw FsError.NotFound("xattr '$name' on $path")
        bytes
    }

    @OptIn(ExperimentalForeignApi::class)
    override suspend fun removeXattr(path: String, name: String): Result<Unit> = runCatching {
        val full = resolve(path)
        if (!fm.fileExistsAtPath(full)) throw FsError.NotFound(path)
        val result = xattr.removexattr(full, name, 0)
        if (result != 0) throw FsError.NotFound("xattr '$name' on $path")
    }

    @OptIn(ExperimentalForeignApi::class)
    override suspend fun listXattrs(path: String): Result<List<String>> = runCatching {
        val full = resolve(path)
        if (!fm.fileExistsAtPath(full)) throw FsError.NotFound(path)
        val bufSize = xattr.listxattr(full, null, 0u, 0)
        if (bufSize < 0) throw FsError.PermissionDenied("listxattr failed on $path")
        if (bufSize == 0L) return@runCatching emptyList()
        val buf = ByteArray(bufSize.toInt())
        val read = buf.usePinned { pinned ->
            xattr.listxattr(full, pinned.addressOf(0), bufSize.toULong(), 0)
        }
        if (read < 0) throw FsError.PermissionDenied("listxattr failed on $path")
        buf.decodeToString().split('\u0000').filter { it.isNotEmpty() }
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
    // Archive — 基于 ArchiveCodec（纯 Kotlin，无平台依赖）
    // ═══════════════════════════════════════════════════════════

    @OptIn(ExperimentalForeignApi::class, BetaInteropApi::class)
    override suspend fun compress(
        sourcePaths: List<String>,
        archivePath: String,
        format: ArchiveFormat
    ): Result<Unit> = runCatching {
        val items = mutableListOf<ArchiveCodec.ArchiveItem>()
        for (srcPath in sourcePaths) {
            val full = resolve(srcPath)
            if (!fm.fileExistsAtPath(full)) throw FsError.NotFound(srcPath)
            collectArchiveItems(full, srcPath.removePrefix("/").substringAfterLast('/').ifEmpty { srcPath.removePrefix("/") }, items)
        }
        val archiveData = when (format) {
            ArchiveFormat.ZIP -> ArchiveCodec.zipEncode(items)
            ArchiveFormat.TAR -> ArchiveCodec.tarEncode(items)
        }
        val archiveFull = resolve(archivePath)
        val parent = (archiveFull as NSString).stringByDeletingLastPathComponent()
        fm.createDirectoryAtPath(parent, withIntermediateDirectories = true, attributes = null, error = null)
        val nsData = archiveData.usePinned { pinned ->
            NSData.create(bytes = pinned.addressOf(0), length = archiveData.size.toULong())
        }
        if (!fm.createFileAtPath(archiveFull, contents = nsData, attributes = null)) {
            if (fm.fileExistsAtPath(archiveFull)) {
                val handle = NSFileHandle.fileHandleForWritingAtPath(archiveFull)
                    ?: throw FsError.Unknown("无法写入归档文件: $archivePath")
                handle.seekToFileOffset(0u)
                handle.writeData(nsData)
                handle.closeFile()
            } else {
                throw FsError.Unknown("创建归档文件失败: $archivePath")
            }
        }
    }

    @OptIn(ExperimentalForeignApi::class)
    override suspend fun extract(
        archivePath: String,
        targetDir: String,
        format: ArchiveFormat?
    ): Result<Unit> = runCatching {
        val archiveFull = resolve(archivePath)
        if (!fm.fileExistsAtPath(archiveFull)) throw FsError.NotFound(archivePath)
        val handle = NSFileHandle.fileHandleForReadingAtPath(archiveFull)
            ?: throw FsError.NotFound(archivePath)
        val nsData = handle.readDataToEndOfFile()
        handle.closeFile()
        val archiveData = nsData.toByteArray()

        val resolvedFormat = format ?: ArchiveCodec.detectFormat(archiveData)
            ?: throw FsError.Unknown("无法检测归档格式")

        val targetFull = resolve(targetDir)
        fm.createDirectoryAtPath(targetFull, withIntermediateDirectories = true, attributes = null, error = null)

        when (resolvedFormat) {
            ArchiveFormat.ZIP -> {
                for (entry in ArchiveCodec.zipDecode(archiveData)) {
                    val entryFull = (targetFull as NSString).stringByAppendingPathComponent(entry.path)
                    if (entry.isDirectory) {
                        fm.createDirectoryAtPath(entryFull, withIntermediateDirectories = true, attributes = null, error = null)
                    } else {
                        val parentDir = (entryFull as NSString).stringByDeletingLastPathComponent()
                        fm.createDirectoryAtPath(parentDir, withIntermediateDirectories = true, attributes = null, error = null)
                        val fileNsData = entry.data.usePinned { pinned ->
                            NSData.create(bytes = pinned.addressOf(0), length = entry.data.size.toULong())
                        }
                        fm.createFileAtPath(entryFull, contents = fileNsData, attributes = null)
                    }
                }
            }
            ArchiveFormat.TAR -> {
                for (entry in ArchiveCodec.tarDecode(archiveData)) {
                    val entryFull = (targetFull as NSString).stringByAppendingPathComponent(entry.path)
                    if (entry.isDirectory) {
                        fm.createDirectoryAtPath(entryFull, withIntermediateDirectories = true, attributes = null, error = null)
                    } else {
                        val parentDir = (entryFull as NSString).stringByDeletingLastPathComponent()
                        fm.createDirectoryAtPath(parentDir, withIntermediateDirectories = true, attributes = null, error = null)
                        val fileNsData = entry.data.usePinned { pinned ->
                            NSData.create(bytes = pinned.addressOf(0), length = entry.data.size.toULong())
                        }
                        fm.createFileAtPath(entryFull, contents = fileNsData, attributes = null)
                    }
                }
            }
        }
    }

    @OptIn(ExperimentalForeignApi::class)
    override suspend fun listArchive(
        archivePath: String,
        format: ArchiveFormat?
    ): Result<List<ArchiveEntry>> = runCatching {
        val archiveFull = resolve(archivePath)
        if (!fm.fileExistsAtPath(archiveFull)) throw FsError.NotFound(archivePath)
        val handle = NSFileHandle.fileHandleForReadingAtPath(archiveFull)
            ?: throw FsError.NotFound(archivePath)
        val nsData = handle.readDataToEndOfFile()
        handle.closeFile()
        val archiveData = nsData.toByteArray()

        val resolvedFormat = format ?: ArchiveCodec.detectFormat(archiveData)
            ?: throw FsError.Unknown("无法检测归档格式")

        when (resolvedFormat) {
            ArchiveFormat.ZIP -> ArchiveCodec.zipList(archiveData)
            ArchiveFormat.TAR -> ArchiveCodec.tarList(archiveData)
        }
    }

    @OptIn(ExperimentalForeignApi::class)
    private fun collectArchiveItems(
        fullPath: String,
        relativePath: String,
        items: MutableList<ArchiveCodec.ArchiveItem>
    ) {
        if (isDirectory(fullPath)) {
            val attrs = fm.attributesOfItemAtPath(fullPath, error = null) ?: emptyMap<Any?, Any?>()
            val modified = (attrs["NSFileModificationDate"] as? platform.Foundation.NSDate)
                ?.timeIntervalSince1970?.toLong()?.times(1000) ?: 0L
            items.add(ArchiveCodec.ArchiveItem(relativePath, FsType.DIRECTORY, ByteArray(0), modified))
            val contents = fm.contentsOfDirectoryAtPath(fullPath, error = null)
            @Suppress("UNCHECKED_CAST")
            val names = (contents as? List<String>) ?: emptyList()
            for (name in names) {
                val childFull = (fullPath as NSString).stringByAppendingPathComponent(name)
                val childRel = "$relativePath/$name"
                collectArchiveItems(childFull, childRel, items)
            }
        } else {
            val attrs = fm.attributesOfItemAtPath(fullPath, error = null) ?: emptyMap<Any?, Any?>()
            val size = (attrs["NSFileSize"] as? Long) ?: 0L
            val modified = (attrs["NSFileModificationDate"] as? platform.Foundation.NSDate)
                ?.timeIntervalSince1970?.toLong()?.times(1000) ?: 0L
            val handle = NSFileHandle.fileHandleForReadingAtPath(fullPath)
            val data = if (handle != null) {
                val nsData = handle.readDataOfLength(size.toULong())
                handle.closeFile()
                nsData.toByteArray()
            } else ByteArray(0)
            items.add(ArchiveCodec.ArchiveItem(relativePath, FsType.FILE, data, modified))
        }
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
