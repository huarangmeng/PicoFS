package com.hrm.fs.platform

import com.hrm.fs.api.DiskFileOperations
import com.hrm.fs.api.FsEntry
import com.hrm.fs.api.FsError
import com.hrm.fs.api.FsMeta
import com.hrm.fs.api.FsPermissions
import com.hrm.fs.api.FsType
import kotlinx.cinterop.BetaInteropApi
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.usePinned
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
import platform.Foundation.writeData
import platform.posix.memcpy

actual fun createDiskFileOperations(rootPath: String): DiskFileOperations = IosDiskFileOperations(rootPath)

internal class IosDiskFileOperations(override val rootPath: String) : DiskFileOperations {
    private val fm = NSFileManager.defaultManager

    private fun resolve(path: String): String {
        val rel = path.removePrefix("/")
        return if (rel.isEmpty()) rootPath
        else (rootPath as NSString).stringByAppendingPathComponent(rel)
    }

    override suspend fun createFile(path: String): Result<Unit> = runCatching {
        val full = resolve(path)
        val parent = (full as NSString).stringByDeletingLastPathComponent
        fm.createDirectoryAtPath(parent, withIntermediateDirectories = true, attributes = null, error = null)
        if (!fm.createFileAtPath(full, contents = null, attributes = null)) {
            if (fm.fileExistsAtPath(full)) throw FsError.AlreadyExists(path)
            else throw FsError.Unknown("创建文件失败: $path")
        }
    }

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

    override suspend fun delete(path: String): Result<Unit> = runCatching {
        val full = resolve(path)
        if (!fm.fileExistsAtPath(full)) throw FsError.NotFound(path)
        val ok = fm.removeItemAtPath(full, error = null)
        if (!ok) throw FsError.Unknown("删除失败: $path")
    }

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
}
