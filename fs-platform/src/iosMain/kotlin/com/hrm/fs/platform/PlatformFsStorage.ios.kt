package com.hrm.fs.platform

import com.hrm.fs.api.FsStorage
import kotlinx.cinterop.BetaInteropApi
import kotlinx.cinterop.ExperimentalForeignApi
import kotlinx.cinterop.addressOf
import kotlinx.cinterop.usePinned
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.IO
import kotlinx.coroutines.withContext
import platform.Foundation.NSData
import platform.Foundation.NSFileHandle
import platform.Foundation.NSFileManager
import platform.Foundation.NSString
import platform.Foundation.closeFile
import platform.Foundation.create
import platform.Foundation.dataWithContentsOfFile
import platform.Foundation.fileHandleForWritingAtPath
import platform.Foundation.seekToEndOfFile
import platform.Foundation.stringByAppendingPathComponent
import platform.Foundation.writeData
import platform.Foundation.writeToFile
import platform.posix.memcpy

actual fun createPlatformFsStorage(dirPath: String): FsStorage = IosFsStorage(dirPath)

/**
 * iOS 平台基于 [NSFileManager] 的 [FsStorage] 实现。
 *
 * 每个 key 编码为文件名存储在 [dirPath] 下。
 */
@OptIn(ExperimentalForeignApi::class, BetaInteropApi::class)
internal class IosFsStorage(private val dirPath: String) : FsStorage {

    private val fm = NSFileManager.defaultManager

    init {
        if (!fm.fileExistsAtPath(dirPath)) {
            fm.createDirectoryAtPath(dirPath, withIntermediateDirectories = true, attributes = null, error = null)
        }
    }

    override suspend fun read(key: String): Result<ByteArray?> = withContext(Dispatchers.IO) {
        runCatching {
            val path = keyToPath(key)
            if (!fm.fileExistsAtPath(path)) return@runCatching null
            val data = NSData.dataWithContentsOfFile(path) ?: return@runCatching null
            data.toByteArray()
        }
    }

    override suspend fun write(key: String, data: ByteArray): Result<Unit> = withContext(Dispatchers.IO) {
        runCatching {
            val path = keyToPath(key)
            val tmpPath = "$path.tmp"
            val nsData = data.toNSData()
            nsData.writeToFile(tmpPath, atomically = true)
            // 原子替换
            if (fm.fileExistsAtPath(path)) {
                fm.removeItemAtPath(path, error = null)
            }
            fm.moveItemAtPath(tmpPath, toPath = path, error = null)
            Unit
        }
    }

    override suspend fun delete(key: String): Result<Unit> = withContext(Dispatchers.IO) {
        runCatching {
            val path = keyToPath(key)
            if (fm.fileExistsAtPath(path)) {
                fm.removeItemAtPath(path, error = null)
            }
            Unit
        }
    }

    override suspend fun append(key: String, data: ByteArray): Result<Unit> = withContext(Dispatchers.IO) {
        runCatching {
            val path = keyToPath(key)
            if (!fm.fileExistsAtPath(path)) {
                // 文件不存在，直接写入
                val nsData = data.toNSData()
                nsData.writeToFile(path, atomically = true)
            } else {
                // 文件存在，使用 NSFileHandle append
                val handle = NSFileHandle.fileHandleForWritingAtPath(path)
                if (handle != null) {
                    handle.seekToEndOfFile()
                    handle.writeData(data.toNSData())
                    handle.closeFile()
                } else {
                    // fallback: read + concat + write
                    val existing = NSData.dataWithContentsOfFile(path)?.toByteArray() ?: ByteArray(0)
                    (existing + data).toNSData().writeToFile(path, atomically = true)
                }
            }
            Unit
        }
    }

    @Suppress("CAST_NEVER_SUCCEEDS")
    private fun keyToPath(key: String): String {
        val safeName = key.map { ch ->
            if (ch.isLetterOrDigit() || ch == '-') ch.toString()
            else "_${ch.code.toString(16).uppercase().padStart(2, '0')}"
        }.joinToString("")
        return (dirPath as NSString).stringByAppendingPathComponent(safeName)
    }

    private fun NSData.toByteArray(): ByteArray {
        val size = length.toInt()
        if (size == 0) return ByteArray(0)
        val bytes = ByteArray(size)
        bytes.usePinned { pinned ->
            memcpy(pinned.addressOf(0), this@toByteArray.bytes, length)
        }
        return bytes
    }

    private fun ByteArray.toNSData(): NSData {
        if (isEmpty()) return NSData()
        return usePinned { pinned ->
            NSData.create(bytes = pinned.addressOf(0), length = size.toULong())
        }
    }
}
