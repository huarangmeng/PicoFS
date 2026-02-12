package com.hrm.fs.platform

import com.hrm.fs.api.FsStorage
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.File

actual fun createPlatformFsStorage(dirPath: String): FsStorage = AndroidFileFsStorage(dirPath)

/**
 * Android 平台基于 [java.io.File] 的 [FsStorage] 实现。
 *
 * 每个 key 编码为文件名存储在 [dirPath] 下。
 */
internal class AndroidFileFsStorage(private val dirPath: String) : FsStorage {

    private val dir = File(dirPath).also { it.mkdirs() }

    override suspend fun read(key: String): Result<ByteArray?> = withContext(Dispatchers.IO) {
        runCatching {
            val file = keyToFile(key)
            if (file.exists()) file.readBytes() else null
        }
    }

    override suspend fun write(key: String, data: ByteArray): Result<Unit> = withContext(Dispatchers.IO) {
        runCatching {
            val file = keyToFile(key)
            val tmp = File(dir, "${file.name}.tmp")
            tmp.writeBytes(data)
            tmp.renameTo(file)
            Unit
        }
    }

    override suspend fun delete(key: String): Result<Unit> = withContext(Dispatchers.IO) {
        runCatching {
            keyToFile(key).delete()
            Unit
        }
    }

    override suspend fun append(key: String, data: ByteArray): Result<Unit> = withContext(Dispatchers.IO) {
        runCatching {
            val file = keyToFile(key)
            java.io.FileOutputStream(file, true).use { it.write(data) }
            Unit
        }
    }

    private fun keyToFile(key: String): File {
        val safeName = key.map { ch ->
            if (ch.isLetterOrDigit() || ch == '-') ch.toString()
            else "_${ch.code.toString(16).uppercase().padStart(2, '0')}"
        }.joinToString("")
        return File(dir, safeName)
    }
}
