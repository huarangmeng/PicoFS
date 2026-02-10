package com.hrm.fs.platform

import com.hrm.fs.api.FsStorage
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.File

actual fun createPlatformFsStorage(dirPath: String): FsStorage = FileFsStorage(dirPath)

/**
 * 基于 [java.io.File] 的 [FsStorage] 实现。
 *
 * 每个 key 编码为文件名存储在 [dirPath] 下。
 */
internal class FileFsStorage(private val dirPath: String) : FsStorage {

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
            // 先写临时文件再原子重命名，保证写入的原子性
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

    /**
     * 将 key 编码为安全的文件名。
     * 使用 URL 编码风格：将非字母数字字符替换为 _XX 形式。
     */
    private fun keyToFile(key: String): File {
        val safeName = key.map { ch ->
            if (ch.isLetterOrDigit() || ch == '-') ch.toString()
            else "_${ch.code.toString(16).uppercase().padStart(2, '0')}"
        }.joinToString("")
        return File(dir, safeName)
    }
}
