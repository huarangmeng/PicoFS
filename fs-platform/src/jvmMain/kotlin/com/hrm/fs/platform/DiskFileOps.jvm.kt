package com.hrm.fs.platform

import com.hrm.fs.api.DiskFileOperations
import com.hrm.fs.api.FsEntry
import com.hrm.fs.api.FsError
import com.hrm.fs.api.FsMeta
import com.hrm.fs.api.FsPermissions
import com.hrm.fs.api.FsType
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.io.File
import java.io.RandomAccessFile

actual fun createDiskFileOperations(rootPath: String): DiskFileOperations = JvmDiskFileOperations(rootPath)

internal class JvmDiskFileOperations(override val rootPath: String) : DiskFileOperations {

    private fun resolve(path: String): File {
        val rel = path.removePrefix("/")
        return if (rel.isEmpty()) File(rootPath) else File(rootPath, rel)
    }

    override suspend fun createFile(path: String): Result<Unit> = withContext(Dispatchers.IO) {
        runCatching {
            val file = resolve(path)
            file.parentFile?.mkdirs()
            if (!file.createNewFile() && !file.isFile) {
                throw FsError.AlreadyExists(path)
            }
        }
    }

    override suspend fun createDir(path: String): Result<Unit> = withContext(Dispatchers.IO) {
        runCatching {
            val dir = resolve(path)
            if (!dir.mkdirs() && !dir.isDirectory) {
                throw FsError.AlreadyExists(path)
            }
        }
    }

    override suspend fun readFile(path: String, offset: Long, length: Int): Result<ByteArray> =
        withContext(Dispatchers.IO) {
            runCatching {
                val file = resolve(path)
                if (!file.exists()) throw FsError.NotFound(path)
                if (!file.isFile) throw FsError.NotFile(path)
                RandomAccessFile(file, "r").use { raf ->
                    if (offset >= raf.length()) return@runCatching ByteArray(0)
                    raf.seek(offset)
                    val available = (raf.length() - offset).toInt()
                    val readLen = minOf(available, length)
                    val buf = ByteArray(readLen)
                    raf.readFully(buf)
                    buf
                }
            }
        }

    override suspend fun writeFile(path: String, offset: Long, data: ByteArray): Result<Unit> =
        withContext(Dispatchers.IO) {
            runCatching {
                val file = resolve(path)
                if (!file.exists()) throw FsError.NotFound(path)
                if (!file.isFile) throw FsError.NotFile(path)
                RandomAccessFile(file, "rw").use { raf ->
                    raf.seek(offset)
                    raf.write(data)
                }
            }
        }

    override suspend fun delete(path: String): Result<Unit> = withContext(Dispatchers.IO) {
        runCatching {
            val file = resolve(path)
            if (!file.exists()) throw FsError.NotFound(path)
            if (file.isDirectory && file.listFiles()?.isNotEmpty() == true) {
                throw FsError.PermissionDenied(path)
            }
            if (!file.delete()) throw FsError.Unknown("删除失败: $path")
        }
    }

    override suspend fun list(path: String): Result<List<FsEntry>> = withContext(Dispatchers.IO) {
        runCatching {
            val dir = resolve(path)
            if (!dir.exists()) throw FsError.NotFound(path)
            if (!dir.isDirectory) throw FsError.NotDirectory(path)
            dir.listFiles().orEmpty().map { f ->
                FsEntry(
                    name = f.name,
                    type = if (f.isDirectory) FsType.DIRECTORY else FsType.FILE
                )
            }
        }
    }

    override suspend fun stat(path: String): Result<FsMeta> = withContext(Dispatchers.IO) {
        runCatching {
            val file = resolve(path)
            if (!file.exists()) throw FsError.NotFound(path)
            FsMeta(
                path = path,
                type = if (file.isDirectory) FsType.DIRECTORY else FsType.FILE,
                size = if (file.isFile) file.length() else 0L,
                createdAtMillis = file.lastModified(),
                modifiedAtMillis = file.lastModified(),
                permissions = FsPermissions(
                    read = file.canRead(),
                    write = file.canWrite(),
                    execute = file.canExecute()
                )
            )
        }
    }

    override suspend fun exists(path: String): Boolean = withContext(Dispatchers.IO) {
        resolve(path).exists()
    }
}
