package com.hrm.fs.api

enum class FsType { FILE, DIRECTORY }

enum class OpenMode { READ, WRITE, READ_WRITE }

enum class FsErrorCode {
    NOT_FOUND,
    ALREADY_EXISTS,
    NOT_DIRECTORY,
    NOT_FILE,
    PERMISSION_DENIED,
    INVALID_PATH,
    UNKNOWN
}

data class FsPermissions(
    val read: Boolean = true,
    val write: Boolean = true,
    val execute: Boolean = true
) {
    fun canRead(): Boolean = read
    fun canWrite(): Boolean = write
    fun canExecute(): Boolean = execute

    companion object {
        val FULL = FsPermissions(true, true, true)
        val READ_ONLY = FsPermissions(read = true, write = false, execute = true)
    }
}

data class FsMeta(
    val path: String,
    val type: FsType,
    val size: Long,
    val createdAtMillis: Long,
    val modifiedAtMillis: Long,
    val permissions: FsPermissions
)

data class FsEntry(
    val name: String,
    val type: FsType
)

interface FileHandle {
    fun readAt(offset: Long, length: Int): Result<ByteArray>
    fun writeAt(offset: Long, data: ByteArray): Result<Unit>
    fun close(): Result<Unit>
}

interface FileSystem {
    fun createFile(path: String): Result<Unit>
    fun createDir(path: String): Result<Unit>
    fun open(path: String, mode: OpenMode): Result<FileHandle>
    fun readDir(path: String): Result<List<FsEntry>>
    fun stat(path: String): Result<FsMeta>
    fun delete(path: String): Result<Unit>
    fun setPermissions(path: String, permissions: FsPermissions): Result<Unit>
}

interface FsStorage {
    fun read(key: String): Result<ByteArray?>
    fun write(key: String, data: ByteArray): Result<Unit>
    fun delete(key: String): Result<Unit>
}

class InMemoryFsStorage : FsStorage {
    private val data: MutableMap<String, ByteArray> = LinkedHashMap()

    override fun read(key: String): Result<ByteArray?> = Result.success(data[key])

    override fun write(key: String, data: ByteArray): Result<Unit> {
        this.data[key] = data
        return Result.success(Unit)
    }

    override fun delete(key: String): Result<Unit> {
        data.remove(key)
        return Result.success(Unit)
    }
}

sealed class FsError(message: String, val code: FsErrorCode) : Exception(message) {
    class NotFound(path: String) : FsError("路径不存在: $path", FsErrorCode.NOT_FOUND)
    class AlreadyExists(path: String) : FsError("已存在: $path", FsErrorCode.ALREADY_EXISTS)
    class NotDirectory(path: String) : FsError("非目录: $path", FsErrorCode.NOT_DIRECTORY)
    class NotFile(path: String) : FsError("非文件: $path", FsErrorCode.NOT_FILE)
    class PermissionDenied(path: String) : FsError("无权限: $path", FsErrorCode.PERMISSION_DENIED)
    class InvalidPath(path: String) : FsError("非法路径: $path", FsErrorCode.INVALID_PATH)
    class Unknown(message: String) : FsError(message, FsErrorCode.UNKNOWN)
}
