package com.hrm.fs.api

import kotlinx.coroutines.flow.Flow

enum class FsType { FILE, DIRECTORY }

enum class OpenMode { READ, WRITE, READ_WRITE }

data class FsConfig(
    val storage: FsStorage? = null
)

enum class FsErrorCode {
    NOT_FOUND,
    ALREADY_EXISTS,
    NOT_DIRECTORY,
    NOT_FILE,
    PERMISSION_DENIED,
    INVALID_PATH,
    NOT_MOUNTED,
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

/**
 * 挂载点权限配置。
 */
data class MountOptions(
    val readOnly: Boolean = false
)

/**
 * 从持久化恢复但尚未被重新 mount 的挂载点信息。
 */
data class PendingMount(
    val virtualPath: String,
    val rootPath: String,
    val readOnly: Boolean = false
)

// ─── 文件监听事件 ────────────────────────────────────────────

enum class FsEventKind { CREATED, MODIFIED, DELETED }

data class FsEvent(
    val path: String,
    val kind: FsEventKind
)

// ─── 句柄 ────────────────────────────────────────────────────

interface FileHandle {
    suspend fun readAt(offset: Long, length: Int): Result<ByteArray>
    suspend fun writeAt(offset: Long, data: ByteArray): Result<Unit>
    suspend fun close(): Result<Unit>
}

/**
 * 虚拟文件系统接口。
 *
 * 上层永远只操作虚拟路径（如 `/docs/hello.txt`），
 * 可以通过 [mount] 将虚拟路径挂载到真实磁盘目录，
 * 挂载点下的操作会自动代理到真实文件系统。
 * 未挂载的路径使用纯内存存储。
 */
interface FileSystem {
    // ─── 基础 CRUD ───────────────────────────────────────────

    suspend fun createFile(path: String): Result<Unit>
    suspend fun createDir(path: String): Result<Unit>
    suspend fun open(path: String, mode: OpenMode): Result<FileHandle>
    suspend fun readDir(path: String): Result<List<FsEntry>>
    suspend fun stat(path: String): Result<FsMeta>
    suspend fun delete(path: String): Result<Unit>
    suspend fun setPermissions(path: String, permissions: FsPermissions): Result<Unit>

    // ─── 递归操作 ────────────────────────────────────────────

    /** 递归创建目录（类似 mkdir -p）。 */
    suspend fun createDirRecursive(path: String): Result<Unit>

    /** 递归删除目录及其所有内容（类似 rm -rf）。 */
    suspend fun deleteRecursive(path: String): Result<Unit>

    // ─── 便捷读写 ────────────────────────────────────────────

    /** 一次性读取文件全部内容。 */
    suspend fun readAll(path: String): Result<ByteArray>

    /** 一次性写入全部内容（覆盖式）。如果文件不存在则自动创建。 */
    suspend fun writeAll(path: String, data: ByteArray): Result<Unit>

    // ─── copy / move / rename ────────────────────────────────

    /** 复制文件或目录（目录时递归复制）。支持跨挂载点。 */
    suspend fun copy(srcPath: String, dstPath: String): Result<Unit>

    /** 移动（重命名）文件或目录。等价于 copy + deleteRecursive。 */
    suspend fun move(srcPath: String, dstPath: String): Result<Unit>

    /** rename 是 move 的别名。 */
    suspend fun rename(srcPath: String, dstPath: String): Result<Unit> = move(srcPath, dstPath)

    // ─── 挂载 ────────────────────────────────────────────────

    /**
     * 将虚拟路径 [virtualPath] 挂载到真实磁盘目录 [diskOps]。
     * 挂载后，对 [virtualPath] 及其子路径的操作将代理到 [diskOps]。
     *
     * @param virtualPath 虚拟挂载点（如 "/data"）
     * @param diskOps 真实磁盘操作接口
     * @param options 挂载选项（如只读）
     */
    suspend fun mount(
        virtualPath: String,
        diskOps: DiskFileOperations,
        options: MountOptions = MountOptions()
    ): Result<Unit>

    /** 卸载虚拟路径 [virtualPath] 上的挂载。 */
    suspend fun unmount(virtualPath: String): Result<Unit>

    /** 列出当前所有挂载点。 */
    suspend fun listMounts(): List<String>

    /**
     * 返回从持久化恢复但尚未被重新 [mount] 的挂载点信息。
     *
     * 每个 [PendingMount] 包含曾经的虚拟路径、磁盘根路径和只读标记，
     * 外部可据此用平台对应的 [DiskFileOperations] 重新调用 [mount] 恢复。
     * 成功 mount 后该条目会自动移除。
     */
    suspend fun pendingMounts(): List<PendingMount>

    // ─── 监听 ────────────────────────────────────────────────

    /**
     * 监听虚拟路径变化事件。
     * 返回的 Flow 会在文件被创建/修改/删除时发出 [FsEvent]。
     *
     * @param path 监听的虚拟路径（监听该路径及其子路径的事件）
     */
    fun watch(path: String): Flow<FsEvent>

    // ─── 流式读写 ────────────────────────────────────────────

    /**
     * 以 Flow 方式流式读取文件内容，适合大文件。
     *
     * @param path 虚拟路径
     * @param chunkSize 每个 chunk 的字节数
     */
    fun readStream(path: String, chunkSize: Int = 8192): Flow<ByteArray>

    /**
     * 以 Flow 方式流式写入文件内容。
     * 会先清空文件再逐 chunk 追加。
     *
     * @param path 虚拟路径
     * @param dataFlow 数据流
     */
    suspend fun writeStream(path: String, dataFlow: Flow<ByteArray>): Result<Unit>
}

/**
 * 真实磁盘文件操作接口。
 *
 * 由平台层（Android/iOS/JVM）各自实现，提供对真实文件系统的操作能力。
 * 所有路径参数均为真实磁盘路径（非虚拟路径）。
 */
interface DiskFileOperations {
    val rootPath: String

    suspend fun createFile(path: String): Result<Unit>
    suspend fun createDir(path: String): Result<Unit>
    suspend fun readFile(path: String, offset: Long, length: Int): Result<ByteArray>
    suspend fun writeFile(path: String, offset: Long, data: ByteArray): Result<Unit>
    suspend fun delete(path: String): Result<Unit>
    suspend fun list(path: String): Result<List<FsEntry>>
    suspend fun stat(path: String): Result<FsMeta>
    suspend fun exists(path: String): Boolean
}

interface FsStorage {
    suspend fun read(key: String): Result<ByteArray?>
    suspend fun write(key: String, data: ByteArray): Result<Unit>
    suspend fun delete(key: String): Result<Unit>
}

class InMemoryFsStorage : FsStorage {
    private val data: MutableMap<String, ByteArray> = LinkedHashMap()

    override suspend fun read(key: String): Result<ByteArray?> = Result.success(data[key])

    override suspend fun write(key: String, data: ByteArray): Result<Unit> {
        this.data[key] = data
        return Result.success(Unit)
    }

    override suspend fun delete(key: String): Result<Unit> {
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
    class NotMounted(path: String) : FsError("未挂载: $path", FsErrorCode.NOT_MOUNTED)
    class Unknown(message: String) : FsError(message, FsErrorCode.UNKNOWN)
}
