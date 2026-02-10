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
    LOCKED,
    QUOTA_EXCEEDED,
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

// ─── 文件锁 ────────────────────────────────────────────────

/**
 * 文件锁类型（类 POSIX flock 语义）。
 *
 * - [SHARED]：共享锁（读锁），允许多个持有者并存，阻止独占锁。
 * - [EXCLUSIVE]：独占锁（写锁），同一时刻只能有一个持有者。
 */
enum class FileLockType { SHARED, EXCLUSIVE }

// ─── 句柄 ────────────────────────────────────────────────────

interface FileHandle {
    suspend fun readAt(offset: Long, length: Int): Result<ByteArray>
    suspend fun writeAt(offset: Long, data: ByteArray): Result<Unit>

    /**
     * 对文件加锁。如果锁不可用，会挂起直到获取成功。
     *
     * - [FileLockType.SHARED]：可与其他共享锁并存，但与独占锁互斥。
     * - [FileLockType.EXCLUSIVE]：与任何其他锁互斥。
     *
     * 同一个 [FileHandle] 重复调用 lock 会先释放旧锁再加新锁。
     */
    suspend fun lock(type: FileLockType = FileLockType.EXCLUSIVE): Result<Unit>

    /**
     * 尝试对文件加锁。如果锁不可用，立即返回失败（[FsError.Locked]）。
     */
    suspend fun tryLock(type: FileLockType = FileLockType.EXCLUSIVE): Result<Unit>

    /**
     * 释放当前句柄持有的文件锁。
     * 如果未持有锁，返回成功（幂等）。
     */
    suspend fun unlock(): Result<Unit>

    /**
     * 关闭句柄，自动释放持有的文件锁。
     */
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

    /**
     * 手动同步指定挂载点路径与真实磁盘的状态。
     *
     * 扫描磁盘目录与上次快照的差异，为新增/删除/修改的文件产生 [FsEvent]。
     * 适用于 [DiskFileWatcher] 不可用的降级场景，或者需要一次性全量同步的场合。
     *
     * @param path 虚拟路径（必须位于某个挂载点下，或为挂载点本身）
     * @return 本次 sync 检测到的事件列表
     */
    suspend fun sync(path: String): Result<List<FsEvent>>

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

    // ─── 可观测性 ─────────────────────────────────────────────

    /**
     * 获取当前文件系统的统计指标快照。
     */
    fun metrics(): FsMetrics

    /**
     * 重置所有统计指标。
     */
    fun resetMetrics()

    // ─── 配额 ────────────────────────────────────────────────

    /**
     * 获取当前虚拟文件系统的空间配额信息。
     * 仅统计内存文件树的使用量（挂载点使用真实磁盘，不计入配额）。
     */
    fun quotaInfo(): QuotaInfo

    // ─── 文件哈希 / 校验 ─────────────────────────────────────────

    /**
     * 计算文件的校验值。
     *
     * 支持内存文件和挂载点文件。对于大文件会读取全部内容后计算。
     *
     * @param path 虚拟路径
     * @param algorithm 校验算法（默认 SHA-256）
     * @return 十六进制小写字符串
     */
    suspend fun checksum(
        path: String,
        algorithm: ChecksumAlgorithm = ChecksumAlgorithm.SHA256
    ): Result<String>

    // ─── 版本历史 ───────────────────────────────────────────────

    /**
     * 获取文件的版本历史列表（最新在前）。
     *
     * 支持内存文件和挂载点文件。
     * 每次通过 [FileHandle.writeAt]、[writeAll]、[writeStream] 写入时自动保存历史版本。
     *
     * @param path 虚拟路径
     * @return 版本列表
     */
    suspend fun fileVersions(path: String): Result<List<FileVersion>>

    /**
     * 读取文件的某个历史版本内容。
     *
     * @param path 虚拟路径
     * @param versionId 版本 ID（从 [fileVersions] 获取）
     * @return 该版本的文件内容
     */
    suspend fun readVersion(path: String, versionId: String): Result<ByteArray>

    /**
     * 恢复文件到某个历史版本。
     *
     * 将指定版本的内容写回文件（同时当前内容也会作为新版本保存）。
     *
     * @param path 虚拟路径
     * @param versionId 要恢复到的版本 ID
     */
    suspend fun restoreVersion(path: String, versionId: String): Result<Unit>
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

/**
 * 磁盘文件变更事件（由 [DiskFileWatcher] 产生）。
 *
 * @param relativePath 相对于监听根目录的路径（如 "/docs/a.txt"）
 * @param kind 变更类型
 */
data class DiskFileEvent(
    val relativePath: String,
    val kind: FsEventKind
)

/**
 * 可选的磁盘文件变更监听接口。
 *
 * [DiskFileOperations] 的实现类如果同时实现了此接口，
 * 则在 [FileSystem.mount] 时会自动启动监听，
 * 将外部程序对磁盘目录的变更同步到虚拟文件系统的事件流中。
 *
 * 使用方式：平台层的 `DiskFileOperations` 实现同时实现此接口即可，
 * 无需额外配置。
 *
 * ```kotlin
 * class JvmDiskFileOperations(...) : DiskFileOperations, DiskFileWatcher {
 *     override fun watchDisk(scope: CoroutineScope): Flow<DiskFileEvent> { ... }
 *     override fun stopWatching() { ... }
 * }
 * ```
 */
interface DiskFileWatcher {
    /**
     * 启动对 [DiskFileOperations.rootPath] 目录的递归监听。
     *
     * 返回一个 [Flow]，在外部程序创建/修改/删除文件时发出 [DiskFileEvent]。
     * 该 Flow 应在给定的 [scope] 内工作，[scope] 取消时监听自动停止。
     *
     * @param scope 协程作用域，控制监听生命周期
     * @return 变更事件流
     */
    fun watchDisk(scope: kotlinx.coroutines.CoroutineScope): Flow<DiskFileEvent>

    /**
     * 手动停止监听。
     * 也可以通过取消 [watchDisk] 传入的 scope 来停止。
     */
    fun stopWatching()
}

// ─── 可观测性指标 ──────────────────────────────────────────

/**
 * 单个操作类型的统计指标。
 */
data class OpMetrics(
    /** 调用次数。 */
    val count: Long = 0,
    /** 成功次数。 */
    val successCount: Long = 0,
    /** 失败次数。 */
    val failureCount: Long = 0,
    /** 累计耗时（毫秒）。 */
    val totalTimeMs: Long = 0,
    /** 最大耗时（毫秒）。 */
    val maxTimeMs: Long = 0
) {
    /** 平均耗时（毫秒），若无调用则为 0。 */
    val avgTimeMs: Double get() = if (count > 0) totalTimeMs.toDouble() / count else 0.0
}

/**
 * 文件系统全局统计指标快照。
 *
 * 每个字段对应一种操作类型的 [OpMetrics]。
 */
data class FsMetrics(
    val createFile: OpMetrics = OpMetrics(),
    val createDir: OpMetrics = OpMetrics(),
    val delete: OpMetrics = OpMetrics(),
    val readDir: OpMetrics = OpMetrics(),
    val stat: OpMetrics = OpMetrics(),
    val open: OpMetrics = OpMetrics(),
    val readAll: OpMetrics = OpMetrics(),
    val writeAll: OpMetrics = OpMetrics(),
    val copy: OpMetrics = OpMetrics(),
    val move: OpMetrics = OpMetrics(),
    val mount: OpMetrics = OpMetrics(),
    val unmount: OpMetrics = OpMetrics(),
    val sync: OpMetrics = OpMetrics(),
    val setPermissions: OpMetrics = OpMetrics(),
    /** 总读取字节数。 */
    val totalBytesRead: Long = 0,
    /** 总写入字节数。 */
    val totalBytesWritten: Long = 0
)

/**
 * 虚拟文件系统的空间配额信息。
 *
 * @param quotaBytes 配额上限（字节），-1 表示无限制
 * @param usedBytes 当前已使用字节数（内存文件树）
 */
data class QuotaInfo(
    val quotaBytes: Long,
    val usedBytes: Long
) {
    /** 剩余可用字节数，无限制时返回 [Long.MAX_VALUE]。 */
    val availableBytes: Long
        get() = if (quotaBytes < 0) Long.MAX_VALUE else maxOf(0, quotaBytes - usedBytes)

    /** 是否启用了配额限制。 */
    val hasQuota: Boolean get() = quotaBytes >= 0
}

// ─── 校验算法 ────────────────────────────────────────────────

/**
 * 支持的文件校验算法。
 */
enum class ChecksumAlgorithm {
    /** CRC32 校验码（8 位十六进制）。 */
    CRC32,
    /** SHA-256 哈希（64 位十六进制）。 */
    SHA256
}

// ─── 文件版本 ────────────────────────────────────────────────

/**
 * 文件历史版本的元数据。
 *
 * @param versionId 版本唯一标识符
 * @param timestampMillis 版本保存时间戳（毫秒）
 * @param size 该版本的文件大小（字节）
 */
data class FileVersion(
    val versionId: String,
    val timestampMillis: Long,
    val size: Long
)

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
    class Locked(path: String) : FsError("文件已被锁定: $path", FsErrorCode.LOCKED)
    class QuotaExceeded(message: String) : FsError(message, FsErrorCode.QUOTA_EXCEEDED)
    class Unknown(message: String) : FsError(message, FsErrorCode.UNKNOWN)
}
