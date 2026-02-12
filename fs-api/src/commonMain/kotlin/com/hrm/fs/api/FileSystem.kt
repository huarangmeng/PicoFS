package com.hrm.fs.api

import com.hrm.fs.api.FileLockType.EXCLUSIVE
import com.hrm.fs.api.FileLockType.SHARED
import kotlinx.coroutines.flow.Flow

enum class FsType { FILE, DIRECTORY, SYMLINK }

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
    SYMLINK_LOOP,
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
    val permissions: FsPermissions,
    /** 符号链接的目标路径，仅当 [type] 为 [FsType.SYMLINK] 时有值。 */
    val target: String? = null
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

// ═══════════════════════════════════════════════════════════════
// 核心文件系统接口
// ═══════════════════════════════════════════════════════════════

/**
 * 虚拟文件系统核心接口。
 *
 * 包含最常用的文件/目录 CRUD 操作。
 * 扩展能力通过子接口属性访问：
 *
 * ```kotlin
 * val fs = createFileSystem()
 *
 * // 核心 CRUD —— 直接调用
 * fs.createFile("/hello.txt")
 * fs.writeAll("/hello.txt", "world".encodeToByteArray())
 *
 * // 扩展能力 —— 通过属性访问
 * fs.mounts.mount("/disk", diskOps)
 * fs.versions.list("/hello.txt")
 * fs.xattr.set("/hello.txt", "tag", "important".encodeToByteArray())
 * fs.search.find(SearchQuery(namePattern = "*.txt"))
 * ```
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

    // ─── 扩展能力 ────────────────────────────────────────────

    /** 挂载管理：mount / unmount / sync / listMounts / pendingMounts */
    val mounts: FsMounts

    /** 版本历史：fileVersions / readVersion / restoreVersion */
    val versions: FsVersions

    /** 搜索查找：find */
    val search: FsSearch

    /** 可观测性：watch / metrics / resetMetrics / quotaInfo */
    val observe: FsObserve

    /** 流式读写：readStream / writeStream */
    val streams: FsStreams

    /** 文件校验：checksum */
    val checksum: FsChecksum

    /** 扩展属性：set / get / remove / list */
    val xattr: FsXattr

    /** 符号链接：create / readLink */
    val symlinks: FsSymlinks

    /** 归档压缩：compress / extract / list */
    val archive: FsArchive

    /** 回收站：moveToTrash / restore / list / purge / purgeAll */
    val trash: FsTrash
}

// ═══════════════════════════════════════════════════════════════
// 扩展能力子接口
// ═══════════════════════════════════════════════════════════════

/**
 * 挂载管理。
 */
interface FsMounts {
    /**
     * 将虚拟路径 [virtualPath] 挂载到真实磁盘目录 [diskOps]。
     * 挂载后，对 [virtualPath] 及其子路径的操作将代理到 [diskOps]。
     */
    suspend fun mount(
        virtualPath: String,
        diskOps: DiskFileOperations,
        options: MountOptions = MountOptions()
    ): Result<Unit>

    /** 卸载虚拟路径 [virtualPath] 上的挂载。 */
    suspend fun unmount(virtualPath: String): Result<Unit>

    /** 列出当前所有挂载点。 */
    suspend fun list(): List<String>

    /**
     * 返回从持久化恢复但尚未被重新 [mount] 的挂载点信息。
     */
    suspend fun pending(): List<PendingMount>

    /**
     * 手动同步指定挂载点路径与真实磁盘的状态。
     *
     * @param path 虚拟路径（必须位于某个挂载点下，或为挂载点本身）
     * @return 本次 sync 检测到的事件列表
     */
    suspend fun sync(path: String): Result<List<FsEvent>>
}

/**
 * 版本历史管理。
 */
interface FsVersions {
    /**
     * 获取文件的版本历史列表（最新在前）。
     */
    suspend fun list(path: String): Result<List<FileVersion>>

    /**
     * 读取文件的某个历史版本内容。
     */
    suspend fun read(path: String, versionId: String): Result<ByteArray>

    /**
     * 恢复文件到某个历史版本。
     */
    suspend fun restore(path: String, versionId: String): Result<Unit>
}

/**
 * 搜索查找。
 */
interface FsSearch {
    /**
     * 在指定目录下递归搜索文件和目录。
     *
     * 支持按文件名模式匹配（类 `find`）和按文件内容匹配（类 `grep`）。
     */
    suspend fun find(query: SearchQuery): Result<List<SearchResult>>
}

/**
 * 可观测性：事件监听 + 指标 + 配额。
 */
interface FsObserve {
    /**
     * 监听虚拟路径变化事件。
     */
    fun watch(path: String): Flow<FsEvent>

    /** 获取当前文件系统的统计指标快照。 */
    fun metrics(): FsMetrics

    /** 重置所有统计指标。 */
    fun resetMetrics()

    /** 获取当前虚拟文件系统的空间配额信息。 */
    fun quotaInfo(): QuotaInfo
}

/**
 * 流式读写（适合大文件）。
 */
interface FsStreams {
    /**
     * 以 Flow 方式流式读取文件内容。
     *
     * @param chunkSize 每个 chunk 的字节数
     */
    fun read(path: String, chunkSize: Int = 8192): Flow<ByteArray>

    /**
     * 以 Flow 方式流式写入文件内容。
     * 会先清空文件再逐 chunk 追加。
     */
    suspend fun write(path: String, dataFlow: Flow<ByteArray>): Result<Unit>
}

/**
 * 文件校验。
 */
interface FsChecksum {
    /**
     * 计算文件的校验值。
     *
     * @param algorithm 校验算法（默认 SHA-256）
     * @return 十六进制小写字符串
     */
    suspend fun compute(
        path: String,
        algorithm: ChecksumAlgorithm = ChecksumAlgorithm.SHA256
    ): Result<String>
}

/**
 * 扩展属性（xattr）。
 */
interface FsXattr {
    /** 设置扩展属性。如果已存在则覆盖。 */
    suspend fun set(path: String, name: String, value: ByteArray): Result<Unit>

    /** 获取扩展属性值。属性不存在时返回 [FsError.NotFound]。 */
    suspend fun get(path: String, name: String): Result<ByteArray>

    /** 删除扩展属性。 */
    suspend fun remove(path: String, name: String): Result<Unit>

    /** 列出所有扩展属性名。 */
    suspend fun list(path: String): Result<List<String>>
}

/**
 * 符号链接。
 */
interface FsSymlinks {
    /**
     * 创建符号链接。
     *
     * 在 [linkPath] 处创建一个指向 [targetPath] 的符号链接。
     * [targetPath] 可以是绝对路径或相对路径。
     * 创建时不验证目标是否存在（允许悬空链接）。
     */
    suspend fun create(linkPath: String, targetPath: String): Result<Unit>

    /**
     * 读取符号链接的目标路径（不解析）。
     */
    suspend fun readLink(path: String): Result<String>
}

/**
 * 归档压缩/解压。
 *
 * 支持 ZIP 和 TAR 两种格式。ZIP 使用 STORE（无压缩）方式打包，
 * TAR 为标准 USTAR 格式。所有操作基于虚拟文件系统内的路径。
 */
interface FsArchive {
    /**
     * 将一组文件/目录打包为归档文件。
     *
     * @param sourcePaths 要打包的文件或目录路径列表（目录会递归包含）
     * @param archivePath 生成的归档文件路径
     * @param format      归档格式（ZIP 或 TAR）
     */
    suspend fun compress(
        sourcePaths: List<String>,
        archivePath: String,
        format: ArchiveFormat = ArchiveFormat.ZIP
    ): Result<Unit>

    /**
     * 解压归档文件到指定目录。
     *
     * @param archivePath 归档文件路径
     * @param targetDir   解压目标目录（不存在则自动创建）
     * @param format      归档格式（ZIP 或 TAR），为 null 时自动检测
     */
    suspend fun extract(
        archivePath: String,
        targetDir: String,
        format: ArchiveFormat? = null
    ): Result<Unit>

    /**
     * 列出归档文件中的条目（不解压）。
     *
     * @param archivePath 归档文件路径
     * @param format      归档格式，为 null 时自动检测
     */
    suspend fun list(
        archivePath: String,
        format: ArchiveFormat? = null
    ): Result<List<ArchiveEntry>>
}

/**
 * 回收站（软删除 + 恢复机制）。
 *
 * 删除的文件/目录进入回收站而非永久删除，可随时恢复或彻底清除。
 * 每个回收站条目记录原始路径、删除时间和唯一 ID。
 */
interface FsTrash {
    /**
     * 将文件/目录移入回收站。
     *
     * 对于 VFS 内存文件，内容保存在 TrashManager 中；
     * 对于挂载点文件，移动到磁盘的 `.trash` 目录中。
     *
     * @param path 要删除的虚拟路径
     * @return 回收站条目 ID
     */
    suspend fun moveToTrash(path: String): Result<String>

    /**
     * 从回收站恢复文件/目录到原始位置。
     *
     * 如果原始路径已存在，返回 [FsError.AlreadyExists]。
     *
     * @param trashId 回收站条目 ID
     */
    suspend fun restore(trashId: String): Result<Unit>

    /**
     * 列出回收站中的所有条目（最近删除的在前）。
     */
    suspend fun list(): Result<List<TrashItem>>

    /**
     * 彻底删除回收站中的指定条目（不可恢复）。
     *
     * @param trashId 回收站条目 ID
     */
    suspend fun purge(trashId: String): Result<Unit>

    /**
     * 清空回收站（彻底删除所有条目）。
     */
    suspend fun purgeAll(): Result<Unit>
}

// ═══════════════════════════════════════════════════════════════
// 磁盘操作接口
// ═══════════════════════════════════════════════════════════════

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

    // ── 扩展属性（xattr） ──────────────────────────────────────

    suspend fun setXattr(path: String, name: String, value: ByteArray): Result<Unit> =
        Result.failure(FsError.PermissionDenied("xattr not supported"))

    suspend fun getXattr(path: String, name: String): Result<ByteArray> =
        Result.failure(FsError.PermissionDenied("xattr not supported"))

    suspend fun removeXattr(path: String, name: String): Result<Unit> =
        Result.failure(FsError.PermissionDenied("xattr not supported"))

    suspend fun listXattrs(path: String): Result<List<String>> =
        Result.failure(FsError.PermissionDenied("xattr not supported"))

    // ── 归档压缩 / 解压 ──────────────────────────────────────

    /**
     * 将磁盘上的一组文件/目录压缩为归档文件。
     *
     * @param sourcePaths 源路径列表（磁盘相对路径）
     * @param archivePath 归档文件输出路径（磁盘相对路径）
     * @param format      归档格式
     */
    suspend fun compress(
        sourcePaths: List<String>,
        archivePath: String,
        format: ArchiveFormat
    ): Result<Unit> =
        Result.failure(FsError.PermissionDenied("archive not supported"))

    /**
     * 解压归档文件到指定目录。
     *
     * @param archivePath 归档文件路径（磁盘相对路径）
     * @param targetDir   解压目标目录（磁盘相对路径）
     * @param format      归档格式，为 null 时自动检测
     */
    suspend fun extract(
        archivePath: String,
        targetDir: String,
        format: ArchiveFormat?
    ): Result<Unit> =
        Result.failure(FsError.PermissionDenied("archive not supported"))

    /**
     * 列出归档文件中的条目（不解压）。
     *
     * @param archivePath 归档文件路径（磁盘相对路径）
     * @param format      归档格式，为 null 时自动检测
     */
    suspend fun listArchive(
        archivePath: String,
        format: ArchiveFormat?
    ): Result<List<ArchiveEntry>> =
        Result.failure(FsError.PermissionDenied("archive not supported"))

    // ── 回收站 ─────────────────────────────────────────────────

    /**
     * 将磁盘文件/目录移入回收站（`.trash` 目录）。
     *
     * @param path 磁盘相对路径
     * @return 回收站条目 ID
     */
    suspend fun moveToTrash(path: String): Result<String> =
        Result.failure(FsError.PermissionDenied("trash not supported"))

    /**
     * 从磁盘回收站恢复到原始路径。
     *
     * @param trashId 回收站条目 ID
     * @param originalRelativePath 原始磁盘相对路径
     */
    suspend fun restoreFromTrash(trashId: String, originalRelativePath: String): Result<Unit> =
        Result.failure(FsError.PermissionDenied("trash not supported"))

    /**
     * 列出磁盘回收站中的条目。
     */
    suspend fun listTrash(): Result<List<TrashItem>> =
        Result.failure(FsError.PermissionDenied("trash not supported"))

    /**
     * 彻底删除磁盘回收站中的指定条目。
     */
    suspend fun purgeTrash(trashId: String): Result<Unit> =
        Result.failure(FsError.PermissionDenied("trash not supported"))

    /**
     * 清空磁盘回收站。
     */
    suspend fun purgeAllTrash(): Result<Unit> =
        Result.failure(FsError.PermissionDenied("trash not supported"))
}

/**
 * 磁盘文件变更事件（由 [DiskFileWatcher] 产生）。
 */
data class DiskFileEvent(
    val relativePath: String,
    val kind: FsEventKind
)

/**
 * 可选的磁盘文件变更监听接口。
 */
interface DiskFileWatcher {
    fun watchDisk(scope: kotlinx.coroutines.CoroutineScope): Flow<DiskFileEvent>
    fun stopWatching()
}

// ═══════════════════════════════════════════════════════════════
// 数据类型
// ═══════════════════════════════════════════════════════════════

data class OpMetrics(
    val count: Long = 0,
    val successCount: Long = 0,
    val failureCount: Long = 0,
    val totalTimeMs: Long = 0,
    val maxTimeMs: Long = 0
) {
    val avgTimeMs: Double get() = if (count > 0) totalTimeMs.toDouble() / count else 0.0
}

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
    val totalBytesRead: Long = 0,
    val totalBytesWritten: Long = 0
)

data class QuotaInfo(
    val quotaBytes: Long,
    val usedBytes: Long
) {
    val availableBytes: Long
        get() = if (quotaBytes < 0) Long.MAX_VALUE else maxOf(0, quotaBytes - usedBytes)
    val hasQuota: Boolean get() = quotaBytes >= 0
}

enum class ChecksumAlgorithm { CRC32, SHA256 }

data class FileVersion(
    val versionId: String,
    val timestampMillis: Long,
    val size: Long
)

data class SearchQuery(
    val rootPath: String = "/",
    val namePattern: String? = null,
    val contentPattern: String? = null,
    val typeFilter: FsType? = null,
    val maxDepth: Int = -1,
    val caseSensitive: Boolean = false
)

data class SearchResult(
    val path: String,
    val type: FsType,
    val size: Long = 0,
    val matchedLines: List<MatchedLine> = emptyList()
)

data class MatchedLine(
    val lineNumber: Int,
    val content: String
)

/** 归档格式。 */
enum class ArchiveFormat { ZIP, TAR }

/**
 * 归档文件中的条目信息。
 */
data class ArchiveEntry(
    /** 条目在归档内的相对路径。 */
    val path: String,
    /** 条目类型（文件或目录）。 */
    val type: FsType,
    /** 文件大小（字节），目录为 0。 */
    val size: Long,
    /** 最后修改时间（毫秒时间戳），不可用时为 0。 */
    val modifiedAtMillis: Long = 0
)

/**
 * 回收站条目信息。
 */
data class TrashItem(
    /** 回收站条目唯一 ID。 */
    val trashId: String,
    /** 文件/目录的原始虚拟路径。 */
    val originalPath: String,
    /** 条目类型（文件或目录）。 */
    val type: FsType,
    /** 文件大小（字节），目录为子条目总大小。 */
    val size: Long,
    /** 删除时间（毫秒时间戳）。 */
    val deletedAtMillis: Long
)

// ═══════════════════════════════════════════════════════════════
// 存储 & 错误
// ═══════════════════════════════════════════════════════════════

interface FsStorage {
    suspend fun read(key: String): Result<ByteArray?>
    suspend fun write(key: String, data: ByteArray): Result<Unit>
    suspend fun delete(key: String): Result<Unit>
    suspend fun append(key: String, data: ByteArray): Result<Unit>
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

    override suspend fun append(key: String, data: ByteArray): Result<Unit> {
        val existing = this.data[key] ?: ByteArray(0)
        this.data[key] = existing + data
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
    class SymlinkLoop(path: String) : FsError("符号链接循环: $path", FsErrorCode.SYMLINK_LOOP)
    class Unknown(message: String) : FsError(message, FsErrorCode.UNKNOWN)
}
