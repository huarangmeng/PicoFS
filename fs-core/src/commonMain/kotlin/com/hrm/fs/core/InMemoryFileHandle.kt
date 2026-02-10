package com.hrm.fs.core

import com.hrm.fs.api.FileHandle
import com.hrm.fs.api.FileLockType
import com.hrm.fs.api.FsError
import com.hrm.fs.api.OpenMode
import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.ExperimentalAtomicApi

/**
 * 全局递增句柄 ID 生成器。
 */
internal object HandleIdGenerator {
    @OptIn(ExperimentalAtomicApi::class)
    private val counter = AtomicLong(0L)

    @OptIn(ExperimentalAtomicApi::class)
    fun next(): Long = counter.fetchAndAdd(1L)
}

internal class InMemoryFileHandle(
    private val fs: InMemoryFileSystem,
    private val node: FileNode,
    private val mode: OpenMode,
    private val virtualPath: String,
    private val lockManager: VfsFileLockManager
) : FileHandle {

    /** 用于在锁管理器中唯一标识此句柄实例。 */
    internal val handleId: Long = HandleIdGenerator.next()

    private var closed = false

    override suspend fun readAt(offset: Long, length: Int): Result<ByteArray> {
        if (closed) return Result.failure(FsError.PermissionDenied("句柄已关闭"))
        if (mode == OpenMode.WRITE || !node.permissions.canRead()) {
            return Result.failure(FsError.PermissionDenied("read"))
        }
        return fs.readAt(node, offset, length)
    }

    override suspend fun writeAt(offset: Long, data: ByteArray): Result<Unit> {
        if (closed) return Result.failure(FsError.PermissionDenied("句柄已关闭"))
        if (mode == OpenMode.READ || !node.permissions.canWrite()) {
            return Result.failure(FsError.PermissionDenied("write"))
        }
        return fs.writeAt(node, offset, data)
    }

    override suspend fun lock(type: FileLockType): Result<Unit> {
        if (closed) return Result.failure(FsError.PermissionDenied("句柄已关闭"))
        return lockManager.lock(virtualPath, handleId, type)
    }

    override suspend fun tryLock(type: FileLockType): Result<Unit> {
        if (closed) return Result.failure(FsError.PermissionDenied("句柄已关闭"))
        return lockManager.tryLock(virtualPath, handleId, type)
    }

    override suspend fun unlock(): Result<Unit> {
        if (closed) return Result.failure(FsError.PermissionDenied("句柄已关闭"))
        return lockManager.unlock(virtualPath, handleId)
    }

    override suspend fun close(): Result<Unit> {
        if (closed) return Result.success(Unit)
        closed = true
        lockManager.unlockAll(handleId)
        return Result.success(Unit)
    }
}
