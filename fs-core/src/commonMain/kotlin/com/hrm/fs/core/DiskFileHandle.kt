package com.hrm.fs.core

import com.hrm.fs.api.DiskFileOperations
import com.hrm.fs.api.FileHandle
import com.hrm.fs.api.FileLockType
import com.hrm.fs.api.FsError
import com.hrm.fs.api.OpenMode

internal class DiskFileHandle(
    private val diskOps: DiskFileOperations,
    private val relativePath: String,
    private val mode: OpenMode,
    private val virtualPath: String,
    private val lockManager: VfsFileLockManager
) : FileHandle {

    /** 用于在锁管理器中唯一标识此句柄实例。 */
    internal val handleId: Long = HandleIdGenerator.next()

    private var closed = false

    override suspend fun readAt(offset: Long, length: Int): Result<ByteArray> {
        if (closed) return Result.failure(FsError.PermissionDenied("句柄已关闭"))
        if (mode == OpenMode.WRITE) {
            return Result.failure(FsError.PermissionDenied("read"))
        }
        return diskOps.readFile(relativePath, offset, length)
    }

    override suspend fun writeAt(offset: Long, data: ByteArray): Result<Unit> {
        if (closed) return Result.failure(FsError.PermissionDenied("句柄已关闭"))
        if (mode == OpenMode.READ) {
            return Result.failure(FsError.PermissionDenied("write"))
        }
        return diskOps.writeFile(relativePath, offset, data)
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
