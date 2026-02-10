package com.hrm.fs.core

import com.hrm.fs.api.DiskFileOperations
import com.hrm.fs.api.FileHandle
import com.hrm.fs.api.FsError
import com.hrm.fs.api.OpenMode

internal class DiskFileHandle(
    private val diskOps: DiskFileOperations,
    private val relativePath: String,
    private val mode: OpenMode
) : FileHandle {

    override suspend fun readAt(offset: Long, length: Int): Result<ByteArray> {
        if (mode == OpenMode.WRITE) {
            return Result.failure(FsError.PermissionDenied("read"))
        }
        return diskOps.readFile(relativePath, offset, length)
    }

    override suspend fun writeAt(offset: Long, data: ByteArray): Result<Unit> {
        if (mode == OpenMode.READ) {
            return Result.failure(FsError.PermissionDenied("write"))
        }
        return diskOps.writeFile(relativePath, offset, data)
    }

    override suspend fun close(): Result<Unit> = Result.success(Unit)
}
