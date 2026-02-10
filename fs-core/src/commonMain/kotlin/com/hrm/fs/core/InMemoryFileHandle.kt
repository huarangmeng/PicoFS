package com.hrm.fs.core

import com.hrm.fs.api.FileHandle
import com.hrm.fs.api.FsError
import com.hrm.fs.api.OpenMode

internal class InMemoryFileHandle(
    private val fs: InMemoryFileSystem,
    private val node: FileNode,
    private val mode: OpenMode
) : FileHandle {

    override fun readAt(offset: Long, length: Int): Result<ByteArray> {
        if (mode == OpenMode.WRITE || !node.permissions.canRead()) {
            return Result.failure(FsError.PermissionDenied("read"))
        }
        return fs.readAt(node, offset, length)
    }

    override fun writeAt(offset: Long, data: ByteArray): Result<Unit> {
        if (mode == OpenMode.READ || !node.permissions.canWrite()) {
            return Result.failure(FsError.PermissionDenied("write"))
        }
        return fs.writeAt(node, offset, data)
    }

    override fun close(): Result<Unit> = Result.success(Unit)
}
