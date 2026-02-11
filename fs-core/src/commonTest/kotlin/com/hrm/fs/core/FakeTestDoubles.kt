package com.hrm.fs.core

import com.hrm.fs.api.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow

// ═════════════════════════════════════════════════════════════════
// Shared helper functions
// ═════════════════════════════════════════════════════════════════

internal fun createFs(storage: FsStorage? = null): FileSystem =
    InMemoryFileSystem(storage = storage)

internal fun createFsWithQuota(quotaBytes: Long, storage: FsStorage? = null): FileSystem =
    InMemoryFileSystem(storage = storage, quotaBytes = quotaBytes)

// ═════════════════════════════════════════════════════════════════
// Fake DiskFileOperations for testing mounts
// ═════════════════════════════════════════════════════════════════

internal class FakeDiskFileOperations : DiskFileOperations {
    override val rootPath: String = "/fake"

    val createdFiles = mutableSetOf<String>()
    val createdDirs = mutableSetOf<String>()
    val files = mutableMapOf<String, ByteArray>()
    val dirs = mutableSetOf<String>("/")
    val xattrs = mutableMapOf<String, MutableMap<String, ByteArray>>()

    override suspend fun createFile(path: String): Result<Unit> {
        createdFiles.add(path)
        files[path] = ByteArray(0)
        return Result.success(Unit)
    }

    override suspend fun createDir(path: String): Result<Unit> {
        createdDirs.add(path)
        dirs.add(path)
        return Result.success(Unit)
    }

    override suspend fun readFile(path: String, offset: Long, length: Int): Result<ByteArray> {
        val data = files[path] ?: return Result.failure(FsError.NotFound(path))
        if (offset >= data.size) return Result.success(ByteArray(0))
        val end = minOf(offset.toInt() + length, data.size)
        return Result.success(data.copyOfRange(offset.toInt(), end))
    }

    override suspend fun writeFile(path: String, offset: Long, data: ByteArray): Result<Unit> {
        val existing = files[path] ?: ByteArray(0)
        val end = offset.toInt() + data.size
        val newData = if (end > existing.size) {
            ByteArray(end).also { existing.copyInto(it) }
        } else {
            existing.copyOf()
        }
        data.copyInto(newData, destinationOffset = offset.toInt())
        files[path] = newData
        return Result.success(Unit)
    }

    override suspend fun delete(path: String): Result<Unit> {
        files.remove(path)
        dirs.remove(path)
        createdFiles.remove(path)
        createdDirs.remove(path)
        return Result.success(Unit)
    }

    override suspend fun list(path: String): Result<List<FsEntry>> {
        val prefix = if (path == "/") "/" else "$path/"
        val entries = mutableListOf<FsEntry>()
        for (f in files.keys) {
            if (f.startsWith(prefix) && f.removePrefix(prefix).count { it == '/' } == 0) {
                entries.add(FsEntry(f.substringAfterLast('/'), FsType.FILE))
            }
        }
        for (d in dirs) {
            if (d != path && d.startsWith(prefix) && d.removePrefix(prefix).count { it == '/' } == 0) {
                entries.add(FsEntry(d.substringAfterLast('/'), FsType.DIRECTORY))
            }
        }
        return Result.success(entries)
    }

    override suspend fun stat(path: String): Result<FsMeta> {
        if (files.containsKey(path)) {
            return Result.success(
                FsMeta(
                    path = path, type = FsType.FILE, size = files[path]!!.size.toLong(),
                    createdAtMillis = 0, modifiedAtMillis = 0, permissions = FsPermissions.FULL
                )
            )
        }
        if (dirs.contains(path)) {
            return Result.success(
                FsMeta(
                    path = path, type = FsType.DIRECTORY, size = 0,
                    createdAtMillis = 0, modifiedAtMillis = 0, permissions = FsPermissions.FULL
                )
            )
        }
        return Result.failure(FsError.NotFound(path))
    }

    override suspend fun exists(path: String): Boolean = files.containsKey(path) || dirs.contains(path)

    override suspend fun setXattr(path: String, name: String, value: ByteArray): Result<Unit> {
        if (!exists(path)) return Result.failure(FsError.NotFound(path))
        xattrs.getOrPut(path) { mutableMapOf() }[name] = value.copyOf()
        return Result.success(Unit)
    }

    override suspend fun getXattr(path: String, name: String): Result<ByteArray> {
        if (!exists(path)) return Result.failure(FsError.NotFound(path))
        val v = xattrs[path]?.get(name) ?: return Result.failure(FsError.NotFound("xattr '$name' on $path"))
        return Result.success(v.copyOf())
    }

    override suspend fun removeXattr(path: String, name: String): Result<Unit> {
        if (!exists(path)) return Result.failure(FsError.NotFound(path))
        if (xattrs[path]?.remove(name) == null) return Result.failure(FsError.NotFound("xattr '$name' on $path"))
        return Result.success(Unit)
    }

    override suspend fun listXattrs(path: String): Result<List<String>> {
        if (!exists(path)) return Result.failure(FsError.NotFound(path))
        return Result.success(xattrs[path]?.keys?.toList() ?: emptyList())
    }
}

// ═════════════════════════════════════════════════════════════════
// Fake DiskFileOperations + DiskFileWatcher for testing external change detection
// ═════════════════════════════════════════════════════════════════

internal class FakeWatchableDiskOps : DiskFileOperations, DiskFileWatcher {
    override val rootPath: String = "/watchable"

    val files = mutableMapOf<String, ByteArray>()
    val dirs = mutableSetOf<String>("/")

    /** 外部通过此 flow 模拟磁盘变更事件。 */
    val externalEvents = MutableSharedFlow<DiskFileEvent>(extraBufferCapacity = 64)

    override fun watchDisk(scope: CoroutineScope): Flow<DiskFileEvent> = externalEvents

    override fun stopWatching() {}

    override suspend fun createFile(path: String): Result<Unit> {
        files[path] = ByteArray(0)
        return Result.success(Unit)
    }

    override suspend fun createDir(path: String): Result<Unit> {
        dirs.add(path)
        return Result.success(Unit)
    }

    override suspend fun readFile(path: String, offset: Long, length: Int): Result<ByteArray> {
        val data = files[path] ?: return Result.failure(FsError.NotFound(path))
        if (offset >= data.size) return Result.success(ByteArray(0))
        val end = minOf(offset.toInt() + length, data.size)
        return Result.success(data.copyOfRange(offset.toInt(), end))
    }

    override suspend fun writeFile(path: String, offset: Long, data: ByteArray): Result<Unit> {
        val existing = files[path] ?: ByteArray(0)
        val end = offset.toInt() + data.size
        val newData = if (end > existing.size) {
            ByteArray(end).also { existing.copyInto(it) }
        } else {
            existing.copyOf()
        }
        data.copyInto(newData, destinationOffset = offset.toInt())
        files[path] = newData
        return Result.success(Unit)
    }

    override suspend fun delete(path: String): Result<Unit> {
        files.remove(path)
        dirs.remove(path)
        return Result.success(Unit)
    }

    override suspend fun list(path: String): Result<List<FsEntry>> {
        val prefix = if (path == "/") "/" else "$path/"
        val entries = mutableListOf<FsEntry>()
        for (f in files.keys) {
            if (f.startsWith(prefix) && f.removePrefix(prefix).count { it == '/' } == 0) {
                entries.add(FsEntry(f.substringAfterLast('/'), FsType.FILE))
            }
        }
        for (d in dirs) {
            if (d != path && d.startsWith(prefix) && d.removePrefix(prefix).count { it == '/' } == 0) {
                entries.add(FsEntry(d.substringAfterLast('/'), FsType.DIRECTORY))
            }
        }
        return Result.success(entries)
    }

    override suspend fun stat(path: String): Result<FsMeta> {
        if (files.containsKey(path)) {
            return Result.success(
                FsMeta(
                    path = path, type = FsType.FILE, size = files[path]!!.size.toLong(),
                    createdAtMillis = 0, modifiedAtMillis = 0, permissions = FsPermissions.FULL
                )
            )
        }
        if (dirs.contains(path)) {
            return Result.success(
                FsMeta(
                    path = path, type = FsType.DIRECTORY, size = 0,
                    createdAtMillis = 0, modifiedAtMillis = 0, permissions = FsPermissions.FULL
                )
            )
        }
        return Result.failure(FsError.NotFound(path))
    }

    override suspend fun exists(path: String): Boolean = files.containsKey(path) || dirs.contains(path)
}
