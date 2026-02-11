package com.hrm.fs.platform

import android.os.Build
import com.hrm.fs.api.DiskFileEvent
import com.hrm.fs.api.DiskFileOperations
import com.hrm.fs.api.DiskFileWatcher
import com.hrm.fs.api.FsEntry
import com.hrm.fs.api.FsError
import com.hrm.fs.api.FsEventKind
import com.hrm.fs.api.FsMeta
import com.hrm.fs.api.FsPermissions
import com.hrm.fs.api.FsType
import com.hrm.fs.api.log.FLog
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.File
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.file.FileSystems
import java.nio.file.FileVisitResult
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.SimpleFileVisitor
import java.nio.file.StandardWatchEventKinds
import java.nio.file.WatchKey
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file.attribute.UserDefinedFileAttributeView

actual fun createDiskFileOperations(rootPath: String): DiskFileOperations =
    AndroidDiskFileOperations(rootPath)

internal class AndroidDiskFileOperations(override val rootPath: String) : DiskFileOperations,
    DiskFileWatcher {

    companion object {
        private const val TAG = "AndroidDiskOps"
    }

    private fun resolve(path: String): File {
        val rel = path.removePrefix("/")
        return if (rel.isEmpty()) File(rootPath) else File(rootPath, rel)
    }

    // ═══════════════════════════════════════════════════════════
    // DiskFileOperations
    // ═══════════════════════════════════════════════════════════

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

    // ═══════════════════════════════════════════════════════════
    // xattr — 基于 UserDefinedFileAttributeView (API 26+)
    //
    // Android API 24-25 不支持 UserDefinedFileAttributeView，
    // 此时回退到默认的 "not supported" 错误。
    // ═══════════════════════════════════════════════════════════

    @Suppress("NewApi")
    override suspend fun setXattr(path: String, name: String, value: ByteArray): Result<Unit> =
        withContext(Dispatchers.IO) {
            if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) {
                return@withContext Result.failure(FsError.PermissionDenied("xattr requires API 26+"))
            }
            runCatching {
                val file = resolve(path)
                if (!file.exists()) throw FsError.NotFound(path)
                val view = Files.getFileAttributeView(
                    file.toPath(),
                    UserDefinedFileAttributeView::class.java
                ) ?: throw FsError.PermissionDenied("xattr not supported on this filesystem: $path")
                view.write(name, ByteBuffer.wrap(value))
                Unit
            }
        }

    @Suppress("NewApi")
    override suspend fun getXattr(path: String, name: String): Result<ByteArray> =
        withContext(Dispatchers.IO) {
            if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) {
                return@withContext Result.failure(FsError.PermissionDenied("xattr requires API 26+"))
            }
            runCatching {
                val file = resolve(path)
                if (!file.exists()) throw FsError.NotFound(path)
                val view = Files.getFileAttributeView(
                    file.toPath(),
                    UserDefinedFileAttributeView::class.java
                ) ?: throw FsError.PermissionDenied("xattr not supported on this filesystem: $path")
                val size = try {
                    view.size(name)
                } catch (_: Exception) {
                    throw FsError.NotFound("xattr '$name' on $path")
                }
                val buf = ByteBuffer.allocate(size)
                view.read(name, buf)
                buf.flip()
                val bytes = ByteArray(buf.remaining())
                buf.get(bytes)
                bytes
            }
        }

    @Suppress("NewApi")
    override suspend fun removeXattr(path: String, name: String): Result<Unit> =
        withContext(Dispatchers.IO) {
            if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) {
                return@withContext Result.failure(FsError.PermissionDenied("xattr requires API 26+"))
            }
            runCatching {
                val file = resolve(path)
                if (!file.exists()) throw FsError.NotFound(path)
                val view = Files.getFileAttributeView(
                    file.toPath(),
                    UserDefinedFileAttributeView::class.java
                ) ?: throw FsError.PermissionDenied("xattr not supported on this filesystem: $path")
                try {
                    view.delete(name)
                } catch (_: Exception) {
                    throw FsError.NotFound("xattr '$name' on $path")
                }
            }
        }

    @Suppress("NewApi")
    override suspend fun listXattrs(path: String): Result<List<String>> =
        withContext(Dispatchers.IO) {
            if (Build.VERSION.SDK_INT < Build.VERSION_CODES.O) {
                return@withContext Result.failure(FsError.PermissionDenied("xattr requires API 26+"))
            }
            runCatching {
                val file = resolve(path)
                if (!file.exists()) throw FsError.NotFound(path)
                val view = Files.getFileAttributeView(
                    file.toPath(),
                    UserDefinedFileAttributeView::class.java
                ) ?: throw FsError.PermissionDenied("xattr not supported on this filesystem: $path")
                view.list()
            }
        }

    // ═══════════════════════════════════════════════════════════
    // DiskFileWatcher — 根据 API level 选择实现策略
    //   API 26+  →  java.nio.file.WatchService（内核级通知，低延迟）
    //   API 24-25 →  定时轮询 + 快照对比（兼容降级方案）
    // ═══════════════════════════════════════════════════════════

    @Volatile
    private var watching = false

    override fun watchDisk(scope: CoroutineScope): Flow<DiskFileEvent> {
        return if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            watchDiskWithWatchService(scope)
        } else {
            watchDiskWithPolling(scope)
        }
    }

    override fun stopWatching() {
        watching = false
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            stopWatchService()
        }
    }

    // ── API 26+: WatchService ────────────────────────────────

    @Volatile
    private var watchServiceRef: Any? = null // 用 Any 避免低版本类加载问题

    @Suppress("NewApi")
    private fun watchDiskWithWatchService(scope: CoroutineScope): Flow<DiskFileEvent> =
        callbackFlow {
            val root = File(rootPath).toPath()
            if (!Files.isDirectory(root)) {
                close()
                return@callbackFlow
            }

            val ws = FileSystems.getDefault().newWatchService()
            watchServiceRef = ws
            watching = true

            val keyToPath = HashMap<WatchKey, Path>()

            fun registerDir(dir: Path) {
                try {
                    val key = dir.register(
                        ws,
                        StandardWatchEventKinds.ENTRY_CREATE,
                        StandardWatchEventKinds.ENTRY_MODIFY,
                        StandardWatchEventKinds.ENTRY_DELETE
                    )
                    keyToPath[key] = dir
                } catch (e: Exception) {
                    FLog.w(TAG, "registerDir failed: $dir: ${e.message}")
                }
            }

            Files.walkFileTree(root, object : SimpleFileVisitor<Path>() {
                override fun preVisitDirectory(
                    dir: Path,
                    attrs: BasicFileAttributes
                ): FileVisitResult {
                    registerDir(dir)
                    return FileVisitResult.CONTINUE
                }
            })

            val pollJob = scope.launch(Dispatchers.IO) {
                while (isActive && watching) {
                    val key = ws.poll(500, java.util.concurrent.TimeUnit.MILLISECONDS) ?: continue
                    val dir = keyToPath[key] ?: continue

                    for (event in key.pollEvents()) {
                        val kind = event.kind()
                        if (kind == StandardWatchEventKinds.OVERFLOW) continue

                        @Suppress("UNCHECKED_CAST")
                        val changedPath = dir.resolve(event.context() as Path)
                        val relativePath =
                            "/" + root.relativize(changedPath).toString().replace('\\', '/')

                        val fsKind = when (kind) {
                            StandardWatchEventKinds.ENTRY_CREATE -> FsEventKind.CREATED
                            StandardWatchEventKinds.ENTRY_MODIFY -> FsEventKind.MODIFIED
                            StandardWatchEventKinds.ENTRY_DELETE -> FsEventKind.DELETED
                            else -> continue
                        }

                        trySend(DiskFileEvent(relativePath, fsKind))

                        if (fsKind == FsEventKind.CREATED && Files.isDirectory(changedPath)) {
                            registerDir(changedPath)
                        }
                    }

                    if (!key.reset()) {
                        keyToPath.remove(key)
                    }
                }
            }

            awaitClose {
                pollJob.cancel()
                ws.close()
                watchServiceRef = null
                watching = false
            }
        }

    @Suppress("NewApi")
    private fun stopWatchService() {
        (watchServiceRef as? java.nio.file.WatchService)?.close()
        watchServiceRef = null
    }

    // ── API 24-25: 轮询对比快照 ──────────────────────────────

    private fun watchDiskWithPolling(scope: CoroutineScope): Flow<DiskFileEvent> = callbackFlow {
        watching = true
        val root = File(rootPath)
        if (!root.isDirectory) {
            close()
            return@callbackFlow
        }

        fun snapshot(): Map<String, Pair<Boolean, Long>> {
            val result = mutableMapOf<String, Pair<Boolean, Long>>()
            fun scan(dir: File, prefix: String) {
                dir.listFiles()?.forEach { child ->
                    val relChild = "$prefix/${child.name}"
                    result[relChild] = child.isDirectory to child.lastModified()
                    if (child.isDirectory) scan(child, relChild)
                }
            }
            scan(root, "")
            return result
        }

        var prevSnapshot = snapshot()

        val pollJob = scope.launch(Dispatchers.IO) {
            while (isActive && watching) {
                delay(1000)
                val currentSnapshot = snapshot()

                for ((path, info) in currentSnapshot) {
                    val prev = prevSnapshot[path]
                    if (prev == null) {
                        trySend(DiskFileEvent(path, FsEventKind.CREATED))
                    } else if (prev.second != info.second) {
                        trySend(DiskFileEvent(path, FsEventKind.MODIFIED))
                    }
                }
                for (path in prevSnapshot.keys) {
                    if (path !in currentSnapshot) {
                        trySend(DiskFileEvent(path, FsEventKind.DELETED))
                    }
                }

                prevSnapshot = currentSnapshot
            }
        }

        awaitClose {
            watching = false
            pollJob.cancel()
        }
    }
}
