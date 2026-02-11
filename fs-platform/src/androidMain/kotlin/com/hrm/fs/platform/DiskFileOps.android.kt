package com.hrm.fs.platform

import android.os.Build
import com.hrm.fs.api.ArchiveEntry
import com.hrm.fs.api.ArchiveFormat
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
    // archive — 基于 java.util.zip
    // ═══════════════════════════════════════════════════════════

    override suspend fun compress(
        sourcePaths: List<String>,
        archivePath: String,
        format: ArchiveFormat
    ): Result<Unit> = withContext(Dispatchers.IO) {
        runCatching {
            val archiveFile = resolve(archivePath)
            archiveFile.parentFile?.mkdirs()
            when (format) {
                ArchiveFormat.ZIP -> zipCompress(sourcePaths, archiveFile)
                ArchiveFormat.TAR -> tarCompress(sourcePaths, archiveFile)
            }
        }
    }

    override suspend fun extract(
        archivePath: String,
        targetDir: String,
        format: ArchiveFormat?
    ): Result<Unit> = withContext(Dispatchers.IO) {
        runCatching {
            val archiveFile = resolve(archivePath)
            if (!archiveFile.exists()) throw FsError.NotFound(archivePath)
            val targetFile = resolve(targetDir)
            targetFile.mkdirs()
            val resolvedFormat = format ?: detectFormat(archiveFile)
                ?: throw FsError.Unknown("无法检测归档格式: $archivePath")
            when (resolvedFormat) {
                ArchiveFormat.ZIP -> zipExtract(archiveFile, targetFile)
                ArchiveFormat.TAR -> tarExtract(archiveFile, targetFile)
            }
        }
    }

    override suspend fun listArchive(
        archivePath: String,
        format: ArchiveFormat?
    ): Result<List<ArchiveEntry>> = withContext(Dispatchers.IO) {
        runCatching {
            val archiveFile = resolve(archivePath)
            if (!archiveFile.exists()) throw FsError.NotFound(archivePath)
            val resolvedFormat = format ?: detectFormat(archiveFile)
                ?: throw FsError.Unknown("无法检测归档格式: $archivePath")
            when (resolvedFormat) {
                ArchiveFormat.ZIP -> zipList(archiveFile)
                ArchiveFormat.TAR -> tarList(archiveFile)
            }
        }
    }

    private fun detectFormat(file: File): ArchiveFormat? {
        if (!file.exists() || file.length() < 4) return null
        val header = ByteArray(264)
        file.inputStream().use { it.read(header) }
        if (header[0] == 0x50.toByte() && header[1] == 0x4B.toByte() &&
            header[2] == 0x03.toByte() && header[3] == 0x04.toByte()
        ) return ArchiveFormat.ZIP
        if (header.size >= 263) {
            val ustar = "ustar"
            if ((0 until 5).all { header[257 + it] == ustar[it].code.toByte() }) return ArchiveFormat.TAR
        }
        return null
    }

    // ── ZIP ──────────────────────────────────────────────────

    private fun zipCompress(sourcePaths: List<String>, archiveFile: File) {
        java.util.zip.ZipOutputStream(archiveFile.outputStream().buffered()).use { zos ->
            for (srcPath in sourcePaths) {
                val srcFile = resolve(srcPath)
                if (!srcFile.exists()) throw FsError.NotFound(srcPath)
                val baseName = srcFile.name
                if (srcFile.isDirectory) {
                    zipAddDirectory(zos, srcFile, baseName)
                } else {
                    zipAddFile(zos, srcFile, baseName)
                }
            }
        }
    }

    private fun zipAddDirectory(zos: java.util.zip.ZipOutputStream, dir: File, prefix: String) {
        val entry = java.util.zip.ZipEntry("$prefix/")
        entry.time = dir.lastModified()
        zos.putNextEntry(entry)
        zos.closeEntry()
        dir.listFiles()?.forEach { child ->
            val childName = "$prefix/${child.name}"
            if (child.isDirectory) {
                zipAddDirectory(zos, child, childName)
            } else {
                zipAddFile(zos, child, childName)
            }
        }
    }

    private fun zipAddFile(zos: java.util.zip.ZipOutputStream, file: File, name: String) {
        val entry = java.util.zip.ZipEntry(name)
        entry.time = file.lastModified()
        zos.putNextEntry(entry)
        file.inputStream().use { it.copyTo(zos) }
        zos.closeEntry()
    }

    private fun zipExtract(archiveFile: File, targetDir: File) {
        java.util.zip.ZipInputStream(archiveFile.inputStream().buffered()).use { zis ->
            var entry = zis.nextEntry
            while (entry != null) {
                val outFile = File(targetDir, entry.name)
                if (entry.isDirectory) {
                    outFile.mkdirs()
                } else {
                    outFile.parentFile?.mkdirs()
                    outFile.outputStream().use { zis.copyTo(it) }
                    if (entry.time > 0) outFile.setLastModified(entry.time)
                }
                zis.closeEntry()
                entry = zis.nextEntry
            }
        }
    }

    private fun zipList(archiveFile: File): List<ArchiveEntry> {
        val entries = mutableListOf<ArchiveEntry>()
        java.util.zip.ZipInputStream(archiveFile.inputStream().buffered()).use { zis ->
            var entry = zis.nextEntry
            while (entry != null) {
                val path = entry.name.trimEnd('/')
                entries.add(
                    ArchiveEntry(
                        path = path,
                        type = if (entry.isDirectory) FsType.DIRECTORY else FsType.FILE,
                        size = if (entry.isDirectory) 0L else entry.size.coerceAtLeast(0),
                        modifiedAtMillis = entry.time.coerceAtLeast(0)
                    )
                )
                zis.closeEntry()
                entry = zis.nextEntry
            }
        }
        return entries
    }

    // ── TAR (USTAR) ─────────────────────────────────────────

    private fun tarCompress(sourcePaths: List<String>, archiveFile: File) {
        archiveFile.outputStream().buffered().use { out ->
            for (srcPath in sourcePaths) {
                val srcFile = resolve(srcPath)
                if (!srcFile.exists()) throw FsError.NotFound(srcPath)
                tarAddEntry(out, srcFile, srcFile.name)
            }
            out.write(ByteArray(1024))
        }
    }

    private fun tarAddEntry(out: java.io.OutputStream, file: File, name: String) {
        if (file.isDirectory) {
            out.write(buildTarHeader("$name/", 0, file.lastModified(), isDir = true))
            file.listFiles()?.forEach { child ->
                tarAddEntry(out, child, "$name/${child.name}")
            }
        } else {
            val size = file.length()
            out.write(buildTarHeader(name, size, file.lastModified(), isDir = false))
            file.inputStream().use { it.copyTo(out) }
            val remainder = (size % 512).toInt()
            if (remainder != 0) out.write(ByteArray(512 - remainder))
        }
    }

    private fun buildTarHeader(name: String, size: Long, mtime: Long, isDir: Boolean): ByteArray {
        val header = ByteArray(512)
        val nameBytes = name.encodeToByteArray()
        nameBytes.copyInto(header, 0, 0, minOf(nameBytes.size, 100))
        writeOctal(header, 100, 8, if (isDir) 493 else 420)
        writeOctal(header, 108, 8, 0)
        writeOctal(header, 116, 8, 0)
        writeOctal(header, 124, 12, size)
        writeOctal(header, 136, 12, mtime / 1000)
        for (i in 148 until 156) header[i] = ' '.code.toByte()
        header[156] = if (isDir) '5'.code.toByte() else '0'.code.toByte()
        val ustar = "ustar\u000000"
        ustar.encodeToByteArray().copyInto(header, 257, 0, minOf(ustar.length, 8))
        var chksum = 0
        for (b in header) chksum += (b.toInt() and 0xFF)
        writeOctal(header, 148, 7, chksum.toLong())
        header[155] = ' '.code.toByte()
        return header
    }

    private fun tarExtract(archiveFile: File, targetDir: File) {
        archiveFile.inputStream().buffered().use { input ->
            val headerBuf = ByteArray(512)
            while (true) {
                val read = readFully(input, headerBuf)
                if (read < 512) break
                if (headerBuf.all { it == 0.toByte() }) break

                val name = readTarString(headerBuf, 0, 100)
                val sizeOctal = readTarString(headerBuf, 124, 12)
                val mtimeOctal = readTarString(headerBuf, 136, 12)
                val typeFlag = headerBuf[156]
                val size = parseOctal(sizeOctal)
                val mtime = parseOctal(mtimeOctal) * 1000
                val isDir = typeFlag == '5'.code.toByte() || name.endsWith("/")

                val outFile = File(targetDir, name.trimEnd('/'))
                if (isDir) {
                    outFile.mkdirs()
                } else {
                    outFile.parentFile?.mkdirs()
                    outFile.outputStream().use { out ->
                        var remaining = size
                        val buf = ByteArray(8192)
                        while (remaining > 0) {
                            val toRead = minOf(remaining.toInt(), buf.size)
                            val n = input.read(buf, 0, toRead)
                            if (n <= 0) break
                            out.write(buf, 0, n)
                            remaining -= n
                        }
                    }
                    val remainder = (size % 512).toInt()
                    if (remainder != 0) input.skip((512 - remainder).toLong())
                    if (mtime > 0) outFile.setLastModified(mtime)
                }
            }
        }
    }

    private fun tarList(archiveFile: File): List<ArchiveEntry> {
        val entries = mutableListOf<ArchiveEntry>()
        archiveFile.inputStream().buffered().use { input ->
            val headerBuf = ByteArray(512)
            while (true) {
                val read = readFully(input, headerBuf)
                if (read < 512) break
                if (headerBuf.all { it == 0.toByte() }) break

                val name = readTarString(headerBuf, 0, 100)
                val sizeOctal = readTarString(headerBuf, 124, 12)
                val mtimeOctal = readTarString(headerBuf, 136, 12)
                val typeFlag = headerBuf[156]
                val size = parseOctal(sizeOctal)
                val mtime = parseOctal(mtimeOctal) * 1000
                val isDir = typeFlag == '5'.code.toByte() || name.endsWith("/")

                entries.add(
                    ArchiveEntry(
                        path = name.trimEnd('/'),
                        type = if (isDir) FsType.DIRECTORY else FsType.FILE,
                        size = if (isDir) 0L else size,
                        modifiedAtMillis = mtime
                    )
                )
                if (size > 0 && !isDir) {
                    val skip = size + ((512 - (size % 512)) % 512)
                    input.skip(skip)
                }
            }
        }
        return entries
    }

    private fun readFully(input: java.io.InputStream, buf: ByteArray): Int {
        var offset = 0
        while (offset < buf.size) {
            val n = input.read(buf, offset, buf.size - offset)
            if (n <= 0) break
            offset += n
        }
        return offset
    }

    private fun readTarString(data: ByteArray, offset: Int, maxLen: Int): String {
        var end = offset
        val limit = minOf(offset + maxLen, data.size)
        while (end < limit && data[end] != 0.toByte()) end++
        return data.decodeToString(offset, end)
    }

    private fun parseOctal(s: String): Long {
        val trimmed = s.trim().trimEnd('\u0000')
        if (trimmed.isEmpty()) return 0
        return trimmed.toLongOrNull(8) ?: 0
    }

    private fun writeOctal(header: ByteArray, offset: Int, fieldLen: Int, value: Long) {
        val octalStr = value.toString(8)
        val padded = octalStr.padStart(fieldLen - 1, '0')
        val bytes = padded.encodeToByteArray()
        val copyLen = minOf(bytes.size, fieldLen - 1)
        bytes.copyInto(header, offset, bytes.size - copyLen, bytes.size)
        header[offset + fieldLen - 1] = 0
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
