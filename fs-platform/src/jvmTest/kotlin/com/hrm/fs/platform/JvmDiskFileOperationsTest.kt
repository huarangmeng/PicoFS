package com.hrm.fs.platform

import com.hrm.fs.api.ArchiveFormat
import com.hrm.fs.api.FsError
import com.hrm.fs.api.FsType
import kotlinx.coroutines.test.runTest
import java.io.File
import kotlin.test.*

class JvmDiskFileOperationsTest {

    private lateinit var tempDir: File
    private lateinit var ops: JvmDiskFileOperations

    @BeforeTest
    fun setup() {
        tempDir = File(System.getProperty("java.io.tmpdir"), "picofs-diskops-test-${System.nanoTime()}")
        tempDir.mkdirs()
        ops = JvmDiskFileOperations(tempDir.absolutePath)
    }

    @AfterTest
    fun cleanup() {
        tempDir.deleteRecursively()
    }

    // ═══════════════════════════════════════════════════════════
    // createFile
    // ═══════════════════════════════════════════════════════════

    @Test
    fun createFile_basic() = runTest {
        ops.createFile("/hello.txt").getOrThrow()
        assertTrue(File(tempDir, "hello.txt").exists())
        assertTrue(File(tempDir, "hello.txt").isFile)
    }

    @Test
    fun createFile_nested() = runTest {
        ops.createFile("/a/b/c.txt").getOrThrow()
        assertTrue(File(tempDir, "a/b/c.txt").isFile)
    }

    @Test
    fun createFile_existing_file_succeeds() = runTest {
        ops.createFile("/dup.txt").getOrThrow()
        // createNewFile returns false if exists, but it's a file so no error
        val result = ops.createFile("/dup.txt")
        assertTrue(result.isSuccess)
    }

    // ═══════════════════════════════════════════════════════════
    // createDir
    // ═══════════════════════════════════════════════════════════

    @Test
    fun createDir_basic() = runTest {
        ops.createDir("/mydir").getOrThrow()
        assertTrue(File(tempDir, "mydir").isDirectory)
    }

    @Test
    fun createDir_nested() = runTest {
        ops.createDir("/a/b/c").getOrThrow()
        assertTrue(File(tempDir, "a/b/c").isDirectory)
    }

    @Test
    fun createDir_existing_succeeds() = runTest {
        ops.createDir("/existing").getOrThrow()
        val result = ops.createDir("/existing")
        assertTrue(result.isSuccess)
    }

    // ═══════════════════════════════════════════════════════════
    // writeFile / readFile
    // ═══════════════════════════════════════════════════════════

    @Test
    fun write_then_read() = runTest {
        ops.createFile("/data.txt").getOrThrow()
        val data = "Hello, PicoFS!".encodeToByteArray()
        ops.writeFile("/data.txt", 0, data).getOrThrow()
        val read = ops.readFile("/data.txt", 0, Int.MAX_VALUE).getOrThrow()
        assertContentEquals(data, read)
    }

    @Test
    fun write_at_offset() = runTest {
        ops.createFile("/offset.txt").getOrThrow()
        ops.writeFile("/offset.txt", 0, "AAABBB".encodeToByteArray()).getOrThrow()
        ops.writeFile("/offset.txt", 3, "CCC".encodeToByteArray()).getOrThrow()
        val read = ops.readFile("/offset.txt", 0, Int.MAX_VALUE).getOrThrow()
        assertEquals("AAACCC", read.decodeToString())
    }

    @Test
    fun read_at_offset_and_length() = runTest {
        ops.createFile("/partial.txt").getOrThrow()
        ops.writeFile("/partial.txt", 0, "0123456789".encodeToByteArray()).getOrThrow()
        val chunk = ops.readFile("/partial.txt", 3, 4).getOrThrow()
        assertEquals("3456", chunk.decodeToString())
    }

    @Test
    fun read_beyond_eof_returns_empty() = runTest {
        ops.createFile("/small.txt").getOrThrow()
        ops.writeFile("/small.txt", 0, "abc".encodeToByteArray()).getOrThrow()
        val result = ops.readFile("/small.txt", 100, 10).getOrThrow()
        assertEquals(0, result.size)
    }

    @Test
    fun read_nonexistent_fails_with_not_found() = runTest {
        val result = ops.readFile("/nope.txt", 0, 10)
        assertTrue(result.isFailure)
        assertIs<FsError.NotFound>(result.exceptionOrNull())
    }

    @Test
    fun write_nonexistent_fails_with_not_found() = runTest {
        val result = ops.writeFile("/nope.txt", 0, "x".encodeToByteArray())
        assertTrue(result.isFailure)
        assertIs<FsError.NotFound>(result.exceptionOrNull())
    }

    @Test
    fun read_directory_fails_with_not_file() = runTest {
        ops.createDir("/adir").getOrThrow()
        val result = ops.readFile("/adir", 0, 10)
        assertTrue(result.isFailure)
        assertIs<FsError.NotFile>(result.exceptionOrNull())
    }

    @Test
    fun write_directory_fails_with_not_file() = runTest {
        ops.createDir("/adir").getOrThrow()
        val result = ops.writeFile("/adir", 0, "x".encodeToByteArray())
        assertTrue(result.isFailure)
        assertIs<FsError.NotFile>(result.exceptionOrNull())
    }

    // ═══════════════════════════════════════════════════════════
    // delete
    // ═══════════════════════════════════════════════════════════

    @Test
    fun delete_file() = runTest {
        ops.createFile("/del.txt").getOrThrow()
        assertTrue(ops.exists("/del.txt"))
        ops.delete("/del.txt").getOrThrow()
        assertFalse(ops.exists("/del.txt"))
    }

    @Test
    fun delete_empty_dir() = runTest {
        ops.createDir("/emptydir").getOrThrow()
        ops.delete("/emptydir").getOrThrow()
        assertFalse(ops.exists("/emptydir"))
    }

    @Test
    fun delete_non_empty_dir_fails() = runTest {
        ops.createDir("/parent").getOrThrow()
        ops.createFile("/parent/child.txt").getOrThrow()
        val result = ops.delete("/parent")
        assertTrue(result.isFailure)
        assertIs<FsError.PermissionDenied>(result.exceptionOrNull())
    }

    @Test
    fun delete_nonexistent_fails() = runTest {
        val result = ops.delete("/ghost.txt")
        assertTrue(result.isFailure)
        assertIs<FsError.NotFound>(result.exceptionOrNull())
    }

    // ═══════════════════════════════════════════════════════════
    // list
    // ═══════════════════════════════════════════════════════════

    @Test
    fun list_empty_dir() = runTest {
        ops.createDir("/empty").getOrThrow()
        val entries = ops.list("/empty").getOrThrow()
        assertTrue(entries.isEmpty())
    }

    @Test
    fun list_with_files_and_dirs() = runTest {
        ops.createFile("/root/a.txt").getOrThrow()
        ops.createDir("/root/sub").getOrThrow()
        val entries = ops.list("/root").getOrThrow()
        assertEquals(2, entries.size)
        val names = entries.map { it.name }.toSet()
        assertTrue("a.txt" in names)
        assertTrue("sub" in names)
        val fileEntry = entries.first { it.name == "a.txt" }
        assertEquals(FsType.FILE, fileEntry.type)
        val dirEntry = entries.first { it.name == "sub" }
        assertEquals(FsType.DIRECTORY, dirEntry.type)
    }

    @Test
    fun list_nonexistent_fails() = runTest {
        val result = ops.list("/nonexistent")
        assertTrue(result.isFailure)
        assertIs<FsError.NotFound>(result.exceptionOrNull())
    }

    @Test
    fun list_file_fails_with_not_directory() = runTest {
        ops.createFile("/afile.txt").getOrThrow()
        val result = ops.list("/afile.txt")
        assertTrue(result.isFailure)
        assertIs<FsError.NotDirectory>(result.exceptionOrNull())
    }

    // ═══════════════════════════════════════════════════════════
    // stat
    // ═══════════════════════════════════════════════════════════

    @Test
    fun stat_file() = runTest {
        ops.createFile("/statfile.txt").getOrThrow()
        ops.writeFile("/statfile.txt", 0, "hello".encodeToByteArray()).getOrThrow()
        val meta = ops.stat("/statfile.txt").getOrThrow()
        assertEquals(FsType.FILE, meta.type)
        assertEquals(5L, meta.size)
    }

    @Test
    fun stat_dir() = runTest {
        ops.createDir("/statdir").getOrThrow()
        val meta = ops.stat("/statdir").getOrThrow()
        assertEquals(FsType.DIRECTORY, meta.type)
        assertEquals(0L, meta.size)
    }

    @Test
    fun stat_nonexistent_fails() = runTest {
        val result = ops.stat("/nopath")
        assertTrue(result.isFailure)
        assertIs<FsError.NotFound>(result.exceptionOrNull())
    }

    // ═══════════════════════════════════════════════════════════
    // exists
    // ═══════════════════════════════════════════════════════════

    @Test
    fun exists_true_for_file() = runTest {
        ops.createFile("/e.txt").getOrThrow()
        assertTrue(ops.exists("/e.txt"))
    }

    @Test
    fun exists_true_for_dir() = runTest {
        ops.createDir("/edir").getOrThrow()
        assertTrue(ops.exists("/edir"))
    }

    @Test
    fun exists_false_for_nonexistent() = runTest {
        assertFalse(ops.exists("/nope"))
    }

    // ═══════════════════════════════════════════════════════════
    // 路径越界 (path traversal)
    // ═══════════════════════════════════════════════════════════

    @Test
    fun path_traversal_blocked() = runTest {
        val result = ops.readFile("/../../etc/passwd", 0, 100)
        assertTrue(result.isFailure)
        val error = result.exceptionOrNull()
        assertIs<FsError.PermissionDenied>(error)
    }

    @Test
    fun path_traversal_createFile_blocked() = runTest {
        val result = ops.createFile("/../../../tmp/evil.txt")
        assertTrue(result.isFailure)
        assertIs<FsError.PermissionDenied>(result.exceptionOrNull())
    }

    // ═══════════════════════════════════════════════════════════
    // empty file
    // ═══════════════════════════════════════════════════════════

    @Test
    fun empty_file_read() = runTest {
        ops.createFile("/empty.txt").getOrThrow()
        val data = ops.readFile("/empty.txt", 0, Int.MAX_VALUE).getOrThrow()
        assertEquals(0, data.size)
    }

    // ═══════════════════════════════════════════════════════════
    // archive: ZIP compress + extract
    // ═══════════════════════════════════════════════════════════

    @Test
    fun zip_compress_and_extract() = runTest {
        ops.createFile("/src/a.txt").getOrThrow()
        ops.writeFile("/src/a.txt", 0, "alpha".encodeToByteArray()).getOrThrow()
        ops.createFile("/src/b.txt").getOrThrow()
        ops.writeFile("/src/b.txt", 0, "beta".encodeToByteArray()).getOrThrow()

        ops.compress(listOf("/src"), "/archive.zip", ArchiveFormat.ZIP).getOrThrow()
        assertTrue(ops.exists("/archive.zip"))

        ops.createDir("/out").getOrThrow()
        ops.extract("/archive.zip", "/out", ArchiveFormat.ZIP).getOrThrow()

        val aData = ops.readFile("/out/src/a.txt", 0, Int.MAX_VALUE).getOrThrow()
        assertEquals("alpha", aData.decodeToString())
        val bData = ops.readFile("/out/src/b.txt", 0, Int.MAX_VALUE).getOrThrow()
        assertEquals("beta", bData.decodeToString())
    }

    @Test
    fun zip_list_entries() = runTest {
        ops.createFile("/zlist/file.txt").getOrThrow()
        ops.writeFile("/zlist/file.txt", 0, "data".encodeToByteArray()).getOrThrow()
        ops.compress(listOf("/zlist"), "/list.zip", ArchiveFormat.ZIP).getOrThrow()

        val entries = ops.listArchive("/list.zip", ArchiveFormat.ZIP).getOrThrow()
        assertTrue(entries.isNotEmpty())
        val names = entries.map { it.path }
        assertTrue(names.any { it.contains("file.txt") })
    }

    // ═══════════════════════════════════════════════════════════
    // archive: TAR compress + extract
    // ═══════════════════════════════════════════════════════════

    @Test
    fun tar_compress_and_extract() = runTest {
        ops.createFile("/tsrc/x.txt").getOrThrow()
        ops.writeFile("/tsrc/x.txt", 0, "xray".encodeToByteArray()).getOrThrow()

        ops.compress(listOf("/tsrc"), "/archive.tar", ArchiveFormat.TAR).getOrThrow()
        assertTrue(ops.exists("/archive.tar"))

        ops.createDir("/tout").getOrThrow()
        ops.extract("/archive.tar", "/tout", ArchiveFormat.TAR).getOrThrow()

        val xData = ops.readFile("/tout/tsrc/x.txt", 0, Int.MAX_VALUE).getOrThrow()
        assertEquals("xray", xData.decodeToString())
    }
}
