package com.hrm.fs.core

import com.hrm.fs.api.*
import kotlinx.coroutines.test.runTest
import kotlin.test.*

class BasicCrudTest {

    @Test
    fun createFile_and_stat() = runTest {
        val fs = createFs()
        fs.createDir("/docs").getOrThrow()
        fs.createFile("/docs/hello.txt").getOrThrow()
        val meta = fs.stat("/docs/hello.txt").getOrThrow()
        assertEquals(FsType.FILE, meta.type)
        assertEquals(0L, meta.size)
    }

    @Test
    fun createFile_in_missing_parent_fails() = runTest {
        val fs = createFs()
        val result = fs.createFile("/missing/hello.txt")
        assertTrue(result.isFailure)
        assertIs<FsError.NotFound>(result.exceptionOrNull())
    }

    @Test
    fun createFile_already_exists() = runTest {
        val fs = createFs()
        fs.createDir("/docs").getOrThrow()
        fs.createFile("/docs/a.txt").getOrThrow()
        val result = fs.createFile("/docs/a.txt")
        assertTrue(result.isFailure)
        assertIs<FsError.AlreadyExists>(result.exceptionOrNull())
    }

    @Test
    fun createDir_and_readDir() = runTest {
        val fs = createFs()
        fs.createDir("/a").getOrThrow()
        fs.createFile("/a/f1.txt").getOrThrow()
        fs.createFile("/a/f2.txt").getOrThrow()
        val entries = fs.readDir("/a").getOrThrow()
        assertEquals(2, entries.size)
        assertTrue(entries.any { it.name == "f1.txt" })
        assertTrue(entries.any { it.name == "f2.txt" })
    }

    @Test
    fun createDir_root_is_noop() = runTest {
        val fs = createFs()
        val result = fs.createDir("/")
        assertTrue(result.isSuccess)
    }

    @Test
    fun readDir_root() = runTest {
        val fs = createFs()
        fs.createDir("/a").getOrThrow()
        fs.createDir("/b").getOrThrow()
        val entries = fs.readDir("/").getOrThrow()
        assertEquals(2, entries.size)
    }

    @Test
    fun delete_file() = runTest {
        val fs = createFs()
        fs.createDir("/d").getOrThrow()
        fs.createFile("/d/f.txt").getOrThrow()
        fs.delete("/d/f.txt").getOrThrow()
        val result = fs.stat("/d/f.txt")
        assertTrue(result.isFailure)
    }

    @Test
    fun delete_nonempty_dir_fails() = runTest {
        val fs = createFs()
        fs.createDir("/d").getOrThrow()
        fs.createFile("/d/f.txt").getOrThrow()
        val result = fs.delete("/d")
        assertTrue(result.isFailure)
    }

    @Test
    fun delete_root_fails() = runTest {
        val fs = createFs()
        val result = fs.delete("/")
        assertTrue(result.isFailure)
    }

    @Test
    fun open_read_write() = runTest {
        val fs = createFs()
        fs.createDir("/d").getOrThrow()
        fs.createFile("/d/f.txt").getOrThrow()

        val writeHandle = fs.open("/d/f.txt", OpenMode.WRITE).getOrThrow()
        writeHandle.writeAt(0, "Hello".encodeToByteArray()).getOrThrow()
        writeHandle.close()

        val readHandle = fs.open("/d/f.txt", OpenMode.READ).getOrThrow()
        val data = readHandle.readAt(0, 100).getOrThrow()
        readHandle.close()

        assertEquals("Hello", data.decodeToString())
    }

    @Test
    fun open_notFound() = runTest {
        val fs = createFs()
        val result = fs.open("/missing.txt", OpenMode.READ)
        assertTrue(result.isFailure)
        assertIs<FsError.NotFound>(result.exceptionOrNull())
    }

    @Test
    fun stat_notFound() = runTest {
        val fs = createFs()
        val result = fs.stat("/nope")
        assertTrue(result.isFailure)
        assertIs<FsError.NotFound>(result.exceptionOrNull())
    }

    // ═════════════════════════════════════════════════════════════
    // 递归操作
    // ═════════════════════════════════════════════════════════════

    @Test
    fun createDirRecursive() = runTest {
        val fs = createFs()
        fs.createDirRecursive("/a/b/c/d").getOrThrow()
        val meta = fs.stat("/a/b/c/d").getOrThrow()
        assertEquals(FsType.DIRECTORY, meta.type)
    }

    @Test
    fun createDirRecursive_already_exists() = runTest {
        val fs = createFs()
        fs.createDirRecursive("/a/b").getOrThrow()
        // 重复调用不报错
        val result = fs.createDirRecursive("/a/b")
        assertTrue(result.isSuccess)
    }

    @Test
    fun deleteRecursive() = runTest {
        val fs = createFs()
        fs.createDirRecursive("/a/b/c").getOrThrow()
        fs.createFile("/a/b/c/f.txt").getOrThrow()
        fs.createFile("/a/b/g.txt").getOrThrow()
        fs.deleteRecursive("/a").getOrThrow()
        val result = fs.stat("/a")
        assertTrue(result.isFailure)
    }

    @Test
    fun deleteRecursive_single_file() = runTest {
        val fs = createFs()
        fs.createDir("/d").getOrThrow()
        fs.createFile("/d/f.txt").getOrThrow()
        fs.deleteRecursive("/d/f.txt").getOrThrow()
        assertTrue(fs.stat("/d/f.txt").isFailure)
        // 目录仍在
        assertTrue(fs.stat("/d").isSuccess)
    }

    @Test
    fun deleteRecursive_root_fails() = runTest {
        val fs = createFs()
        val result = fs.deleteRecursive("/")
        assertTrue(result.isFailure)
    }

    // ═════════════════════════════════════════════════════════════
    // 便捷读写
    // ═════════════════════════════════════════════════════════════

    @Test
    fun readAll_writeAll() = runTest {
        val fs = createFs()
        val text = "Hello PicoFS writeAll/readAll"
        fs.writeAll("/auto/file.txt", text.encodeToByteArray()).getOrThrow()
        val data = fs.readAll("/auto/file.txt").getOrThrow()
        assertEquals(text, data.decodeToString())
    }

    @Test
    fun writeAll_creates_parents() = runTest {
        val fs = createFs()
        fs.writeAll("/x/y/z/f.txt", "data".encodeToByteArray()).getOrThrow()
        val meta = fs.stat("/x/y/z").getOrThrow()
        assertEquals(FsType.DIRECTORY, meta.type)
    }

    @Test
    fun readAll_notFound() = runTest {
        val fs = createFs()
        val result = fs.readAll("/nope.txt")
        assertTrue(result.isFailure)
    }

    @Test
    fun writeAll_overwrite() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "first".encodeToByteArray()).getOrThrow()
        fs.writeAll("/f.txt", "second".encodeToByteArray()).getOrThrow()
        val data = fs.readAll("/f.txt").getOrThrow()
        assertEquals("second", data.decodeToString())
    }

    // ═════════════════════════════════════════════════════════════
    // 边界情况
    // ═════════════════════════════════════════════════════════════

    @Test
    fun readAt_beyond_eof_returns_empty() = runTest {
        val fs = createFs()
        fs.createDir("/d").getOrThrow()
        fs.createFile("/d/f.txt").getOrThrow()
        // 文件为空，偏移量超出
        val handle = fs.open("/d/f.txt", OpenMode.READ).getOrThrow()
        val data = handle.readAt(100, 10).getOrThrow()
        assertTrue(data.isEmpty())
        handle.close()
    }

    @Test
    fun writeAt_with_offset() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "AAAA".encodeToByteArray()).getOrThrow()

        val handle = fs.open("/f.txt", OpenMode.WRITE).getOrThrow()
        handle.writeAt(2, "BB".encodeToByteArray()).getOrThrow()
        handle.close()

        val data = fs.readAll("/f.txt").getOrThrow().decodeToString()
        assertEquals("AABB", data)
    }

    @Test
    fun stat_file_size() = runTest {
        val fs = createFs()
        val content = "twelve bytes"
        fs.writeAll("/sized.txt", content.encodeToByteArray()).getOrThrow()
        val meta = fs.stat("/sized.txt").getOrThrow()
        assertEquals(content.length.toLong(), meta.size)
    }

    @Test
    fun open_dir_as_file_fails() = runTest {
        val fs = createFs()
        fs.createDir("/d").getOrThrow()
        val result = fs.open("/d", OpenMode.READ)
        assertTrue(result.isFailure)
        assertIs<FsError.NotFile>(result.exceptionOrNull())
    }
}
