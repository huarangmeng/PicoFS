package com.hrm.fs.core

import kotlinx.coroutines.test.runTest
import kotlin.test.*

class CopyMoveTest {

    @Test
    fun copy_file() = runTest {
        val fs = createFs()
        fs.writeAll("/src.txt", "content".encodeToByteArray()).getOrThrow()
        fs.copy("/src.txt", "/dst.txt").getOrThrow()
        assertEquals("content", fs.readAll("/dst.txt").getOrThrow().decodeToString())
        // 源文件仍在
        assertTrue(fs.stat("/src.txt").isSuccess)
    }

    @Test
    fun copy_directory() = runTest {
        val fs = createFs()
        fs.createDirRecursive("/src/sub").getOrThrow()
        fs.writeAll("/src/a.txt", "aaa".encodeToByteArray()).getOrThrow()
        fs.writeAll("/src/sub/b.txt", "bbb".encodeToByteArray()).getOrThrow()

        fs.copy("/src", "/dst").getOrThrow()

        assertEquals("aaa", fs.readAll("/dst/a.txt").getOrThrow().decodeToString())
        assertEquals("bbb", fs.readAll("/dst/sub/b.txt").getOrThrow().decodeToString())
    }

    @Test
    fun move_file() = runTest {
        val fs = createFs()
        fs.writeAll("/src.txt", "content".encodeToByteArray()).getOrThrow()
        fs.move("/src.txt", "/dst.txt").getOrThrow()
        assertEquals("content", fs.readAll("/dst.txt").getOrThrow().decodeToString())
        // 源文件已删
        assertTrue(fs.stat("/src.txt").isFailure)
    }

    @Test
    fun rename_alias() = runTest {
        val fs = createFs()
        fs.writeAll("/old.txt", "data".encodeToByteArray()).getOrThrow()
        fs.rename("/old.txt", "/new.txt").getOrThrow()
        assertEquals("data", fs.readAll("/new.txt").getOrThrow().decodeToString())
        assertTrue(fs.stat("/old.txt").isFailure)
    }
}
