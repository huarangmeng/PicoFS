package com.hrm.fs.core

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.test.runTest
import kotlin.test.*

class ConcurrencyTest {

    @Test
    fun concurrent_writes_to_different_files() = runTest {
        val fs = createFs()
        val count = 50
        val jobs = (0 until count).map { i ->
            async {
                fs.writeAll("/file_$i.txt", "content_$i".encodeToByteArray()).getOrThrow()
            }
        }
        jobs.awaitAll()

        for (i in 0 until count) {
            val data = fs.readAll("/file_$i.txt").getOrThrow()
            assertEquals("content_$i", data.decodeToString())
        }
    }

    @Test
    fun concurrent_writes_to_same_file() = runTest {
        val fs = createFs()
        fs.createFile("/shared.txt").getOrThrow()
        val count = 50
        val jobs = (0 until count).map { i ->
            async {
                fs.writeAll("/shared.txt", "v$i".encodeToByteArray())
            }
        }
        jobs.awaitAll()

        // 文件应包含某次写入的完整值，不应出现混合数据
        val data = fs.readAll("/shared.txt").getOrThrow().decodeToString()
        assertTrue(data.startsWith("v"), "Data should be one of the written values, got: $data")
    }

    @Test
    fun concurrent_reads_while_writing() = runTest {
        val fs = createFs()
        fs.writeAll("/read_write.txt", "initial".encodeToByteArray()).getOrThrow()

        val count = 50
        val jobs = (0 until count).map { i ->
            async {
                if (i % 2 == 0) {
                    fs.readAll("/read_write.txt").getOrThrow()
                } else {
                    fs.writeAll("/read_write.txt", "updated_$i".encodeToByteArray()).getOrThrow()
                    ByteArray(0)
                }
            }
        }
        val results = jobs.awaitAll()

        // 所有读取结果应是完整值（initial 或某次 updated_X），不应崩溃
        for ((i, data) in results.withIndex()) {
            if (i % 2 == 0 && data.isNotEmpty()) {
                val str = data.decodeToString()
                assertTrue(
                    str == "initial" || str.startsWith("updated_"),
                    "Unexpected read result: $str"
                )
            }
        }
    }

    @Test
    fun concurrent_create_and_delete() = runTest {
        val fs = createFs()
        val count = 30
        val jobs = (0 until count).map { i ->
            async {
                val path = "/cd_$i.txt"
                fs.writeAll(path, "data_$i".encodeToByteArray())
                fs.delete(path)
            }
        }
        jobs.awaitAll()
        // 不应崩溃，所有文件应已删除
        for (i in 0 until count) {
            assertTrue(fs.stat("/cd_$i.txt").isFailure)
        }
    }

    @Test
    fun concurrent_mkdir_recursive() = runTest {
        val fs = createFs()
        val count = 30
        val jobs = (0 until count).map { i ->
            async {
                fs.createDirRecursive("/deep/$i/sub/dir").getOrThrow()
            }
        }
        jobs.awaitAll()

        for (i in 0 until count) {
            val meta = fs.stat("/deep/$i/sub/dir").getOrThrow()
            assertEquals(com.hrm.fs.api.FsType.DIRECTORY, meta.type)
        }
    }

    @Test
    fun concurrent_copy_operations() = runTest {
        val fs = createFs()
        fs.writeAll("/original.txt", "origin_data".encodeToByteArray()).getOrThrow()

        val count = 30
        val jobs = (0 until count).map { i ->
            async {
                fs.copy("/original.txt", "/copy_$i.txt").getOrThrow()
            }
        }
        jobs.awaitAll()

        // 源文件仍在
        assertEquals("origin_data", fs.readAll("/original.txt").getOrThrow().decodeToString())
        // 所有副本正确
        for (i in 0 until count) {
            assertEquals("origin_data", fs.readAll("/copy_$i.txt").getOrThrow().decodeToString())
        }
    }

    @Test
    fun concurrent_stat_operations() = runTest {
        val fs = createFs()
        val fileCount = 20
        for (i in 0 until fileCount) {
            fs.writeAll("/s_$i.txt", "data_$i".encodeToByteArray()).getOrThrow()
        }

        val readCount = 100
        val jobs = (0 until readCount).map { i ->
            async {
                val idx = i % fileCount
                fs.stat("/s_$idx.txt").getOrThrow()
            }
        }
        val results = jobs.awaitAll()
        assertEquals(readCount, results.size)
        for (meta in results) {
            assertEquals(com.hrm.fs.api.FsType.FILE, meta.type)
        }
    }

    @Test
    fun concurrent_readDir_operations() = runTest {
        val fs = createFs()
        for (i in 0 until 10) {
            fs.writeAll("/dir/file_$i.txt", "x".encodeToByteArray()).getOrThrow()
        }

        val jobs = (0 until 50).map {
            async {
                fs.readDir("/dir").getOrThrow()
            }
        }
        val results = jobs.awaitAll()
        for (entries in results) {
            assertEquals(10, entries.size)
        }
    }

    @Test
    fun concurrent_xattr_operations() = runTest {
        val fs = createFs()
        fs.createFile("/xattr_file.txt").getOrThrow()

        val count = 30
        val jobs = (0 until count).map { i ->
            async {
                fs.xattr.set("/xattr_file.txt", "attr_$i", "val_$i".encodeToByteArray()).getOrThrow()
            }
        }
        jobs.awaitAll()

        val attrs = fs.xattr.list("/xattr_file.txt").getOrThrow()
        assertEquals(count, attrs.size)
        for (i in 0 until count) {
            val v = fs.xattr.get("/xattr_file.txt", "attr_$i").getOrThrow()
            assertEquals("val_$i", v.decodeToString())
        }
    }

    @Test
    fun concurrent_mixed_operations_no_crash() = runTest {
        val fs = createFs()
        fs.createDir("/mix").getOrThrow()

        val count = 50
        val jobs = (0 until count).map { i ->
            async {
                when (i % 5) {
                    0 -> fs.writeAll("/mix/f_$i.txt", "d$i".encodeToByteArray())
                    1 -> fs.createDirRecursive("/mix/d_$i/sub")
                    2 -> fs.readDir("/mix")
                    3 -> fs.stat("/mix")
                    4 -> {
                        fs.writeAll("/mix/tmp_$i.txt", "t".encodeToByteArray())
                        fs.delete("/mix/tmp_$i.txt")
                    }
                    else -> Result.success(Unit)
                }
            }
        }
        // 不应抛异常
        jobs.awaitAll()
    }
}
