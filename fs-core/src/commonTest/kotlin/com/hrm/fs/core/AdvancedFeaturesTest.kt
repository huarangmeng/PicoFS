package com.hrm.fs.core

import com.hrm.fs.api.*
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.test.runTest
import kotlin.test.*

class AdvancedFeaturesTest {

    // ═══════════════════════════════════════════════════════════
    // 流式读写
    // ═══════════════════════════════════════════════════════════

    @Test
    fun readStream_chunked() = runTest {
        val fs = createFs()
        val content = "ABCDEFGHIJKLMNOP" // 16 bytes
        fs.writeAll("/big.txt", content.encodeToByteArray()).getOrThrow()

        val chunks = fs.streams.read("/big.txt", chunkSize = 5).toList()
        // 16 bytes / 5 = 4 chunks (5, 5, 5, 1)
        assertEquals(4, chunks.size)
        assertEquals("ABCDE", chunks[0].decodeToString())
        assertEquals("FGHIJ", chunks[1].decodeToString())
        assertEquals("KLMNO", chunks[2].decodeToString())
        assertEquals("P", chunks[3].decodeToString())
    }

    @Test
    fun writeStream_from_flow() = runTest {
        val fs = createFs()
        val dataFlow = flowOf(
            "Hello ".encodeToByteArray(),
            "World".encodeToByteArray()
        )
        fs.streams.write("/stream.txt", dataFlow).getOrThrow()
        val content = fs.readAll("/stream.txt").getOrThrow().decodeToString()
        assertEquals("Hello World", content)
    }

    // ═══════════════════════════════════════════════════════════
    // WAL 持久化
    // ═══════════════════════════════════════════════════════════

    @Test
    fun persistence_basic() = runTest {
        val storage = InMemoryFsStorage()

        // 写入数据
        val fs1 = createFs(storage)
        fs1.createDir("/data").getOrThrow()
        fs1.createFile("/data/file.txt").getOrThrow()
        val handle = fs1.open("/data/file.txt", OpenMode.WRITE).getOrThrow()
        handle.writeAt(0, "persisted".encodeToByteArray()).getOrThrow()
        handle.close()

        // 使用相同 storage 创建新的 fs，应恢复数据
        val fs2 = createFs(storage)
        val meta = fs2.stat("/data/file.txt").getOrThrow()
        assertEquals(FsType.FILE, meta.type)
        val data = fs2.readAll("/data/file.txt").getOrThrow()
        assertEquals("persisted", data.decodeToString())
    }

    @Test
    fun persistence_delete_replayed() = runTest {
        val storage = InMemoryFsStorage()

        val fs1 = createFs(storage)
        fs1.createDir("/d").getOrThrow()
        fs1.createFile("/d/f.txt").getOrThrow()
        fs1.delete("/d/f.txt").getOrThrow()

        val fs2 = createFs(storage)
        assertTrue(fs2.stat("/d/f.txt").isFailure)
        assertTrue(fs2.stat("/d").isSuccess)
    }

    // ═══════════════════════════════════════════════════════════
    // metrics
    // ═══════════════════════════════════════════════════════════

    @Test
    fun metrics_counts_operations() = runTest {
        val fs = createFs()
        fs.createDir("/a").getOrThrow()
        fs.createFile("/a/f.txt").getOrThrow()
        fs.stat("/a/f.txt").getOrThrow()
        fs.readDir("/a").getOrThrow()
        fs.delete("/a/f.txt").getOrThrow()

        val m = fs.observe.metrics()
        assertEquals(1L, m.createDir.count)
        assertEquals(1L, m.createDir.successCount)
        assertEquals(1L, m.createFile.count)
        assertEquals(1L, m.stat.count)
        assertEquals(1L, m.readDir.count)
        assertEquals(1L, m.delete.count)
    }

    @Test
    fun metrics_tracks_bytes() = runTest {
        val fs = createFs()
        val data = "hello".encodeToByteArray()
        fs.writeAll("/f.txt", data).getOrThrow()
        fs.readAll("/f.txt").getOrThrow()

        val m = fs.observe.metrics()
        assertEquals(data.size.toLong(), m.totalBytesWritten)
        assertEquals(data.size.toLong(), m.totalBytesRead)
    }

    @Test
    fun metrics_tracks_failures() = runTest {
        val fs = createFs()
        // stat 不存在的路径
        fs.stat("/no-such")
        // delete 不存在的路径
        fs.delete("/no-such")

        val m = fs.observe.metrics()
        assertEquals(1L, m.stat.failureCount)
        assertEquals(1L, m.delete.failureCount)
    }

    @Test
    fun metrics_reset() = runTest {
        val fs = createFs()
        fs.createDir("/a").getOrThrow()
        assertTrue(fs.observe.metrics().createDir.count > 0)

        fs.observe.resetMetrics()
        val m = fs.observe.metrics()
        assertEquals(0L, m.createDir.count)
        assertEquals(0L, m.totalBytesRead)
        assertEquals(0L, m.totalBytesWritten)
    }

    // ═══════════════════════════════════════════════════════════
    // 缓存策略
    // ═══════════════════════════════════════════════════════════

    @Test
    fun cache_stat_returns_consistent_result_for_mount() = runTest {
        val fs = createFs()
        val diskOps = FakeDiskFileOperations()
        fs.mounts.mount("/mnt", diskOps).getOrThrow()
        diskOps.files["/hello.txt"] = "world".encodeToByteArray()

        // 第一次 stat 从磁盘读取
        val meta1 = fs.stat("/mnt/hello.txt").getOrThrow()
        assertEquals(FsType.FILE, meta1.type)
        assertEquals(5L, meta1.size)

        // 第二次 stat 应该来自缓存，结果一致
        val meta2 = fs.stat("/mnt/hello.txt").getOrThrow()
        assertEquals(meta1.size, meta2.size)
        assertEquals(meta1.type, meta2.type)
    }

    @Test
    fun cache_readDir_returns_consistent_result_for_mount() = runTest {
        val fs = createFs()
        val diskOps = FakeDiskFileOperations()
        fs.mounts.mount("/mnt", diskOps).getOrThrow()
        diskOps.files["/a.txt"] = ByteArray(0)
        diskOps.files["/b.txt"] = ByteArray(0)

        val list1 = fs.readDir("/mnt").getOrThrow()
        assertEquals(2, list1.size)

        val list2 = fs.readDir("/mnt").getOrThrow()
        assertEquals(list1.size, list2.size)
    }

    @Test
    fun cache_invalidated_on_createFile() = runTest {
        val fs = createFs()
        val diskOps = FakeDiskFileOperations()
        fs.mounts.mount("/mnt", diskOps).getOrThrow()

        // 填充 readDir 缓存
        val list1 = fs.readDir("/mnt").getOrThrow()
        assertEquals(0, list1.size)

        // 创建文件后缓存应失效，返回新结果
        fs.createFile("/mnt/new.txt").getOrThrow()
        val list2 = fs.readDir("/mnt").getOrThrow()
        assertEquals(1, list2.size)
        assertTrue(list2.any { it.name == "new.txt" })
    }

    @Test
    fun cache_invalidated_on_delete() = runTest {
        val fs = createFs()
        val diskOps = FakeDiskFileOperations()
        fs.mounts.mount("/mnt", diskOps).getOrThrow()
        fs.createFile("/mnt/f.txt").getOrThrow()

        // 填充 stat 缓存
        fs.stat("/mnt/f.txt").getOrThrow()

        // 删除后 stat 应从磁盘重新读取（文件不存在了）
        fs.delete("/mnt/f.txt").getOrThrow()
        val result = fs.stat("/mnt/f.txt")
        assertTrue(result.isFailure)
    }

    @Test
    fun cache_cleared_on_unmount() = runTest {
        val fs = createFs()
        val diskOps = FakeDiskFileOperations()
        fs.mounts.mount("/mnt", diskOps).getOrThrow()
        diskOps.files["/f.txt"] = "data".encodeToByteArray()

        // 填充缓存
        fs.stat("/mnt/f.txt").getOrThrow()
        fs.readDir("/mnt").getOrThrow()

        // unmount 后再 mount，缓存应已清空
        fs.mounts.unmount("/mnt").getOrThrow()
        fs.mounts.mount("/mnt", diskOps).getOrThrow()

        // 修改磁盘数据
        diskOps.files["/f.txt"] = "updated-data".encodeToByteArray()

        // 应该返回更新后的数据（非旧缓存）
        val meta = fs.stat("/mnt/f.txt").getOrThrow()
        assertEquals("updated-data".length.toLong(), meta.size)
    }

    // ═══════════════════════════════════════════════════════════
    // 大文件分块存储
    // ═══════════════════════════════════════════════════════════

    @Test
    fun large_file_write_and_read() = runTest {
        val fs = createFs()
        // 写入 200KB 数据（超过默认 64KB block 大小，跨多个 block）
        val size = 200 * 1024
        val data = ByteArray(size) { (it % 256).toByte() }
        fs.writeAll("/big.bin", data).getOrThrow()

        val readBack = fs.readAll("/big.bin").getOrThrow()
        assertEquals(size, readBack.size)
        assertTrue(data.contentEquals(readBack))
    }

    @Test
    fun large_file_partial_read() = runTest {
        val fs = createFs()
        val size = 150 * 1024
        val data = ByteArray(size) { (it % 256).toByte() }
        fs.writeAll("/big.bin", data).getOrThrow()

        // 从中间读取跨越 block 边界的数据
        val handle = fs.open("/big.bin", OpenMode.READ).getOrThrow()
        val offset = 60 * 1024L  // block 0 的末尾附近
        val length = 10 * 1024   // 跨越 block 0 → block 1
        val chunk = handle.readAt(offset, length).getOrThrow()
        handle.close()

        assertEquals(length, chunk.size)
        val expected = data.copyOfRange(offset.toInt(), offset.toInt() + length)
        assertTrue(expected.contentEquals(chunk))
    }

    @Test
    fun large_file_overwrite_middle() = runTest {
        val fs = createFs()
        val size = 128 * 1024
        val data = ByteArray(size) { 0 }
        fs.writeAll("/f.bin", data).getOrThrow()

        // 在中间写入一段数据
        val handle = fs.open("/f.bin", OpenMode.WRITE).getOrThrow()
        val patch = ByteArray(1024) { 0xFF.toByte() }
        handle.writeAt(64 * 1024L - 512, patch).getOrThrow()  // 跨越 block 边界
        handle.close()

        val readBack = fs.readAll("/f.bin").getOrThrow()
        assertEquals(size, readBack.size)
        // 验证 patch 区域
        for (i in patch.indices) {
            assertEquals(0xFF.toByte(), readBack[64 * 1024 - 512 + i], "byte at ${64 * 1024 - 512 + i}")
        }
        // 验证 patch 前后仍为 0
        assertEquals(0, readBack[0])
        assertEquals(0, readBack[size - 1])
    }

    @Test
    fun large_file_stream_read() = runTest {
        val fs = createFs()
        val size = 200 * 1024
        val data = ByteArray(size) { (it % 256).toByte() }
        fs.writeAll("/big.bin", data).getOrThrow()

        // 用 readStream 分块读取
        val chunks = fs.streams.read("/big.bin", chunkSize = 32 * 1024).toList()
        // 200KB / 32KB = 6.25 → 7 chunks
        assertEquals(7, chunks.size)

        // 拼回来检查完整性
        val reassembled = ByteArray(size)
        var offset = 0
        for (chunk in chunks) {
            chunk.copyInto(reassembled, offset)
            offset += chunk.size
        }
        assertEquals(size, offset)
        assertTrue(data.contentEquals(reassembled))
    }

    // ═══════════════════════════════════════════════════════════
    // 磁盘空间配额
    // ═══════════════════════════════════════════════════════════

    @Test
    fun quota_info_no_limit() = runTest {
        val fs = createFs()
        val info = fs.observe.quotaInfo()
        assertFalse(info.hasQuota)
        assertEquals(-1L, info.quotaBytes)
        assertEquals(Long.MAX_VALUE, info.availableBytes)
    }

    @Test
    fun quota_info_tracks_usage() = runTest {
        val fs = createFsWithQuota(1024)
        val info1 = fs.observe.quotaInfo()
        assertEquals(1024L, info1.quotaBytes)
        assertEquals(0L, info1.usedBytes)
        assertEquals(1024L, info1.availableBytes)

        fs.writeAll("/f.txt", ByteArray(100)).getOrThrow()
        val info2 = fs.observe.quotaInfo()
        assertEquals(100L, info2.usedBytes)
        assertEquals(924L, info2.availableBytes)
    }

    @Test
    fun quota_blocks_write_when_exceeded() = runTest {
        val fs = createFsWithQuota(100)
        fs.writeAll("/f.txt", ByteArray(50)).getOrThrow()

        // 写入 60 字节会使总量达到 110，超出 100 的配额
        val result = fs.writeAll("/g.txt", ByteArray(60))
        assertTrue(result.isFailure)
        assertIs<FsError.QuotaExceeded>(result.exceptionOrNull())
    }

    @Test
    fun quota_allows_overwrite_within_limit() = runTest {
        val fs = createFsWithQuota(100)
        fs.writeAll("/f.txt", ByteArray(80)).getOrThrow()

        // 覆盖写入，文件大小不变（80 → 80），不应该超出配额
        fs.writeAll("/f.txt", ByteArray(80)).getOrThrow()
        assertEquals(80L, fs.observe.quotaInfo().usedBytes)
    }

    @Test
    fun quota_freed_on_delete() = runTest {
        val fs = createFsWithQuota(200)
        fs.writeAll("/a.txt", ByteArray(100)).getOrThrow()
        assertEquals(100L, fs.observe.quotaInfo().usedBytes)

        fs.delete("/a.txt").getOrThrow()
        assertEquals(0L, fs.observe.quotaInfo().usedBytes)
        assertEquals(200L, fs.observe.quotaInfo().availableBytes)

        // 删除后空间释放，可以写入新文件
        fs.writeAll("/b.txt", ByteArray(180)).getOrThrow()
        assertEquals(180L, fs.observe.quotaInfo().usedBytes)
    }

    @Test
    fun quota_no_limit_allows_large_write() = runTest {
        val fs = createFs()  // 无配额限制
        // 写入较大数据不应报错
        fs.writeAll("/big.bin", ByteArray(1024 * 1024)).getOrThrow()
        assertTrue(fs.observe.quotaInfo().usedBytes >= 1024 * 1024)
    }

    // ═══════════════════════════════════════════════════════════
    // 文件哈希 / 校验
    // ═══════════════════════════════════════════════════════════

    @Test
    fun checksum_crc32() = runTest {
        val fs = createFs()
        val content = "Hello PicoFS"
        fs.writeAll("/f.txt", content.encodeToByteArray()).getOrThrow()

        val hash = fs.checksum.compute("/f.txt", ChecksumAlgorithm.CRC32).getOrThrow()
        // CRC32 应返回 8 位十六进制字符串
        assertEquals(8, hash.length)
        assertTrue(hash.all { it in '0'..'9' || it in 'a'..'f' })
    }

    @Test
    fun checksum_sha256() = runTest {
        val fs = createFs()
        val content = "Hello PicoFS"
        fs.writeAll("/f.txt", content.encodeToByteArray()).getOrThrow()

        val hash = fs.checksum.compute("/f.txt", ChecksumAlgorithm.SHA256).getOrThrow()
        // SHA-256 应返回 64 位十六进制字符串
        assertEquals(64, hash.length)
        assertTrue(hash.all { it in '0'..'9' || it in 'a'..'f' })
    }

    @Test
    fun checksum_same_content_same_hash() = runTest {
        val fs = createFs()
        val content = "identical content".encodeToByteArray()
        fs.writeAll("/a.txt", content).getOrThrow()
        fs.writeAll("/b.txt", content).getOrThrow()

        val hashA = fs.checksum.compute("/a.txt").getOrThrow()
        val hashB = fs.checksum.compute("/b.txt").getOrThrow()
        assertEquals(hashA, hashB)
    }

    @Test
    fun checksum_different_content_different_hash() = runTest {
        val fs = createFs()
        fs.writeAll("/a.txt", "aaa".encodeToByteArray()).getOrThrow()
        fs.writeAll("/b.txt", "bbb".encodeToByteArray()).getOrThrow()

        val hashA = fs.checksum.compute("/a.txt").getOrThrow()
        val hashB = fs.checksum.compute("/b.txt").getOrThrow()
        assertNotEquals(hashA, hashB)
    }

    @Test
    fun checksum_empty_file() = runTest {
        val fs = createFs()
        fs.createFile("/empty.txt").getOrThrow()

        val hash = fs.checksum.compute("/empty.txt").getOrThrow()
        assertEquals(64, hash.length) // SHA-256 默认
    }

    @Test
    fun checksum_not_found() = runTest {
        val fs = createFs()
        val result = fs.checksum.compute("/missing.txt")
        assertTrue(result.isFailure)
        assertIs<FsError.NotFound>(result.exceptionOrNull())
    }

    @Test
    fun checksum_on_directory_fails() = runTest {
        val fs = createFs()
        fs.createDir("/d").getOrThrow()
        val result = fs.checksum.compute("/d")
        assertTrue(result.isFailure)
        assertIs<FsError.NotFile>(result.exceptionOrNull())
    }

    @Test
    fun checksum_sha256_known_value() = runTest {
        // 验证 SHA-256 实现正确性：空字节数组的 SHA-256
        val fs = createFs()
        fs.createFile("/empty.txt").getOrThrow()
        val hash = fs.checksum.compute("/empty.txt", ChecksumAlgorithm.SHA256).getOrThrow()
        // SHA-256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        assertEquals("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", hash)
    }

    @Test
    fun checksum_crc32_known_value() = runTest {
        val fs = createFs()
        fs.createFile("/empty.txt").getOrThrow()
        val hash = fs.checksum.compute("/empty.txt", ChecksumAlgorithm.CRC32).getOrThrow()
        // CRC32("") = 00000000
        assertEquals("00000000", hash)
    }
}
