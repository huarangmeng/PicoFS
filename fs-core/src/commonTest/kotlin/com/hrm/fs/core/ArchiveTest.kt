package com.hrm.fs.core

import com.hrm.fs.api.ArchiveFormat
import com.hrm.fs.api.FsType
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ArchiveTest {

    // ═══════════════════════════════════════════════════════════
    // ZIP 基础测试
    // ═══════════════════════════════════════════════════════════

    @Test
    fun zipCompressSingleFile() = runTest {
        val fs = createFs()
        fs.writeAll("/hello.txt", "Hello, World!".encodeToByteArray())

        val result = fs.archive.compress(listOf("/hello.txt"), "/out.zip", ArchiveFormat.ZIP)
        assertTrue(result.isSuccess, "compress should succeed")

        val archiveData = fs.readAll("/out.zip").getOrThrow()
        assertTrue(archiveData.size > 0, "archive should not be empty")
        // ZIP magic number
        assertEquals(0x50, archiveData[0].toInt() and 0xFF)
        assertEquals(0x4B, archiveData[1].toInt() and 0xFF)
    }

    @Test
    fun zipCompressAndExtract() = runTest {
        val fs = createFs()
        fs.writeAll("/src/a.txt", "AAA".encodeToByteArray())
        fs.writeAll("/src/b.txt", "BBB".encodeToByteArray())

        fs.archive.compress(listOf("/src/a.txt", "/src/b.txt"), "/out.zip", ArchiveFormat.ZIP).getOrThrow()
        fs.archive.extract("/out.zip", "/dst").getOrThrow()

        assertEquals("AAA", fs.readAll("/dst/a.txt").getOrThrow().decodeToString())
        assertEquals("BBB", fs.readAll("/dst/b.txt").getOrThrow().decodeToString())
    }

    @Test
    fun zipCompressDirectory() = runTest {
        val fs = createFs()
        fs.writeAll("/project/src/main.kt", "fun main() {}".encodeToByteArray())
        fs.writeAll("/project/src/util.kt", "object Util {}".encodeToByteArray())
        fs.writeAll("/project/readme.txt", "README".encodeToByteArray())

        fs.archive.compress(listOf("/project"), "/archive.zip", ArchiveFormat.ZIP).getOrThrow()
        fs.archive.extract("/archive.zip", "/extracted").getOrThrow()

        assertEquals("fun main() {}", fs.readAll("/extracted/project/src/main.kt").getOrThrow().decodeToString())
        assertEquals("object Util {}", fs.readAll("/extracted/project/src/util.kt").getOrThrow().decodeToString())
        assertEquals("README", fs.readAll("/extracted/project/readme.txt").getOrThrow().decodeToString())
    }

    @Test
    fun zipList() = runTest {
        val fs = createFs()
        fs.writeAll("/a.txt", "111".encodeToByteArray())
        fs.writeAll("/b.txt", "2222".encodeToByteArray())

        fs.archive.compress(listOf("/a.txt", "/b.txt"), "/out.zip", ArchiveFormat.ZIP).getOrThrow()

        val entries = fs.archive.list("/out.zip").getOrThrow()
        assertEquals(2, entries.size)

        val byName = entries.associateBy { it.path }
        assertEquals(FsType.FILE, byName["a.txt"]?.type)
        assertEquals(3L, byName["a.txt"]?.size)
        assertEquals(FsType.FILE, byName["b.txt"]?.type)
        assertEquals(4L, byName["b.txt"]?.size)
    }

    @Test
    fun zipEmptyFile() = runTest {
        val fs = createFs()
        fs.createFile("/empty.txt")

        fs.archive.compress(listOf("/empty.txt"), "/out.zip", ArchiveFormat.ZIP).getOrThrow()
        fs.archive.extract("/out.zip", "/dst").getOrThrow()

        val data = fs.readAll("/dst/empty.txt").getOrThrow()
        assertEquals(0, data.size)
    }

    @Test
    fun zipAutoDetectFormat() = runTest {
        val fs = createFs()
        fs.writeAll("/test.txt", "content".encodeToByteArray())

        fs.archive.compress(listOf("/test.txt"), "/test.zip", ArchiveFormat.ZIP).getOrThrow()
        // extract with format=null (auto detect)
        fs.archive.extract("/test.zip", "/out", format = null).getOrThrow()

        assertEquals("content", fs.readAll("/out/test.txt").getOrThrow().decodeToString())
    }

    // ═══════════════════════════════════════════════════════════
    // TAR 基础测试
    // ═══════════════════════════════════════════════════════════

    @Test
    fun tarCompressSingleFile() = runTest {
        val fs = createFs()
        fs.writeAll("/hello.txt", "Hello, TAR!".encodeToByteArray())

        val result = fs.archive.compress(listOf("/hello.txt"), "/out.tar", ArchiveFormat.TAR)
        assertTrue(result.isSuccess, "compress should succeed")

        val archiveData = fs.readAll("/out.tar").getOrThrow()
        assertTrue(archiveData.size >= 512, "tar should have at least one block")
    }

    @Test
    fun tarCompressAndExtract() = runTest {
        val fs = createFs()
        fs.writeAll("/src/x.txt", "XXX".encodeToByteArray())
        fs.writeAll("/src/y.txt", "YYY".encodeToByteArray())

        fs.archive.compress(listOf("/src/x.txt", "/src/y.txt"), "/out.tar", ArchiveFormat.TAR).getOrThrow()
        fs.archive.extract("/out.tar", "/dst", ArchiveFormat.TAR).getOrThrow()

        assertEquals("XXX", fs.readAll("/dst/x.txt").getOrThrow().decodeToString())
        assertEquals("YYY", fs.readAll("/dst/y.txt").getOrThrow().decodeToString())
    }

    @Test
    fun tarCompressDirectory() = runTest {
        val fs = createFs()
        fs.writeAll("/proj/code.kt", "val x = 1".encodeToByteArray())
        fs.writeAll("/proj/data/info.json", "{}".encodeToByteArray())

        fs.archive.compress(listOf("/proj"), "/archive.tar", ArchiveFormat.TAR).getOrThrow()
        fs.archive.extract("/archive.tar", "/out", ArchiveFormat.TAR).getOrThrow()

        assertEquals("val x = 1", fs.readAll("/out/proj/code.kt").getOrThrow().decodeToString())
        assertEquals("{}", fs.readAll("/out/proj/data/info.json").getOrThrow().decodeToString())
    }

    @Test
    fun tarList() = runTest {
        val fs = createFs()
        fs.writeAll("/a.txt", "hello".encodeToByteArray())
        fs.createDir("/dir")

        fs.archive.compress(listOf("/a.txt", "/dir"), "/out.tar", ArchiveFormat.TAR).getOrThrow()

        val entries = fs.archive.list("/out.tar", ArchiveFormat.TAR).getOrThrow()
        assertTrue(entries.size >= 2)
        assertTrue(entries.any { it.path == "a.txt" && it.type == FsType.FILE })
        assertTrue(entries.any { it.path == "dir" && it.type == FsType.DIRECTORY })
    }

    @Test
    fun tarAutoDetectFormat() = runTest {
        val fs = createFs()
        fs.writeAll("/test.txt", "tar content".encodeToByteArray())

        fs.archive.compress(listOf("/test.txt"), "/test.tar", ArchiveFormat.TAR).getOrThrow()
        // extract with format=null (auto detect)
        fs.archive.extract("/test.tar", "/out", format = null).getOrThrow()

        assertEquals("tar content", fs.readAll("/out/test.txt").getOrThrow().decodeToString())
    }

    // ═══════════════════════════════════════════════════════════
    // 多源路径打包
    // ═══════════════════════════════════════════════════════════

    @Test
    fun compressMultipleSources() = runTest {
        val fs = createFs()
        fs.writeAll("/a.txt", "A".encodeToByteArray())
        fs.writeAll("/dir/b.txt", "B".encodeToByteArray())

        fs.archive.compress(listOf("/a.txt", "/dir"), "/out.zip", ArchiveFormat.ZIP).getOrThrow()

        val entries = fs.archive.list("/out.zip").getOrThrow()
        assertTrue(entries.any { it.path == "a.txt" })
        assertTrue(entries.any { it.path == "dir" })
        assertTrue(entries.any { it.path == "dir/b.txt" })

        fs.archive.extract("/out.zip", "/result").getOrThrow()
        assertEquals("A", fs.readAll("/result/a.txt").getOrThrow().decodeToString())
        assertEquals("B", fs.readAll("/result/dir/b.txt").getOrThrow().decodeToString())
    }

    // ═══════════════════════════════════════════════════════════
    // 大文件 / 二进制内容
    // ═══════════════════════════════════════════════════════════

    @Test
    fun zipBinaryContent() = runTest {
        val fs = createFs()
        val binaryData = ByteArray(1024) { (it % 256).toByte() }
        fs.writeAll("/bin.dat", binaryData)

        fs.archive.compress(listOf("/bin.dat"), "/out.zip", ArchiveFormat.ZIP).getOrThrow()
        fs.archive.extract("/out.zip", "/dst").getOrThrow()

        val restored = fs.readAll("/dst/bin.dat").getOrThrow()
        assertTrue(binaryData.contentEquals(restored), "binary content should match")
    }

    @Test
    fun tarBinaryContent() = runTest {
        val fs = createFs()
        val binaryData = ByteArray(2000) { ((it * 7) % 256).toByte() }
        fs.writeAll("/bin.dat", binaryData)

        fs.archive.compress(listOf("/bin.dat"), "/out.tar", ArchiveFormat.TAR).getOrThrow()
        fs.archive.extract("/out.tar", "/dst", ArchiveFormat.TAR).getOrThrow()

        val restored = fs.readAll("/dst/bin.dat").getOrThrow()
        assertTrue(binaryData.contentEquals(restored), "binary content should match")
    }

    // ═══════════════════════════════════════════════════════════
    // 错误场景
    // ═══════════════════════════════════════════════════════════

    @Test
    fun extractNonExistentArchive() = runTest {
        val fs = createFs()
        val result = fs.archive.extract("/no-such-file.zip", "/dst")
        assertTrue(result.isFailure, "extract non-existent file should fail")
    }

    @Test
    fun compressNonExistentSource() = runTest {
        val fs = createFs()
        val result = fs.archive.compress(listOf("/no-such-file.txt"), "/out.zip", ArchiveFormat.ZIP)
        assertTrue(result.isFailure, "compress non-existent source should fail")
    }

    @Test
    fun extractInvalidArchive() = runTest {
        val fs = createFs()
        fs.writeAll("/bad.zip", "this is not a zip".encodeToByteArray())
        val result = fs.archive.extract("/bad.zip", "/dst")
        assertTrue(result.isFailure, "extract invalid archive should fail")
    }

    // ═══════════════════════════════════════════════════════════
    // ZIP 与 TAR 互不混淆
    // ═══════════════════════════════════════════════════════════

    @Test
    fun zipAndTarProduceDifferentOutput() = runTest {
        val fs = createFs()
        fs.writeAll("/test.txt", "same content".encodeToByteArray())

        fs.archive.compress(listOf("/test.txt"), "/out.zip", ArchiveFormat.ZIP).getOrThrow()
        fs.archive.compress(listOf("/test.txt"), "/out.tar", ArchiveFormat.TAR).getOrThrow()

        val zipData = fs.readAll("/out.zip").getOrThrow()
        val tarData = fs.readAll("/out.tar").getOrThrow()

        // 两者格式不同，数据不同
        assertTrue(!zipData.contentEquals(tarData), "ZIP and TAR should differ")

        // 但解压后内容一致
        fs.archive.extract("/out.zip", "/zip-out").getOrThrow()
        fs.archive.extract("/out.tar", "/tar-out").getOrThrow()
        assertEquals(
            fs.readAll("/zip-out/test.txt").getOrThrow().decodeToString(),
            fs.readAll("/tar-out/test.txt").getOrThrow().decodeToString()
        )
    }

    // ═══════════════════════════════════════════════════════════
    // 嵌套目录结构
    // ═══════════════════════════════════════════════════════════

    @Test
    fun deepNestedDirectoryZip() = runTest {
        val fs = createFs()
        fs.writeAll("/root/a/b/c/d.txt", "deep".encodeToByteArray())

        fs.archive.compress(listOf("/root"), "/nested.zip", ArchiveFormat.ZIP).getOrThrow()
        fs.archive.extract("/nested.zip", "/out").getOrThrow()

        assertEquals("deep", fs.readAll("/out/root/a/b/c/d.txt").getOrThrow().decodeToString())
    }

    @Test
    fun deepNestedDirectoryTar() = runTest {
        val fs = createFs()
        fs.writeAll("/root/a/b/c/d.txt", "deep".encodeToByteArray())

        fs.archive.compress(listOf("/root"), "/nested.tar", ArchiveFormat.TAR).getOrThrow()
        fs.archive.extract("/nested.tar", "/out", ArchiveFormat.TAR).getOrThrow()

        assertEquals("deep", fs.readAll("/out/root/a/b/c/d.txt").getOrThrow().decodeToString())
    }
}
