package com.hrm.fs.core

import com.hrm.fs.api.FsType
import com.hrm.fs.api.SearchQuery
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class SearchTest {

    // ═════════════════════════════════════════════════════════════
    // 按文件名搜索（类 find）
    // ═════════════════════════════════════════════════════════════

    @Test
    fun `find by exact name`() = runTest {
        val fs = createFs()
        fs.writeAll("/docs/readme.txt", "hello".encodeToByteArray())
        fs.writeAll("/docs/notes.txt", "world".encodeToByteArray())
        fs.writeAll("/src/main.kt", "code".encodeToByteArray())

        val results = fs.search.find(SearchQuery(namePattern = "readme.txt")).getOrThrow()
        assertEquals(1, results.size)
        assertEquals("/docs/readme.txt", results[0].path)
        assertEquals(FsType.FILE, results[0].type)
    }

    @Test
    fun `find by glob wildcard star`() = runTest {
        val fs = createFs()
        fs.writeAll("/a.txt", "1".encodeToByteArray())
        fs.writeAll("/b.txt", "2".encodeToByteArray())
        fs.writeAll("/c.md", "3".encodeToByteArray())
        fs.writeAll("/dir/d.txt", "4".encodeToByteArray())

        val results = fs.search.find(SearchQuery(namePattern = "*.txt")).getOrThrow()
        assertEquals(3, results.size)
        assertTrue(results.any { it.path == "/a.txt" })
        assertTrue(results.any { it.path == "/b.txt" })
        assertTrue(results.any { it.path == "/dir/d.txt" })
    }

    @Test
    fun `find by glob wildcard question mark`() = runTest {
        val fs = createFs()
        fs.writeAll("/a1.txt", "x".encodeToByteArray())
        fs.writeAll("/a2.txt", "x".encodeToByteArray())
        fs.writeAll("/ab.txt", "x".encodeToByteArray())
        fs.writeAll("/abc.txt", "x".encodeToByteArray())

        val results = fs.search.find(SearchQuery(namePattern = "a?.txt")).getOrThrow()
        assertEquals(3, results.size)
        assertTrue(results.any { it.path == "/a1.txt" })
        assertTrue(results.any { it.path == "/a2.txt" })
        assertTrue(results.any { it.path == "/ab.txt" })
    }

    @Test
    fun `find case insensitive name match`() = runTest {
        val fs = createFs()
        fs.writeAll("/README.md", "x".encodeToByteArray())
        fs.writeAll("/readme.txt", "x".encodeToByteArray())

        val results = fs.search.find(SearchQuery(namePattern = "readme*", caseSensitive = false)).getOrThrow()
        assertEquals(2, results.size)
    }

    @Test
    fun `find case sensitive name match`() = runTest {
        val fs = createFs()
        fs.writeAll("/README.md", "x".encodeToByteArray())
        fs.writeAll("/readme.txt", "x".encodeToByteArray())

        val results = fs.search.find(SearchQuery(namePattern = "README*", caseSensitive = true)).getOrThrow()
        assertEquals(1, results.size)
        assertEquals("/README.md", results[0].path)
    }

    // ═════════════════════════════════════════════════════════════
    // 按类型过滤
    // ═════════════════════════════════════════════════════════════

    @Test
    fun `find only directories`() = runTest {
        val fs = createFs()
        fs.createDirRecursive("/a/b/c")
        fs.writeAll("/a/file.txt", "x".encodeToByteArray())

        val results = fs.search.find(SearchQuery(rootPath = "/a", typeFilter = FsType.DIRECTORY)).getOrThrow()
        assertTrue(results.all { it.type == FsType.DIRECTORY })
        assertTrue(results.any { it.path == "/a/b" })
        assertTrue(results.any { it.path == "/a/b/c" })
    }

    @Test
    fun `find only files`() = runTest {
        val fs = createFs()
        fs.createDirRecursive("/a/b")
        fs.writeAll("/a/file1.txt", "x".encodeToByteArray())
        fs.writeAll("/a/b/file2.txt", "y".encodeToByteArray())

        val results = fs.search.find(SearchQuery(rootPath = "/a", typeFilter = FsType.FILE)).getOrThrow()
        assertEquals(2, results.size)
        assertTrue(results.all { it.type == FsType.FILE })
    }

    // ═════════════════════════════════════════════════════════════
    // 按内容搜索（类 grep）
    // ═════════════════════════════════════════════════════════════

    @Test
    fun `grep content match`() = runTest {
        val fs = createFs()
        fs.writeAll("/a.txt", "hello world\nfoo bar\nhello again".encodeToByteArray())
        fs.writeAll("/b.txt", "nothing here".encodeToByteArray())

        val results = fs.search.find(SearchQuery(contentPattern = "hello")).getOrThrow()
        assertEquals(1, results.size)
        assertEquals("/a.txt", results[0].path)
        assertEquals(2, results[0].matchedLines.size)
        assertEquals(1, results[0].matchedLines[0].lineNumber)
        assertEquals("hello world", results[0].matchedLines[0].content)
        assertEquals(3, results[0].matchedLines[1].lineNumber)
        assertEquals("hello again", results[0].matchedLines[1].content)
    }

    @Test
    fun `grep case insensitive`() = runTest {
        val fs = createFs()
        fs.writeAll("/a.txt", "Hello World\nHELLO\nhello".encodeToByteArray())

        val results = fs.search.find(SearchQuery(contentPattern = "hello", caseSensitive = false)).getOrThrow()
        assertEquals(1, results.size)
        assertEquals(3, results[0].matchedLines.size)
    }

    @Test
    fun `grep case sensitive`() = runTest {
        val fs = createFs()
        fs.writeAll("/a.txt", "Hello World\nHELLO\nhello".encodeToByteArray())

        val results = fs.search.find(SearchQuery(contentPattern = "hello", caseSensitive = true)).getOrThrow()
        assertEquals(1, results.size)
        assertEquals(1, results[0].matchedLines.size)
        assertEquals(3, results[0].matchedLines[0].lineNumber)
    }

    @Test
    fun `grep combined with name pattern`() = runTest {
        val fs = createFs()
        fs.writeAll("/a.kt", "fun main() {}".encodeToByteArray())
        fs.writeAll("/b.kt", "val x = 1".encodeToByteArray())
        fs.writeAll("/c.txt", "fun test() {}".encodeToByteArray())

        val results = fs.search.find(SearchQuery(namePattern = "*.kt", contentPattern = "fun")).getOrThrow()
        assertEquals(1, results.size)
        assertEquals("/a.kt", results[0].path)
    }

    // ═════════════════════════════════════════════════════════════
    // maxDepth 控制
    // ═════════════════════════════════════════════════════════════

    @Test
    fun `find with maxDepth 1 only immediate children`() = runTest {
        val fs = createFs()
        fs.writeAll("/a.txt", "x".encodeToByteArray())
        fs.writeAll("/sub/b.txt", "x".encodeToByteArray())
        fs.writeAll("/sub/deep/c.txt", "x".encodeToByteArray())

        val results = fs.search.find(SearchQuery(namePattern = "*.txt", maxDepth = 1)).getOrThrow()
        assertEquals(1, results.size)
        assertEquals("/a.txt", results[0].path)
    }

    @Test
    fun `find with maxDepth 2`() = runTest {
        val fs = createFs()
        fs.writeAll("/a.txt", "x".encodeToByteArray())
        fs.writeAll("/sub/b.txt", "x".encodeToByteArray())
        fs.writeAll("/sub/deep/c.txt", "x".encodeToByteArray())

        val results = fs.search.find(SearchQuery(namePattern = "*.txt", maxDepth = 2)).getOrThrow()
        assertEquals(2, results.size)
        assertTrue(results.any { it.path == "/a.txt" })
        assertTrue(results.any { it.path == "/sub/b.txt" })
    }

    @Test
    fun `find with unlimited depth`() = runTest {
        val fs = createFs()
        fs.writeAll("/a.txt", "x".encodeToByteArray())
        fs.writeAll("/sub/b.txt", "x".encodeToByteArray())
        fs.writeAll("/sub/deep/c.txt", "x".encodeToByteArray())
        fs.writeAll("/sub/deep/deeper/d.txt", "x".encodeToByteArray())

        val results = fs.search.find(SearchQuery(namePattern = "*.txt")).getOrThrow()
        assertEquals(4, results.size)
    }

    // ═════════════════════════════════════════════════════════════
    // rootPath 限制搜索范围
    // ═════════════════════════════════════════════════════════════

    @Test
    fun `find scoped to subdirectory`() = runTest {
        val fs = createFs()
        fs.writeAll("/docs/a.txt", "x".encodeToByteArray())
        fs.writeAll("/docs/sub/b.txt", "x".encodeToByteArray())
        fs.writeAll("/src/c.txt", "x".encodeToByteArray())

        val results = fs.search.find(SearchQuery(rootPath = "/docs", namePattern = "*.txt")).getOrThrow()
        assertEquals(2, results.size)
        assertTrue(results.all { it.path.startsWith("/docs") })
    }

    // ═════════════════════════════════════════════════════════════
    // 挂载点搜索
    // ═════════════════════════════════════════════════════════════

    @Test
    fun `find in mounted directory by name`() = runTest {
        val fs = createFs()
        val diskOps = FakeDiskFileOperations()
        diskOps.files["/readme.txt"] = "hello".encodeToByteArray()
        diskOps.files["/docs/guide.txt"] = "world".encodeToByteArray()
        diskOps.dirs.add("/docs")

        fs.mounts.mount("/mnt", diskOps)

        val results = fs.search.find(SearchQuery(namePattern = "*.txt")).getOrThrow()
        assertTrue(results.any { it.path == "/mnt/readme.txt" })
        assertTrue(results.any { it.path == "/mnt/docs/guide.txt" })
    }

    @Test
    fun `find in mounted directory by content`() = runTest {
        val fs = createFs()
        val diskOps = FakeDiskFileOperations()
        diskOps.files["/hello.txt"] = "greeting: hello world".encodeToByteArray()
        diskOps.files["/other.txt"] = "nothing special".encodeToByteArray()

        fs.mounts.mount("/mnt", diskOps)

        val results = fs.search.find(SearchQuery(contentPattern = "hello")).getOrThrow()
        assertEquals(1, results.size)
        assertEquals("/mnt/hello.txt", results[0].path)
        assertEquals(1, results[0].matchedLines.size)
    }

    @Test
    fun `find across memory and mount`() = runTest {
        val fs = createFs()
        fs.writeAll("/mem/a.txt", "hello from memory".encodeToByteArray())

        val diskOps = FakeDiskFileOperations()
        diskOps.files["/b.txt"] = "hello from disk".encodeToByteArray()
        fs.mounts.mount("/disk", diskOps)

        val results = fs.search.find(SearchQuery(contentPattern = "hello")).getOrThrow()
        assertEquals(2, results.size)
        assertTrue(results.any { it.path == "/mem/a.txt" })
        assertTrue(results.any { it.path == "/disk/b.txt" })
    }

    // ═════════════════════════════════════════════════════════════
    // 边界情况
    // ═════════════════════════════════════════════════════════════

    @Test
    fun `find no matches returns empty list`() = runTest {
        val fs = createFs()
        fs.writeAll("/a.txt", "hello".encodeToByteArray())

        val results = fs.search.find(SearchQuery(namePattern = "*.md")).getOrThrow()
        assertTrue(results.isEmpty())
    }

    @Test
    fun `find in nonexistent directory returns empty`() = runTest {
        val fs = createFs()
        val results = fs.search.find(SearchQuery(rootPath = "/nonexistent", namePattern = "*")).getOrThrow()
        assertTrue(results.isEmpty())
    }

    @Test
    fun `find with no filters returns all nodes`() = runTest {
        val fs = createFs()
        fs.writeAll("/a.txt", "x".encodeToByteArray())
        fs.createDir("/sub")

        val results = fs.search.find(SearchQuery()).getOrThrow()
        // root + a.txt + sub = 3
        assertTrue(results.size >= 3)
    }

    @Test
    fun `find returns file sizes`() = runTest {
        val fs = createFs()
        val data = "hello world".encodeToByteArray()
        fs.writeAll("/test.txt", data)

        val results = fs.search.find(SearchQuery(namePattern = "test.txt")).getOrThrow()
        assertEquals(1, results.size)
        assertEquals(data.size.toLong(), results[0].size)
    }

    @Test
    fun `grep empty file returns no matches`() = runTest {
        val fs = createFs()
        fs.createFile("/empty.txt")

        val results = fs.search.find(SearchQuery(contentPattern = "hello")).getOrThrow()
        assertTrue(results.isEmpty())
    }

    @Test
    fun `find with symlink`() = runTest {
        val fs = createFs()
        fs.writeAll("/real.txt", "target content".encodeToByteArray())
        fs.symlinks.create("/link.txt", "/real.txt")

        // Symlinks are not returned when typeFilter is FILE (symlink resolves to file but type is SYMLINK)
        val allResults = fs.search.find(SearchQuery(namePattern = "*.txt")).getOrThrow()
        assertTrue(allResults.any { it.path == "/real.txt" })
    }
}
