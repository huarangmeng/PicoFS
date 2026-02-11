package com.hrm.fs.core

import com.hrm.fs.api.FsError
import com.hrm.fs.api.FsType
import com.hrm.fs.api.InMemoryFsStorage
import kotlinx.coroutines.test.runTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class TrashTest {

    // ═══════════════════════════════════════════════════════════
    // 基本文件回收站操作
    // ═══════════════════════════════════════════════════════════

    @Test
    fun moveFileToTrashAndList() = runTest {
        val fs = createFs()
        fs.writeAll("/hello.txt", "Hello".encodeToByteArray())

        val trashId = fs.trash.moveToTrash("/hello.txt").getOrThrow()
        assertTrue(trashId.isNotEmpty())

        // 文件应该不存在了
        assertTrue(fs.stat("/hello.txt").isFailure)

        // 回收站应该有一条记录
        val items = fs.trash.list().getOrThrow()
        assertEquals(1, items.size)
        assertEquals(trashId, items[0].trashId)
        assertEquals("/hello.txt", items[0].originalPath)
        assertEquals(FsType.FILE, items[0].type)
        assertEquals(5L, items[0].size)
    }

    @Test
    fun restoreFileFromTrash() = runTest {
        val fs = createFs()
        fs.writeAll("/hello.txt", "Hello".encodeToByteArray())

        val trashId = fs.trash.moveToTrash("/hello.txt").getOrThrow()
        assertTrue(fs.stat("/hello.txt").isFailure)

        fs.trash.restore(trashId).getOrThrow()

        // 文件应该恢复了
        val content = fs.readAll("/hello.txt").getOrThrow()
        assertEquals("Hello", content.decodeToString())

        // 回收站应该为空
        val items = fs.trash.list().getOrThrow()
        assertTrue(items.isEmpty())
    }

    @Test
    fun moveEmptyFileToTrashAndRestore() = runTest {
        val fs = createFs()
        fs.createFile("/empty.txt")

        val trashId = fs.trash.moveToTrash("/empty.txt").getOrThrow()
        assertTrue(fs.stat("/empty.txt").isFailure)

        fs.trash.restore(trashId).getOrThrow()
        val content = fs.readAll("/empty.txt").getOrThrow()
        assertEquals(0, content.size)
    }

    // ═══════════════════════════════════════════════════════════
    // 目录回收站操作
    // ═══════════════════════════════════════════════════════════

    @Test
    fun moveDirectoryToTrashAndRestore() = runTest {
        val fs = createFs()
        fs.createDirRecursive("/docs/sub")
        fs.writeAll("/docs/a.txt", "AAA".encodeToByteArray())
        fs.writeAll("/docs/sub/b.txt", "BBB".encodeToByteArray())

        val trashId = fs.trash.moveToTrash("/docs").getOrThrow()

        // 目录及子内容应该不存在
        assertTrue(fs.stat("/docs").isFailure)
        assertTrue(fs.stat("/docs/a.txt").isFailure)
        assertTrue(fs.stat("/docs/sub/b.txt").isFailure)

        // 回收站记录
        val items = fs.trash.list().getOrThrow()
        assertEquals(1, items.size)
        assertEquals(FsType.DIRECTORY, items[0].type)

        // 恢复
        fs.trash.restore(trashId).getOrThrow()

        // 文件和子目录都应该恢复
        val a = fs.readAll("/docs/a.txt").getOrThrow()
        assertEquals("AAA", a.decodeToString())
        val b = fs.readAll("/docs/sub/b.txt").getOrThrow()
        assertEquals("BBB", b.decodeToString())
        assertTrue(fs.trash.list().getOrThrow().isEmpty())
    }

    @Test
    fun moveEmptyDirectoryToTrashAndRestore() = runTest {
        val fs = createFs()
        fs.createDir("/emptydir")

        val trashId = fs.trash.moveToTrash("/emptydir").getOrThrow()
        assertTrue(fs.stat("/emptydir").isFailure)

        fs.trash.restore(trashId).getOrThrow()
        val meta = fs.stat("/emptydir").getOrThrow()
        assertEquals(FsType.DIRECTORY, meta.type)
    }

    // ═══════════════════════════════════════════════════════════
    // Purge 操作
    // ═══════════════════════════════════════════════════════════

    @Test
    fun purgeSingleItem() = runTest {
        val fs = createFs()
        fs.writeAll("/a.txt", "A".encodeToByteArray())
        fs.writeAll("/b.txt", "B".encodeToByteArray())

        val trashIdA = fs.trash.moveToTrash("/a.txt").getOrThrow()
        fs.trash.moveToTrash("/b.txt").getOrThrow()

        assertEquals(2, fs.trash.list().getOrThrow().size)

        fs.trash.purge(trashIdA).getOrThrow()
        val items = fs.trash.list().getOrThrow()
        assertEquals(1, items.size)
        assertEquals("/b.txt", items[0].originalPath)
    }

    @Test
    fun purgeAll() = runTest {
        val fs = createFs()
        fs.writeAll("/a.txt", "A".encodeToByteArray())
        fs.writeAll("/b.txt", "B".encodeToByteArray())

        fs.trash.moveToTrash("/a.txt").getOrThrow()
        fs.trash.moveToTrash("/b.txt").getOrThrow()
        assertEquals(2, fs.trash.list().getOrThrow().size)

        fs.trash.purgeAll().getOrThrow()
        assertTrue(fs.trash.list().getOrThrow().isEmpty())
    }

    @Test
    fun purgeNonExistentTrashId() = runTest {
        val fs = createFs()
        val result = fs.trash.purge("nonexistent")
        assertTrue(result.isFailure)
        assertIs<FsError.NotFound>(result.exceptionOrNull())
    }

    // ═══════════════════════════════════════════════════════════
    // 错误场景
    // ═══════════════════════════════════════════════════════════

    @Test
    fun moveToTrashNonExistentPath() = runTest {
        val fs = createFs()
        val result = fs.trash.moveToTrash("/nonexistent.txt")
        assertTrue(result.isFailure)
    }

    @Test
    fun moveToTrashRootPath() = runTest {
        val fs = createFs()
        val result = fs.trash.moveToTrash("/")
        assertTrue(result.isFailure)
        assertIs<FsError.PermissionDenied>(result.exceptionOrNull())
    }

    @Test
    fun restoreToExistingPath() = runTest {
        val fs = createFs()
        fs.writeAll("/hello.txt", "Hello".encodeToByteArray())

        val trashId = fs.trash.moveToTrash("/hello.txt").getOrThrow()

        // 在原始路径创建新文件
        fs.writeAll("/hello.txt", "New".encodeToByteArray())

        // 恢复应该失败
        val result = fs.trash.restore(trashId)
        assertTrue(result.isFailure)
        assertIs<FsError.AlreadyExists>(result.exceptionOrNull())
    }

    @Test
    fun restoreNonExistentTrashId() = runTest {
        val fs = createFs()
        val result = fs.trash.restore("nonexistent")
        assertTrue(result.isFailure)
        assertIs<FsError.NotFound>(result.exceptionOrNull())
    }

    // ═══════════════════════════════════════════════════════════
    // 符号链接回收站
    // ═══════════════════════════════════════════════════════════

    @Test
    fun moveSymlinkToTrashAndRestore() = runTest {
        val fs = createFs()
        fs.writeAll("/target.txt", "Target".encodeToByteArray())
        fs.symlinks.create("/link", "/target.txt")

        val trashId = fs.trash.moveToTrash("/link").getOrThrow()
        assertTrue(fs.stat("/link").isFailure)

        fs.trash.restore(trashId).getOrThrow()

        // 符号链接应该恢复
        val linkTarget = fs.symlinks.readLink("/link").getOrThrow()
        assertEquals("/target.txt", linkTarget)
    }

    // ═══════════════════════════════════════════════════════════
    // 多次删除-恢复循环
    // ═══════════════════════════════════════════════════════════

    @Test
    fun multipleTrashRestoreCycles() = runTest {
        val fs = createFs()
        fs.writeAll("/file.txt", "v1".encodeToByteArray())

        // 第一次删除恢复
        val id1 = fs.trash.moveToTrash("/file.txt").getOrThrow()
        fs.trash.restore(id1).getOrThrow()
        assertEquals("v1", fs.readAll("/file.txt").getOrThrow().decodeToString())

        // 修改后第二次
        fs.writeAll("/file.txt", "v2".encodeToByteArray())
        val id2 = fs.trash.moveToTrash("/file.txt").getOrThrow()
        fs.trash.restore(id2).getOrThrow()
        assertEquals("v2", fs.readAll("/file.txt").getOrThrow().decodeToString())
    }

    @Test
    fun trashListOrderIsNewestFirst() = runTest {
        val fs = createFs()
        fs.writeAll("/a.txt", "A".encodeToByteArray())
        fs.writeAll("/b.txt", "B".encodeToByteArray())
        fs.writeAll("/c.txt", "C".encodeToByteArray())

        fs.trash.moveToTrash("/a.txt").getOrThrow()
        fs.trash.moveToTrash("/b.txt").getOrThrow()
        fs.trash.moveToTrash("/c.txt").getOrThrow()

        val items = fs.trash.list().getOrThrow()
        assertEquals(3, items.size)
        // 最近删除的在前
        assertEquals("/c.txt", items[0].originalPath)
        assertEquals("/b.txt", items[1].originalPath)
        assertEquals("/a.txt", items[2].originalPath)
    }

    // ═══════════════════════════════════════════════════════════
    // 嵌套目录回收站
    // ═══════════════════════════════════════════════════════════

    @Test
    fun deepNestedDirectoryTrashAndRestore() = runTest {
        val fs = createFs()
        fs.createDirRecursive("/a/b/c")
        fs.writeAll("/a/b/c/deep.txt", "deep content".encodeToByteArray())
        fs.writeAll("/a/top.txt", "top".encodeToByteArray())

        val trashId = fs.trash.moveToTrash("/a").getOrThrow()

        assertTrue(fs.stat("/a").isFailure)
        assertTrue(fs.stat("/a/b/c/deep.txt").isFailure)

        fs.trash.restore(trashId).getOrThrow()

        assertEquals("deep content", fs.readAll("/a/b/c/deep.txt").getOrThrow().decodeToString())
        assertEquals("top", fs.readAll("/a/top.txt").getOrThrow().decodeToString())
    }

    // ═══════════════════════════════════════════════════════════
    // 挂载点文件回收站
    // ═══════════════════════════════════════════════════════════

    @Test
    fun moveToTrashMountedFile() = runTest {
        val fs = createFs()
        val diskOps = FakeDiskFileOperations()
        diskOps.files["/hello.txt"] = "Hello from disk".encodeToByteArray()

        fs.mounts.mount("/disk", diskOps)

        val trashId = fs.trash.moveToTrash("/disk/hello.txt").getOrThrow()
        assertTrue(trashId.isNotEmpty())

        // 回收站列表
        val items = fs.trash.list().getOrThrow()
        assertEquals(1, items.size)
        assertEquals("/disk/hello.txt", items[0].originalPath)
    }

    @Test
    fun restoreMountedFile() = runTest {
        val fs = createFs()
        val diskOps = FakeDiskFileOperations()
        diskOps.files["/hello.txt"] = "Hello from disk".encodeToByteArray()

        fs.mounts.mount("/disk", diskOps)

        val trashId = fs.trash.moveToTrash("/disk/hello.txt").getOrThrow()
        fs.trash.restore(trashId).getOrThrow()

        // 文件应该被恢复到 disk 上
        assertTrue(diskOps.files.containsKey("/hello.txt"))
        assertEquals("Hello from disk", diskOps.files["/hello.txt"]!!.decodeToString())
    }

    // ═══════════════════════════════════════════════════════════
    // 持久化 & 恢复
    // ═══════════════════════════════════════════════════════════

    @Test
    fun trashPersistsAcrossReloads() = runTest {
        val storage = InMemoryFsStorage()

        // 第一个 fs 实例：创建文件并移入回收站
        val fs1 = InMemoryFileSystem(storage = storage)
        fs1.writeAll("/persist.txt", "persisted".encodeToByteArray())
        val trashId = fs1.trash.moveToTrash("/persist.txt").getOrThrow()

        // 第二个 fs 实例：从持久化恢复
        val fs2 = InMemoryFileSystem(storage = storage)
        val items = fs2.trash.list().getOrThrow()
        assertEquals(1, items.size)
        assertEquals(trashId, items[0].trashId)
        assertEquals("/persist.txt", items[0].originalPath)

        // 从新实例恢复
        fs2.trash.restore(trashId).getOrThrow()
        val content = fs2.readAll("/persist.txt").getOrThrow()
        assertEquals("persisted", content.decodeToString())
    }

    // ═══════════════════════════════════════════════════════════
    // 锁定文件不能移入回收站
    // ═══════════════════════════════════════════════════════════

    @Test
    fun lockedFileCannotBeTrashed() = runTest {
        val fs = createFs()
        fs.writeAll("/locked.txt", "data".encodeToByteArray())

        val handle = fs.open("/locked.txt", com.hrm.fs.api.OpenMode.WRITE).getOrThrow()
        handle.lock().getOrThrow()

        val result = fs.trash.moveToTrash("/locked.txt")
        assertTrue(result.isFailure)
        assertIs<FsError.Locked>(result.exceptionOrNull())

        handle.unlock()
        handle.close()
    }

    // ═══════════════════════════════════════════════════════════
    // 恢复到已删除的父目录（自动重建）
    // ═══════════════════════════════════════════════════════════

    @Test
    fun restoreCreatesParentDirectories() = runTest {
        val fs = createFs()
        fs.createDirRecursive("/parent/child")
        fs.writeAll("/parent/child/file.txt", "data".encodeToByteArray())

        val trashId = fs.trash.moveToTrash("/parent/child/file.txt").getOrThrow()
        // 删除父目录
        fs.deleteRecursive("/parent")

        // 恢复文件时应自动重建父目录
        fs.trash.restore(trashId).getOrThrow()
        val content = fs.readAll("/parent/child/file.txt").getOrThrow()
        assertEquals("data", content.decodeToString())
    }
}
