package com.hrm.fs.core

import com.hrm.fs.api.*
import com.hrm.fs.core.persistence.PersistenceConfig
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlin.test.*

class SymlinkTest {

    @Test
    fun symlink_create_and_readLink() = runTest {
        val fs = createFs()
        fs.writeAll("/target.txt", "hello".encodeToByteArray()).getOrThrow()
        fs.createSymlink("/link.txt", "/target.txt").getOrThrow()

        val target = fs.readLink("/link.txt").getOrThrow()
        assertEquals("/target.txt", target)
    }

    @Test
    fun symlink_stat_follows_to_target() = runTest {
        val fs = createFs()
        fs.writeAll("/target.txt", "hello".encodeToByteArray()).getOrThrow()
        fs.createSymlink("/link.txt", "/target.txt").getOrThrow()

        // stat 跟随 symlink，返回目标文件信息
        val meta = fs.stat("/link.txt").getOrThrow()
        assertEquals(FsType.FILE, meta.type)
        assertEquals(5L, meta.size)
    }

    @Test
    fun symlink_read_through_link() = runTest {
        val fs = createFs()
        fs.writeAll("/real.txt", "content via symlink".encodeToByteArray()).getOrThrow()
        fs.createSymlink("/alias.txt", "/real.txt").getOrThrow()

        // 通过 symlink 读取
        val data = fs.readAll("/alias.txt").getOrThrow().decodeToString()
        assertEquals("content via symlink", data)
    }

    @Test
    fun symlink_write_through_link() = runTest {
        val fs = createFs()
        fs.writeAll("/real.txt", "old".encodeToByteArray()).getOrThrow()
        fs.createSymlink("/alias.txt", "/real.txt").getOrThrow()

        // 通过 symlink 写入
        fs.writeAll("/alias.txt", "new content".encodeToByteArray()).getOrThrow()

        // 原始文件被更新
        val data = fs.readAll("/real.txt").getOrThrow().decodeToString()
        assertEquals("new content", data)
    }

    @Test
    fun symlink_to_directory() = runTest {
        val fs = createFs()
        fs.createDirRecursive("/real/sub").getOrThrow()
        fs.writeAll("/real/sub/f.txt", "data".encodeToByteArray()).getOrThrow()
        fs.createSymlink("/link_dir", "/real").getOrThrow()

        // 通过 symlink 目录读取子内容
        val entries = fs.readDir("/link_dir").getOrThrow()
        assertTrue(entries.any { it.name == "sub" })

        // 深层访问
        val data = fs.readAll("/link_dir/sub/f.txt").getOrThrow().decodeToString()
        assertEquals("data", data)
    }

    @Test
    fun symlink_readDir_shows_symlink_type() = runTest {
        val fs = createFs()
        fs.createDir("/d").getOrThrow()
        fs.writeAll("/d/real.txt", "data".encodeToByteArray()).getOrThrow()
        fs.createSymlink("/d/link.txt", "/d/real.txt").getOrThrow()

        // readDir 应展示 symlink 的实际类型
        val entries = fs.readDir("/d").getOrThrow()
        val linkEntry = entries.find { it.name == "link.txt" }
        assertNotNull(linkEntry)
        assertEquals(FsType.SYMLINK, linkEntry.type)
    }

    @Test
    fun symlink_dangling_stat_fails() = runTest {
        val fs = createFs()
        fs.createSymlink("/dangling", "/nonexistent").getOrThrow()

        // 悬空链接的 stat 应失败（目标不存在）
        val result = fs.stat("/dangling")
        assertTrue(result.isFailure)
        assertIs<FsError.NotFound>(result.exceptionOrNull())

        // 但 readLink 仍可读取目标路径
        val target = fs.readLink("/dangling").getOrThrow()
        assertEquals("/nonexistent", target)
    }

    @Test
    fun symlink_already_exists_fails() = runTest {
        val fs = createFs()
        fs.writeAll("/target.txt", "data".encodeToByteArray()).getOrThrow()
        fs.createSymlink("/link.txt", "/target.txt").getOrThrow()

        // 重复创建应失败
        val result = fs.createSymlink("/link.txt", "/other.txt")
        assertTrue(result.isFailure)
        assertIs<FsError.AlreadyExists>(result.exceptionOrNull())
    }

    @Test
    fun symlink_delete_removes_link_not_target() = runTest {
        val fs = createFs()
        fs.writeAll("/target.txt", "survive".encodeToByteArray()).getOrThrow()
        fs.createSymlink("/link.txt", "/target.txt").getOrThrow()

        // 删除 symlink
        fs.delete("/link.txt").getOrThrow()

        // 目标文件仍在
        val data = fs.readAll("/target.txt").getOrThrow().decodeToString()
        assertEquals("survive", data)

        // symlink 已不存在
        assertTrue(fs.stat("/link.txt").isFailure)
    }

    @Test
    fun symlink_chain() = runTest {
        val fs = createFs()
        fs.writeAll("/real.txt", "chain".encodeToByteArray()).getOrThrow()
        fs.createSymlink("/link1", "/real.txt").getOrThrow()
        fs.createSymlink("/link2", "/link1").getOrThrow()

        // 跟随链：link2 -> link1 -> real.txt
        val data = fs.readAll("/link2").getOrThrow().decodeToString()
        assertEquals("chain", data)
    }

    @Test
    fun symlink_relative_path() = runTest {
        val fs = createFs()
        fs.createDir("/a").getOrThrow()
        fs.writeAll("/a/target.txt", "relative".encodeToByteArray()).getOrThrow()
        // 相对路径 symlink（相对于 /a 目录）
        fs.createSymlink("/a/link.txt", "target.txt").getOrThrow()

        val data = fs.readAll("/a/link.txt").getOrThrow().decodeToString()
        assertEquals("relative", data)
    }

    @Test
    fun symlink_persistence() = runTest {
        val storage = InMemoryFsStorage()
        val persistCfg = PersistenceConfig(autoSnapshotEvery = 1)

        val fs1 = InMemoryFileSystem(storage = storage, persistenceConfig = persistCfg)
        fs1.writeAll("/target.txt", "persisted".encodeToByteArray()).getOrThrow()
        fs1.createSymlink("/link.txt", "/target.txt").getOrThrow()

        // 使用相同 storage 创建新实例，symlink 应通过持久化保留
        val fs2 = InMemoryFileSystem(storage = storage, persistenceConfig = persistCfg)
        val target = fs2.readLink("/link.txt").getOrThrow()
        assertEquals("/target.txt", target)

        val data = fs2.readAll("/link.txt").getOrThrow().decodeToString()
        assertEquals("persisted", data)
    }

    @Test
    fun symlink_readLink_on_non_symlink_fails() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "data".encodeToByteArray()).getOrThrow()

        // 对普通文件调用 readLink 应失败
        val result = fs.readLink("/f.txt")
        assertTrue(result.isFailure)
        assertIs<FsError.InvalidPath>(result.exceptionOrNull())
    }

    @Test
    fun symlink_deleteRecursive_does_not_follow() = runTest {
        val fs = createFs()
        fs.createDir("/real_dir").getOrThrow()
        fs.writeAll("/real_dir/f.txt", "keep".encodeToByteArray()).getOrThrow()
        fs.createSymlink("/link_dir", "/real_dir").getOrThrow()

        // deleteRecursive 删除 symlink 本身，不跟随到目标目录
        fs.deleteRecursive("/link_dir").getOrThrow()

        // 目标目录和文件仍在
        assertTrue(fs.stat("/real_dir").isSuccess)
        val data = fs.readAll("/real_dir/f.txt").getOrThrow().decodeToString()
        assertEquals("keep", data)

        // symlink 已删除
        assertTrue(fs.readLink("/link_dir").isFailure)
    }

    @Test
    fun symlink_watch_emits_event() = runTest {
        val fs = createFs()
        fs.createDir("/d").getOrThrow()
        val events = mutableListOf<FsEvent>()
        val job = launch {
            fs.watch("/d").collect { events.add(it) }
        }
        testScheduler.advanceUntilIdle()

        fs.createSymlink("/d/link", "/target").getOrThrow()
        testScheduler.advanceUntilIdle()

        assertTrue(events.any { it.kind == FsEventKind.CREATED && it.path == "/d/link" })

        job.cancel()
    }
}
