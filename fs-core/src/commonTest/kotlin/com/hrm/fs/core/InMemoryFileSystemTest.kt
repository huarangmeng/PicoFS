package com.hrm.fs.core

import com.hrm.fs.api.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runTest
import kotlin.test.*

class InMemoryFileSystemTest {

    private fun createFs(storage: FsStorage? = null): FileSystem =
        InMemoryFileSystem(storage = storage)

    private fun createFsWithQuota(quotaBytes: Long, storage: FsStorage? = null): FileSystem =
        InMemoryFileSystem(storage = storage, quotaBytes = quotaBytes)

    // ═════════════════════════════════════════════════════════════
    // PathUtils
    // ═════════════════════════════════════════════════════════════

    @Test
    fun pathNormalize_basic() {
        assertEquals("/", PathUtils.normalize(""))
        assertEquals("/", PathUtils.normalize("/"))
        assertEquals("/a/b", PathUtils.normalize("/a/b"))
        assertEquals("/a/b", PathUtils.normalize("/a/b/"))
        assertEquals("/a/b", PathUtils.normalize("//a//b//"))
    }

    @Test
    fun pathNormalize_dots() {
        assertEquals("/a", PathUtils.normalize("/a/b/.."))
        assertEquals("/", PathUtils.normalize("/a/.."))
        assertEquals("/a/c", PathUtils.normalize("/a/./c"))
        assertEquals("/b", PathUtils.normalize("/a/../b"))
    }

    // ═════════════════════════════════════════════════════════════
    // 基础 CRUD
    // ═════════════════════════════════════════════════════════════

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
    // 权限
    // ═════════════════════════════════════════════════════════════

    @Test
    fun setPermissions_readonly_blocks_write() = runTest {
        val fs = createFs()
        fs.createDir("/d").getOrThrow()
        fs.createFile("/d/f.txt").getOrThrow()
        fs.setPermissions("/d/f.txt", FsPermissions.READ_ONLY).getOrThrow()

        val handle = fs.open("/d/f.txt", OpenMode.READ).getOrThrow()
        val writeResult = handle.writeAt(0, "test".encodeToByteArray())
        assertTrue(writeResult.isFailure)
        handle.close()
    }

    @Test
    fun setPermissions_readonly_blocks_open_write() = runTest {
        val fs = createFs()
        fs.createDir("/d").getOrThrow()
        fs.createFile("/d/f.txt").getOrThrow()
        fs.setPermissions("/d/f.txt", FsPermissions.READ_ONLY).getOrThrow()

        val result = fs.open("/d/f.txt", OpenMode.WRITE)
        assertTrue(result.isFailure)
        assertIs<FsError.PermissionDenied>(result.exceptionOrNull())
    }

    @Test
    fun createFile_in_readonly_dir_fails() = runTest {
        val fs = createFs()
        fs.createDir("/d").getOrThrow()
        fs.setPermissions("/d", FsPermissions.READ_ONLY).getOrThrow()
        val result = fs.createFile("/d/f.txt")
        assertTrue(result.isFailure)
        assertIs<FsError.PermissionDenied>(result.exceptionOrNull())
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
    // copy / move
    // ═════════════════════════════════════════════════════════════

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

    // ═════════════════════════════════════════════════════════════
    // 挂载（使用 FakeDiskOps 模拟）
    // ═════════════════════════════════════════════════════════════

    @Test
    fun mount_and_operations() = runTest {
        val fs = createFs()
        val fakeDisk = FakeDiskFileOperations()

        fs.mount("/mnt", fakeDisk).getOrThrow()
        assertEquals(listOf("/mnt"), fs.listMounts())

        // 创建文件
        fs.createDir("/mnt/sub").getOrThrow()
        fs.createFile("/mnt/sub/test.txt").getOrThrow()

        assertTrue(fakeDisk.createdDirs.contains("/sub"))
        assertTrue(fakeDisk.createdFiles.contains("/sub/test.txt"))
    }

    @Test
    fun mount_root_fails() = runTest {
        val fs = createFs()
        val result = fs.mount("/", FakeDiskFileOperations())
        assertTrue(result.isFailure)
    }

    @Test
    fun unmount_removes() = runTest {
        val fs = createFs()
        val fakeDisk = FakeDiskFileOperations()
        fs.mount("/mnt", fakeDisk).getOrThrow()
        fs.unmount("/mnt").getOrThrow()
        assertTrue(fs.listMounts().isEmpty())
    }

    @Test
    fun unmount_notMounted_fails() = runTest {
        val fs = createFs()
        val result = fs.unmount("/nowhere")
        assertTrue(result.isFailure)
        assertIs<FsError.NotMounted>(result.exceptionOrNull())
    }

    @Test
    fun mount_readOnly_blocks_write() = runTest {
        val fs = createFs()
        val fakeDisk = FakeDiskFileOperations()
        fs.mount("/ro", fakeDisk, MountOptions(readOnly = true)).getOrThrow()

        val result = fs.createFile("/ro/test.txt")
        assertTrue(result.isFailure)
        assertIs<FsError.PermissionDenied>(result.exceptionOrNull())
    }

    @Test
    fun mount_readOnly_allows_read() = runTest {
        val fs = createFs()
        val fakeDisk = FakeDiskFileOperations()
        // 先用可写挂载创建文件
        fs.mount("/rw", fakeDisk).getOrThrow()
        fs.createDir("/rw/sub").getOrThrow()
        fs.unmount("/rw").getOrThrow()

        // 以只读重新挂载
        fs.mount("/rw", fakeDisk, MountOptions(readOnly = true)).getOrThrow()
        val result = fs.readDir("/rw/sub")
        assertTrue(result.isSuccess)
    }

    @Test
    fun delete_mountpoint_fails() = runTest {
        val fs = createFs()
        fs.mount("/mnt", FakeDiskFileOperations()).getOrThrow()
        val result = fs.delete("/mnt")
        assertTrue(result.isFailure)
        assertIs<FsError.PermissionDenied>(result.exceptionOrNull())
    }

    @Test
    fun mount_persistence_produces_pendingMounts() = runTest {
        val storage = InMemoryFsStorage()

        // fs1: 挂载 /mnt，持久化保存挂载信息
        val fs1 = createFs(storage)
        val fakeDisk = FakeDiskFileOperations()
        fs1.mount("/mnt", fakeDisk).getOrThrow()
        assertEquals(listOf("/mnt"), fs1.listMounts())

        // fs2: 使用同一个 storage，不手动 mount
        val fs2 = createFs(storage)
        // 活跃挂载为空（没重新 mount）
        assertTrue(fs2.listMounts().isEmpty())
        // pending 中应该有 /mnt
        val pending = fs2.pendingMounts()
        assertEquals(1, pending.size)
        assertEquals("/mnt", pending[0].virtualPath)
        assertEquals("/fake", pending[0].rootPath)
        assertFalse(pending[0].readOnly)
    }

    @Test
    fun mount_persistence_readOnly() = runTest {
        val storage = InMemoryFsStorage()

        val fs1 = createFs(storage)
        fs1.mount("/ro", FakeDiskFileOperations(), MountOptions(readOnly = true)).getOrThrow()

        val fs2 = createFs(storage)
        val pending = fs2.pendingMounts()
        assertEquals(1, pending.size)
        assertTrue(pending[0].readOnly)
    }

    @Test
    fun mount_clears_pending() = runTest {
        val storage = InMemoryFsStorage()

        val fs1 = createFs(storage)
        fs1.mount("/mnt", FakeDiskFileOperations()).getOrThrow()

        val fs2 = createFs(storage)
        assertEquals(1, fs2.pendingMounts().size)

        // 重新挂载后 pending 应该清除
        fs2.mount("/mnt", FakeDiskFileOperations()).getOrThrow()
        assertTrue(fs2.pendingMounts().isEmpty())
        assertEquals(listOf("/mnt"), fs2.listMounts())
    }

    // ═════════════════════════════════════════════════════════════
    // watch（事件）
    // ═════════════════════════════════════════════════════════════

    @Test
    fun watch_receives_events() = runTest {
        val fs = createFs()
        val events = mutableListOf<FsEvent>()

        // 先启动监听，确保 collector 已准备好
        val job = launch {
            fs.watch("/docs").collect { events.add(it) }
        }
        testScheduler.advanceUntilIdle()

        fs.createDir("/docs").getOrThrow()
        testScheduler.advanceUntilIdle()
        fs.createFile("/docs/a.txt").getOrThrow()
        testScheduler.advanceUntilIdle()
        fs.writeAll("/docs/b.txt", "data".encodeToByteArray()).getOrThrow()
        testScheduler.advanceUntilIdle()
        fs.delete("/docs/a.txt").getOrThrow()
        testScheduler.advanceUntilIdle()

        assertTrue(events.any { it.kind == FsEventKind.CREATED && it.path == "/docs" })
        assertTrue(events.any { it.kind == FsEventKind.CREATED && it.path == "/docs/a.txt" })
        assertTrue(events.any { it.kind == FsEventKind.DELETED && it.path == "/docs/a.txt" })

        job.cancel()
    }

    @Test
    fun watch_filters_by_path() = runTest {
        val fs = createFs()
        val events = mutableListOf<FsEvent>()

        val job = launch {
            fs.watch("/a").collect { events.add(it) }
        }
        testScheduler.advanceUntilIdle()

        fs.createDir("/a").getOrThrow()
        testScheduler.advanceUntilIdle()
        fs.createDir("/b").getOrThrow()
        testScheduler.advanceUntilIdle()
        fs.createFile("/a/f.txt").getOrThrow()
        testScheduler.advanceUntilIdle()
        fs.createFile("/b/g.txt").getOrThrow()
        testScheduler.advanceUntilIdle()

        // /b 下的事件不应该被捕获
        assertTrue(events.none { it.path.startsWith("/b") })
        assertTrue(events.any { it.path == "/a/f.txt" })

        job.cancel()
    }

    // ═════════════════════════════════════════════════════════════
    // 流式读写
    // ═════════════════════════════════════════════════════════════

    @Test
    fun readStream_chunked() = runTest {
        val fs = createFs()
        val content = "ABCDEFGHIJKLMNOP" // 16 bytes
        fs.writeAll("/big.txt", content.encodeToByteArray()).getOrThrow()

        val chunks = fs.readStream("/big.txt", chunkSize = 5).toList()
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
        fs.writeStream("/stream.txt", dataFlow).getOrThrow()
        val content = fs.readAll("/stream.txt").getOrThrow().decodeToString()
        assertEquals("Hello World", content)
    }

    // ═════════════════════════════════════════════════════════════
    // WAL 持久化
    // ═════════════════════════════════════════════════════════════

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

    // ═════════════════════════════════════════════════════════════
    // 外部文件变更感知
    // ═════════════════════════════════════════════════════════════

    @OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
    @Test
    fun disk_watcher_bridge_emits_vfs_events() = runTest {
        val dispatcher = UnconfinedTestDispatcher(testScheduler)
        val watchScope = CoroutineScope(SupervisorJob() + dispatcher)
        val fs = InMemoryFileSystem(watcherScope = watchScope)
        val diskOps = FakeWatchableDiskOps()
        fs.mount("/ext", diskOps).getOrThrow()

        val events = mutableListOf<FsEvent>()
        val collector = launch(dispatcher) {
            fs.watch("/ext").collect { events.add(it) }
        }

        diskOps.externalEvents.tryEmit(DiskFileEvent("/newfile.txt", FsEventKind.CREATED))
        diskOps.externalEvents.tryEmit(DiskFileEvent("/newfile.txt", FsEventKind.MODIFIED))
        diskOps.externalEvents.tryEmit(DiskFileEvent("/newfile.txt", FsEventKind.DELETED))

        assertEquals(3, events.size)
        assertEquals(FsEvent("/ext/newfile.txt", FsEventKind.CREATED), events[0])
        assertEquals(FsEvent("/ext/newfile.txt", FsEventKind.MODIFIED), events[1])
        assertEquals(FsEvent("/ext/newfile.txt", FsEventKind.DELETED), events[2])

        collector.cancel()
        watchScope.cancel()
    }

    @OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
    @Test
    fun disk_watcher_stops_on_unmount() = runTest {
        val dispatcher = UnconfinedTestDispatcher(testScheduler)
        val watchScope = CoroutineScope(SupervisorJob() + dispatcher)
        val fs = InMemoryFileSystem(watcherScope = watchScope)
        val diskOps = FakeWatchableDiskOps()
        fs.mount("/ext", diskOps).getOrThrow()

        val events = mutableListOf<FsEvent>()
        val collector = launch(dispatcher) {
            fs.watch("/ext").collect { events.add(it) }
        }

        // 卸载后事件应不再转发
        fs.unmount("/ext").getOrThrow()

        diskOps.externalEvents.tryEmit(DiskFileEvent("/after.txt", FsEventKind.CREATED))

        // unmount 后的磁盘事件不应出现在 VFS 事件流中
        assertTrue(events.none { it.path == "/ext/after.txt" })

        collector.cancel()
        watchScope.cancel()
    }

    @Test
    fun sync_on_non_mount_path_fails() = runTest {
        val fs = createFs()
        fs.createDir("/local").getOrThrow()

        val result = fs.sync("/local")
        assertTrue(result.isFailure)
        assertIs<FsError.NotMounted>(result.exceptionOrNull())
    }

    @Test
    fun sync_returns_events_for_mounted_path() = runTest {
        val fs = createFs()
        val diskOps = FakeDiskFileOperations()
        fs.mount("/mnt", diskOps).getOrThrow()

        // 模拟外部直接在磁盘添加文件
        diskOps.files["/a.txt"] = "hello".encodeToByteArray()
        diskOps.files["/b.txt"] = "world".encodeToByteArray()

        val events = fs.sync("/mnt").getOrThrow()
        assertTrue(events.isNotEmpty())
        val paths = events.map { it.path }.toSet()
        assertTrue("/mnt/a.txt" in paths)
        assertTrue("/mnt/b.txt" in paths)
    }

    // ═════════════════════════════════════════════════════════════
    // 文件锁（flock）
    // ═════════════════════════════════════════════════════════════

    @Test
    fun tryLock_exclusive_blocks_second_handle() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "data".encodeToByteArray()).getOrThrow()

        val h1 = fs.open("/f.txt", OpenMode.READ_WRITE).getOrThrow()
        val h2 = fs.open("/f.txt", OpenMode.READ_WRITE).getOrThrow()

        h1.tryLock(FileLockType.EXCLUSIVE).getOrThrow()

        // 第二个句柄尝试加独占锁应失败
        val result = h2.tryLock(FileLockType.EXCLUSIVE)
        assertTrue(result.isFailure)
        assertIs<FsError.Locked>(result.exceptionOrNull())

        h1.close()
        h2.close()
    }

    @Test
    fun tryLock_shared_allows_multiple_readers() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "data".encodeToByteArray()).getOrThrow()

        val h1 = fs.open("/f.txt", OpenMode.READ).getOrThrow()
        val h2 = fs.open("/f.txt", OpenMode.READ).getOrThrow()
        val h3 = fs.open("/f.txt", OpenMode.READ).getOrThrow()

        // 多个共享锁可以并存
        h1.tryLock(FileLockType.SHARED).getOrThrow()
        h2.tryLock(FileLockType.SHARED).getOrThrow()
        h3.tryLock(FileLockType.SHARED).getOrThrow()

        h1.close()
        h2.close()
        h3.close()
    }

    @Test
    fun tryLock_shared_blocks_exclusive() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "data".encodeToByteArray()).getOrThrow()

        val h1 = fs.open("/f.txt", OpenMode.READ).getOrThrow()
        val h2 = fs.open("/f.txt", OpenMode.READ_WRITE).getOrThrow()

        h1.tryLock(FileLockType.SHARED).getOrThrow()

        // 有共享锁时，独占锁应失败
        val result = h2.tryLock(FileLockType.EXCLUSIVE)
        assertTrue(result.isFailure)
        assertIs<FsError.Locked>(result.exceptionOrNull())

        h1.close()
        h2.close()
    }

    @Test
    fun tryLock_exclusive_blocks_shared() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "data".encodeToByteArray()).getOrThrow()

        val h1 = fs.open("/f.txt", OpenMode.READ_WRITE).getOrThrow()
        val h2 = fs.open("/f.txt", OpenMode.READ).getOrThrow()

        h1.tryLock(FileLockType.EXCLUSIVE).getOrThrow()

        // 有独占锁时，共享锁也应失败
        val result = h2.tryLock(FileLockType.SHARED)
        assertTrue(result.isFailure)
        assertIs<FsError.Locked>(result.exceptionOrNull())

        h1.close()
        h2.close()
    }

    @Test
    fun unlock_releases_lock() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "data".encodeToByteArray()).getOrThrow()

        val h1 = fs.open("/f.txt", OpenMode.READ_WRITE).getOrThrow()
        val h2 = fs.open("/f.txt", OpenMode.READ_WRITE).getOrThrow()

        h1.tryLock(FileLockType.EXCLUSIVE).getOrThrow()

        // 解锁后第二个句柄应可获取锁
        h1.unlock().getOrThrow()
        h2.tryLock(FileLockType.EXCLUSIVE).getOrThrow()

        h1.close()
        h2.close()
    }

    @Test
    fun close_releases_lock_automatically() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "data".encodeToByteArray()).getOrThrow()

        val h1 = fs.open("/f.txt", OpenMode.READ_WRITE).getOrThrow()
        val h2 = fs.open("/f.txt", OpenMode.READ_WRITE).getOrThrow()

        h1.tryLock(FileLockType.EXCLUSIVE).getOrThrow()

        // close 会自动释放锁
        h1.close()
        h2.tryLock(FileLockType.EXCLUSIVE).getOrThrow()

        h2.close()
    }

    @Test
    fun lock_suspends_until_available() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "data".encodeToByteArray()).getOrThrow()

        val h1 = fs.open("/f.txt", OpenMode.READ_WRITE).getOrThrow()
        val h2 = fs.open("/f.txt", OpenMode.READ_WRITE).getOrThrow()

        h1.tryLock(FileLockType.EXCLUSIVE).getOrThrow()

        // h2.lock() 会挂起等待
        var h2Locked = false
        val lockJob = launch {
            h2.lock(FileLockType.EXCLUSIVE).getOrThrow()
            h2Locked = true
        }
        testScheduler.advanceUntilIdle()
        assertFalse(h2Locked) // 应还未获取到

        // h1 释放后 h2 应获取
        h1.unlock().getOrThrow()
        testScheduler.advanceUntilIdle()
        assertTrue(h2Locked)

        lockJob.cancel()
        h1.close()
        h2.close()
    }

    @Test
    fun delete_locked_file_fails() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "data".encodeToByteArray()).getOrThrow()

        val handle = fs.open("/f.txt", OpenMode.READ_WRITE).getOrThrow()
        handle.tryLock(FileLockType.EXCLUSIVE).getOrThrow()

        // 被锁定的文件不能删除
        val result = fs.delete("/f.txt")
        assertTrue(result.isFailure)
        assertIs<FsError.Locked>(result.exceptionOrNull())

        handle.close()

        // 释放后可以删除
        fs.delete("/f.txt").getOrThrow()
        assertTrue(fs.stat("/f.txt").isFailure)
    }

    @Test
    fun lock_upgrade_shared_to_exclusive() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "data".encodeToByteArray()).getOrThrow()

        val handle = fs.open("/f.txt", OpenMode.READ_WRITE).getOrThrow()

        // 先获取共享锁
        handle.tryLock(FileLockType.SHARED).getOrThrow()
        // 升级为独占锁（只有自己持有时允许）
        handle.tryLock(FileLockType.EXCLUSIVE).getOrThrow()

        handle.close()
    }

    @Test
    fun lock_different_files_independent() = runTest {
        val fs = createFs()
        fs.writeAll("/a.txt", "aaa".encodeToByteArray()).getOrThrow()
        fs.writeAll("/b.txt", "bbb".encodeToByteArray()).getOrThrow()

        val ha = fs.open("/a.txt", OpenMode.READ_WRITE).getOrThrow()
        val hb = fs.open("/b.txt", OpenMode.READ_WRITE).getOrThrow()

        // 不同文件的锁互不影响
        ha.tryLock(FileLockType.EXCLUSIVE).getOrThrow()
        hb.tryLock(FileLockType.EXCLUSIVE).getOrThrow()

        ha.close()
        hb.close()
    }

    @Test
    fun unlock_without_lock_is_noop() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "data".encodeToByteArray()).getOrThrow()

        val handle = fs.open("/f.txt", OpenMode.READ).getOrThrow()

        // 未加锁时 unlock 应成功（幂等）
        handle.unlock().getOrThrow()

        handle.close()
    }

    @Test
    fun closed_handle_operations_fail() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "data".encodeToByteArray()).getOrThrow()

        val handle = fs.open("/f.txt", OpenMode.READ_WRITE).getOrThrow()
        handle.close()

        // 关闭后所有操作应失败
        assertTrue(handle.readAt(0, 10).isFailure)
        assertTrue(handle.writeAt(0, "x".encodeToByteArray()).isFailure)
        assertTrue(handle.tryLock(FileLockType.EXCLUSIVE).isFailure)
        assertTrue(handle.lock(FileLockType.EXCLUSIVE).isFailure)
        assertTrue(handle.unlock().isFailure)

        // 重复 close 应幂等
        assertTrue(handle.close().isSuccess)
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

        val m = fs.metrics()
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

        val m = fs.metrics()
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

        val m = fs.metrics()
        assertEquals(1L, m.stat.failureCount)
        assertEquals(1L, m.delete.failureCount)
    }

    @Test
    fun metrics_reset() = runTest {
        val fs = createFs()
        fs.createDir("/a").getOrThrow()
        assertTrue(fs.metrics().createDir.count > 0)

        fs.resetMetrics()
        val m = fs.metrics()
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
        fs.mount("/mnt", diskOps).getOrThrow()
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
        fs.mount("/mnt", diskOps).getOrThrow()
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
        fs.mount("/mnt", diskOps).getOrThrow()

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
        fs.mount("/mnt", diskOps).getOrThrow()
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
        fs.mount("/mnt", diskOps).getOrThrow()
        diskOps.files["/f.txt"] = "data".encodeToByteArray()

        // 填充缓存
        fs.stat("/mnt/f.txt").getOrThrow()
        fs.readDir("/mnt").getOrThrow()

        // unmount 后再 mount，缓存应已清空
        fs.unmount("/mnt").getOrThrow()
        fs.mount("/mnt", diskOps).getOrThrow()

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
        val chunks = fs.readStream("/big.bin", chunkSize = 32 * 1024).toList()
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
        val info = fs.quotaInfo()
        assertFalse(info.hasQuota)
        assertEquals(-1L, info.quotaBytes)
        assertEquals(Long.MAX_VALUE, info.availableBytes)
    }

    @Test
    fun quota_info_tracks_usage() = runTest {
        val fs = createFsWithQuota(1024)
        val info1 = fs.quotaInfo()
        assertEquals(1024L, info1.quotaBytes)
        assertEquals(0L, info1.usedBytes)
        assertEquals(1024L, info1.availableBytes)

        fs.writeAll("/f.txt", ByteArray(100)).getOrThrow()
        val info2 = fs.quotaInfo()
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
        assertEquals(80L, fs.quotaInfo().usedBytes)
    }

    @Test
    fun quota_freed_on_delete() = runTest {
        val fs = createFsWithQuota(200)
        fs.writeAll("/a.txt", ByteArray(100)).getOrThrow()
        assertEquals(100L, fs.quotaInfo().usedBytes)

        fs.delete("/a.txt").getOrThrow()
        assertEquals(0L, fs.quotaInfo().usedBytes)
        assertEquals(200L, fs.quotaInfo().availableBytes)

        // 删除后空间释放，可以写入新文件
        fs.writeAll("/b.txt", ByteArray(180)).getOrThrow()
        assertEquals(180L, fs.quotaInfo().usedBytes)
    }

    @Test
    fun quota_no_limit_allows_large_write() = runTest {
        val fs = createFs()  // 无配额限制
        // 写入较大数据不应报错
        fs.writeAll("/big.bin", ByteArray(1024 * 1024)).getOrThrow()
        assertTrue(fs.quotaInfo().usedBytes >= 1024 * 1024)
    }

    // ═══════════════════════════════════════════════════════════
    // 文件哈希 / 校验
    // ═══════════════════════════════════════════════════════════

    @Test
    fun checksum_crc32() = runTest {
        val fs = createFs()
        val content = "Hello PicoFS"
        fs.writeAll("/f.txt", content.encodeToByteArray()).getOrThrow()

        val hash = fs.checksum("/f.txt", ChecksumAlgorithm.CRC32).getOrThrow()
        // CRC32 应返回 8 位十六进制字符串
        assertEquals(8, hash.length)
        assertTrue(hash.all { it in '0'..'9' || it in 'a'..'f' })
    }

    @Test
    fun checksum_sha256() = runTest {
        val fs = createFs()
        val content = "Hello PicoFS"
        fs.writeAll("/f.txt", content.encodeToByteArray()).getOrThrow()

        val hash = fs.checksum("/f.txt", ChecksumAlgorithm.SHA256).getOrThrow()
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

        val hashA = fs.checksum("/a.txt").getOrThrow()
        val hashB = fs.checksum("/b.txt").getOrThrow()
        assertEquals(hashA, hashB)
    }

    @Test
    fun checksum_different_content_different_hash() = runTest {
        val fs = createFs()
        fs.writeAll("/a.txt", "aaa".encodeToByteArray()).getOrThrow()
        fs.writeAll("/b.txt", "bbb".encodeToByteArray()).getOrThrow()

        val hashA = fs.checksum("/a.txt").getOrThrow()
        val hashB = fs.checksum("/b.txt").getOrThrow()
        assertNotEquals(hashA, hashB)
    }

    @Test
    fun checksum_empty_file() = runTest {
        val fs = createFs()
        fs.createFile("/empty.txt").getOrThrow()

        val hash = fs.checksum("/empty.txt").getOrThrow()
        assertEquals(64, hash.length) // SHA-256 默认
    }

    @Test
    fun checksum_not_found() = runTest {
        val fs = createFs()
        val result = fs.checksum("/missing.txt")
        assertTrue(result.isFailure)
        assertIs<FsError.NotFound>(result.exceptionOrNull())
    }

    @Test
    fun checksum_on_directory_fails() = runTest {
        val fs = createFs()
        fs.createDir("/d").getOrThrow()
        val result = fs.checksum("/d")
        assertTrue(result.isFailure)
        assertIs<FsError.NotFile>(result.exceptionOrNull())
    }

    @Test
    fun checksum_sha256_known_value() = runTest {
        // 验证 SHA-256 实现正确性：空字节数组的 SHA-256
        val fs = createFs()
        fs.createFile("/empty.txt").getOrThrow()
        val hash = fs.checksum("/empty.txt", ChecksumAlgorithm.SHA256).getOrThrow()
        // SHA-256("") = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
        assertEquals("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", hash)
    }

    @Test
    fun checksum_crc32_known_value() = runTest {
        val fs = createFs()
        fs.createFile("/empty.txt").getOrThrow()
        val hash = fs.checksum("/empty.txt", ChecksumAlgorithm.CRC32).getOrThrow()
        // CRC32("") = 00000000
        assertEquals("00000000", hash)
    }

    // ═══════════════════════════════════════════════════════════
    // 版本历史
    // ═══════════════════════════════════════════════════════════

    @Test
    fun version_history_empty_for_new_file() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "v1".encodeToByteArray()).getOrThrow()
        // 第一次写入时文件大小为 0，不会保存空版本
        val versions = fs.fileVersions("/f.txt").getOrThrow()
        assertTrue(versions.isEmpty())
    }

    @Test
    fun version_history_saved_on_write() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "v1".encodeToByteArray()).getOrThrow()
        fs.writeAll("/f.txt", "version 2".encodeToByteArray()).getOrThrow()

        val versions = fs.fileVersions("/f.txt").getOrThrow()
        assertEquals(1, versions.size)
        assertEquals(2L, versions[0].size) // "v1" 是 2 字节
    }

    @Test
    fun version_history_multiple_writes() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "v1".encodeToByteArray()).getOrThrow()
        fs.writeAll("/f.txt", "v2".encodeToByteArray()).getOrThrow()
        fs.writeAll("/f.txt", "v3".encodeToByteArray()).getOrThrow()

        val versions = fs.fileVersions("/f.txt").getOrThrow()
        // v1→v2 保存一个版本(v1)，v2→v3 保存一个版本(v2)
        assertEquals(2, versions.size)
        // 最新在前
        assertEquals(2L, versions[0].size) // "v2"
        assertEquals(2L, versions[1].size) // "v1"
    }

    @Test
    fun read_version_content() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "original".encodeToByteArray()).getOrThrow()
        fs.writeAll("/f.txt", "modified".encodeToByteArray()).getOrThrow()

        val versions = fs.fileVersions("/f.txt").getOrThrow()
        assertEquals(1, versions.size)

        val oldContent = fs.readVersion("/f.txt", versions[0].versionId).getOrThrow()
        assertEquals("original", oldContent.decodeToString())
    }

    @Test
    fun restore_version() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "v1-content".encodeToByteArray()).getOrThrow()
        fs.writeAll("/f.txt", "v2-content".encodeToByteArray()).getOrThrow()

        val versions = fs.fileVersions("/f.txt").getOrThrow()
        val v1Id = versions[0].versionId

        // 恢复到 v1
        fs.restoreVersion("/f.txt", v1Id).getOrThrow()

        // 当前内容应为 v1
        val current = fs.readAll("/f.txt").getOrThrow().decodeToString()
        assertEquals("v1-content", current)

        // 恢复操作应该保存了 v2 为新的历史版本
        val versionsAfter = fs.fileVersions("/f.txt").getOrThrow()
        assertTrue(versionsAfter.size >= 2)
    }

    @Test
    fun version_history_not_found() = runTest {
        val fs = createFs()
        val result = fs.fileVersions("/missing.txt")
        assertTrue(result.isFailure)
        assertIs<FsError.NotFound>(result.exceptionOrNull())
    }

    @Test
    fun read_version_invalid_id() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "data".encodeToByteArray()).getOrThrow()
        val result = fs.readVersion("/f.txt", "non-existent-id")
        assertTrue(result.isFailure)
        assertIs<FsError.NotFound>(result.exceptionOrNull())
    }

    @Test
    fun version_history_mount_supports_versions() = runTest {
        val fs = createFs()
        val diskOps = FakeDiskFileOperations()
        fs.mount("/mnt", diskOps).getOrThrow()

        // 初次写入不产生版本（因为是新文件）
        fs.writeAll("/mnt/f.txt", "v1".encodeToByteArray()).getOrThrow()
        val versions0 = fs.fileVersions("/mnt/f.txt").getOrThrow()
        assertTrue(versions0.isEmpty())

        // 第二次写入应保存 v1 为历史版本
        fs.writeAll("/mnt/f.txt", "v2".encodeToByteArray()).getOrThrow()
        val versions1 = fs.fileVersions("/mnt/f.txt").getOrThrow()
        assertEquals(1, versions1.size)
        assertEquals(2L, versions1[0].size) // "v1" 是 2 字节

        // 读取历史版本
        val oldContent = fs.readVersion("/mnt/f.txt", versions1[0].versionId).getOrThrow()
        assertEquals("v1", oldContent.decodeToString())
    }

    @Test
    fun version_history_mount_restore() = runTest {
        val fs = createFs()
        val diskOps = FakeDiskFileOperations()
        fs.mount("/mnt", diskOps).getOrThrow()

        fs.writeAll("/mnt/f.txt", "original".encodeToByteArray()).getOrThrow()
        fs.writeAll("/mnt/f.txt", "modified".encodeToByteArray()).getOrThrow()

        val versions = fs.fileVersions("/mnt/f.txt").getOrThrow()
        assertEquals(1, versions.size)

        // 恢复到 original
        fs.restoreVersion("/mnt/f.txt", versions[0].versionId).getOrThrow()

        // 当前磁盘内容应为 original
        val current = fs.readAll("/mnt/f.txt").getOrThrow().decodeToString()
        assertEquals("original", current)

        // 恢复操作应保存了 modified 为新版本
        val versionsAfter = fs.fileVersions("/mnt/f.txt").getOrThrow()
        assertTrue(versionsAfter.size >= 2)
    }

    @Test
    fun version_persisted_across_instances() = runTest {
        val storage = InMemoryFsStorage()
        val persistCfg = com.hrm.fs.core.persistence.PersistenceConfig(autoSnapshotEvery = 1)

        val fs1 = InMemoryFileSystem(storage = storage, persistenceConfig = persistCfg)
        fs1.writeAll("/f.txt", "v1".encodeToByteArray()).getOrThrow()
        fs1.writeAll("/f.txt", "v2".encodeToByteArray()).getOrThrow()

        val versionsFs1 = fs1.fileVersions("/f.txt").getOrThrow()
        assertEquals(1, versionsFs1.size)

        // 使用相同 storage 创建新实例，版本历史应通过快照保留
        val fs2 = InMemoryFileSystem(storage = storage, persistenceConfig = persistCfg)
        val versions = fs2.fileVersions("/f.txt").getOrThrow()
        assertEquals(1, versions.size)

        val oldContent = fs2.readVersion("/f.txt", versions[0].versionId).getOrThrow()
        assertEquals("v1", oldContent.decodeToString())
    }

    // ═══════════════════════════════════════════════════════════
    // 外部变更版本追踪
    // ═══════════════════════════════════════════════════════════

    @Test
    fun sync_saves_version_for_external_changes() = runTest {
        val fs = createFs()
        val diskOps = FakeDiskFileOperations()
        fs.mount("/mnt", diskOps).getOrThrow()

        // 通过 VFS 写入初始内容
        fs.writeAll("/mnt/f.txt", "initial".encodeToByteArray()).getOrThrow()
        // 首次写入（新文件）不产生版本，此时版本数为 0

        // 模拟外部程序直接修改磁盘文件（绕过 VFS）
        diskOps.files["/f.txt"] = "external-edit-1".encodeToByteArray()

        // sync 应检测到变更并保存当前磁盘内容为版本快照
        fs.sync("/mnt").getOrThrow()

        val versions = fs.fileVersions("/mnt/f.txt").getOrThrow()
        // sync 保存了 1 个版本（"external-edit-1" 的快照）
        assertEquals(1, versions.size)

        // 再次外部修改
        diskOps.files["/f.txt"] = "external-edit-2".encodeToByteArray()
        fs.sync("/mnt").getOrThrow()

        val versions2 = fs.fileVersions("/mnt/f.txt").getOrThrow()
        assertEquals(2, versions2.size)

        // 可以读取到第一次 sync 保存的版本内容
        val v = versions2.find { v ->
            fs.readVersion("/mnt/f.txt", v.versionId).getOrNull()
                ?.decodeToString() == "external-edit-1"
        }
        assertNotNull(v)
    }

    @OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
    @Test
    fun disk_watcher_saves_version_on_external_modify() = runTest {
        val dispatcher = UnconfinedTestDispatcher(testScheduler)
        val watchScope = CoroutineScope(SupervisorJob() + dispatcher)
        val fs = InMemoryFileSystem(watcherScope = watchScope)
        val diskOps = FakeWatchableDiskOps()
        fs.mount("/ext", diskOps).getOrThrow()

        // 模拟磁盘上已有文件
        diskOps.files["/doc.txt"] = "version-A".encodeToByteArray()

        // 外部修改事件：watcher 检测到 MODIFIED
        diskOps.externalEvents.tryEmit(DiskFileEvent("/doc.txt", FsEventKind.MODIFIED))
        testScheduler.advanceUntilIdle()

        // 应保存了版本快照
        val versions1 = fs.fileVersions("/ext/doc.txt").getOrThrow()
        assertEquals(1, versions1.size)

        // 外部再次修改
        diskOps.files["/doc.txt"] = "version-B".encodeToByteArray()
        diskOps.externalEvents.tryEmit(DiskFileEvent("/doc.txt", FsEventKind.MODIFIED))
        testScheduler.advanceUntilIdle()

        val versions2 = fs.fileVersions("/ext/doc.txt").getOrThrow()
        assertEquals(2, versions2.size)

        // 可以读到 version-A 的快照
        val oldContent = fs.readVersion("/ext/doc.txt", versions2[1].versionId).getOrThrow()
        assertEquals("version-A", oldContent.decodeToString())

        watchScope.cancel()
    }
}

// ═════════════════════════════════════════════════════════════════
// Fake DiskFileOperations for testing mounts
// ═════════════════════════════════════════════════════════════════

private class FakeDiskFileOperations : DiskFileOperations {
    override val rootPath: String = "/fake"

    val createdFiles = mutableSetOf<String>()
    val createdDirs = mutableSetOf<String>()
    val files = mutableMapOf<String, ByteArray>()
    val dirs = mutableSetOf<String>("/")

    override suspend fun createFile(path: String): Result<Unit> {
        createdFiles.add(path)
        files[path] = ByteArray(0)
        return Result.success(Unit)
    }

    override suspend fun createDir(path: String): Result<Unit> {
        createdDirs.add(path)
        dirs.add(path)
        return Result.success(Unit)
    }

    override suspend fun readFile(path: String, offset: Long, length: Int): Result<ByteArray> {
        val data = files[path] ?: return Result.failure(FsError.NotFound(path))
        if (offset >= data.size) return Result.success(ByteArray(0))
        val end = minOf(offset.toInt() + length, data.size)
        return Result.success(data.copyOfRange(offset.toInt(), end))
    }

    override suspend fun writeFile(path: String, offset: Long, data: ByteArray): Result<Unit> {
        val existing = files[path] ?: ByteArray(0)
        val end = offset.toInt() + data.size
        val newData = if (end > existing.size) {
            ByteArray(end).also { existing.copyInto(it) }
        } else {
            existing.copyOf()
        }
        data.copyInto(newData, destinationOffset = offset.toInt())
        files[path] = newData
        return Result.success(Unit)
    }

    override suspend fun delete(path: String): Result<Unit> {
        files.remove(path)
        dirs.remove(path)
        createdFiles.remove(path)
        createdDirs.remove(path)
        return Result.success(Unit)
    }

    override suspend fun list(path: String): Result<List<FsEntry>> {
        val prefix = if (path == "/") "/" else "$path/"
        val entries = mutableListOf<FsEntry>()
        for (f in files.keys) {
            if (f.startsWith(prefix) && f.removePrefix(prefix).count { it == '/' } == 0) {
                entries.add(FsEntry(f.substringAfterLast('/'), FsType.FILE))
            }
        }
        for (d in dirs) {
            if (d != path && d.startsWith(prefix) && d.removePrefix(prefix).count { it == '/' } == 0) {
                entries.add(FsEntry(d.substringAfterLast('/'), FsType.DIRECTORY))
            }
        }
        return Result.success(entries)
    }

    override suspend fun stat(path: String): Result<FsMeta> {
        if (files.containsKey(path)) {
            return Result.success(
                FsMeta(
                    path = path, type = FsType.FILE, size = files[path]!!.size.toLong(),
                    createdAtMillis = 0, modifiedAtMillis = 0, permissions = FsPermissions.FULL
                )
            )
        }
        if (dirs.contains(path)) {
            return Result.success(
                FsMeta(
                    path = path, type = FsType.DIRECTORY, size = 0,
                    createdAtMillis = 0, modifiedAtMillis = 0, permissions = FsPermissions.FULL
                )
            )
        }
        return Result.failure(FsError.NotFound(path))
    }

    override suspend fun exists(path: String): Boolean = files.containsKey(path) || dirs.contains(path)
}

// ═════════════════════════════════════════════════════════════════
// Fake DiskFileOperations + DiskFileWatcher for testing external change detection
// ═════════════════════════════════════════════════════════════════

private class FakeWatchableDiskOps : DiskFileOperations, DiskFileWatcher {
    override val rootPath: String = "/watchable"

    val files = mutableMapOf<String, ByteArray>()
    val dirs = mutableSetOf<String>("/")

    /** 外部通过此 flow 模拟磁盘变更事件。 */
    val externalEvents = MutableSharedFlow<DiskFileEvent>(extraBufferCapacity = 64)

    override fun watchDisk(scope: CoroutineScope): Flow<DiskFileEvent> = externalEvents

    override fun stopWatching() {}

    override suspend fun createFile(path: String): Result<Unit> {
        files[path] = ByteArray(0)
        return Result.success(Unit)
    }

    override suspend fun createDir(path: String): Result<Unit> {
        dirs.add(path)
        return Result.success(Unit)
    }

    override suspend fun readFile(path: String, offset: Long, length: Int): Result<ByteArray> {
        val data = files[path] ?: return Result.failure(FsError.NotFound(path))
        if (offset >= data.size) return Result.success(ByteArray(0))
        val end = minOf(offset.toInt() + length, data.size)
        return Result.success(data.copyOfRange(offset.toInt(), end))
    }

    override suspend fun writeFile(path: String, offset: Long, data: ByteArray): Result<Unit> {
        val existing = files[path] ?: ByteArray(0)
        val end = offset.toInt() + data.size
        val newData = if (end > existing.size) {
            ByteArray(end).also { existing.copyInto(it) }
        } else {
            existing.copyOf()
        }
        data.copyInto(newData, destinationOffset = offset.toInt())
        files[path] = newData
        return Result.success(Unit)
    }

    override suspend fun delete(path: String): Result<Unit> {
        files.remove(path)
        dirs.remove(path)
        return Result.success(Unit)
    }

    override suspend fun list(path: String): Result<List<FsEntry>> {
        val prefix = if (path == "/") "/" else "$path/"
        val entries = mutableListOf<FsEntry>()
        for (f in files.keys) {
            if (f.startsWith(prefix) && f.removePrefix(prefix).count { it == '/' } == 0) {
                entries.add(FsEntry(f.substringAfterLast('/'), FsType.FILE))
            }
        }
        for (d in dirs) {
            if (d != path && d.startsWith(prefix) && d.removePrefix(prefix).count { it == '/' } == 0) {
                entries.add(FsEntry(d.substringAfterLast('/'), FsType.DIRECTORY))
            }
        }
        return Result.success(entries)
    }

    override suspend fun stat(path: String): Result<FsMeta> {
        if (files.containsKey(path)) {
            return Result.success(
                FsMeta(
                    path = path, type = FsType.FILE, size = files[path]!!.size.toLong(),
                    createdAtMillis = 0, modifiedAtMillis = 0, permissions = FsPermissions.FULL
                )
            )
        }
        if (dirs.contains(path)) {
            return Result.success(
                FsMeta(
                    path = path, type = FsType.DIRECTORY, size = 0,
                    createdAtMillis = 0, modifiedAtMillis = 0, permissions = FsPermissions.FULL
                )
            )
        }
        return Result.failure(FsError.NotFound(path))
    }

    override suspend fun exists(path: String): Boolean = files.containsKey(path) || dirs.contains(path)
}
