package com.hrm.fs.core

import com.hrm.fs.api.*
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runTest
import kotlin.test.*

class WatchEventTest {

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
}
