package com.hrm.fs.core

import com.hrm.fs.api.*
import com.hrm.fs.core.persistence.PersistenceConfig
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.runTest
import kotlin.test.*

class VersionHistoryTest {

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
        val persistCfg = PersistenceConfig(autoSnapshotEvery = 1)

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
