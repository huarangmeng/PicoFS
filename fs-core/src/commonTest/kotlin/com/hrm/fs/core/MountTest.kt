package com.hrm.fs.core

import com.hrm.fs.api.*
import kotlinx.coroutines.test.runTest
import kotlin.test.*

class MountTest {

    @Test
    fun mount_and_operations() = runTest {
        val fs = createFs()
        val fakeDisk = FakeDiskFileOperations()

        fs.mounts.mount("/mnt", fakeDisk).getOrThrow()
        assertEquals(listOf("/mnt"), fs.mounts.list())

        // 创建文件
        fs.createDir("/mnt/sub").getOrThrow()
        fs.createFile("/mnt/sub/test.txt").getOrThrow()

        assertTrue(fakeDisk.createdDirs.contains("/sub"))
        assertTrue(fakeDisk.createdFiles.contains("/sub/test.txt"))
    }

    @Test
    fun mount_root_fails() = runTest {
        val fs = createFs()
        val result = fs.mounts.mount("/", FakeDiskFileOperations())
        assertTrue(result.isFailure)
    }

    @Test
    fun unmount_removes() = runTest {
        val fs = createFs()
        val fakeDisk = FakeDiskFileOperations()
        fs.mounts.mount("/mnt", fakeDisk).getOrThrow()
        fs.mounts.unmount("/mnt").getOrThrow()
        assertTrue(fs.mounts.list().isEmpty())
    }

    @Test
    fun unmount_notMounted_fails() = runTest {
        val fs = createFs()
        val result = fs.mounts.unmount("/nowhere")
        assertTrue(result.isFailure)
        assertIs<FsError.NotMounted>(result.exceptionOrNull())
    }

    @Test
    fun mount_readOnly_blocks_write() = runTest {
        val fs = createFs()
        val fakeDisk = FakeDiskFileOperations()
        fs.mounts.mount("/ro", fakeDisk, MountOptions(readOnly = true)).getOrThrow()

        val result = fs.createFile("/ro/test.txt")
        assertTrue(result.isFailure)
        assertIs<FsError.PermissionDenied>(result.exceptionOrNull())
    }

    @Test
    fun mount_readOnly_allows_read() = runTest {
        val fs = createFs()
        val fakeDisk = FakeDiskFileOperations()
        // 先用可写挂载创建文件
        fs.mounts.mount("/rw", fakeDisk).getOrThrow()
        fs.createDir("/rw/sub").getOrThrow()
        fs.mounts.unmount("/rw").getOrThrow()

        // 以只读重新挂载
        fs.mounts.mount("/rw", fakeDisk, MountOptions(readOnly = true)).getOrThrow()
        val result = fs.readDir("/rw/sub")
        assertTrue(result.isSuccess)
    }

    @Test
    fun delete_mountpoint_fails() = runTest {
        val fs = createFs()
        fs.mounts.mount("/mnt", FakeDiskFileOperations()).getOrThrow()
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
        fs1.mounts.mount("/mnt", fakeDisk).getOrThrow()
        assertEquals(listOf("/mnt"), fs1.mounts.list())

        // fs2: 使用同一个 storage，不手动 mount
        val fs2 = createFs(storage)
        // 活跃挂载为空（没重新 mount）
        assertTrue(fs2.mounts.list().isEmpty())
        // pending 中应该有 /mnt
        val pending = fs2.mounts.pending()
        assertEquals(1, pending.size)
        assertEquals("/mnt", pending[0].virtualPath)
        assertEquals("/fake", pending[0].rootPath)
        assertFalse(pending[0].readOnly)
    }

    @Test
    fun mount_persistence_readOnly() = runTest {
        val storage = InMemoryFsStorage()

        val fs1 = createFs(storage)
        fs1.mounts.mount("/ro", FakeDiskFileOperations(), MountOptions(readOnly = true)).getOrThrow()

        val fs2 = createFs(storage)
        val pending = fs2.mounts.pending()
        assertEquals(1, pending.size)
        assertTrue(pending[0].readOnly)
    }

    @Test
    fun mount_clears_pending() = runTest {
        val storage = InMemoryFsStorage()

        val fs1 = createFs(storage)
        fs1.mounts.mount("/mnt", FakeDiskFileOperations()).getOrThrow()

        val fs2 = createFs(storage)
        assertEquals(1, fs2.mounts.pending().size)

        // 重新挂载后 pending 应该清除
        fs2.mounts.mount("/mnt", FakeDiskFileOperations()).getOrThrow()
        assertTrue(fs2.mounts.pending().isEmpty())
        assertEquals(listOf("/mnt"), fs2.mounts.list())
    }
}
