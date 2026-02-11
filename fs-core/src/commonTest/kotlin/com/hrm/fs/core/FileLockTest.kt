package com.hrm.fs.core

import com.hrm.fs.api.*
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlin.test.*

class FileLockTest {

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
}
