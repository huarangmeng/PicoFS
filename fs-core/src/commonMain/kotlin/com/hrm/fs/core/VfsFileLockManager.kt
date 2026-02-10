package com.hrm.fs.core

import com.hrm.fs.api.FileLockType
import com.hrm.fs.api.FsError
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * 文件级锁管理器（类 POSIX flock 语义）。
 *
 * 锁粒度为文件路径。每个文件路径维护一个 [LockState]：
 * - 共享锁模式：允许多个持有者，阻止独占锁
 * - 独占锁模式：只允许一个持有者
 *
 * **线程安全**：内部使用自己的 [Mutex]，调用方无需额外同步。
 * 注意：此锁独立于 InMemoryFileSystem 的全局 Mutex，
 * 调用方应在全局 Mutex **外部**调用 [lock]，避免死锁。
 */
internal class VfsFileLockManager {

    private val mutex = Mutex()

    /**
     * 每个文件路径的锁状态。
     * key = 归一化虚拟路径, value = 当前锁状态。
     */
    private val locks = LinkedHashMap<String, LockState>()

    /**
     * 等待获取锁的挂起协程。
     * 当锁释放时，唤醒所有等待者让它们重新竞争。
     */
    private val waiters = LinkedHashMap<String, MutableList<Mutex>>()

    /**
     * 锁状态。
     *
     * @param type 当前锁类型
     * @param holders 持有锁的句柄标识集合（使用 identity hashCode 区分不同句柄实例）
     */
    internal data class LockState(
        val type: FileLockType,
        val holders: MutableSet<Long>
    )

    /**
     * 尝试加锁，不可用则立即返回失败。
     *
     * @param path 归一化虚拟路径
     * @param handleId 句柄的唯一标识
     * @param type 锁类型
     */
    suspend fun tryLock(path: String, handleId: Long, type: FileLockType): Result<Unit> {
        mutex.withLock {
            // 如果该 handle 已持有此文件的锁，先释放再重新加
            releaseLockInternal(path, handleId)
            return if (canAcquire(path, handleId, type)) {
                acquireInternal(path, handleId, type)
                Result.success(Unit)
            } else {
                Result.failure(FsError.Locked(path))
            }
        }
    }

    /**
     * 加锁，不可用则挂起等待。
     *
     * @param path 归一化虚拟路径
     * @param handleId 句柄的唯一标识
     * @param type 锁类型
     */
    suspend fun lock(path: String, handleId: Long, type: FileLockType): Result<Unit> {
        // 先尝试一次
        mutex.withLock {
            releaseLockInternal(path, handleId)
            if (canAcquire(path, handleId, type)) {
                acquireInternal(path, handleId, type)
                return Result.success(Unit)
            }
        }

        // 不可用，进入等待循环
        while (true) {
            val waiterMutex = Mutex(locked = true)
            mutex.withLock {
                // 再检查一次（可能在等待注册前已释放）
                if (canAcquire(path, handleId, type)) {
                    acquireInternal(path, handleId, type)
                    return Result.success(Unit)
                }
                waiters.getOrPut(path) { mutableListOf() }.add(waiterMutex)
            }
            // 挂起直到被唤醒
            waiterMutex.lock()

            // 被唤醒后重新尝试
            mutex.withLock {
                if (canAcquire(path, handleId, type)) {
                    acquireInternal(path, handleId, type)
                    return Result.success(Unit)
                }
                // 获取失败，继续循环等待
            }
        }
    }

    /**
     * 释放指定句柄在指定路径上的锁。
     * 幂等：未持有锁时返回成功。
     */
    suspend fun unlock(path: String, handleId: Long): Result<Unit> {
        mutex.withLock {
            releaseLockInternal(path, handleId)
            notifyWaiters(path)
        }
        return Result.success(Unit)
    }

    /**
     * 释放指定句柄在所有路径上持有的锁。
     * 用于 FileHandle.close() 时调用。
     */
    suspend fun unlockAll(handleId: Long) {
        mutex.withLock {
            val pathsToNotify = mutableListOf<String>()
            val iterator = locks.iterator()
            while (iterator.hasNext()) {
                val (path, state) = iterator.next()
                if (state.holders.remove(handleId)) {
                    pathsToNotify.add(path)
                    if (state.holders.isEmpty()) {
                        iterator.remove()
                    }
                }
            }
            for (path in pathsToNotify) {
                notifyWaiters(path)
            }
        }
    }

    /**
     * 检查文件是否被任何句柄锁定。
     */
    suspend fun isLocked(path: String): Boolean {
        mutex.withLock {
            return locks[path] != null
        }
    }

    /**
     * 检查文件是否被独占锁定。
     */
    suspend fun isExclusivelyLocked(path: String): Boolean {
        mutex.withLock {
            val state = locks[path] ?: return false
            return state.type == FileLockType.EXCLUSIVE
        }
    }

    /**
     * 获取指定句柄当前持有锁的路径（用于 close 时需要知道释放了哪些路径）。
     */
    suspend fun getLockedPath(handleId: Long): String? {
        mutex.withLock {
            for ((path, state) in locks) {
                if (handleId in state.holders) return path
            }
            return null
        }
    }

    // ─── 内部方法（调用方需持有 mutex） ────────────────────────

    private fun canAcquire(path: String, handleId: Long, type: FileLockType): Boolean {
        val state = locks[path] ?: return true // 无锁，可获取
        return when (type) {
            FileLockType.SHARED -> {
                // 共享锁：当前必须也是共享锁模式（或只有自己持有）
                state.type == FileLockType.SHARED ||
                        (state.holders.size == 1 && handleId in state.holders)
            }
            FileLockType.EXCLUSIVE -> {
                // 独占锁：必须没有其他持有者（只有自己或无人）
                state.holders.isEmpty() ||
                        (state.holders.size == 1 && handleId in state.holders)
            }
        }
    }

    private fun acquireInternal(path: String, handleId: Long, type: FileLockType) {
        val state = locks[path]
        if (state == null) {
            locks[path] = LockState(type, mutableSetOf(handleId))
        } else if (state.holders.size == 1 && handleId in state.holders) {
            // 升级/降级：仅自己持有时可以改变锁类型
            locks[path] = LockState(type, state.holders)
        } else {
            // 共享锁追加
            state.holders.add(handleId)
        }
    }

    private fun releaseLockInternal(path: String, handleId: Long) {
        val state = locks[path] ?: return
        state.holders.remove(handleId)
        if (state.holders.isEmpty()) {
            locks.remove(path)
        }
    }

    private fun notifyWaiters(path: String) {
        val list = waiters.remove(path) ?: return
        for (waiter in list) {
            try {
                waiter.unlock()
            } catch (_: IllegalStateException) {
                // 已被取消的等待者，忽略
            }
        }
    }
}
