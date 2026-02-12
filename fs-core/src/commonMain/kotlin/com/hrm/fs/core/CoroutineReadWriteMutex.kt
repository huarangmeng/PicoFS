package com.hrm.fs.core

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * 轻量级协程读写锁（写优先）。
 *
 * - 多个读操作可并发执行
 * - 写操作独占，与所有读/写互斥
 * - 写优先：有写等待时，新读请求会被阻塞，避免写饥饿
 *
 * 实现原理：
 *   state mutex 保护 readerCount / writerActive / writerWaiting 三个计数器，
 *   readGate / writeGate 分别作为读/写操作的等待门控。
 */
internal class CoroutineReadWriteMutex {

    /** 保护内部状态的短临界区锁 */
    private val state = Mutex()

    /** 读操作门控：writerActive 或 writerWaiting > 0 时，读操作在此挂起 */
    private val readGate = Mutex()

    /** 写操作门控：readerCount > 0 或另一个 writer 持锁时，写操作在此挂起 */
    private val writeGate = Mutex()

    private var readerCount = 0
    private var writerActive = false
    private var writerWaiting = 0

    /**
     * 获取读锁。多个读操作可并发进入。
     */
    suspend fun lockRead() {
        // 如果有 writer 活跃或等待，先等它释放
        while (true) {
            state.withLock {
                if (!writerActive && writerWaiting == 0) {
                    readerCount++
                    // 第一个 reader 锁住 writeGate，阻止 writer 进入
                    if (readerCount == 1 && !writeGate.isLocked) {
                        writeGate.tryLock()
                    }
                    return
                }
            }
            // 有 writer，在 readGate 上挂起等待
            readGate.lock()
            readGate.unlock()
        }
    }

    /**
     * 释放读锁。
     */
    suspend fun unlockRead() {
        state.withLock {
            readerCount--
            if (readerCount == 0) {
                // 最后一个 reader 离开，释放 writeGate 让 writer 进入
                if (writeGate.isLocked) {
                    writeGate.unlock()
                }
            }
        }
    }

    /**
     * 获取写锁。独占访问，与所有读/写互斥。
     */
    suspend fun lockWrite() {
        state.withLock {
            writerWaiting++
            // 锁住 readGate，阻止新 reader 进入（写优先）
            if (!readGate.isLocked) {
                readGate.tryLock()
            }
        }
        // 等待所有 reader 离开 + 上一个 writer 完成
        writeGate.lock()
        state.withLock {
            writerWaiting--
            writerActive = true
        }
    }

    /**
     * 释放写锁。
     */
    suspend fun unlockWrite() {
        state.withLock {
            writerActive = false
            // 释放 writeGate
            writeGate.unlock()
            // 如果没有更多 writer 等待，放行 reader
            if (writerWaiting == 0 && readGate.isLocked) {
                readGate.unlock()
            }
        }
    }
}

/**
 * 在读锁保护下执行 [block]。
 */
internal suspend inline fun <T> CoroutineReadWriteMutex.withReadLock(block: () -> T): T {
    lockRead()
    try {
        return block()
    } finally {
        unlockRead()
    }
}

/**
 * 在写锁保护下执行 [block]。
 */
internal suspend inline fun <T> CoroutineReadWriteMutex.withWriteLock(block: () -> T): T {
    lockWrite()
    try {
        return block()
    } finally {
        unlockWrite()
    }
}
