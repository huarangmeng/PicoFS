package com.hrm.fs.core

import com.hrm.fs.api.FsMetrics
import com.hrm.fs.api.OpMetrics
import kotlin.concurrent.atomics.AtomicLong
import kotlin.concurrent.atomics.ExperimentalAtomicApi
import kotlin.time.TimeMark
import kotlin.time.TimeSource

/**
 * VFS 操作统计指标收集器（无锁实现）。
 *
 * 使用 [AtomicLong] 替代 synchronized 锁，完全消除争用：
 * - 每个操作类型的每个计数器都是独立的原子变量
 * - begin/end/addBytesRead/addBytesWritten 全部无锁
 * - snapshot() 读取的是近似一致的快照（各计数器之间无强一致性，但统计场景可接受）
 *
 * 性能影响：零锁争用，不阻塞任何协程/线程。
 */
@OptIn(ExperimentalAtomicApi::class)
internal class VfsMetricsCollector {

    private val ops = Array(Op.entries.size) { AtomicOpData() }
    private val bytesRead = AtomicLong(0L)
    private val bytesWritten = AtomicLong(0L)

    enum class Op {
        CREATE_FILE, CREATE_DIR, DELETE, READ_DIR, STAT, OPEN,
        READ_ALL, WRITE_ALL, COPY, MOVE, MOUNT, UNMOUNT, SYNC, SET_PERMISSIONS
    }

    /** 记录一次操作开始，返回一个 [TimeMark] 用于 [end]。 */
    fun begin(): TimeMark = TimeSource.Monotonic.markNow()

    /** 记录操作结束（无锁）。 */
    fun <T> end(op: Op, mark: TimeMark, result: Result<T>) {
        val elapsed = mark.elapsedNow().inWholeMilliseconds
        val d = ops[op.ordinal]
        d.count.fetchAndAdd(1L)
        if (result.isSuccess) d.successCount.fetchAndAdd(1L) else d.failureCount.fetchAndAdd(1L)
        d.totalTimeMs.fetchAndAdd(elapsed)
        // CAS 更新 maxTimeMs（无锁 max）
        while (true) {
            val cur = d.maxTimeMs.load()
            if (elapsed <= cur) break
            if (d.maxTimeMs.compareAndSet(cur, elapsed)) break
        }
    }

    /** 记录读取字节数（无锁）。 */
    fun addBytesRead(n: Long) {
        bytesRead.fetchAndAdd(n)
    }

    /** 记录写入字节数（无锁）。 */
    fun addBytesWritten(n: Long) {
        bytesWritten.fetchAndAdd(n)
    }

    /** 获取当前指标快照（近似一致性，各计数器独立读取）。 */
    fun snapshot(): FsMetrics {
        return FsMetrics(
            createFile = ops[Op.CREATE_FILE.ordinal].toOpMetrics(),
            createDir = ops[Op.CREATE_DIR.ordinal].toOpMetrics(),
            delete = ops[Op.DELETE.ordinal].toOpMetrics(),
            readDir = ops[Op.READ_DIR.ordinal].toOpMetrics(),
            stat = ops[Op.STAT.ordinal].toOpMetrics(),
            open = ops[Op.OPEN.ordinal].toOpMetrics(),
            readAll = ops[Op.READ_ALL.ordinal].toOpMetrics(),
            writeAll = ops[Op.WRITE_ALL.ordinal].toOpMetrics(),
            copy = ops[Op.COPY.ordinal].toOpMetrics(),
            move = ops[Op.MOVE.ordinal].toOpMetrics(),
            mount = ops[Op.MOUNT.ordinal].toOpMetrics(),
            unmount = ops[Op.UNMOUNT.ordinal].toOpMetrics(),
            sync = ops[Op.SYNC.ordinal].toOpMetrics(),
            setPermissions = ops[Op.SET_PERMISSIONS.ordinal].toOpMetrics(),
            totalBytesRead = bytesRead.load(),
            totalBytesWritten = bytesWritten.load()
        )
    }

    /** 重置所有指标。 */
    fun reset() {
        for (d in ops) d.reset()
        bytesRead.store(0L)
        bytesWritten.store(0L)
    }

    private class AtomicOpData {
        val count = AtomicLong(0L)
        val successCount = AtomicLong(0L)
        val failureCount = AtomicLong(0L)
        val totalTimeMs = AtomicLong(0L)
        val maxTimeMs = AtomicLong(0L)

        fun toOpMetrics(): OpMetrics = OpMetrics(
            count = count.load(),
            successCount = successCount.load(),
            failureCount = failureCount.load(),
            totalTimeMs = totalTimeMs.load(),
            maxTimeMs = maxTimeMs.load()
        )

        fun reset() {
            count.store(0L)
            successCount.store(0L)
            failureCount.store(0L)
            totalTimeMs.store(0L)
            maxTimeMs.store(0L)
        }
    }
}
