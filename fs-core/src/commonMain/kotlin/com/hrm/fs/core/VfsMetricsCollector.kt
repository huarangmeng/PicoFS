package com.hrm.fs.core

import com.hrm.fs.api.FsMetrics
import com.hrm.fs.api.OpMetrics
import kotlinx.coroutines.InternalCoroutinesApi
import kotlinx.coroutines.internal.SynchronizedObject
import kotlinx.coroutines.internal.synchronized
import kotlin.time.TimeSource

/**
 * VFS 操作统计指标收集器。
 *
 * 使用简单的同步访问（`@Synchronized`），不使用 Mutex，
 * 这样 [snapshot] 和 [reset] 可以在非挂起上下文中调用。
 */
internal class VfsMetricsCollector {

    private val data = mutableMapOf<Op, MutableOpData>()
    private var bytesRead = 0L
    private var bytesWritten = 0L

    @OptIn(InternalCoroutinesApi::class)
    private val lock = SynchronizedObject()

    enum class Op {
        CREATE_FILE, CREATE_DIR, DELETE, READ_DIR, STAT, OPEN,
        READ_ALL, WRITE_ALL, COPY, MOVE, MOUNT, UNMOUNT, SYNC, SET_PERMISSIONS
    }

    /** 记录一次操作开始，返回一个 [TimeMark] 用于 [end]。 */
    fun begin(): kotlin.time.TimeMark = TimeSource.Monotonic.markNow()

    /** 记录操作结束。 */
    @OptIn(InternalCoroutinesApi::class)
    fun <T> end(op: Op, mark: kotlin.time.TimeMark, result: Result<T>) {
        val elapsed = mark.elapsedNow().inWholeMilliseconds
        synchronized(lock) {
            val d = data.getOrPut(op) { MutableOpData() }
            d.count++
            if (result.isSuccess) d.successCount++ else d.failureCount++
            d.totalTimeMs += elapsed
            if (elapsed > d.maxTimeMs) d.maxTimeMs = elapsed
        }
    }

    /** 记录读取字节数。 */
    @OptIn(InternalCoroutinesApi::class)
    fun addBytesRead(n: Long) {
        synchronized(lock) { bytesRead += n }
    }

    /** 记录写入字节数。 */
    @OptIn(InternalCoroutinesApi::class)
    fun addBytesWritten(n: Long) {
        synchronized(lock) { bytesWritten += n }
    }

    /** 获取当前指标快照。 */
    @OptIn(InternalCoroutinesApi::class)
    fun snapshot(): FsMetrics {
        synchronized(lock) {
            return FsMetrics(
                createFile = data[Op.CREATE_FILE].toOpMetrics(),
                createDir = data[Op.CREATE_DIR].toOpMetrics(),
                delete = data[Op.DELETE].toOpMetrics(),
                readDir = data[Op.READ_DIR].toOpMetrics(),
                stat = data[Op.STAT].toOpMetrics(),
                open = data[Op.OPEN].toOpMetrics(),
                readAll = data[Op.READ_ALL].toOpMetrics(),
                writeAll = data[Op.WRITE_ALL].toOpMetrics(),
                copy = data[Op.COPY].toOpMetrics(),
                move = data[Op.MOVE].toOpMetrics(),
                mount = data[Op.MOUNT].toOpMetrics(),
                unmount = data[Op.UNMOUNT].toOpMetrics(),
                sync = data[Op.SYNC].toOpMetrics(),
                setPermissions = data[Op.SET_PERMISSIONS].toOpMetrics(),
                totalBytesRead = bytesRead,
                totalBytesWritten = bytesWritten
            )
        }
    }

    /** 重置所有指标。 */
    @OptIn(InternalCoroutinesApi::class)
    fun reset() {
        synchronized(lock) {
            data.clear()
            bytesRead = 0
            bytesWritten = 0
        }
    }

    private class MutableOpData(
        var count: Long = 0,
        var successCount: Long = 0,
        var failureCount: Long = 0,
        var totalTimeMs: Long = 0,
        var maxTimeMs: Long = 0
    )

    private fun MutableOpData?.toOpMetrics(): OpMetrics =
        if (this == null) OpMetrics()
        else OpMetrics(
            count = count,
            successCount = successCount,
            failureCount = failureCount,
            totalTimeMs = totalTimeMs,
            maxTimeMs = maxTimeMs
        )
}
