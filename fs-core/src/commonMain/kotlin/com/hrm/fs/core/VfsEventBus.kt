package com.hrm.fs.core

import com.hrm.fs.api.FsEvent
import com.hrm.fs.api.FsEventKind
import com.hrm.fs.api.PathUtils
import com.hrm.fs.api.log.FLog
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.filter

/**
 * 文件系统事件总线。
 *
 * 内部使用 [MutableSharedFlow] + extraBufferCapacity 做发布/订阅，
 * 调用方通过 [watch] 获得过滤后的 [Flow]。
 *
 * 缓冲区设为 256，当缓冲区满（订阅者消费太慢）时记录警告日志，
 * 避免事件静默丢失。
 */
internal class VfsEventBus {

    companion object {
        private const val TAG = "VfsEventBus"
    }

    private val flow = MutableSharedFlow<FsEvent>(extraBufferCapacity = 256)

    /** 发射一个事件（非挂起，tryEmit）。缓冲区满时记录警告。 */
    fun emit(path: String, kind: FsEventKind) {
        val event = FsEvent(path, kind)
        if (!flow.tryEmit(event)) {
            FLog.w(TAG, "事件缓冲区已满，丢弃事件: path=$path, kind=$kind")
        }
    }

    /**
     * 监听 [path] 及其子路径下的事件。
     */
    fun watch(path: String): Flow<FsEvent> {
        val normalized = PathUtils.normalize(path)
        return flow.asSharedFlow().filter { event ->
            event.path == normalized || event.path.startsWith("$normalized/")
        }
    }
}
