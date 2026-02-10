package com.hrm.fs.core

import com.hrm.fs.api.FsEvent
import com.hrm.fs.api.FsEventKind
import com.hrm.fs.api.PathUtils
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.filter

/**
 * 文件系统事件总线。
 *
 * 内部使用 [MutableSharedFlow] + extraBufferCapacity 做发布/订阅，
 * 调用方通过 [watch] 获得过滤后的 [Flow]。
 */
internal class VfsEventBus {

    private val flow = MutableSharedFlow<FsEvent>(extraBufferCapacity = 64)

    /** 发射一个事件（非挂起，tryEmit）。 */
    fun emit(path: String, kind: FsEventKind) {
        flow.tryEmit(FsEvent(path, kind))
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
