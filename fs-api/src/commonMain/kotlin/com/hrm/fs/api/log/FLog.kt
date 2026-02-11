package com.hrm.fs.api.log

/**
 * PicoFS 全局日志门面（单例）。
 *
 * 使用方式：
 * ```
 * // 1. 初始化时注入日志实现
 * FLog.setLogger(myLoggerImpl)
 *
 * // 2. 在代码中使用
 * FLog.d("InMemoryFS", "createFile: path=$path")
 * FLog.e("InMemoryFS", "writeAll failed", exception)
 * ```
 *
 * 若未注入 logger，所有日志调用将被静默忽略（不会抛异常）。
 */
object FLog {
    private var loggerImpl: IFsLogger? = null

    fun setLogger(logger: IFsLogger?) {
        loggerImpl = logger
    }

    fun v(tag: String, message: String) {
        loggerImpl?.v(tag, message)
    }

    fun d(tag: String, message: String) {
        loggerImpl?.d(tag, message)
    }

    fun i(tag: String, message: String) {
        loggerImpl?.i(tag, message)
    }

    fun w(tag: String, message: String) {
        loggerImpl?.w(tag, message)
    }

    fun e(tag: String, message: String, throwable: Throwable? = null) {
        loggerImpl?.e(tag, message, throwable)
    }
}
