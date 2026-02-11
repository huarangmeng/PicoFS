package com.hrm.fs.api.log

/**
 * PicoFS 日志接口。
 * 由外部在初始化时注入具体实现（Android Logcat、iOS NSLog、JVM SLF4J 等）。
 */
interface IFsLogger {
    fun v(tag: String, message: String)
    fun d(tag: String, message: String)
    fun i(tag: String, message: String)
    fun w(tag: String, message: String)
    fun e(tag: String, message: String, throwable: Throwable? = null)
}
