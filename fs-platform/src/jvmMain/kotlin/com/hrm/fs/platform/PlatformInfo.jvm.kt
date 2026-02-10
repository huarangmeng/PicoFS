package com.hrm.fs.platform

class JVMPlatformInfo : PlatformInfo {
    override val name: String = "Java ${System.getProperty("java.version")}"
}

actual fun getPlatformInfo(): PlatformInfo = JVMPlatformInfo()
