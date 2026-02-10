package com.hrm.fs.platform

interface PlatformInfo {
    val name: String
}

expect fun getPlatformInfo(): PlatformInfo