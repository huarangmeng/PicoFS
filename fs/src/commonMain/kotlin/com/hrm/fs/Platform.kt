package com.hrm.fs

interface Platform {
    val name: String
}

expect fun getPlatform(): Platform