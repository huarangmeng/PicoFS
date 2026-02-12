package com.hrm.fs.core

import com.hrm.fs.api.FileSystem
import com.hrm.fs.api.FsConfig

fun createFileSystem(config: FsConfig = FsConfig()): FileSystem {
    return InMemoryFileSystem(storage = config.storage)
}