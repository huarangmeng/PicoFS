package com.hrm.fs.core

import com.hrm.fs.api.FileSystem
import com.hrm.fs.api.FsConfig
import com.hrm.fs.api.FsStorage

fun createFileSystem(config: FsConfig = FsConfig()): FileSystem {
    return InMemoryFileSystem(storage = config.storage)
}

fun createInMemoryFileSystem(storage: FsStorage? = null): FileSystem = InMemoryFileSystem(storage)
