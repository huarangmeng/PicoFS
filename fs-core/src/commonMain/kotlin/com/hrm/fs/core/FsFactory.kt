package com.hrm.fs.core

import com.hrm.fs.api.FileSystem
import com.hrm.fs.api.FsStorage

fun createInMemoryFileSystem(storage: FsStorage? = null): FileSystem = InMemoryFileSystem(storage)
