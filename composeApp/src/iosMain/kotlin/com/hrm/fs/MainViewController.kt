package com.hrm.fs

import androidx.compose.ui.window.ComposeUIViewController
import com.hrm.fs.platform.createDiskFileOperations
import platform.Foundation.NSHomeDirectory

fun MainViewController() = ComposeUIViewController {
    val diskOps = createDiskFileOperations(NSHomeDirectory() + "/Documents/PicoFS")
    App(diskOps = diskOps)
}
