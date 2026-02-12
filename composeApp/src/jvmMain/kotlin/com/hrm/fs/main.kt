package com.hrm.fs

import androidx.compose.ui.window.Window
import androidx.compose.ui.window.application
import com.hrm.fs.platform.createDiskFileOperations

fun main() = application {
    val basePath = System.getProperty("user.home") + "/PicoFS"
    val diskOps = createDiskFileOperations(basePath)
    Window(
        onCloseRequest = ::exitApplication,
        title = "PicoFS",
    ) {
        App(diskOps = diskOps, storageDirPath = basePath)
    }
}
