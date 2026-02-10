package com.hrm.fs

import androidx.compose.ui.window.Window
import androidx.compose.ui.window.application
import com.hrm.fs.platform.createDiskFileOperations

fun main() = application {
    val diskOps = createDiskFileOperations(System.getProperty("user.home") + "/PicoFS")
    Window(
        onCloseRequest = ::exitApplication,
        title = "PicoFS",
    ) {
        App(diskOps = diskOps)
    }
}
