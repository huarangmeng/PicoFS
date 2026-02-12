package com.hrm.fs.demo

import android.os.Bundle
import android.util.Log
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.compose.runtime.Composable
import androidx.compose.ui.tooling.preview.Preview
import com.hrm.fs.App
import com.hrm.fs.api.log.FLog
import com.hrm.fs.api.log.IFsLogger
import com.hrm.fs.platform.createDiskFileOperations

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        enableEdgeToEdge()
        super.onCreate(savedInstanceState)

        val basePath = filesDir.absolutePath
        val diskOps = createDiskFileOperations(basePath)
        FLog.setLogger(object : IFsLogger {
            override fun v(tag: String, message: String) {
                Log.v(tag, message)
            }

            override fun d(tag: String, message: String) {
                Log.d(tag, message)
            }

            override fun i(tag: String, message: String) {
                Log.i(tag, message)
            }

            override fun w(tag: String, message: String) {
                Log.w(tag, message)
            }

            override fun e(
                tag: String,
                message: String,
                throwable: Throwable?
            ) {
                Log.e(tag, message, throwable)
            }

        })
        setContent {
            App(diskOps = diskOps, storageDirPath = basePath)
        }
    }
}

@Preview
@Composable
fun AppAndroidPreview() {
    App()
}
