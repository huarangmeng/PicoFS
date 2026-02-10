package com.hrm.fs.demo

import android.os.Bundle
import androidx.activity.ComponentActivity
import androidx.activity.compose.setContent
import androidx.activity.enableEdgeToEdge
import androidx.compose.runtime.Composable
import androidx.compose.ui.tooling.preview.Preview
import com.hrm.fs.App
import com.hrm.fs.platform.createDiskFileOperations

class MainActivity : ComponentActivity() {
    override fun onCreate(savedInstanceState: Bundle?) {
        enableEdgeToEdge()
        super.onCreate(savedInstanceState)

        val diskOps = createDiskFileOperations(filesDir.absolutePath)
        setContent {
            App(diskOps = diskOps)
        }
    }
}

@Preview
@Composable
fun AppAndroidPreview() {
    App()
}
