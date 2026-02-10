package com.hrm.fs

import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.safeContentPadding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.Button
import androidx.compose.material3.Card
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateListOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Modifier
import androidx.compose.ui.unit.dp
import com.hrm.fs.api.FsPermissions
import com.hrm.fs.api.InMemoryFsStorage
import com.hrm.fs.api.OpenMode
import com.hrm.fs.core.createInMemoryFileSystem

@Composable
fun App() {
    MaterialTheme {
        val storage = remember { InMemoryFsStorage() }
        val fs = remember { createInMemoryFileSystem(storage) }
        val logs = remember { mutableStateListOf<String>() }
        var lastRead by remember { mutableStateOf("(empty)") }
        var readOnly by remember { mutableStateOf(false) }

        fun log(message: String) {
            logs.add(0, message)
        }

        val dirPath = "/docs"
        val filePath = "/docs/hello.txt"

        Column(
            modifier = Modifier
                .fillMaxSize()
                .safeContentPadding()
                .padding(16.dp)
                .verticalScroll(rememberScrollState()),
            verticalArrangement = Arrangement.spacedBy(12.dp)
        ) {
            Text("PicoFS VFS 演示", style = MaterialTheme.typography.titleLarge)
            Text("目录: $dirPath")
            Text("文件: $filePath")

            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                Button(onClick = {
                    val result = fs.createDir(dirPath)
                    log(result.fold({ "创建目录成功" }, { "创建目录失败: ${it.message}" }))
                }) { Text("创建目录") }

                Button(onClick = {
                    val result = fs.createFile(filePath)
                    log(result.fold({ "创建文件成功" }, { "创建文件失败: ${it.message}" }))
                }) { Text("创建文件") }
            }

            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                Button(onClick = {
                    val handle = fs.open(filePath, OpenMode.WRITE)
                    val result = handle.fold(
                        onSuccess = {
                            val writeResult = it.writeAt(0, "Hello PicoFS".encodeToByteArray())
                            it.close()
                            writeResult
                        },
                        onFailure = { Result.failure(it) }
                    )
                    log(result.fold({ "写入成功" }, { "写入失败: ${it.message}" }))
                }) { Text("写入") }

                Button(onClick = {
                    val handle = fs.open(filePath, OpenMode.READ)
                    val result = handle.fold(
                        onSuccess = {
                            val readResult = it.readAt(0, 1024)
                            it.close()
                            readResult
                        },
                        onFailure = { Result.failure(it) }
                    )
                    result.onSuccess { lastRead = it.decodeToString() }
                    log(result.fold({ "读取成功: ${lastRead}" }, { "读取失败: ${it.message}" }))
                }) { Text("读取") }
            }

            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                Button(onClick = {
                    val result = fs.readDir("/")
                    log(
                        result.fold(
                        { "根目录: ${it.joinToString { entry -> entry.name }}" },
                        { "列目录失败: ${it.message}" }
                    ))
                }) { Text("列根目录") }

                Button(onClick = {
                    val result = fs.readDir(dirPath)
                    log(
                        result.fold(
                        { "目录内容: ${it.joinToString { entry -> entry.name }}" },
                        { "列目录失败: ${it.message}" }
                    ))
                }) { Text("列 /docs") }
            }

            Button(onClick = {
                readOnly = !readOnly
                val perms = if (readOnly) FsPermissions.READ_ONLY else FsPermissions.FULL
                val result = fs.setPermissions(filePath, perms)
                val label = if (readOnly) "只读" else "可写"
                log(result.fold({ "权限更新: $label" }, { "权限更新失败: ${it.message}" }))
            }) { Text(if (readOnly) "设置可写" else "设置只读") }

            Card(modifier = Modifier.fillMaxWidth()) {
                Column(
                    modifier = Modifier.padding(12.dp),
                    verticalArrangement = Arrangement.spacedBy(8.dp)
                ) {
                    Text("最近读取内容: $lastRead")
                }
            }

            Card(modifier = Modifier.fillMaxWidth()) {
                Column(
                    modifier = Modifier.padding(12.dp),
                    verticalArrangement = Arrangement.spacedBy(6.dp)
                ) {
                    Text("日志", style = MaterialTheme.typography.titleMedium)
                    logs.take(20).forEach { Text(it) }
                }
            }
        }
    }
}