package com.hrm.fs

import androidx.compose.animation.AnimatedVisibility
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.safeContentPadding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.FilledTonalButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.Scaffold
import androidx.compose.material3.SnackbarHost
import androidx.compose.material3.SnackbarHostState
import androidx.compose.material3.Surface
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateListOf
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.rememberCoroutineScope
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.hrm.fs.api.DiskFileOperations
import com.hrm.fs.api.FsConfig
import com.hrm.fs.api.FsEntry
import com.hrm.fs.api.FsType
import com.hrm.fs.api.FileSystem
import com.hrm.fs.core.createFileSystem
import kotlinx.coroutines.launch

@Composable
fun App(diskOps: DiskFileOperations? = null) {
    MaterialTheme {
        val fs = remember { createFileSystem(FsConfig()) }
        val scope = rememberCoroutineScope()
        val snackbar = remember { SnackbarHostState() }
        var mounted by remember { mutableStateOf(false) }

        // 自动挂载
        LaunchedEffect(diskOps) {
            if (diskOps != null && !mounted) {
                fs.mount("/disk", diskOps).fold(
                    { mounted = true },
                    { snackbar.showSnackbar("挂载失败: ${it.message}") }
                )
            }
        }

        Scaffold(
            snackbarHost = { SnackbarHost(snackbar) }
        ) { padding ->
            FileExplorer(
                fs = fs,
                modifier = Modifier
                    .fillMaxSize()
                    .padding(padding)
                    .safeContentPadding(),
                onMessage = { msg -> scope.launch { snackbar.showSnackbar(msg) } }
            )
        }
    }
}

// ═════════════════════════════════════════════════════════════════
// 文件浏览器主体
// ═════════════════════════════════════════════════════════════════

@Composable
private fun FileExplorer(
    fs: FileSystem,
    modifier: Modifier = Modifier,
    onMessage: (String) -> Unit
) {
    val scope = rememberCoroutineScope()
    var currentPath by remember { mutableStateOf("/") }
    val entries = remember { mutableStateListOf<FsEntry>() }
    var showNewFileDialog by remember { mutableStateOf(false) }
    var showNewDirDialog by remember { mutableStateOf(false) }
    var renameTarget by remember { mutableStateOf<FsEntry?>(null) }
    var deleteTarget by remember { mutableStateOf<FsEntry?>(null) }

    fun refresh() {
        scope.launch {
            fs.readDir(currentPath).fold(
                { list ->
                    entries.clear()
                    val sorted = list.sortedWith(compareBy<FsEntry> {
                        if (it.type == FsType.DIRECTORY) 0 else 1
                    }.thenBy { it.name })
                    entries.addAll(sorted)
                },
                { onMessage("读取失败: ${it.message}") }
            )
        }
    }

    fun navigateTo(path: String) {
        currentPath = path
    }

    fun goUp() {
        if (currentPath == "/") return
        val parent = currentPath.substringBeforeLast('/', "/")
        navigateTo(if (parent.isEmpty()) "/" else parent)
    }

    // 自动刷新
    LaunchedEffect(currentPath) { refresh() }

    Column(modifier = modifier) {
        // ── 顶部导航栏 ──
        PathBar(
            currentPath = currentPath,
            onGoUp = ::goUp,
            onNavigate = ::navigateTo
        )

        // ── 工具栏 ──
        ToolBar(
            onNewFile = { showNewFileDialog = true },
            onNewDir = { showNewDirDialog = true },
            onRefresh = ::refresh
        )

        // ── 文件列表 ──
        if (entries.isEmpty()) {
            Box(
                modifier = Modifier.fillMaxSize(),
                contentAlignment = Alignment.Center
            ) {
                Text(
                    "空目录",
                    style = MaterialTheme.typography.bodyLarge,
                    color = MaterialTheme.colorScheme.onSurfaceVariant
                )
            }
        } else {
            LazyColumn(
                modifier = Modifier.fillMaxSize(),
                verticalArrangement = Arrangement.spacedBy(2.dp)
            ) {
                items(entries, key = { it.name }) { entry ->
                    FileRow(
                        entry = entry,
                        onClick = {
                            if (entry.type == FsType.DIRECTORY) {
                                val next = if (currentPath == "/") "/${entry.name}"
                                else "$currentPath/${entry.name}"
                                navigateTo(next)
                            } else {
                                scope.launch {
                                    fs.readAll(
                                        if (currentPath == "/") "/${entry.name}"
                                        else "$currentPath/${entry.name}"
                                    ).fold(
                                        { data ->
                                            val text = data.decodeToString()
                                            val preview = if (text.length > 200) text.take(200) + "..." else text
                                            onMessage("内容: $preview")
                                        },
                                        { onMessage("读取失败: ${it.message}") }
                                    )
                                }
                            }
                        },
                        onRename = { renameTarget = entry },
                        onDelete = { deleteTarget = entry }
                    )
                }
            }
        }
    }

    // ── 弹窗 ──
    if (showNewFileDialog) {
        InputDialog(
            title = "新建文件",
            placeholder = "输入文件名",
            onConfirm = { name ->
                showNewFileDialog = false
                scope.launch {
                    val path = if (currentPath == "/") "/$name" else "$currentPath/$name"
                    fs.createFile(path).fold(
                        { refresh(); onMessage("已创建: $name") },
                        { onMessage("创建失败: ${it.message}") }
                    )
                }
            },
            onDismiss = { showNewFileDialog = false }
        )
    }

    if (showNewDirDialog) {
        InputDialog(
            title = "新建文件夹",
            placeholder = "输入文件夹名",
            onConfirm = { name ->
                showNewDirDialog = false
                scope.launch {
                    val path = if (currentPath == "/") "/$name" else "$currentPath/$name"
                    fs.createDir(path).fold(
                        { refresh(); onMessage("已创建: $name") },
                        { onMessage("创建失败: ${it.message}") }
                    )
                }
            },
            onDismiss = { showNewDirDialog = false }
        )
    }

    renameTarget?.let { entry ->
        InputDialog(
            title = "重命名",
            placeholder = "输入新名称",
            initialValue = entry.name,
            onConfirm = { newName ->
                renameTarget = null
                scope.launch {
                    val oldPath = if (currentPath == "/") "/${entry.name}" else "$currentPath/${entry.name}"
                    val newPath = if (currentPath == "/") "/$newName" else "$currentPath/$newName"
                    fs.rename(oldPath, newPath).fold(
                        { refresh(); onMessage("已重命名: ${entry.name} -> $newName") },
                        { onMessage("重命名失败: ${it.message}") }
                    )
                }
            },
            onDismiss = { renameTarget = null }
        )
    }

    deleteTarget?.let { entry ->
        ConfirmDialog(
            title = "删除",
            text = "确定删除「${entry.name}」${if (entry.type == FsType.DIRECTORY) "及其所有内容" else ""}？",
            onConfirm = {
                deleteTarget = null
                scope.launch {
                    val path = if (currentPath == "/") "/${entry.name}" else "$currentPath/${entry.name}"
                    val result = if (entry.type == FsType.DIRECTORY) fs.deleteRecursive(path) else fs.delete(path)
                    result.fold(
                        { refresh(); onMessage("已删除: ${entry.name}") },
                        { onMessage("删除失败: ${it.message}") }
                    )
                }
            },
            onDismiss = { deleteTarget = null }
        )
    }
}

// ═════════════════════════════════════════════════════════════════
// 路径导航栏
// ═════════════════════════════════════════════════════════════════

@Composable
private fun PathBar(
    currentPath: String,
    onGoUp: () -> Unit,
    onNavigate: (String) -> Unit
) {
    Surface(
        tonalElevation = 2.dp,
        modifier = Modifier.fillMaxWidth()
    ) {
        Row(
            modifier = Modifier.padding(horizontal = 12.dp, vertical = 8.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            // 返回上级
            FilledTonalButton(
                onClick = onGoUp,
                enabled = currentPath != "/",
                modifier = Modifier.size(36.dp),
                shape = CircleShape,
                contentPadding = ButtonDefaults.TextButtonWithIconContentPadding
            ) {
                Text("↑", fontSize = 16.sp)
            }

            Spacer(Modifier.width(8.dp))

            // 面包屑导航
            val segments = if (currentPath == "/") listOf("/")
            else listOf("/") + currentPath.removePrefix("/").split("/")
            var buildPath = ""

            Row(
                modifier = Modifier.weight(1f),
                verticalAlignment = Alignment.CenterVertically
            ) {
                segments.forEachIndexed { index, seg ->
                    if (index > 0) {
                        Text(
                            " / ",
                            color = MaterialTheme.colorScheme.onSurfaceVariant,
                            fontSize = 14.sp
                        )
                    }
                    val targetPath = if (index == 0) "/" else {
                        buildPath = "$buildPath/$seg"
                        buildPath
                    }
                    val isLast = index == segments.lastIndex
                    Text(
                        text = if (index == 0) "root" else seg,
                        color = if (isLast) MaterialTheme.colorScheme.primary
                        else MaterialTheme.colorScheme.onSurfaceVariant,
                        fontWeight = if (isLast) FontWeight.Bold else FontWeight.Normal,
                        fontSize = 14.sp,
                        modifier = if (!isLast) Modifier.clickable { onNavigate(targetPath) }
                        else Modifier
                    )
                }
            }
        }
    }
}

// ═════════════════════════════════════════════════════════════════
// 工具栏
// ═════════════════════════════════════════════════════════════════

@Composable
private fun ToolBar(
    onNewFile: () -> Unit,
    onNewDir: () -> Unit,
    onRefresh: () -> Unit
) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .padding(horizontal = 12.dp, vertical = 6.dp),
        horizontalArrangement = Arrangement.spacedBy(8.dp),
        verticalAlignment = Alignment.CenterVertically
    ) {
        FilledTonalButton(onClick = onNewFile) {
            Text("+ 文件")
        }
        FilledTonalButton(onClick = onNewDir) {
            Text("+ 文件夹")
        }
        Spacer(Modifier.weight(1f))
        TextButton(onClick = onRefresh) {
            Text("刷新")
        }
    }
}

// ═════════════════════════════════════════════════════════════════
// 文件行
// ═════════════════════════════════════════════════════════════════

@Composable
private fun FileRow(
    entry: FsEntry,
    onClick: () -> Unit,
    onRename: () -> Unit,
    onDelete: () -> Unit
) {
    val isDir = entry.type == FsType.DIRECTORY
    var showActions by remember { mutableStateOf(false) }

    Card(
        modifier = Modifier
            .fillMaxWidth()
            .padding(horizontal = 12.dp),
        shape = RoundedCornerShape(10.dp),
        colors = CardDefaults.cardColors(
            containerColor = MaterialTheme.colorScheme.surface
        ),
        elevation = CardDefaults.cardElevation(defaultElevation = 0.5.dp)
    ) {
        Column {
            Row(
                modifier = Modifier
                    .fillMaxWidth()
                    .clickable(onClick = onClick)
                    .padding(horizontal = 14.dp, vertical = 12.dp),
                verticalAlignment = Alignment.CenterVertically
            ) {
                // 图标
                Box(
                    modifier = Modifier
                        .size(38.dp)
                        .clip(RoundedCornerShape(8.dp))
                        .background(
                            if (isDir) MaterialTheme.colorScheme.primaryContainer
                            else MaterialTheme.colorScheme.tertiaryContainer
                        ),
                    contentAlignment = Alignment.Center
                ) {
                    Text(
                        text = if (isDir) "\uD83D\uDCC1" else "\uD83D\uDCC4",
                        fontSize = 18.sp
                    )
                }

                Spacer(Modifier.width(12.dp))

                // 名字 + 类型
                Column(modifier = Modifier.weight(1f)) {
                    Text(
                        text = entry.name,
                        style = MaterialTheme.typography.bodyLarge,
                        fontWeight = FontWeight.Medium,
                        maxLines = 1,
                        overflow = TextOverflow.Ellipsis
                    )
                    Text(
                        text = if (isDir) "文件夹" else "文件",
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }

                // 展开操作按钮
                TextButton(onClick = { showActions = !showActions }) {
                    Text(if (showActions) "收起" else "操作", fontSize = 13.sp)
                }

                if (isDir) {
                    Text("›", fontSize = 20.sp, color = MaterialTheme.colorScheme.onSurfaceVariant)
                }
            }

            // 操作面板
            AnimatedVisibility(visible = showActions) {
                Row(
                    modifier = Modifier
                        .fillMaxWidth()
                        .padding(start = 64.dp, end = 14.dp, bottom = 10.dp),
                    horizontalArrangement = Arrangement.spacedBy(8.dp)
                ) {
                    FilledTonalButton(onClick = { showActions = false; onRename() }) {
                        Text("重命名")
                    }
                    Button(
                        onClick = { showActions = false; onDelete() },
                        colors = ButtonDefaults.buttonColors(
                            containerColor = MaterialTheme.colorScheme.errorContainer,
                            contentColor = MaterialTheme.colorScheme.onErrorContainer
                        )
                    ) {
                        Text("删除")
                    }
                }
            }
        }
    }
}

// ═════════════════════════════════════════════════════════════════
// 输入弹窗
// ═════════════════════════════════════════════════════════════════

@Composable
private fun InputDialog(
    title: String,
    placeholder: String,
    initialValue: String = "",
    onConfirm: (String) -> Unit,
    onDismiss: () -> Unit
) {
    var value by remember { mutableStateOf(initialValue) }

    AlertDialog(
        onDismissRequest = onDismiss,
        title = { Text(title) },
        text = {
            OutlinedTextField(
                value = value,
                onValueChange = { value = it },
                placeholder = { Text(placeholder) },
                singleLine = true,
                modifier = Modifier.fillMaxWidth()
            )
        },
        confirmButton = {
            Button(
                onClick = { if (value.isNotBlank()) onConfirm(value.trim()) },
                enabled = value.isNotBlank()
            ) {
                Text("确定")
            }
        },
        dismissButton = {
            TextButton(onClick = onDismiss) { Text("取消") }
        }
    )
}

// ═════════════════════════════════════════════════════════════════
// 确认弹窗
// ═════════════════════════════════════════════════════════════════

@Composable
private fun ConfirmDialog(
    title: String,
    text: String,
    onConfirm: () -> Unit,
    onDismiss: () -> Unit
) {
    AlertDialog(
        onDismissRequest = onDismiss,
        title = { Text(title) },
        text = { Text(text) },
        confirmButton = {
            Button(
                onClick = onConfirm,
                colors = ButtonDefaults.buttonColors(
                    containerColor = MaterialTheme.colorScheme.error
                )
            ) {
                Text("删除")
            }
        },
        dismissButton = {
            TextButton(onClick = onDismiss) { Text("取消") }
        }
    )
}
