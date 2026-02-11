package com.hrm.fs

import androidx.compose.animation.AnimatedVisibility
import androidx.compose.foundation.background
import androidx.compose.foundation.clickable
import androidx.compose.foundation.horizontalScroll
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.ExperimentalLayoutApi
import androidx.compose.foundation.layout.FlowRow
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.offset
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.width
import androidx.compose.foundation.lazy.LazyColumn
import androidx.compose.foundation.lazy.items
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.text.KeyboardActions
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.foundation.verticalScroll
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.AssistChip
import androidx.compose.material3.Button
import androidx.compose.material3.ButtonDefaults
import androidx.compose.material3.Card
import androidx.compose.material3.CardDefaults
import androidx.compose.material3.ElevatedCard
import androidx.compose.material3.FilledTonalButton
import androidx.compose.material3.HorizontalDivider
import androidx.compose.material3.LinearProgressIndicator
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.NavigationBar
import androidx.compose.material3.NavigationBarItem
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
import androidx.compose.ui.focus.FocusRequester
import androidx.compose.ui.focus.focusRequester
import androidx.compose.ui.text.TextRange
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.input.TextFieldValue
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import com.hrm.fs.api.ArchiveFormat
import com.hrm.fs.api.ChecksumAlgorithm
import com.hrm.fs.api.DiskFileOperations
import com.hrm.fs.api.FileSystem
import com.hrm.fs.api.FileVersion
import com.hrm.fs.api.FsConfig
import com.hrm.fs.api.FsEntry
import com.hrm.fs.api.FsMeta
import com.hrm.fs.api.FsType
import com.hrm.fs.api.TrashItem
import com.hrm.fs.core.createFileSystem
import kotlinx.coroutines.launch

// ═════════════════════════════════════════════════════════════════
// 顶层 Tab 页
// ═════════════════════════════════════════════════════════════════

private enum class AppTab(val label: String, val icon: String) {
    FILES("文件", "\uD83D\uDCC2"),
    SEARCH("搜索", "\uD83D\uDD0D"),
    TRASH("回收站", "\uD83D\uDDD1"),
    METRICS("指标", "\uD83D\uDCCA")
}

@Composable
fun App(diskOps: DiskFileOperations? = null) {
    MaterialTheme {
        val fs = remember { createFileSystem(FsConfig()) }
        val scope = rememberCoroutineScope()
        val snackbar = remember { SnackbarHostState() }
        var mounted by remember { mutableStateOf(false) }
        var currentTab by remember { mutableStateOf(AppTab.FILES) }

        LaunchedEffect(diskOps) {
            if (diskOps != null && !mounted) {
                fs.mounts.mount("/disk", diskOps).fold(
                    { mounted = true },
                    { snackbar.showSnackbar("挂载失败: ${it.message}") }
                )
            }
        }

        Scaffold(
            snackbarHost = { SnackbarHost(snackbar) },
            bottomBar = {
                NavigationBar {
                    AppTab.entries.forEach { tab ->
                        NavigationBarItem(
                            selected = currentTab == tab,
                            onClick = { currentTab = tab },
                            icon = { Text(tab.icon, fontSize = 20.sp) },
                            label = { Text(tab.label, fontSize = 12.sp) }
                        )
                    }
                }
            }
        ) { padding ->
            val mod = Modifier
                .fillMaxSize()
                .padding(padding)
            val msg: (String) -> Unit = { m -> scope.launch { snackbar.showSnackbar(m) } }

            // 所有 tab 始终存活，非活跃页移到屏幕外，保留组合状态
            Box(mod) {
                val hide = Modifier.fillMaxSize().offset(x = 9999.dp)
                val show = Modifier.fillMaxSize()

                Box(if (currentTab == AppTab.FILES) show else hide) {
                    FileExplorer(fs = fs, modifier = Modifier.fillMaxSize(), onMessage = msg)
                }
                Box(if (currentTab == AppTab.SEARCH) show else hide) {
                    SearchPage(fs = fs, modifier = Modifier.fillMaxSize(), onMessage = msg)
                }
                Box(if (currentTab == AppTab.TRASH) show else hide) {
                    TrashPage(fs = fs, modifier = Modifier.fillMaxSize(), onMessage = msg)
                }
                Box(if (currentTab == AppTab.METRICS) show else hide) {
                    MetricsPage(fs = fs, modifier = Modifier.fillMaxSize())
                }
            }
        }
    }
}

// ═════════════════════════════════════════════════════════════════
// Tab 1: 文件浏览器（含全部操作）
// ═════════════════════════════════════════════════════════════════

@OptIn(ExperimentalLayoutApi::class)
@Composable
private fun FileExplorer(
    fs: FileSystem,
    modifier: Modifier = Modifier,
    onMessage: (String) -> Unit
) {
    val scope = rememberCoroutineScope()
    var currentPath by remember { mutableStateOf("/") }
    val entries = remember { mutableStateListOf<FsEntry>() }

    // 弹窗状态
    var showNewFileDialog by remember { mutableStateOf(false) }
    var showNewDirDialog by remember { mutableStateOf(false) }
    var showWriteDialog by remember { mutableStateOf(false) }
    var showSymlinkDialog by remember { mutableStateOf(false) }
    var renameTarget by remember { mutableStateOf<FsEntry?>(null) }
    var deleteTarget by remember { mutableStateOf<FsEntry?>(null) }
    var copyTarget by remember { mutableStateOf<FsEntry?>(null) }
    var moveTarget by remember { mutableStateOf<FsEntry?>(null) }
    var detailTarget by remember { mutableStateOf<FsEntry?>(null) }
    var versionTarget by remember { mutableStateOf<FsEntry?>(null) }
    var xattrTarget by remember { mutableStateOf<FsEntry?>(null) }
    var checksumTarget by remember { mutableStateOf<FsEntry?>(null) }
    var editTarget by remember { mutableStateOf<FsEntry?>(null) }
    var archiveTarget by remember { mutableStateOf<FsEntry?>(null) }
    var showExtractDialog by remember { mutableStateOf(false) }

    fun fullPath(name: String) = if (currentPath == "/") "/$name" else "$currentPath/$name"

    fun refresh() {
        scope.launch {
            fs.readDir(currentPath).fold(
                { list ->
                    entries.clear()
                    entries.addAll(list.sortedWith(
                        compareBy<FsEntry> { if (it.type == FsType.DIRECTORY) 0 else 1 }.thenBy { it.name }
                    ))
                },
                { onMessage("读取失败: ${it.message}") }
            )
        }
    }

    fun navigateTo(path: String) { currentPath = path }
    fun goUp() {
        if (currentPath != "/") {
            navigateTo(currentPath.substringBeforeLast('/', "/").ifEmpty { "/" })
        }
    }

    LaunchedEffect(currentPath) { refresh() }

    Column(modifier = modifier) {
        PathBar(currentPath, ::goUp, ::navigateTo)

        // ── 工具栏（两行，展示更多操作）──
        FlowRow(
            modifier = Modifier.fillMaxWidth().padding(horizontal = 12.dp, vertical = 4.dp),
            horizontalArrangement = Arrangement.spacedBy(6.dp),
            verticalArrangement = Arrangement.spacedBy(4.dp)
        ) {
            AssistChip(onClick = { showNewFileDialog = true }, label = { Text("+ 文件", fontSize = 12.sp) })
            AssistChip(onClick = { showNewDirDialog = true }, label = { Text("+ 文件夹", fontSize = 12.sp) })
            AssistChip(onClick = { showWriteDialog = true }, label = { Text("+ 写入", fontSize = 12.sp) })
            AssistChip(onClick = { showSymlinkDialog = true }, label = { Text("+ 符号链接", fontSize = 12.sp) })
            AssistChip(onClick = { showExtractDialog = true }, label = { Text("解压到此", fontSize = 12.sp) })
            AssistChip(onClick = { refresh() }, label = { Text("刷新", fontSize = 12.sp) })
        }

        // ── 文件列表 ──
        if (entries.isEmpty()) {
            Box(Modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
                Text("空目录", style = MaterialTheme.typography.bodyLarge,
                    color = MaterialTheme.colorScheme.onSurfaceVariant)
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
                                navigateTo(fullPath(entry.name))
                            } else {
                                scope.launch {
                                    fs.readAll(fullPath(entry.name)).fold(
                                        { data ->
                                            val text = data.decodeToString()
                                            val preview = if (text.length > 200) text.take(200) + "…" else text
                                            onMessage("内容: $preview")
                                        },
                                        { onMessage("读取失败: ${it.message}") }
                                    )
                                }
                            }
                        },
                        onRename = { renameTarget = entry },
                        onDelete = { deleteTarget = entry },
                        onCopy = { copyTarget = entry },
                        onMove = { moveTarget = entry },
                        onDetail = { detailTarget = entry },
                        onVersions = { versionTarget = entry },
                        onXattr = { xattrTarget = entry },
                        onChecksum = { checksumTarget = entry },
                        onEdit = { editTarget = entry },
                        onArchive = { archiveTarget = entry },
                        onTrash = {
                            scope.launch {
                                fs.trash.moveToTrash(fullPath(entry.name)).fold(
                                    { id -> refresh(); onMessage("已移入回收站 (ID: $id)") },
                                    { onMessage("回收站失败: ${it.message}") }
                                )
                            }
                        }
                    )
                }
            }
        }
    }

    // ═══════════════════════════════════════════════════════════
    // 各种弹窗
    // ═══════════════════════════════════════════════════════════

    // 新建文件
    if (showNewFileDialog) {
        InputDialog("新建文件", "文件名", onConfirm = { name ->
            showNewFileDialog = false
            scope.launch {
                fs.createFile(fullPath(name)).fold(
                    { refresh(); onMessage("已创建: $name") },
                    { onMessage("创建失败: ${it.message}") })
            }
        }, onDismiss = { showNewFileDialog = false })
    }

    // 新建文件夹
    if (showNewDirDialog) {
        InputDialog("新建文件夹", "文件夹名", onConfirm = { name ->
            showNewDirDialog = false
            scope.launch {
                fs.createDir(fullPath(name)).fold(
                    { refresh(); onMessage("已创建: $name") },
                    { onMessage("创建失败: ${it.message}") })
            }
        }, onDismiss = { showNewDirDialog = false })
    }

    // 写入文件（新建+写入内容）
    if (showWriteDialog) {
        TwoFieldDialog("写入文件", "文件路径 (相对当前目录)", "内容",
            onConfirm = { name, content ->
                showWriteDialog = false
                scope.launch {
                    fs.writeAll(fullPath(name), content.encodeToByteArray()).fold(
                        { refresh(); onMessage("已写入: $name") },
                        { onMessage("写入失败: ${it.message}") })
                }
            }, onDismiss = { showWriteDialog = false })
    }

    // 创建符号链接
    if (showSymlinkDialog) {
        TwoFieldDialog("创建符号链接", "链接名", "目标路径",
            onConfirm = { name, target ->
                showSymlinkDialog = false
                scope.launch {
                    fs.symlinks.create(fullPath(name), target).fold(
                        { refresh(); onMessage("已创建符号链接: $name -> $target") },
                        { onMessage("创建失败: ${it.message}") })
                }
            }, onDismiss = { showSymlinkDialog = false })
    }

    // 解压到当前目录
    if (showExtractDialog) {
        InputDialog("解压归档", "归档文件路径 (如 /archive.zip)", onConfirm = { archivePath ->
            showExtractDialog = false
            scope.launch {
                fs.archive.extract(archivePath, currentPath).fold(
                    { refresh(); onMessage("已解压到 $currentPath") },
                    { onMessage("解压失败: ${it.message}") })
            }
        }, onDismiss = { showExtractDialog = false })
    }

    // 重命名
    renameTarget?.let { entry ->
        InputDialog("重命名", "新名称", initialValue = entry.name, onConfirm = { newName ->
            renameTarget = null
            scope.launch {
                fs.rename(fullPath(entry.name), fullPath(newName)).fold(
                    { refresh(); onMessage("已重命名: ${entry.name} -> $newName") },
                    { onMessage("重命名失败: ${it.message}") })
            }
        }, onDismiss = { renameTarget = null })
    }

    // 删除确认
    deleteTarget?.let { entry ->
        ConfirmDialog("删除", "确定永久删除「${entry.name}」${if (entry.type == FsType.DIRECTORY) "及其所有内容" else ""}？",
            confirmLabel = "删除",
            onConfirm = {
                deleteTarget = null
                scope.launch {
                    val path = fullPath(entry.name)
                    val r = if (entry.type == FsType.DIRECTORY) fs.deleteRecursive(path) else fs.delete(path)
                    r.fold({ refresh(); onMessage("已删除: ${entry.name}") },
                        { onMessage("删除失败: ${it.message}") })
                }
            }, onDismiss = { deleteTarget = null })
    }

    // 复制
    copyTarget?.let { entry ->
        InputDialog("复制到", "目标路径", initialValue = fullPath(entry.name + "_copy"),
            onConfirm = { dst ->
                copyTarget = null
                scope.launch {
                    fs.copy(fullPath(entry.name), dst).fold(
                        { refresh(); onMessage("已复制到 $dst") },
                        { onMessage("复制失败: ${it.message}") })
                }
            }, onDismiss = { copyTarget = null })
    }

    // 移动
    moveTarget?.let { entry ->
        InputDialog("移动到", "目标路径", initialValue = fullPath(entry.name),
            onConfirm = { dst ->
                moveTarget = null
                scope.launch {
                    fs.move(fullPath(entry.name), dst).fold(
                        { refresh(); onMessage("已移动到 $dst") },
                        { onMessage("移动失败: ${it.message}") })
                }
            }, onDismiss = { moveTarget = null })
    }

    // 文件详情 (stat)
    detailTarget?.let { entry ->
        FileDetailDialog(fs, fullPath(entry.name), onDismiss = { detailTarget = null })
    }

    // 版本历史
    versionTarget?.let { entry ->
        VersionHistoryDialog(fs, fullPath(entry.name),
            onMessage = onMessage, onDismiss = { versionTarget = null })
    }

    // 扩展属性 (xattr)
    xattrTarget?.let { entry ->
        XattrDialog(fs, fullPath(entry.name),
            onMessage = onMessage, onDismiss = { xattrTarget = null })
    }

    // 校验和
    checksumTarget?.let { entry ->
        ChecksumDialog(fs, fullPath(entry.name), onDismiss = { checksumTarget = null })
    }

    // 编辑文件
    editTarget?.let { entry ->
        EditFileDialog(fs, fullPath(entry.name),
            onMessage = onMessage, onDismiss = { editTarget = null })
    }

    // 归档（压缩）
    archiveTarget?.let { entry ->
        ArchiveDialog(fs, fullPath(entry.name),
            onMessage = { msg -> refresh(); onMessage(msg) },
            onDismiss = { archiveTarget = null })
    }
}

// ═════════════════════════════════════════════════════════════════
// Tab 2: 搜索页
// ═════════════════════════════════════════════════════════════════

@Composable
private fun SearchPage(
    fs: FileSystem,
    modifier: Modifier = Modifier,
    onMessage: (String) -> Unit
) {
    val scope = rememberCoroutineScope()
    var namePattern by remember { mutableStateOf("") }
    var contentPattern by remember { mutableStateOf("") }
    var rootPath by remember { mutableStateOf("/") }
    var results by remember { mutableStateOf<List<String>>(emptyList()) }
    var searching by remember { mutableStateOf(false) }

    Column(modifier = modifier.padding(16.dp)) {
        Text("搜索文件", style = MaterialTheme.typography.headlineSmall, fontWeight = FontWeight.Bold)
        Spacer(Modifier.height(12.dp))

        OutlinedTextField(value = rootPath, onValueChange = { rootPath = it },
            label = { Text("搜索目录") }, modifier = Modifier.fillMaxWidth(), singleLine = true)
        Spacer(Modifier.height(8.dp))
        OutlinedTextField(value = namePattern, onValueChange = { namePattern = it },
            label = { Text("文件名模式 (如 *.txt)") }, modifier = Modifier.fillMaxWidth(), singleLine = true)
        Spacer(Modifier.height(8.dp))
        OutlinedTextField(value = contentPattern, onValueChange = { contentPattern = it },
            label = { Text("内容关键词 (可选)") }, modifier = Modifier.fillMaxWidth(), singleLine = true)
        Spacer(Modifier.height(12.dp))

        Button(onClick = {
            searching = true
            scope.launch {
                val query = com.hrm.fs.api.SearchQuery(
                    rootPath = rootPath.ifBlank { "/" },
                    namePattern = namePattern.ifBlank { null },
                    contentPattern = contentPattern.ifBlank { null }
                )
                fs.search.find(query).fold(
                    { list ->
                        results = list.map { r ->
                            val matchInfo = if (r.matchedLines.isNotEmpty())
                                " (${r.matchedLines.size} 行匹配)" else ""
                            "${r.path}  [${r.type}]  ${r.size}B$matchInfo"
                        }
                        if (list.isEmpty()) onMessage("未找到匹配项")
                    },
                    { onMessage("搜索失败: ${it.message}") }
                )
                searching = false
            }
        }, enabled = !searching) {
            Text(if (searching) "搜索中…" else "搜索")
        }

        if (searching) {
            Spacer(Modifier.height(8.dp))
            LinearProgressIndicator(Modifier.fillMaxWidth())
        }

        Spacer(Modifier.height(12.dp))

        if (results.isNotEmpty()) {
            Text("搜索结果 (${results.size})", style = MaterialTheme.typography.titleSmall)
            Spacer(Modifier.height(8.dp))
            LazyColumn(verticalArrangement = Arrangement.spacedBy(4.dp)) {
                items(results) { line ->
                    Card(
                        modifier = Modifier.fillMaxWidth(),
                        colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surfaceVariant)
                    ) {
                        Text(line, modifier = Modifier.padding(12.dp),
                            fontSize = 13.sp, fontFamily = FontFamily.Monospace)
                    }
                }
            }
        }
    }
}

// ═════════════════════════════════════════════════════════════════
// Tab 3: 回收站
// ═════════════════════════════════════════════════════════════════

@Composable
private fun TrashPage(
    fs: FileSystem,
    modifier: Modifier = Modifier,
    onMessage: (String) -> Unit
) {
    val scope = rememberCoroutineScope()
    val items = remember { mutableStateListOf<TrashItem>() }

    fun refresh() {
        scope.launch {
            fs.trash.list().fold(
                { list -> items.clear(); items.addAll(list) },
                { onMessage("加载回收站失败: ${it.message}") }
            )
        }
    }

    LaunchedEffect(Unit) { refresh() }

    Column(modifier = modifier.padding(16.dp)) {
        Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = Alignment.CenterVertically) {
            Text("回收站", style = MaterialTheme.typography.headlineSmall, fontWeight = FontWeight.Bold)
            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                TextButton(onClick = { refresh() }) { Text("刷新") }
                Button(onClick = {
                    scope.launch {
                        fs.trash.purgeAll().fold(
                            { refresh(); onMessage("回收站已清空") },
                            { onMessage("清空失败: ${it.message}") })
                    }
                }, colors = ButtonDefaults.buttonColors(containerColor = MaterialTheme.colorScheme.error)) {
                    Text("清空")
                }
            }
        }
        Spacer(Modifier.height(12.dp))

        if (items.isEmpty()) {
            Box(Modifier.fillMaxSize(), contentAlignment = Alignment.Center) {
                Text("回收站为空", color = MaterialTheme.colorScheme.onSurfaceVariant)
            }
        } else {
            LazyColumn(verticalArrangement = Arrangement.spacedBy(6.dp)) {
                items(items, key = { it.trashId }) { item ->
                    ElevatedCard(Modifier.fillMaxWidth()) {
                        Column(Modifier.padding(14.dp)) {
                            Text(item.originalPath, fontWeight = FontWeight.Medium)
                            Text("类型: ${item.type}  |  大小: ${item.size}B  |  ID: ${item.trashId}",
                                fontSize = 12.sp, color = MaterialTheme.colorScheme.onSurfaceVariant)
                            Spacer(Modifier.height(8.dp))
                            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                                FilledTonalButton(onClick = {
                                    scope.launch {
                                        fs.trash.restore(item.trashId).fold(
                                            { refresh(); onMessage("已恢复: ${item.originalPath}") },
                                            { onMessage("恢复失败: ${it.message}") })
                                    }
                                }) { Text("恢复") }
                                TextButton(onClick = {
                                    scope.launch {
                                        fs.trash.purge(item.trashId).fold(
                                            { refresh(); onMessage("已彻底删除") },
                                            { onMessage("删除失败: ${it.message}") })
                                    }
                                }) { Text("彻底删除", color = MaterialTheme.colorScheme.error) }
                            }
                        }
                    }
                }
            }
        }
    }
}

// ═════════════════════════════════════════════════════════════════
// Tab 4: 指标/配额
// ═════════════════════════════════════════════════════════════════

@Composable
private fun MetricsPage(
    fs: FileSystem,
    modifier: Modifier = Modifier
) {
    val scope = rememberCoroutineScope()
    var metricsText by remember { mutableStateOf("") }
    var quotaText by remember { mutableStateOf("") }
    var mountsText by remember { mutableStateOf("") }

    fun refresh() {
        scope.launch {
            val m = fs.observe.metrics()
            val q = fs.observe.quotaInfo()
            val mounts = fs.mounts.list()

            metricsText = buildString {
                appendLine("══ 操作统计 ══")
                fun fmt1(d: Double): String { val r = (d * 10).toLong(); return "${r / 10}.${r % 10}" }
                fun row(label: String, c: Long, s: Long, f: Long, avg: Double) {
                    if (c > 0) appendLine("$label: 总${c}次, 成功${s}, 失败${f}, 平均${fmt1(avg)}ms")
                }
                row("createFile", m.createFile.count, m.createFile.successCount, m.createFile.failureCount, m.createFile.avgTimeMs)
                row("createDir", m.createDir.count, m.createDir.successCount, m.createDir.failureCount, m.createDir.avgTimeMs)
                row("delete", m.delete.count, m.delete.successCount, m.delete.failureCount, m.delete.avgTimeMs)
                row("readDir", m.readDir.count, m.readDir.successCount, m.readDir.failureCount, m.readDir.avgTimeMs)
                row("stat", m.stat.count, m.stat.successCount, m.stat.failureCount, m.stat.avgTimeMs)
                row("readAll", m.readAll.count, m.readAll.successCount, m.readAll.failureCount, m.readAll.avgTimeMs)
                row("writeAll", m.writeAll.count, m.writeAll.successCount, m.writeAll.failureCount, m.writeAll.avgTimeMs)
                row("copy", m.copy.count, m.copy.successCount, m.copy.failureCount, m.copy.avgTimeMs)
                row("move", m.move.count, m.move.successCount, m.move.failureCount, m.move.avgTimeMs)
                row("open", m.open.count, m.open.successCount, m.open.failureCount, m.open.avgTimeMs)
                row("mount", m.mount.count, m.mount.successCount, m.mount.failureCount, m.mount.avgTimeMs)
                row("sync", m.sync.count, m.sync.successCount, m.sync.failureCount, m.sync.avgTimeMs)
                appendLine()
                appendLine("总读取: ${formatBytes(m.totalBytesRead)}")
                appendLine("总写入: ${formatBytes(m.totalBytesWritten)}")
            }
            quotaText = buildString {
                appendLine("══ 空间配额 ══")
                appendLine("限额: ${if (q.hasQuota) formatBytes(q.quotaBytes) else "无限制"}")
                appendLine("已用: ${formatBytes(q.usedBytes)}")
                appendLine("可用: ${if (q.hasQuota) formatBytes(q.availableBytes) else "无限制"}")
            }
            mountsText = buildString {
                appendLine("══ 挂载点 ══")
                if (mounts.isEmpty()) appendLine("(无)")
                else mounts.forEach { appendLine("  $it") }
            }
        }
    }

    LaunchedEffect(Unit) { refresh() }

    Column(modifier = modifier.padding(16.dp).verticalScroll(rememberScrollState())) {
        Row(Modifier.fillMaxWidth(), horizontalArrangement = Arrangement.SpaceBetween,
            verticalAlignment = Alignment.CenterVertically) {
            Text("系统指标", style = MaterialTheme.typography.headlineSmall, fontWeight = FontWeight.Bold)
            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                TextButton(onClick = { refresh() }) { Text("刷新") }
                TextButton(onClick = { fs.observe.resetMetrics(); refresh() }) { Text("重置") }
            }
        }
        Spacer(Modifier.height(12.dp))

        MonoCard(quotaText)
        Spacer(Modifier.height(8.dp))
        MonoCard(mountsText)
        Spacer(Modifier.height(8.dp))
        MonoCard(metricsText)
    }
}

@Composable
private fun MonoCard(text: String) {
    Card(
        Modifier.fillMaxWidth(),
        colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surfaceVariant)
    ) {
        Text(text, modifier = Modifier.padding(14.dp),
            fontSize = 13.sp, fontFamily = FontFamily.Monospace, lineHeight = 20.sp)
    }
}

private fun formatBytes(bytes: Long): String = when {
    bytes < 1024 -> "${bytes}B"
    bytes < 1024 * 1024 -> "${bytes / 1024}KB"
    else -> { val r = (bytes * 10 / 1024 / 1024); "${r / 10}.${r % 10}MB" }
}

// ═════════════════════════════════════════════════════════════════
// 文件详情弹窗 (stat + readLink)
// ═════════════════════════════════════════════════════════════════

@Composable
private fun FileDetailDialog(fs: FileSystem, path: String, onDismiss: () -> Unit) {
    val scope = rememberCoroutineScope()
    var info by remember { mutableStateOf("加载中…") }

    LaunchedEffect(path) {
        scope.launch {
            fs.stat(path).fold(
                { meta ->
                    info = buildString {
                        appendLine("路径: ${meta.path}")
                        appendLine("类型: ${meta.type}")
                        appendLine("大小: ${formatBytes(meta.size)}")
                        appendLine("创建: ${meta.createdAtMillis}")
                        appendLine("修改: ${meta.modifiedAtMillis}")
                        appendLine("权限: R=${meta.permissions.read} W=${meta.permissions.write} X=${meta.permissions.execute}")
                        if (meta.type == FsType.SYMLINK && meta.target != null) {
                            appendLine("链接目标: ${meta.target}")
                        }
                    }
                    if (meta.type == FsType.SYMLINK) {
                        fs.symlinks.readLink(path).fold(
                            { target -> info += "readLink: $target\n" },
                            { }
                        )
                    }
                },
                { info = "加载失败: ${it.message}" }
            )
        }
    }

    AlertDialog(onDismissRequest = onDismiss,
        title = { Text("文件详情") },
        text = { Text(info, fontFamily = FontFamily.Monospace, fontSize = 13.sp, lineHeight = 20.sp) },
        confirmButton = { TextButton(onClick = onDismiss) { Text("关闭") } })
}

// ═════════════════════════════════════════════════════════════════
// 版本历史弹窗
// ═════════════════════════════════════════════════════════════════

@Composable
private fun VersionHistoryDialog(
    fs: FileSystem,
    path: String,
    onMessage: (String) -> Unit,
    onDismiss: () -> Unit
) {
    val scope = rememberCoroutineScope()
    val versions = remember { mutableStateListOf<FileVersion>() }
    var loading by remember { mutableStateOf(true) }

    LaunchedEffect(path) {
        fs.versions.list(path).fold(
            { list -> versions.clear(); versions.addAll(list); loading = false },
            { onMessage("加载版本历史失败: ${it.message}"); loading = false }
        )
    }

    AlertDialog(onDismissRequest = onDismiss,
        title = { Text("版本历史: ${path.substringAfterLast('/')}") },
        text = {
            if (loading) {
                Text("加载中…")
            } else if (versions.isEmpty()) {
                Text("暂无历史版本")
            } else {
                Column(Modifier.verticalScroll(rememberScrollState())) {
                    versions.forEachIndexed { idx, v ->
                        Row(Modifier.fillMaxWidth().padding(vertical = 4.dp),
                            horizontalArrangement = Arrangement.SpaceBetween,
                            verticalAlignment = Alignment.CenterVertically) {
                            Column(Modifier.weight(1f)) {
                                Text("版本 ${idx + 1}", fontWeight = FontWeight.Medium, fontSize = 14.sp)
                                Text("${formatBytes(v.size)}  |  ID: ${v.versionId}",
                                    fontSize = 11.sp, color = MaterialTheme.colorScheme.onSurfaceVariant)
                            }
                            Row(horizontalArrangement = Arrangement.spacedBy(4.dp)) {
                                TextButton(onClick = {
                                    scope.launch {
                                        fs.versions.read(path, v.versionId).fold(
                                            { data ->
                                                val preview = data.decodeToString().take(200)
                                                onMessage("版本内容: $preview")
                                            },
                                            { onMessage("读取失败: ${it.message}") })
                                    }
                                }) { Text("查看", fontSize = 12.sp) }
                                TextButton(onClick = {
                                    scope.launch {
                                        fs.versions.restore(path, v.versionId).fold(
                                            { onMessage("已恢复到版本 ${v.versionId}") },
                                            { onMessage("恢复失败: ${it.message}") })
                                    }
                                }) { Text("恢复", fontSize = 12.sp) }
                            }
                        }
                        if (idx < versions.lastIndex) HorizontalDivider()
                    }
                }
            }
        },
        confirmButton = { TextButton(onClick = onDismiss) { Text("关闭") } })
}

// ═════════════════════════════════════════════════════════════════
// 扩展属性 (xattr) 弹窗
// ═════════════════════════════════════════════════════════════════

@Composable
private fun XattrDialog(
    fs: FileSystem,
    path: String,
    onMessage: (String) -> Unit,
    onDismiss: () -> Unit
) {
    val scope = rememberCoroutineScope()
    val attrs = remember { mutableStateListOf<Pair<String, String>>() }
    var newKey by remember { mutableStateOf("") }
    var newValue by remember { mutableStateOf("") }

    fun refresh() {
        scope.launch {
            fs.xattr.list(path).fold(
                { names ->
                    val loaded = names.map { name ->
                        val value = fs.xattr.get(path, name).getOrNull()?.decodeToString() ?: "(读取失败)"
                        name to value
                    }
                    attrs.clear()
                    attrs.addAll(loaded)
                },
                { onMessage("加载 xattr 失败: ${it.message}") }
            )
        }
    }

    LaunchedEffect(path) { refresh() }

    AlertDialog(onDismissRequest = onDismiss,
        title = { Text("扩展属性: ${path.substringAfterLast('/')}") },
        text = {
            Column(Modifier.verticalScroll(rememberScrollState())) {
                if (attrs.isEmpty()) {
                    Text("暂无扩展属性", color = MaterialTheme.colorScheme.onSurfaceVariant)
                } else {
                    attrs.forEach { (key, value) ->
                        Row(Modifier.fillMaxWidth().padding(vertical = 4.dp),
                            horizontalArrangement = Arrangement.SpaceBetween,
                            verticalAlignment = Alignment.CenterVertically) {
                            Column(Modifier.weight(1f)) {
                                Text(key, fontWeight = FontWeight.Medium, fontSize = 14.sp)
                                Text(value, fontSize = 12.sp, color = MaterialTheme.colorScheme.onSurfaceVariant,
                                    maxLines = 2, overflow = TextOverflow.Ellipsis)
                            }
                            TextButton(onClick = {
                                scope.launch {
                                    fs.xattr.remove(path, key).fold(
                                        { refresh(); onMessage("已删除: $key") },
                                        { onMessage("删除失败: ${it.message}") })
                                }
                            }) { Text("删除", fontSize = 12.sp, color = MaterialTheme.colorScheme.error) }
                        }
                        HorizontalDivider()
                    }
                }
                Spacer(Modifier.height(12.dp))
                Text("添加属性", fontWeight = FontWeight.Bold, fontSize = 14.sp)
                Spacer(Modifier.height(6.dp))
                OutlinedTextField(value = newKey, onValueChange = { newKey = it },
                    label = { Text("属性名") }, modifier = Modifier.fillMaxWidth(), singleLine = true)
                Spacer(Modifier.height(4.dp))
                OutlinedTextField(value = newValue, onValueChange = { newValue = it },
                    label = { Text("属性值") }, modifier = Modifier.fillMaxWidth(), singleLine = true)
                Spacer(Modifier.height(8.dp))
                Button(onClick = {
                    if (newKey.isNotBlank()) {
                        scope.launch {
                            fs.xattr.set(path, newKey.trim(), newValue.encodeToByteArray()).fold(
                                { newKey = ""; newValue = ""; refresh(); onMessage("已设置") },
                                { onMessage("设置失败: ${it.message}") })
                        }
                    }
                }, enabled = newKey.isNotBlank()) { Text("添加") }
            }
        },
        confirmButton = { TextButton(onClick = onDismiss) { Text("关闭") } })
}

// ═════════════════════════════════════════════════════════════════
// 校验和弹窗
// ═════════════════════════════════════════════════════════════════

@Composable
private fun ChecksumDialog(fs: FileSystem, path: String, onDismiss: () -> Unit) {
    val scope = rememberCoroutineScope()
    var crc32 by remember { mutableStateOf("计算中…") }
    var sha256 by remember { mutableStateOf("计算中…") }

    LaunchedEffect(path) {
        scope.launch {
            fs.checksum.compute(path, ChecksumAlgorithm.CRC32).fold(
                { crc32 = it }, { crc32 = "失败: ${it.message}" })
        }
        scope.launch {
            fs.checksum.compute(path, ChecksumAlgorithm.SHA256).fold(
                { sha256 = it }, { sha256 = "失败: ${it.message}" })
        }
    }

    AlertDialog(onDismissRequest = onDismiss,
        title = { Text("校验和: ${path.substringAfterLast('/')}") },
        text = {
            Column {
                Text("CRC32:", fontWeight = FontWeight.Bold, fontSize = 13.sp)
                Text(crc32, fontFamily = FontFamily.Monospace, fontSize = 12.sp)
                Spacer(Modifier.height(10.dp))
                Text("SHA-256:", fontWeight = FontWeight.Bold, fontSize = 13.sp)
                Text(sha256, fontFamily = FontFamily.Monospace, fontSize = 12.sp)
            }
        },
        confirmButton = { TextButton(onClick = onDismiss) { Text("关闭") } })
}

// ═════════════════════════════════════════════════════════════════
// 编辑文件弹窗 (readAll + writeAll)
// ═════════════════════════════════════════════════════════════════

@Composable
private fun EditFileDialog(
    fs: FileSystem,
    path: String,
    onMessage: (String) -> Unit,
    onDismiss: () -> Unit
) {
    val scope = rememberCoroutineScope()
    var content by remember { mutableStateOf("") }
    var loaded by remember { mutableStateOf(false) }

    LaunchedEffect(path) {
        fs.readAll(path).fold(
            { data -> content = data.decodeToString(); loaded = true },
            { onMessage("读取失败: ${it.message}"); loaded = true }
        )
    }

    AlertDialog(onDismissRequest = onDismiss,
        title = { Text("编辑: ${path.substringAfterLast('/')}") },
        text = {
            if (!loaded) {
                Text("加载中…")
            } else {
                OutlinedTextField(value = content, onValueChange = { content = it },
                    modifier = Modifier.fillMaxWidth().height(200.dp),
                    label = { Text("文件内容") })
            }
        },
        confirmButton = {
            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                TextButton(onClick = onDismiss) { Text("取消") }
                Button(onClick = {
                    scope.launch {
                        fs.writeAll(path, content.encodeToByteArray()).fold(
                            { onMessage("已保存"); onDismiss() },
                            { onMessage("保存失败: ${it.message}") })
                    }
                }, enabled = loaded) { Text("保存") }
            }
        })
}

// ═════════════════════════════════════════════════════════════════
// 归档弹窗 (compress)
// ═════════════════════════════════════════════════════════════════

@Composable
private fun ArchiveDialog(
    fs: FileSystem,
    sourcePath: String,
    onMessage: (String) -> Unit,
    onDismiss: () -> Unit
) {
    val scope = rememberCoroutineScope()
    var archivePath by remember { mutableStateOf("$sourcePath.zip") }
    var format by remember { mutableStateOf(ArchiveFormat.ZIP) }

    AlertDialog(onDismissRequest = onDismiss,
        title = { Text("打包归档") },
        text = {
            Column {
                Text("源: $sourcePath", fontSize = 13.sp, color = MaterialTheme.colorScheme.onSurfaceVariant)
                Spacer(Modifier.height(10.dp))
                OutlinedTextField(value = archivePath, onValueChange = { archivePath = it },
                    label = { Text("归档文件路径") }, modifier = Modifier.fillMaxWidth(), singleLine = true)
                Spacer(Modifier.height(8.dp))
                Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                    ArchiveFormat.entries.forEach { f ->
                        FilledTonalButton(
                            onClick = {
                                format = f
                                archivePath = archivePath.substringBeforeLast('.') + ".${f.name.lowercase()}"
                            },
                            colors = if (format == f)
                                ButtonDefaults.filledTonalButtonColors(containerColor = MaterialTheme.colorScheme.primaryContainer)
                            else ButtonDefaults.filledTonalButtonColors()
                        ) { Text(f.name) }
                    }
                }
            }
        },
        confirmButton = {
            Row(horizontalArrangement = Arrangement.spacedBy(8.dp)) {
                TextButton(onClick = onDismiss) { Text("取消") }
                Button(onClick = {
                    scope.launch {
                        fs.archive.compress(listOf(sourcePath), archivePath, format).fold(
                            { onMessage("已打包: $archivePath"); onDismiss() },
                            { onMessage("打包失败: ${it.message}") })
                    }
                }) { Text("打包") }
            }
        })
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
    Surface(tonalElevation = 2.dp, modifier = Modifier.fillMaxWidth()) {
        Row(
            modifier = Modifier.padding(horizontal = 12.dp, vertical = 8.dp),
            verticalAlignment = Alignment.CenterVertically
        ) {
            FilledTonalButton(
                onClick = onGoUp, enabled = currentPath != "/",
                modifier = Modifier.size(36.dp), shape = CircleShape,
                contentPadding = ButtonDefaults.TextButtonWithIconContentPadding
            ) { Text("↑", fontSize = 16.sp) }

            Spacer(Modifier.width(8.dp))

            val segments = if (currentPath == "/") listOf("/")
            else listOf("/") + currentPath.removePrefix("/").split("/")
            var buildPath = ""

            Row(
                modifier = Modifier.weight(1f).horizontalScroll(rememberScrollState()),
                verticalAlignment = Alignment.CenterVertically
            ) {
                segments.forEachIndexed { index, seg ->
                    if (index > 0) Text(" / ", color = MaterialTheme.colorScheme.onSurfaceVariant, fontSize = 14.sp)
                    val targetPath = if (index == 0) "/" else { buildPath = "$buildPath/$seg"; buildPath }
                    val isLast = index == segments.lastIndex
                    Text(
                        text = if (index == 0) "root" else seg,
                        color = if (isLast) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurfaceVariant,
                        fontWeight = if (isLast) FontWeight.Bold else FontWeight.Normal,
                        fontSize = 14.sp,
                        modifier = if (!isLast) Modifier.clickable { onNavigate(targetPath) } else Modifier
                    )
                }
            }
        }
    }
}

// ═════════════════════════════════════════════════════════════════
// 文件行（含完整操作菜单）
// ═════════════════════════════════════════════════════════════════

@OptIn(ExperimentalLayoutApi::class)
@Composable
private fun FileRow(
    entry: FsEntry,
    onClick: () -> Unit,
    onRename: () -> Unit,
    onDelete: () -> Unit,
    onCopy: () -> Unit,
    onMove: () -> Unit,
    onDetail: () -> Unit,
    onVersions: () -> Unit,
    onXattr: () -> Unit,
    onChecksum: () -> Unit,
    onEdit: () -> Unit,
    onArchive: () -> Unit,
    onTrash: () -> Unit
) {
    val isDir = entry.type == FsType.DIRECTORY
    val isSymlink = entry.type == FsType.SYMLINK
    var showActions by remember { mutableStateOf(false) }

    Card(
        modifier = Modifier.fillMaxWidth().padding(horizontal = 12.dp),
        shape = RoundedCornerShape(10.dp),
        colors = CardDefaults.cardColors(containerColor = MaterialTheme.colorScheme.surface),
        elevation = CardDefaults.cardElevation(defaultElevation = 0.5.dp)
    ) {
        Column {
            Row(
                modifier = Modifier.fillMaxWidth().clickable(onClick = onClick)
                    .padding(horizontal = 14.dp, vertical = 12.dp),
                verticalAlignment = Alignment.CenterVertically
            ) {
                Box(
                    modifier = Modifier.size(38.dp).clip(RoundedCornerShape(8.dp))
                        .background(
                            when {
                                isDir -> MaterialTheme.colorScheme.primaryContainer
                                isSymlink -> MaterialTheme.colorScheme.secondaryContainer
                                else -> MaterialTheme.colorScheme.tertiaryContainer
                            }
                        ),
                    contentAlignment = Alignment.Center
                ) {
                    Text(
                        text = when {
                            isDir -> "\uD83D\uDCC1"
                            isSymlink -> "\uD83D\uDD17"
                            else -> "\uD83D\uDCC4"
                        },
                        fontSize = 18.sp
                    )
                }
                Spacer(Modifier.width(12.dp))
                Column(modifier = Modifier.weight(1f)) {
                    Text(entry.name, style = MaterialTheme.typography.bodyLarge,
                        fontWeight = FontWeight.Medium, maxLines = 1, overflow = TextOverflow.Ellipsis)
                    Text(
                        when {
                            isDir -> "文件夹"
                            isSymlink -> "符号链接"
                            else -> "文件"
                        },
                        style = MaterialTheme.typography.bodySmall,
                        color = MaterialTheme.colorScheme.onSurfaceVariant
                    )
                }
                TextButton(onClick = { showActions = !showActions }) {
                    Text(if (showActions) "收起" else "操作", fontSize = 13.sp)
                }
                if (isDir) Text("›", fontSize = 20.sp, color = MaterialTheme.colorScheme.onSurfaceVariant)
            }

            AnimatedVisibility(visible = showActions) {
                FlowRow(
                    modifier = Modifier.fillMaxWidth().padding(start = 14.dp, end = 14.dp, bottom = 10.dp),
                    horizontalArrangement = Arrangement.spacedBy(6.dp),
                    verticalArrangement = Arrangement.spacedBy(4.dp)
                ) {
                    AssistChip(onClick = { showActions = false; onDetail() }, label = { Text("详情", fontSize = 12.sp) })
                    if (!isDir) {
                        AssistChip(onClick = { showActions = false; onEdit() }, label = { Text("编辑", fontSize = 12.sp) })
                        AssistChip(onClick = { showActions = false; onVersions() }, label = { Text("版本", fontSize = 12.sp) })
                        AssistChip(onClick = { showActions = false; onChecksum() }, label = { Text("校验", fontSize = 12.sp) })
                    }
                    AssistChip(onClick = { showActions = false; onXattr() }, label = { Text("xattr", fontSize = 12.sp) })
                    AssistChip(onClick = { showActions = false; onCopy() }, label = { Text("复制", fontSize = 12.sp) })
                    AssistChip(onClick = { showActions = false; onMove() }, label = { Text("移动", fontSize = 12.sp) })
                    AssistChip(onClick = { showActions = false; onRename() }, label = { Text("重命名", fontSize = 12.sp) })
                    AssistChip(onClick = { showActions = false; onArchive() }, label = { Text("打包", fontSize = 12.sp) })
                    AssistChip(onClick = { showActions = false; onTrash() }, label = { Text("回收站", fontSize = 12.sp) })
                    AssistChip(onClick = { showActions = false; onDelete() },
                        label = { Text("删除", fontSize = 12.sp, color = MaterialTheme.colorScheme.error) })
                }
            }
        }
    }
}

// ═════════════════════════════════════════════════════════════════
// 通用弹窗
// ═════════════════════════════════════════════════════════════════

@Composable
private fun InputDialog(
    title: String,
    placeholder: String,
    initialValue: String = "",
    onConfirm: (String) -> Unit,
    onDismiss: () -> Unit
) {
    var textFieldValue by remember {
        mutableStateOf(TextFieldValue(initialValue, TextRange(0, initialValue.length)))
    }
    val focusRequester = remember { FocusRequester() }
    LaunchedEffect(Unit) { focusRequester.requestFocus() }

    AlertDialog(onDismissRequest = onDismiss,
        title = { Text(title) },
        text = {
            OutlinedTextField(
                value = textFieldValue,
                onValueChange = { textFieldValue = it },
                placeholder = { Text(placeholder) },
                singleLine = true,
                modifier = Modifier.fillMaxWidth().focusRequester(focusRequester),
                keyboardOptions = KeyboardOptions.Default,
                keyboardActions = KeyboardActions(onDone = {
                    if (textFieldValue.text.isNotBlank()) onConfirm(textFieldValue.text.trim())
                })
            )
        },
        confirmButton = {
            Button(onClick = { if (textFieldValue.text.isNotBlank()) onConfirm(textFieldValue.text.trim()) },
                enabled = textFieldValue.text.isNotBlank()) { Text("确定") }
        },
        dismissButton = { TextButton(onClick = onDismiss) { Text("取消") } })
}

@Composable
private fun TwoFieldDialog(
    title: String,
    label1: String,
    label2: String,
    onConfirm: (String, String) -> Unit,
    onDismiss: () -> Unit
) {
    var value1 by remember { mutableStateOf(TextFieldValue("")) }
    var value2 by remember { mutableStateOf("") }
    val focusRequester = remember { FocusRequester() }
    LaunchedEffect(Unit) { focusRequester.requestFocus() }

    AlertDialog(onDismissRequest = onDismiss,
        title = { Text(title) },
        text = {
            Column {
                OutlinedTextField(
                    value = value1,
                    onValueChange = { value1 = it },
                    label = { Text(label1) },
                    singleLine = true,
                    modifier = Modifier.fillMaxWidth().focusRequester(focusRequester),
                    keyboardOptions = KeyboardOptions.Default
                )
                Spacer(Modifier.height(8.dp))
                OutlinedTextField(value = value2, onValueChange = { value2 = it },
                    label = { Text(label2) }, modifier = Modifier.fillMaxWidth())
            }
        },
        confirmButton = {
            Button(onClick = { if (value1.text.isNotBlank()) onConfirm(value1.text.trim(), value2) },
                enabled = value1.text.isNotBlank()) { Text("确定") }
        },
        dismissButton = { TextButton(onClick = onDismiss) { Text("取消") } })
}

@Composable
private fun ConfirmDialog(
    title: String,
    text: String,
    confirmLabel: String = "确定",
    onConfirm: () -> Unit,
    onDismiss: () -> Unit
) {
    AlertDialog(onDismissRequest = onDismiss,
        title = { Text(title) },
        text = { Text(text) },
        confirmButton = {
            Button(onClick = onConfirm,
                colors = ButtonDefaults.buttonColors(containerColor = MaterialTheme.colorScheme.error)
            ) { Text(confirmLabel) }
        },
        dismissButton = { TextButton(onClick = onDismiss) { Text("取消") } })
}
