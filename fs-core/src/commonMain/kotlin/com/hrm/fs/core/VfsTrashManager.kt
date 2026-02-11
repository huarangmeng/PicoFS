package com.hrm.fs.core

import com.hrm.fs.api.FsError
import com.hrm.fs.api.FsType
import com.hrm.fs.api.TrashItem
import com.hrm.fs.api.log.FLog
import com.hrm.fs.core.persistence.SnapshotTrashEntry
import kotlin.time.Clock

/**
 * 回收站管理器。
 *
 * 维护已删除文件/目录的元数据和内容，支持恢复和彻底清除。
 * **无线程安全保证**，外部需自行加锁。
 *
 * ## 设计思路
 *
 * 1. **VFS 内存文件**：删除时将文件内容和子目录结构完整保存到 trash store。
 * 2. **挂载点文件**：委托给 `DiskFileOperations.moveToTrash()` 在磁盘 `.trash` 目录中存储。
 *    TrashManager 仅保存元数据（路径、类型、删除时间），内容由磁盘管理。
 * 3. **持久化**：通过 snapshot 持久化 trash store，崩溃恢复后可继续使用。
 */
internal class VfsTrashManager(
    /** 回收站最大条目数，超出后自动清除最旧条目。 */
    val maxItems: Int = DEFAULT_MAX_ITEMS,
    /** 回收站总字节数上限（-1 = 不限制），超出后自动清除最旧条目。 */
    val maxBytes: Long = DEFAULT_MAX_BYTES
) {
    companion object {
        const val DEFAULT_MAX_ITEMS = 100
        const val DEFAULT_MAX_BYTES: Long = 50L * 1024 * 1024 // 50 MB
        private const val TAG = "VfsTrash"

        private var trashCounter: Long = 0
        internal fun nextTrashId(): String = "trash_${++trashCounter}"

        /** 从已恢复的 trash ID 同步 counter，避免重启后 ID 冲突。 */
        internal fun syncCounter(ids: Iterable<String>) {
            for (id in ids) {
                val num = id.removePrefix("trash_").toLongOrNull() ?: continue
                if (num > trashCounter) trashCounter = num
            }
        }

        private fun nowMillis(): Long = Clock.System.now().toEpochMilliseconds()
    }

    /**
     * 回收站内部条目（VFS 内存文件包含完整内容）。
     */
    internal data class TrashEntry(
        val trashId: String,
        val originalPath: String,
        val type: FsType,
        val deletedAtMillis: Long,
        /** 文件内容（仅 VFS 内存文件有值，挂载点文件为 null）。 */
        val content: ByteArray? = null,
        /** 目录的子条目列表（仅目录有值）。 */
        val children: List<TrashChildEntry>? = null,
        /** 是否为挂载点文件（内容由磁盘管理）。 */
        val isMounted: Boolean = false
    ) {
        val size: Long
            get() = when {
                content != null -> content.size.toLong()
                children != null -> children.sumOf { it.totalSize }
                else -> 0L
            }
    }

    /**
     * 目录子条目（递归结构）。
     */
    internal data class TrashChildEntry(
        val relativePath: String,
        val type: FsType,
        val content: ByteArray? = null,
        val children: List<TrashChildEntry>? = null
    ) {
        val totalSize: Long
            get() = when {
                content != null -> content.size.toLong()
                children != null -> children.sumOf { it.totalSize }
                else -> 0L
            }
    }

    /** trashId → TrashEntry，使用 ArrayList 维护插入顺序（最新在前）。 */
    private val storeMap = LinkedHashMap<String, TrashEntry>()
    private val storeOrder = ArrayList<String>() // 索引 0 = 最新

    /** 当前回收站总字节数缓存。 */
    private var _totalBytes: Long = 0L

    /**
     * 将文件移入回收站（VFS 内存文件）。
     *
     * @return 回收站条目 ID
     */
    fun moveToTrash(
        originalPath: String,
        type: FsType,
        content: ByteArray? = null,
        children: List<TrashChildEntry>? = null
    ): String {
        val trashId = nextTrashId()
        val entry = TrashEntry(
            trashId = trashId,
            originalPath = originalPath,
            type = type,
            deletedAtMillis = nowMillis(),
            content = content?.copyOf(),
            children = children,
            isMounted = false
        )
        // 插入到头部（O(n) shift 但比创建全新 LinkedHashMap 开销小）
        storeMap[trashId] = entry
        storeOrder.add(0, trashId)
        _totalBytes += entry.size
        trimOldest()
        FLog.d(TAG, "moveToTrash: trashId=$trashId, path=$originalPath, type=$type")
        return trashId
    }

    /**
     * 记录挂载点文件的回收站元数据（内容由 diskOps 管理）。
     */
    fun recordMountedTrash(
        trashId: String,
        originalPath: String,
        type: FsType
    ) {
        val entry = TrashEntry(
            trashId = trashId,
            originalPath = originalPath,
            type = type,
            deletedAtMillis = nowMillis(),
            isMounted = true
        )
        storeMap[trashId] = entry
        storeOrder.add(0, trashId)
        _totalBytes += entry.size
        trimOldest()
        FLog.d(TAG, "recordMountedTrash: trashId=$trashId, path=$originalPath")
    }

    /** 获取回收站条目。 */
    fun getEntry(trashId: String): TrashEntry? = storeMap[trashId]

    /** 列出所有回收站条目（最新在前）。 */
    fun listItems(): List<TrashItem> = storeOrder.mapNotNull { id ->
        storeMap[id]?.let { entry ->
            TrashItem(
                trashId = entry.trashId,
                originalPath = entry.originalPath,
                type = entry.type,
                size = entry.size,
                deletedAtMillis = entry.deletedAtMillis
            )
        }
    }

    /** 从回收站移除条目（恢复或清除后调用）。 */
    fun remove(trashId: String): TrashEntry? {
        val removed = storeMap.remove(trashId)
        if (removed != null) {
            storeOrder.remove(trashId)
            _totalBytes -= removed.size
            FLog.d(TAG, "remove: trashId=$trashId, path=${removed.originalPath}")
        }
        return removed
    }

    /** 清空所有回收站条目。 */
    fun clear(): List<TrashEntry> {
        val all = storeOrder.mapNotNull { storeMap[it] }
        storeMap.clear()
        storeOrder.clear()
        _totalBytes = 0L
        FLog.d(TAG, "clear: purged ${all.size} items")
        return all
    }

    /** 是否为空。 */
    fun isEmpty(): Boolean = storeMap.isEmpty()

    /** 条目数。 */
    fun size(): Int = storeMap.size

    private fun trimOldest() {
        while (storeOrder.size > maxItems || (maxBytes >= 0 && _totalBytes > maxBytes)) {
            if (storeOrder.isEmpty()) break
            val oldestId = storeOrder.removeAt(storeOrder.size - 1)
            val removed = storeMap.remove(oldestId)
            if (removed != null) {
                _totalBytes -= removed.size
                FLog.d(TAG, "trimOldest: auto-purged trashId=$oldestId")
            }
        }
    }

    // ── 快照持久化 ──────────────────────────────────────────────

    fun toSnapshotEntries(): List<SnapshotTrashEntry> = storeOrder.mapNotNull { id ->
        storeMap[id]?.let { entry ->
            SnapshotTrashEntry(
                trashId = entry.trashId,
                originalPath = entry.originalPath,
                type = entry.type.name,
                deletedAtMillis = entry.deletedAtMillis,
                content = entry.content,
                children = entry.children?.map { childToSnapshot(it) },
                isMounted = entry.isMounted
            )
        }
    }

    fun restoreFromSnapshot(entries: List<SnapshotTrashEntry>) {
        storeMap.clear()
        storeOrder.clear()
        _totalBytes = 0L
        for (e in entries) {
            val trashEntry = TrashEntry(
                trashId = e.trashId,
                originalPath = e.originalPath,
                type = FsType.valueOf(e.type),
                deletedAtMillis = e.deletedAtMillis,
                content = e.content,
                children = e.children?.map { childFromSnapshot(it) },
                isMounted = e.isMounted
            )
            storeMap[e.trashId] = trashEntry
            storeOrder.add(e.trashId)
            _totalBytes += trashEntry.size
        }
        // 同步 counter 避免重启后 ID 冲突
        syncCounter(storeMap.keys)
        FLog.d(TAG, "restoreFromSnapshot: restored ${entries.size} trash entries, totalBytes=$_totalBytes")
    }

    private fun childToSnapshot(child: TrashChildEntry): SnapshotTrashEntry.SnapshotTrashChild =
        SnapshotTrashEntry.SnapshotTrashChild(
            relativePath = child.relativePath,
            type = child.type.name,
            content = child.content,
            children = child.children?.map { childToSnapshot(it) }
        )

    private fun childFromSnapshot(child: SnapshotTrashEntry.SnapshotTrashChild): TrashChildEntry =
        TrashChildEntry(
            relativePath = child.relativePath,
            type = FsType.valueOf(child.type),
            content = child.content,
            children = child.children?.map { childFromSnapshot(it) }
        )
}
