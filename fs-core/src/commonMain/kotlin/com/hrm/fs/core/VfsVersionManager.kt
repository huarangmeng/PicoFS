package com.hrm.fs.core

import com.hrm.fs.api.FileVersion
import com.hrm.fs.api.FsError
import com.hrm.fs.api.log.FLog
import com.hrm.fs.core.persistence.SnapshotVersionData
import com.hrm.fs.core.persistence.SnapshotVersionEntry
import kotlin.time.Clock

/**
 * 独立的版本管理器，按虚拟路径索引。
 *
 * 与 [FileNode] / [VfsTree] 解耦，同时支持内存文件和挂载点文件的版本追踪。
 * **无线程安全保证**，外部需自行加锁。
 */
internal class VfsVersionManager(
    /** 每个文件保留的最大版本数。 */
    val maxVersions: Int = DEFAULT_MAX_VERSIONS
) {
    companion object {
        const val DEFAULT_MAX_VERSIONS = 10
        private const val TAG = "VfsVersion"

        private var versionCounter: Long = 0
        internal fun nextVersionId(): String = "v${++versionCounter}"

        /** 从已恢复的 version ID 同步 counter，避免重启后 ID 冲突。 */
        internal fun syncCounter(ids: Iterable<String>) {
            for (id in ids) {
                val num = id.removePrefix("v").toLongOrNull() ?: continue
                if (num > versionCounter) versionCounter = num
            }
        }

        private fun nowMillis(): Long = Clock.System.now().toEpochMilliseconds()
    }

    /** path → versions list（最新在前）。 */
    private val store = LinkedHashMap<String, MutableList<VersionSnapshot>>()

    /**
     * 保存 [data] 为指定路径的一个历史版本。
     *
     * 使用 XOR delta 编码：如果已有版本，存储与最新版本的 XOR 差异而非完整副本。
     * 第一个版本（base）始终存储完整数据。
     */
    fun saveVersion(path: String, data: ByteArray) {
        if (data.isEmpty()) return
        val versions = store.getOrPut(path) { mutableListOf() }
        val storedData: ByteArray
        val isBase: Boolean
        if (versions.isEmpty()) {
            // 第一个版本 = base，存完整数据
            storedData = data.copyOf()
            isBase = true
        } else {
            // 后续版本：对最新的完整数据做 XOR delta
            val latestFull = reconstructFull(versions, 0)
            storedData = xorDelta(latestFull, data)
            isBase = false
        }
        val snapshot = VersionSnapshot(
            versionId = nextVersionId(),
            timestampMillis = nowMillis(),
            data = storedData,
            isBase = isBase
        )
        versions.add(0, snapshot)
        while (versions.size > maxVersions) {
            versions.removeAt(versions.size - 1)
        }
        // 确保裁剪后最老版本是 base（完整数据）
        ensureBaseAtTail(path, versions)
        FLog.d(TAG, "saveVersion: path=$path, versionId=${snapshot.versionId}, size=${data.size}, delta=${!isBase}, total=${versions.size}")
    }

    /** 获取文件的版本历史列表。 */
    fun fileVersions(path: String): List<FileVersion> {
        val versions = store[path] ?: return emptyList()
        return versions.mapIndexed { index, v ->
            val fullSize = reconstructFull(versions, index).size.toLong()
            FileVersion(
                versionId = v.versionId,
                timestampMillis = v.timestampMillis,
                size = fullSize
            )
        }
    }

    /** 读取某个历史版本的内容。 */
    fun readVersion(path: String, versionId: String): Result<ByteArray> {
        val versions = store[path] ?: return Result.failure(FsError.NotFound("version $versionId of $path").also {
            FLog.w(TAG, "readVersion: no versions for path=$path")
        })
        val index = versions.indexOfFirst { it.versionId == versionId }
        if (index < 0) {
            return Result.failure(FsError.NotFound("version $versionId of $path").also {
                FLog.w(TAG, "readVersion: versionId=$versionId not found for path=$path")
            })
        }
        return Result.success(reconstructFull(versions, index))
    }

    /**
     * 恢复文件到某个历史版本。
     *
     * 先将 [currentData] 保存为新版本，然后返回历史版本的内容。
     * 调用方需将返回的内容写回文件。
     *
     * @return 历史版本的完整内容
     */
    fun restoreVersion(path: String, versionId: String, currentData: ByteArray): Result<ByteArray> {
        val versions = store[path] ?: return Result.failure(FsError.NotFound("version $versionId of $path").also {
            FLog.w(TAG, "restoreVersion: no versions for path=$path")
        })
        val index = versions.indexOfFirst { it.versionId == versionId }
        if (index < 0) {
            return Result.failure(FsError.NotFound("version $versionId of $path").also {
                FLog.w(TAG, "restoreVersion: versionId=$versionId not found for path=$path")
            })
        }
        val historicalData = reconstructFull(versions, index)
        // 保存当前内容为新版本
        saveVersion(path, currentData)
        FLog.i(TAG, "restoreVersion: path=$path, restored to versionId=$versionId")
        return Result.success(historicalData)
    }

    /** 删除文件时清除其版本历史。 */
    fun removeVersions(path: String) {
        store.remove(path)
    }

    /** 移动/重命名时迁移版本历史。 */
    fun moveVersions(srcPath: String, dstPath: String) {
        val versions = store.remove(srcPath) ?: return
        store[dstPath] = versions
    }

    // ── 快照持久化 ──────────────────────────────────────────────

    /** 导出为可序列化的 [SnapshotVersionData]。导出时重建完整数据以保持兼容性。 */
    fun toSnapshotData(): SnapshotVersionData = SnapshotVersionData(toSnapshotMap())

    /** 导出为可序列化的 map（path → versions），导出完整数据。 */
    fun toSnapshotMap(): Map<String, List<SnapshotVersionEntry>> {
        val result = LinkedHashMap<String, List<SnapshotVersionEntry>>()
        for ((path, versions) in store) {
            if (versions.isNotEmpty()) {
                result[path] = versions.mapIndexed { index, v ->
                    SnapshotVersionEntry(v.versionId, v.timestampMillis, reconstructFull(versions, index))
                }
            }
        }
        return result
    }

    /** 从持久化数据恢复。持久化数据中存储的是完整数据，恢复后重建 delta 链。 */
    fun restoreFromSnapshot(data: Map<String, List<SnapshotVersionEntry>>) {
        store.clear()
        val allVersionIds = mutableListOf<String>()
        for ((path, entries) in data) {
            // 重建 delta 链：entries[0] 最新，entries[last] 最老
            val versions = mutableListOf<VersionSnapshot>()
            for ((index, e) in entries.withIndex()) {
                allVersionIds.add(e.versionId)
                if (index == entries.size - 1) {
                    // 最老版本 = base
                    versions.add(VersionSnapshot(e.versionId, e.timestampMillis, e.data.copyOf(), isBase = true))
                } else {
                    // delta = XOR(当前完整数据, 下一个版本完整数据)
                    val nextFull = entries[index + 1].data
                    versions.add(VersionSnapshot(e.versionId, e.timestampMillis, xorDelta(nextFull, e.data), isBase = false))
                }
            }
            store[path] = versions
        }
        // 同步 counter 避免重启后 ID 冲突
        syncCounter(allVersionIds)
        FLog.d(TAG, "restoreFromSnapshot: restored ${data.size} files' version data")
    }

    /** 是否有版本数据。 */
    fun isEmpty(): Boolean = store.isEmpty()

    // ── Delta 编码辅助 ──────────────────────────────────────────

    /**
     * XOR delta：对两个字节数组逐字节异或。
     * 如果长度不同，短的一方以 0 补齐。
     */
    private fun xorDelta(base: ByteArray, target: ByteArray): ByteArray {
        val len = maxOf(base.size, target.size)
        val result = ByteArray(len)
        for (i in 0 until len) {
            val b = if (i < base.size) base[i] else 0
            val t = if (i < target.size) target[i] else 0
            result[i] = (b.toInt() xor t.toInt()).toByte()
        }
        return result
    }

    /**
     * 从 delta 链重建指定索引的完整数据。
     * 从最近的 base 版本开始，逐步应用 XOR delta。
     *
     * 链结构：[0]=最新, ..., [n]=最老(base)
     * 每个非 base 版本存储的是 delta(前一版本完整数据, 本版本完整数据)
     * 即 versions[i].data = XOR(full[i+1], full[i])
     * 恢复：full[i] = XOR(full[i+1], versions[i].data)
     */
    private fun reconstructFull(versions: List<VersionSnapshot>, targetIndex: Int): ByteArray {
        // 找从 targetIndex 开始到尾部最近的 base
        var baseIndex = targetIndex
        while (baseIndex < versions.size && !versions[baseIndex].isBase) {
            baseIndex++
        }
        if (baseIndex >= versions.size) {
            // 没有找到 base，回退到最后一个版本作为 base
            baseIndex = versions.size - 1
        }
        // 从 base 开始，向目标方向应用 delta
        var current = versions[baseIndex].data.copyOf()
        // base 在链尾方向，delta 从 base 向 targetIndex 方向恢复
        // versions[i].data = XOR(full[i+1], full[i]) → full[i] = XOR(full[i+1], delta[i])
        var i = baseIndex - 1
        while (i >= targetIndex) {
            current = xorDelta(current, versions[i].data)
            i--
        }
        return current
    }

    /**
     * 确保裁剪后最后一个版本是 base（完整数据）。
     * 如果不是，重建其完整数据并标记为 base。
     */
    private fun ensureBaseAtTail(path: String, versions: MutableList<VersionSnapshot>) {
        if (versions.isEmpty()) return
        val last = versions.last()
        if (!last.isBase) {
            val fullData = reconstructFull(versions, versions.size - 1)
            versions[versions.size - 1] = last.copy(data = fullData, isBase = true)
        }
    }
}
