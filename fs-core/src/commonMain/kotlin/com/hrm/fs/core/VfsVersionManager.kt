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

        private fun nowMillis(): Long = Clock.System.now().toEpochMilliseconds()
    }

    /** path → versions list（最新在前）。 */
    private val store = LinkedHashMap<String, MutableList<VersionSnapshot>>()

    /**
     * 保存 [data] 为指定路径的一个历史版本。
     *
     * 仅当 [data] 非空时保存（空文件无需保存空版本）。
     */
    fun saveVersion(path: String, data: ByteArray) {
        if (data.isEmpty()) return
        val snapshot = VersionSnapshot(
            versionId = nextVersionId(),
            timestampMillis = nowMillis(),
            data = data.copyOf()
        )
        val versions = store.getOrPut(path) { mutableListOf() }
        versions.add(0, snapshot)
        while (versions.size > maxVersions) {
            versions.removeAt(versions.size - 1)
        }
        FLog.d(TAG, "saveVersion: path=$path, versionId=${snapshot.versionId}, size=${data.size}, total=${versions.size}")
    }

    /** 获取文件的版本历史列表。 */
    fun fileVersions(path: String): List<FileVersion> {
        val versions = store[path] ?: return emptyList()
        return versions.map { v ->
            FileVersion(
                versionId = v.versionId,
                timestampMillis = v.timestampMillis,
                size = v.data.size.toLong()
            )
        }
    }

    /** 读取某个历史版本的内容。 */
    fun readVersion(path: String, versionId: String): Result<ByteArray> {
        val versions = store[path] ?: return Result.failure(FsError.NotFound("version $versionId of $path").also {
            FLog.w(TAG, "readVersion: no versions for path=$path")
        })
        val version = versions.find { it.versionId == versionId }
            ?: return Result.failure(FsError.NotFound("version $versionId of $path").also {
                FLog.w(TAG, "readVersion: versionId=$versionId not found for path=$path")
            })
        return Result.success(version.data.copyOf())
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
        val version = versions.find { it.versionId == versionId }
            ?: return Result.failure(FsError.NotFound("version $versionId of $path").also {
                FLog.w(TAG, "restoreVersion: versionId=$versionId not found for path=$path")
            })
        // 保存当前内容为新版本
        saveVersion(path, currentData)
        FLog.i(TAG, "restoreVersion: path=$path, restored to versionId=$versionId")
        return Result.success(version.data.copyOf())
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

    /** 导出为可序列化的 [SnapshotVersionData]。 */
    fun toSnapshotData(): SnapshotVersionData = SnapshotVersionData(toSnapshotMap())

    /** 导出为可序列化的 map（path → versions）。 */
    fun toSnapshotMap(): Map<String, List<SnapshotVersionEntry>> {
        val result = LinkedHashMap<String, List<SnapshotVersionEntry>>()
        for ((path, versions) in store) {
            if (versions.isNotEmpty()) {
                result[path] = versions.map { v ->
                    SnapshotVersionEntry(v.versionId, v.timestampMillis, v.data)
                }
            }
        }
        return result
    }

    /** 从持久化数据恢复。 */
    fun restoreFromSnapshot(data: Map<String, List<SnapshotVersionEntry>>) {
        store.clear()
        for ((path, entries) in data) {
            store[path] = entries.map { e ->
                VersionSnapshot(e.versionId, e.timestampMillis, e.data)
            }.toMutableList()
        }
        FLog.d(TAG, "restoreFromSnapshot: restored ${data.size} files' version data")
    }

    /** 是否有版本数据。 */
    fun isEmpty(): Boolean = store.isEmpty()
}
