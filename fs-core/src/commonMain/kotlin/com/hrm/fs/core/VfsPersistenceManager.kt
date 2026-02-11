package com.hrm.fs.core

import com.hrm.fs.api.FsStorage
import com.hrm.fs.api.log.FLog
import com.hrm.fs.core.persistence.CorruptedDataException
import com.hrm.fs.core.persistence.MountInfo
import com.hrm.fs.core.persistence.PersistenceConfig
import com.hrm.fs.core.persistence.SnapshotNode
import com.hrm.fs.core.persistence.SnapshotVersionData
import com.hrm.fs.core.persistence.VfsPersistenceCodec
import com.hrm.fs.core.persistence.WalEntry

/**
 * 持久化管理器。
 *
 * 封装 WAL 追加、自动快照、挂载点持久化的所有逻辑。
 * **无线程安全保证**，外部需自行加锁。
 *
 * ## 崩溃恢复策略
 *
 * 写入顺序保证：
 * - WAL 追加：先编码带 CRC 的数据，再写入 storage
 * - Snapshot 保存：先写 snapshot → 再写 versions → 最后清空 WAL
 *   即使在写入 snapshot 后、清空 WAL 前崩溃，下次恢复时会重放 WAL（幂等）
 *
 * 损坏容错：
 * - Snapshot 损坏 → 跳过快照，从空树开始 + 尝试回放 WAL
 * - WAL 损坏 → 跳过 WAL 回放，仅使用 snapshot（可能丢失最近的修改）
 * - 两者都损坏 → 从空树开始（数据全丢失，但不崩溃）
 * - Mounts/Versions 损坏 → 各自独立降级为空
 */
internal class VfsPersistenceManager(
    private val storage: FsStorage?,
    private val config: PersistenceConfig = PersistenceConfig()
) {
    companion object {
        private const val TAG = "VfsPersistence"
    }

    private val walEntries: MutableList<WalEntry> = mutableListOf()
    private var opsSinceSnapshot: Int = 0
    private var loaded: Boolean = false

    val persistenceConfig: PersistenceConfig get() = config

    // ── 加载 ─────────────────────────────────────────────────

    /**
     * 首次调用时从 storage 加载快照 + WAL + 挂载信息。
     * 返回 (snapshot, walEntries, mountInfos, versionData, recoveryWarnings)，调用方自行决定如何应用。
     */
    data class LoadResult(
        val snapshot: SnapshotNode?,
        val walEntries: List<WalEntry>,
        val mountInfos: List<MountInfo>,
        val versionData: SnapshotVersionData?,
        /** 恢复过程中遇到的警告信息（数据损坏等），空列表表示完全正常。 */
        val recoveryWarnings: List<String> = emptyList()
    )

    suspend fun ensureLoaded(): LoadResult? {
        if (loaded) return null
        loaded = true
        return load()
    }

    private suspend fun load(): LoadResult? {
        if (storage == null) return null
        FLog.d(TAG, "load: reading from storage")
        val warnings = mutableListOf<String>()

        // ── 读取 Snapshot（容错） ──
        val snapshot = try {
            storage.read(config.snapshotKey).getOrNull()?.let {
                VfsPersistenceCodec.decodeSnapshot(it)
            }
        } catch (e: CorruptedDataException) {
            val msg = "Snapshot corrupted (CRC mismatch), starting from empty tree: ${e.message}"
            FLog.w(TAG, msg)
            warnings.add(msg)
            null
        } catch (e: Exception) {
            val msg = "Snapshot decode failed, starting from empty tree: ${e.message}"
            FLog.w(TAG, msg)
            warnings.add(msg)
            null
        }

        // ── 读取 WAL（容错） ──
        val wal = try {
            storage.read(config.walKey).getOrNull()?.let {
                VfsPersistenceCodec.decodeWal(it)
            }.orEmpty()
        } catch (e: CorruptedDataException) {
            val msg = "WAL corrupted (CRC mismatch), skipping WAL replay: ${e.message}"
            FLog.w(TAG, msg)
            warnings.add(msg)
            // WAL 损坏时清除损坏的 WAL 文件，防止下次加载再次失败
            try {
                storage.delete(config.walKey)
            } catch (_: Exception) { }
            emptyList()
        } catch (e: Exception) {
            val msg = "WAL decode failed, skipping WAL replay: ${e.message}"
            FLog.w(TAG, msg)
            warnings.add(msg)
            try {
                storage.delete(config.walKey)
            } catch (_: Exception) { }
            emptyList()
        }

        if (wal.isNotEmpty()) {
            walEntries.clear()
            walEntries.addAll(wal)
        }

        // ── 读取 Mounts（容错） ──
        val mountInfos = try {
            storage.read(config.mountsKey).getOrNull()?.let {
                VfsPersistenceCodec.decodeMounts(it)
            }.orEmpty()
        } catch (e: Exception) {
            val msg = "Mounts data corrupted, using empty mounts: ${e.message}"
            FLog.w(TAG, msg)
            warnings.add(msg)
            emptyList()
        }

        // ── 读取 Versions（容错） ──
        val versionData = try {
            storage.read(config.versionsKey).getOrNull()?.let {
                VfsPersistenceCodec.decodeVersionData(it)
            }
        } catch (e: Exception) {
            val msg = "Versions data corrupted, using empty versions: ${e.message}"
            FLog.w(TAG, msg)
            warnings.add(msg)
            null
        }

        if (warnings.isNotEmpty()) {
            FLog.w(TAG, "load completed with ${warnings.size} recovery warning(s)")
        }
        FLog.i(TAG, "load completed: snapshot=${snapshot != null}, walEntries=${wal.size}, mounts=${mountInfos.size}, versions=${versionData?.entries?.size ?: 0}")
        return LoadResult(snapshot, wal, mountInfos, versionData, warnings)
    }

    // ── WAL ──────────────────────────────────────────────────

    /**
     * 追加一条 WAL 条目并持久化。
     * 达到阈值时自动触发快照。
     *
     * 写入数据包含 CRC32 校验值，保证完整性可验证。
     *
     * @param snapshotProvider 生成当前快照的回调（惰性，仅在需要快照时调用）
     * @param versionDataProvider 生成版本数据的回调（惰性）
     */
    suspend fun appendWal(
        entry: WalEntry,
        snapshotProvider: () -> SnapshotNode,
        versionDataProvider: () -> SnapshotVersionData
    ) {
        if (storage == null) return
        walEntries.add(entry)
        opsSinceSnapshot++
        storage.write(config.walKey, VfsPersistenceCodec.encodeWal(walEntries))
        FLog.v(TAG, "appendWal: opsSinceSnapshot=$opsSinceSnapshot, entry=$entry")
        if (opsSinceSnapshot >= config.autoSnapshotEvery) {
            FLog.d(TAG, "appendWal: auto snapshot triggered at $opsSinceSnapshot ops")
            saveSnapshot(snapshotProvider(), versionDataProvider())
        }
    }

    /**
     * 保存快照并清空 WAL。
     *
     * 写入顺序：snapshot → versions → 清空 WAL。
     * 如果在写 snapshot 之后、清 WAL 之前崩溃，
     * 下次恢复会 snapshot + WAL 重放（WAL 操作是幂等的，不会出错）。
     */
    suspend fun saveSnapshot(snapshot: SnapshotNode, versionData: SnapshotVersionData? = null) {
        if (storage == null) return
        FLog.d(TAG, "saveSnapshot: saving snapshot and clearing WAL")
        // Step 1: 写入 snapshot（带 CRC）
        storage.write(config.snapshotKey, VfsPersistenceCodec.encodeSnapshot(snapshot))
        // Step 2: 写入 versions（带 CRC）
        if (versionData != null) {
            storage.write(config.versionsKey, VfsPersistenceCodec.encodeVersionData(versionData))
        }
        // Step 3: 清空 WAL（最后执行，保证崩溃安全）
        walEntries.clear()
        storage.write(config.walKey, VfsPersistenceCodec.encodeWal(walEntries))
        opsSinceSnapshot = 0
    }

    // ── 挂载点持久化 ─────────────────────────────────────────

    suspend fun persistMounts(mountInfos: List<MountInfo>) {
        if (storage == null) return
        storage.write(config.mountsKey, VfsPersistenceCodec.encodeMounts(mountInfos))
    }
}
