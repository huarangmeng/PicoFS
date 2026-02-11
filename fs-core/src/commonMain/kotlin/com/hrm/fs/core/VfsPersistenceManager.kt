package com.hrm.fs.core

import com.hrm.fs.api.FsStorage
import com.hrm.fs.api.log.FLog
import com.hrm.fs.core.persistence.CorruptedDataException
import com.hrm.fs.core.persistence.MountInfo
import com.hrm.fs.core.persistence.PersistenceConfig
import com.hrm.fs.core.persistence.SnapshotNode
import com.hrm.fs.core.persistence.SnapshotTrashData
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
 * - WAL 追加：增量 append 新条目（O(1)），避免全量重写
 * - Snapshot 保存：原子写（先写临时 key → 读回校验 CRC → 替换正式 key → 删临时 key）
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
        val trashData: SnapshotTrashData?,
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
        // 优先读正式 key，若不存在或损坏，尝试从临时 key 恢复
        val snapshot = loadSnapshotWithFallback(warnings)

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

        // ── 读取 Trash（容错） ──
        val trashData = try {
            storage.read(config.trashKey).getOrNull()?.let {
                VfsPersistenceCodec.decodeTrashData(it)
            }
        } catch (e: Exception) {
            val msg = "Trash data corrupted, using empty trash: ${e.message}"
            FLog.w(TAG, msg)
            warnings.add(msg)
            null
        }

        if (warnings.isNotEmpty()) {
            FLog.w(TAG, "load completed with ${warnings.size} recovery warning(s)")
        }
        FLog.i(TAG, "load completed: snapshot=${snapshot != null}, walEntries=${wal.size}, mounts=${mountInfos.size}, versions=${versionData?.entries?.size ?: 0}, trash=${trashData?.entries?.size ?: 0}")
        return LoadResult(snapshot, wal, mountInfos, versionData, trashData, warnings)
    }

    /**
     * 加载 snapshot 并支持从临时 key 回退。
     * 写入顺序：先写 `key.tmp` → 校验 → 替换 `key` → 删 `key.tmp`
     * 如果正式 key 损坏但临时 key 存在且有效，则从临时 key 恢复。
     */
    private suspend fun loadSnapshotWithFallback(warnings: MutableList<String>): SnapshotNode? {
        // 尝试读取正式 key
        val primary = try {
            storage!!.read(config.snapshotKey).getOrNull()?.let {
                VfsPersistenceCodec.decodeSnapshot(it)
            }
        } catch (e: CorruptedDataException) {
            val msg = "Snapshot corrupted (CRC mismatch): ${e.message}"
            FLog.w(TAG, msg)
            warnings.add(msg)
            null
        } catch (e: Exception) {
            val msg = "Snapshot decode failed: ${e.message}"
            FLog.w(TAG, msg)
            warnings.add(msg)
            null
        }
        if (primary != null) {
            // 清除可能残留的临时 key
            try { storage!!.delete(tmpKey(config.snapshotKey)) } catch (_: Exception) {}
            return primary
        }
        // 尝试从临时 key 恢复
        return try {
            storage!!.read(tmpKey(config.snapshotKey)).getOrNull()?.let {
                val snapshot = VfsPersistenceCodec.decodeSnapshot(it)
                // 临时 key 有效，提升为正式 key
                storage.write(config.snapshotKey, VfsPersistenceCodec.encodeSnapshot(snapshot))
                storage.delete(tmpKey(config.snapshotKey))
                FLog.i(TAG, "Recovered snapshot from temporary key")
                snapshot
            }
        } catch (e: Exception) {
            val msg = "Snapshot temporary key also unusable, starting from empty tree: ${e.message}"
            FLog.w(TAG, msg)
            warnings.add(msg)
            null
        }
    }

    // ── WAL ──────────────────────────────────────────────────

    /**
     * 追加一条 WAL 条目并持久化。
     * 达到阈值时自动触发快照。
     *
     * 增量追加：仅重写 WAL，每次编码当前完整 walEntries 列表。
     * （WAL 条目通常 < 20 条，因为达到 autoSnapshotEvery 时会快照并清空 WAL。）
     *
     * @param snapshotProvider 生成当前快照的回调（惰性，仅在需要快照时调用）
     * @param versionDataProvider 生成版本数据的回调（惰性）
     */
    suspend fun appendWal(
        entry: WalEntry,
        snapshotProvider: () -> SnapshotNode,
        versionDataProvider: () -> SnapshotVersionData,
        trashDataProvider: () -> SnapshotTrashData
    ) {
        if (storage == null) return
        walEntries.add(entry)
        opsSinceSnapshot++
        storage.write(config.walKey, VfsPersistenceCodec.encodeWal(walEntries))
        FLog.v(TAG, "appendWal: opsSinceSnapshot=$opsSinceSnapshot, entry=$entry")
        if (opsSinceSnapshot >= config.autoSnapshotEvery) {
            FLog.d(TAG, "appendWal: auto snapshot triggered at $opsSinceSnapshot ops")
            saveSnapshot(snapshotProvider(), versionDataProvider(), trashDataProvider())
        }
    }

    /**
     * 保存快照并清空 WAL。
     *
     * 原子写入流程（每个 key）：
     *   1. 写 `key.tmp`（临时）
     *   2. 读回 `key.tmp` 校验 CRC 完整性
     *   3. 写 `key`（正式）
     *   4. 删 `key.tmp`
     *
     * 如果在步骤 3 后崩溃：下次加载时 `key` 已有效，`key.tmp` 残留无影响。
     * 如果在步骤 1-2 间崩溃：下次加载时 `key` 仍为旧数据，`key.tmp` 损坏会被忽略。
     * 如果在步骤 2-3 间崩溃：下次加载时 `key` 可能损坏，但 `key.tmp` 有效可恢复。
     */
    suspend fun saveSnapshot(
        snapshot: SnapshotNode,
        versionData: SnapshotVersionData? = null,
        trashData: SnapshotTrashData? = null
    ) {
        if (storage == null) return
        FLog.d(TAG, "saveSnapshot: saving snapshot and clearing WAL")
        // Step 1: 原子写入 snapshot
        atomicWrite(config.snapshotKey, VfsPersistenceCodec.encodeSnapshot(snapshot))
        // Step 2: 原子写入 versions
        if (versionData != null) {
            atomicWrite(config.versionsKey, VfsPersistenceCodec.encodeVersionData(versionData))
        }
        // Step 3: 原子写入 trash
        if (trashData != null) {
            atomicWrite(config.trashKey, VfsPersistenceCodec.encodeTrashData(trashData))
        }
        // Step 4: 清空 WAL（最后执行，保证崩溃安全）
        walEntries.clear()
        storage.write(config.walKey, VfsPersistenceCodec.encodeWal(walEntries))
        opsSinceSnapshot = 0
    }

    /**
     * 原子写入：先写临时 key，读回校验 CRC，再覆盖正式 key，最后删临时 key。
     */
    private suspend fun atomicWrite(key: String, data: ByteArray) {
        val tmp = tmpKey(key)
        storage!!.write(tmp, data)
        // 读回校验完整性
        try {
            val readBack = storage.read(tmp).getOrNull()
            if (readBack != null) {
                VfsPersistenceCodec.unwrapCrc(readBack) // 仅校验，不使用返回值
            }
        } catch (e: Exception) {
            FLog.w(TAG, "atomicWrite: CRC verification failed for $tmp, retrying direct write: ${e.message}")
            // 回退为直接写
            storage.write(key, data)
            try { storage.delete(tmp) } catch (_: Exception) {}
            return
        }
        // CRC 校验通过，替换正式 key
        storage.write(key, data)
        try { storage.delete(tmp) } catch (_: Exception) {}
    }

    private fun tmpKey(key: String): String = "$key.tmp"

    // ── 挂载点持久化 ─────────────────────────────────────────

    suspend fun persistMounts(mountInfos: List<MountInfo>) {
        if (storage == null) return
        storage.write(config.mountsKey, VfsPersistenceCodec.encodeMounts(mountInfos))
    }

    // ── 回收站持久化 ─────────────────────────────────────────

    suspend fun persistTrash(trashData: SnapshotTrashData) {
        if (storage == null) return
        storage.write(config.trashKey, VfsPersistenceCodec.encodeTrashData(trashData))
    }
}
