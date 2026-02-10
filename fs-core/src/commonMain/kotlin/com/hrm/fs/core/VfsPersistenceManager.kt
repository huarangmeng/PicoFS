package com.hrm.fs.core

import com.hrm.fs.api.FsStorage
import com.hrm.fs.core.persistence.MountInfo
import com.hrm.fs.core.persistence.PersistenceConfig
import com.hrm.fs.core.persistence.SnapshotNode
import com.hrm.fs.core.persistence.VfsPersistenceCodec
import com.hrm.fs.core.persistence.WalEntry

/**
 * 持久化管理器。
 *
 * 封装 WAL 追加、自动快照、挂载点持久化的所有逻辑。
 * **无线程安全保证**，外部需自行加锁。
 */
internal class VfsPersistenceManager(
    private val storage: FsStorage?,
    private val config: PersistenceConfig = PersistenceConfig()
) {
    private val walEntries: MutableList<WalEntry> = mutableListOf()
    private var opsSinceSnapshot: Int = 0
    private var loaded: Boolean = false

    val persistenceConfig: PersistenceConfig get() = config

    // ── 加载 ─────────────────────────────────────────────────

    /**
     * 首次调用时从 storage 加载快照 + WAL + 挂载信息。
     * 返回 (snapshot, walEntries, mountInfos)，调用方自行决定如何应用。
     */
    data class LoadResult(
        val snapshot: SnapshotNode?,
        val walEntries: List<WalEntry>,
        val mountInfos: List<MountInfo>
    )

    suspend fun ensureLoaded(): LoadResult? {
        if (loaded) return null
        loaded = true
        return load()
    }

    private suspend fun load(): LoadResult? {
        if (storage == null) return null

        val snapshot = storage.read(config.snapshotKey).getOrNull()?.let {
            VfsPersistenceCodec.decodeSnapshot(it)
        }

        val wal = storage.read(config.walKey).getOrNull()?.let {
            VfsPersistenceCodec.decodeWal(it)
        }.orEmpty()

        if (wal.isNotEmpty()) {
            walEntries.clear()
            walEntries.addAll(wal)
        }

        val mountInfos = storage.read(config.mountsKey).getOrNull()?.let {
            VfsPersistenceCodec.decodeMounts(it)
        }.orEmpty()

        return LoadResult(snapshot, wal, mountInfos)
    }

    // ── WAL ──────────────────────────────────────────────────

    /**
     * 追加一条 WAL 条目并持久化。
     * 达到阈值时自动触发快照。
     *
     * @param snapshotProvider 生成当前快照的回调（惰性，仅在需要快照时调用）
     */
    suspend fun appendWal(entry: WalEntry, snapshotProvider: () -> SnapshotNode) {
        if (storage == null) return
        walEntries.add(entry)
        opsSinceSnapshot++
        storage.write(config.walKey, VfsPersistenceCodec.encodeWal(walEntries))
        if (opsSinceSnapshot >= config.autoSnapshotEvery) {
            saveSnapshot(snapshotProvider())
        }
    }

    suspend fun saveSnapshot(snapshot: SnapshotNode) {
        if (storage == null) return
        storage.write(config.snapshotKey, VfsPersistenceCodec.encodeSnapshot(snapshot))
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
