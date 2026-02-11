package com.hrm.fs.core

import com.hrm.fs.api.DiskFileOperations
import com.hrm.fs.api.FsError
import com.hrm.fs.api.MountOptions
import com.hrm.fs.api.log.FLog
import com.hrm.fs.core.persistence.MountInfo

/**
 * 挂载表管理。
 *
 * 维护 虚拟路径 -> (DiskFileOperations, MountOptions) 的映射表，
 * 提供最长前缀匹配和待恢复挂载点列表。
 *
 * **无线程安全保证**，外部需自行加锁。
 */
internal class MountTable {

    companion object {
        private const val TAG = "MountTable"
    }

    /** 活跃挂载：虚拟路径 -> (DiskFileOperations, MountOptions) */
    private val active: MutableMap<String, Pair<DiskFileOperations, MountOptions>> = LinkedHashMap()

    /**
     * 持久化恢复出来、但尚未被外部 [mount] 重新挂载的条目。
     * key = virtualPath, value = 完整的 MountInfo（含 rootPath / readOnly）。
     * 外部可通过 [pendingMounts] 拿到列表，再调用 [mount] 使其生效。
     */
    private val pending: MutableMap<String, MountInfo> = LinkedHashMap()

    // ── 挂载 / 卸载 ─────────────────────────────────────────

    fun mount(normalizedPath: String, diskOps: DiskFileOperations, options: MountOptions): Result<Unit> {
        if (normalizedPath == "/") {
            FLog.w(TAG, "mount failed: cannot mount root path")
            return Result.failure(FsError.InvalidPath("不能挂载根路径"))
        }
        active[normalizedPath] = diskOps to options
        pending.remove(normalizedPath)
        FLog.d(TAG, "mount: $normalizedPath -> ${diskOps.rootPath}")
        return Result.success(Unit)
    }

    fun unmount(normalizedPath: String): Result<Unit> {
        if (active.remove(normalizedPath) == null) {
            FLog.w(TAG, "unmount failed: not mounted $normalizedPath")
            return Result.failure(FsError.NotMounted(normalizedPath))
        }
        FLog.d(TAG, "unmount: $normalizedPath")
        return Result.success(Unit)
    }

    fun listMounts(): List<String> = active.keys.toList()

    fun isMountPoint(normalizedPath: String): Boolean = active.containsKey(normalizedPath)

    // ── 路径匹配 ─────────────────────────────────────────────

    data class MountMatch(
        val mountPoint: String,
        val diskOps: DiskFileOperations,
        val relativePath: String,
        val options: MountOptions
    )

    /**
     * 对 [normalizedPath] 做最长前缀匹配，找到对应的活跃挂载。
     * 返回 null 表示该路径不在任何挂载点下。
     */
    fun findMount(normalizedPath: String): MountMatch? {
        for ((mountPoint, pair) in active.entries.sortedByDescending { it.key.length }) {
            val (diskOps, options) = pair
            if (normalizedPath == mountPoint) {
                return MountMatch(mountPoint, diskOps, "/", options)
            }
            if (normalizedPath.startsWith("$mountPoint/")) {
                val relative = normalizedPath.removePrefix(mountPoint)
                return MountMatch(mountPoint, diskOps, relative, options)
            }
        }
        return null
    }

    // ── 持久化恢复 ───────────────────────────────────────────

    /**
     * 从持久化的 [MountInfo] 列表中恢复待挂载信息。
     * 不会真正挂载（因为没有 DiskFileOperations），只记录到 pending 列表。
     */
    fun restoreFromPersistence(mountInfos: List<MountInfo>) {
        for (info in mountInfos) {
            if (active.containsKey(info.virtualPath)) continue
            pending[info.virtualPath] = info
        }
        FLog.d(TAG, "restoreFromPersistence: ${mountInfos.size} entries, pending=${pending.size}")
    }

    /**
     * 返回"待恢复"挂载列表（含 virtualPath / rootPath / readOnly）。
     * 外部拿到后可用对应平台的 [DiskFileOperations] 重新 mount。
     */
    fun pendingMounts(): List<MountInfo> = pending.values.toList()

    /**
     * 导出当前活跃挂载为 [MountInfo] 列表，用于持久化写入。
     */
    fun toMountInfoList(): List<MountInfo> {
        return active.map { (path, pair) ->
            MountInfo(path, pair.first.rootPath, pair.second.readOnly)
        }
    }
}
