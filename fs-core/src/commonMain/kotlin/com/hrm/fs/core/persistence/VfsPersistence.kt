package com.hrm.fs.core.persistence

import com.hrm.fs.api.FsPermissions
import com.hrm.fs.api.FsType
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable

internal data class PersistenceConfig(
    val snapshotKey: String = "vfs_snapshot.bin",
    val walKey: String = "vfs_wal.bin",
    val mountsKey: String = "vfs_mounts.bin",
    val versionsKey: String = "vfs_versions.bin",
    val trashKey: String = "vfs_trash.bin",
    val autoSnapshotEvery: Int = 20,
    val codecType: CodecType = CodecType.TLV
) {
    /** 根据 codecType 创建编解码器实例。 */
    fun createCodec(): VfsCodec = when (codecType) {
        CodecType.CBOR -> CborCodec()
        CodecType.TLV -> TlvCodec()
    }
}

@Serializable
internal data class MountInfo(
    val virtualPath: String,
    val rootPath: String,
    val readOnly: Boolean = false
)

/**
 * 持久化数据损坏异常。
 * 加载时检测到 CRC 校验失败或数据格式错误时抛出。
 */
internal class CorruptedDataException(message: String) : Exception(message)

@Serializable
internal data class SnapshotPermissions(
    val read: Boolean,
    val write: Boolean,
    val execute: Boolean
) {
    companion object {
        fun from(perms: FsPermissions) = SnapshotPermissions(perms.read, perms.write, perms.execute)
    }

    fun toFsPermissions(): FsPermissions = FsPermissions(read, write, execute)
}

@Serializable
internal data class SnapshotNode(
    val name: String,
    val type: String,
    val createdAtMillis: Long,
    val modifiedAtMillis: Long,
    val permissions: SnapshotPermissions,
    val content: ByteArray? = null,
    val children: List<SnapshotNode>? = null,
    val versions: List<SnapshotVersionEntry>? = null,
    /** 符号链接目标路径，仅 type == "SYMLINK" 时有值。 */
    val target: String? = null,
    /** 扩展属性（xattr），为空时不序列化。 */
    val xattrs: Map<String, ByteArray>? = null
) {
    fun fsType(): FsType = when (type) {
        "DIRECTORY" -> FsType.DIRECTORY
        "SYMLINK" -> FsType.SYMLINK
        else -> FsType.FILE
    }
}

@Serializable
internal data class SnapshotVersionEntry(
    val versionId: String,
    val timestampMillis: Long,
    val data: ByteArray
)

/**
 * 按路径索引的版本历史快照。
 */
@Serializable
internal data class SnapshotVersionData(
    val entries: Map<String, List<SnapshotVersionEntry>> = emptyMap()
)

/**
 * 回收站条目快照（可序列化）。
 */
@Serializable
internal data class SnapshotTrashEntry(
    val trashId: String,
    val originalPath: String,
    val type: String,
    val deletedAtMillis: Long,
    val content: ByteArray? = null,
    val children: List<SnapshotTrashChild>? = null,
    val isMounted: Boolean = false
) {
    @Serializable
    data class SnapshotTrashChild(
        val relativePath: String,
        val type: String,
        val content: ByteArray? = null,
        val children: List<SnapshotTrashChild>? = null
    )
}

/**
 * 回收站快照数据包装。
 */
@Serializable
internal data class SnapshotTrashData(
    val entries: List<SnapshotTrashEntry> = emptyList()
)

@Serializable
internal sealed class WalEntry {
    @Serializable
    @SerialName("CreateFile")
    data class CreateFile(val path: String) : WalEntry()

    @Serializable
    @SerialName("CreateDir")
    data class CreateDir(val path: String) : WalEntry()

    @Serializable
    @SerialName("CreateSymlink")
    data class CreateSymlink(val path: String, val target: String) : WalEntry()

    @Serializable
    @SerialName("Delete")
    data class Delete(val path: String) : WalEntry()

    @Serializable
    @SerialName("Write")
    data class Write(val path: String, val offset: Long, val data: ByteArray) : WalEntry()

    @Serializable
    @SerialName("SetPermissions")
    data class SetPermissions(val path: String, val permissions: SnapshotPermissions) : WalEntry()

    @Serializable
    @SerialName("SetXattr")
    data class SetXattr(val path: String, val name: String, val value: ByteArray) : WalEntry()

    @Serializable
    @SerialName("RemoveXattr")
    data class RemoveXattr(val path: String, val name: String) : WalEntry()

    @Serializable
    @SerialName("Copy")
    data class Copy(val src: String, val dst: String) : WalEntry()

    @Serializable
    @SerialName("Move")
    data class Move(val src: String, val dst: String) : WalEntry()

    @Serializable
    @SerialName("MoveToTrash")
    data class MoveToTrash(val path: String, val trashId: String) : WalEntry()

    @Serializable
    @SerialName("RestoreFromTrash")
    data class RestoreFromTrash(val trashId: String, val path: String) : WalEntry()
}
