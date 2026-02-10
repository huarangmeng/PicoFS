package com.hrm.fs.core.persistence

import com.hrm.fs.api.FsPermissions
import com.hrm.fs.api.FsType
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.json.Json

internal data class PersistenceConfig(
    val snapshotKey: String = "vfs_snapshot.json",
    val walKey: String = "vfs_wal.json",
    val autoSnapshotEvery: Int = 20
)

internal object VfsPersistenceCodec {
    private val json = Json { encodeDefaults = true; ignoreUnknownKeys = true }

    fun encodeSnapshot(snapshot: SnapshotNode): ByteArray =
        json.encodeToString(SnapshotNode.serializer(), snapshot).encodeToByteArray()

    fun decodeSnapshot(bytes: ByteArray): SnapshotNode =
        json.decodeFromString(SnapshotNode.serializer(), bytes.decodeToString())

    fun encodeWal(entries: List<WalEntry>): ByteArray =
        json.encodeToString(ListSerializer(WalEntry.serializer()), entries).encodeToByteArray()

    fun decodeWal(bytes: ByteArray): List<WalEntry> =
        json.decodeFromString(ListSerializer(WalEntry.serializer()), bytes.decodeToString())
}

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
    val children: List<SnapshotNode>? = null
) {
    fun fsType(): FsType = if (type == "DIRECTORY") FsType.DIRECTORY else FsType.FILE
}

@Serializable
internal sealed class WalEntry {
    @Serializable
    @SerialName("CreateFile")
    data class CreateFile(val path: String) : WalEntry()

    @Serializable
    @SerialName("CreateDir")
    data class CreateDir(val path: String) : WalEntry()

    @Serializable
    @SerialName("Delete")
    data class Delete(val path: String) : WalEntry()

    @Serializable
    @SerialName("Write")
    data class Write(val path: String, val offset: Long, val data: ByteArray) : WalEntry()

    @Serializable
    @SerialName("SetPermissions")
    data class SetPermissions(val path: String, val permissions: SnapshotPermissions) : WalEntry()
}
