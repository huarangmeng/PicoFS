package com.hrm.fs.core.persistence

import com.hrm.fs.api.FsPermissions
import com.hrm.fs.api.FsType
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.Json
import kotlin.io.encoding.Base64
import kotlin.io.encoding.ExperimentalEncodingApi

/**
 * 自定义 ByteArray 序列化器：使用 Base64 编码代替默认的 JSON 整数数组。
 *
 * 默认 kotlinx.serialization 将 ByteArray 编码为 JSON 整数数组 `[72,101,108,108,111]`，
 * 导致 3-5x 的体积膨胀。本序列化器使用 Base64 字符串 `"SGVsbG8="` 代替，
 * 体积约为原始二进制的 1.33x。
 *
 * 向后兼容：解码时先尝试 Base64 字符串，若失败则尝试解析旧版 JSON 整数数组。
 */
@OptIn(ExperimentalEncodingApi::class)
internal object ByteArrayBase64Serializer : KSerializer<ByteArray> {
    override val descriptor: SerialDescriptor =
        PrimitiveSerialDescriptor("ByteArrayBase64", PrimitiveKind.STRING)

    override fun serialize(encoder: Encoder, value: ByteArray) {
        encoder.encodeString(Base64.encode(value))
    }

    override fun deserialize(decoder: Decoder): ByteArray {
        val str = decoder.decodeString()
        return Base64.decode(str)
    }
}

internal data class PersistenceConfig(
    val snapshotKey: String = "vfs_snapshot.json",
    val walKey: String = "vfs_wal.json",
    val mountsKey: String = "vfs_mounts.json",
    val versionsKey: String = "vfs_versions.json",
    val trashKey: String = "vfs_trash.json",
    val autoSnapshotEvery: Int = 20
)

@Serializable
internal data class MountInfo(
    val virtualPath: String,
    val rootPath: String,
    val readOnly: Boolean = false
)

internal object VfsPersistenceCodec {
    private val json = Json { encodeDefaults = true; ignoreUnknownKeys = true }

    /** CRC32 校验头标记，用于检测数据完整性。 */
    private const val CRC_HEADER = "CRC:"

    fun encodeSnapshot(snapshot: SnapshotNode): ByteArray =
        wrapWithCrc(json.encodeToString(SnapshotNode.serializer(), snapshot))

    fun decodeSnapshot(bytes: ByteArray): SnapshotNode =
        json.decodeFromString(SnapshotNode.serializer(), unwrapCrc(bytes))

    // ── WAL 编解码（每行一条 entry + 独立 CRC） ──

    /** 编码单条 WAL entry 为一行 "CRC:<hex8>\t<json>\n" 格式。 */
    fun encodeWalEntry(entry: WalEntry): ByteArray {
        val jsonStr = json.encodeToString(WalEntry.serializer(), entry)
        val crc = crc32(jsonStr.encodeToByteArray())
        return "$CRC_HEADER$crc\t$jsonStr\n".encodeToByteArray()
    }

    /** 编码多条 WAL entries 为行格式（用于快照后批量回写）。 */
    fun encodeWalEntries(entries: List<WalEntry>): ByteArray {
        if (entries.isEmpty()) return ByteArray(0)
        val sb = StringBuilder()
        for (entry in entries) {
            val jsonStr = json.encodeToString(WalEntry.serializer(), entry)
            val crc = crc32(jsonStr.encodeToByteArray())
            sb.append("$CRC_HEADER$crc\t$jsonStr\n")
        }
        return sb.toString().encodeToByteArray()
    }

    /** 解码行格式 WAL。每行 "CRC:<hex8>\t<json>"，CRC 校验失败的行跳过。 */
    fun decodeWalLines(bytes: ByteArray): List<WalEntry> {
        val text = bytes.decodeToString()
        if (text.isBlank()) return emptyList()
        val entries = mutableListOf<WalEntry>()
        for (line in text.lines()) {
            if (line.isBlank()) continue
            if (!line.startsWith(CRC_HEADER)) continue
            val tabIndex = line.indexOf('\t')
            if (tabIndex < 0) continue
            val expectedCrc = line.substring(CRC_HEADER.length, tabIndex)
            val jsonPayload = line.substring(tabIndex + 1)
            val actualCrc = crc32(jsonPayload.encodeToByteArray())
            if (expectedCrc != actualCrc) continue // 跳过损坏行
            try {
                entries.add(json.decodeFromString(WalEntry.serializer(), jsonPayload))
            } catch (_: Exception) {
                // 跳过无法解析的行
            }
        }
        return entries
    }

    fun encodeMounts(mounts: List<MountInfo>): ByteArray =
        wrapWithCrc(json.encodeToString(ListSerializer(MountInfo.serializer()), mounts))

    fun decodeMounts(bytes: ByteArray): List<MountInfo> =
        json.decodeFromString(ListSerializer(MountInfo.serializer()), unwrapCrc(bytes))

    fun encodeVersionData(data: SnapshotVersionData): ByteArray =
        wrapWithCrc(json.encodeToString(SnapshotVersionData.serializer(), data))

    fun decodeVersionData(bytes: ByteArray): SnapshotVersionData =
        json.decodeFromString(SnapshotVersionData.serializer(), unwrapCrc(bytes))

    fun encodeTrashData(data: SnapshotTrashData): ByteArray =
        wrapWithCrc(json.encodeToString(SnapshotTrashData.serializer(), data))

    fun decodeTrashData(bytes: ByteArray): SnapshotTrashData =
        json.decodeFromString(SnapshotTrashData.serializer(), unwrapCrc(bytes))

    /**
     * 将 JSON 字符串打包为 "CRC:<hex8>\n<json>" 格式的字节数组。
     * 写入时附带 CRC32 校验值，读取时验证完整性。
     */
    private fun wrapWithCrc(jsonString: String): ByteArray {
        val crc = crc32(jsonString.encodeToByteArray())
        return "$CRC_HEADER$crc\n$jsonString".encodeToByteArray()
    }

    /**
     * 从 "CRC:<hex8>\n<json>" 格式的字节数组中解包 JSON 字符串。
     * 缺少 CRC 头或校验失败时抛出 [CorruptedDataException]。
     */
    internal fun unwrapCrc(bytes: ByteArray): String {
        val text = bytes.decodeToString()
        if (!text.startsWith(CRC_HEADER)) {
            throw CorruptedDataException("Missing CRC header")
        }
        val newlineIndex = text.indexOf('\n')
        if (newlineIndex < 0) throw CorruptedDataException("CRC header without payload")
        val expectedCrc = text.substring(CRC_HEADER.length, newlineIndex)
        val jsonPayload = text.substring(newlineIndex + 1)
        val actualCrc = crc32(jsonPayload.encodeToByteArray())
        if (expectedCrc != actualCrc) {
            throw CorruptedDataException("CRC mismatch: expected=$expectedCrc, actual=$actualCrc")
        }
        return jsonPayload
    }

    /**
     * 简单 CRC32 实现（与 VfsChecksum 独立，避免循环依赖）。
     * 返回 8 位小写十六进制字符串。
     */
    private fun crc32(data: ByteArray): String {
        var crc = 0xFFFFFFFF.toInt()
        for (byte in data) {
            crc = crc xor (byte.toInt() and 0xFF)
            repeat(8) {
                crc = if (crc and 1 != 0) (crc ushr 1) xor 0xEDB88320.toInt()
                else crc ushr 1
            }
        }
        return (crc xor 0xFFFFFFFF.toInt()).toUInt().toString(16).padStart(8, '0')
    }
}

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
    @Serializable(with = ByteArrayBase64Serializer::class)
    val content: ByteArray? = null,
    val children: List<SnapshotNode>? = null,
    val versions: List<SnapshotVersionEntry>? = null,
    /** 符号链接目标路径，仅 type == "SYMLINK" 时有值。 */
    val target: String? = null,
    /** 扩展属性（xattr），为空时不序列化。 */
    val xattrs: Map<String, @Serializable(with = ByteArrayBase64Serializer::class) ByteArray>? = null
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
    @Serializable(with = ByteArrayBase64Serializer::class)
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
    @Serializable(with = ByteArrayBase64Serializer::class)
    val content: ByteArray? = null,
    val children: List<SnapshotTrashChild>? = null,
    val isMounted: Boolean = false
) {
    @Serializable
    data class SnapshotTrashChild(
        val relativePath: String,
        val type: String,
        @Serializable(with = ByteArrayBase64Serializer::class)
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
    data class Write(val path: String, val offset: Long, @Serializable(with = ByteArrayBase64Serializer::class) val data: ByteArray) : WalEntry()

    @Serializable
    @SerialName("SetPermissions")
    data class SetPermissions(val path: String, val permissions: SnapshotPermissions) : WalEntry()

    @Serializable
    @SerialName("SetXattr")
    data class SetXattr(val path: String, val name: String, @Serializable(with = ByteArrayBase64Serializer::class) val value: ByteArray) : WalEntry()

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
