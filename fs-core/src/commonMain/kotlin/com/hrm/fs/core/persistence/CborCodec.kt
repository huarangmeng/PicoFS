@file:OptIn(kotlinx.serialization.ExperimentalSerializationApi::class)

package com.hrm.fs.core.persistence

import kotlinx.serialization.builtins.ListSerializer
import kotlinx.serialization.cbor.Cbor

/**
 * CBOR 编解码器。
 *
 * 二进制格式，无字段名字符串开销，ByteArray 直接写入（无需 Base64）。
 * 体积约为 JSON 的 40%，编解码速度 2-3x。
 *
 * WAL 格式：每条 entry 编码为 [4字节CRC32][4字节length][CBOR payload]，可逐条追加。
 * Snapshot/Mounts/Versions/Trash 格式：[4字节CRC32][CBOR payload]。
 */
internal class CborCodec : VfsCodec {

    override val name: String = "CBOR"

    private val cbor = Cbor { ignoreUnknownKeys = true }

    // ── Snapshot ──

    override fun encodeSnapshot(snapshot: SnapshotNode): ByteArray =
        wrapCrc(cbor.encodeToByteArray(SnapshotNode.serializer(), snapshot))

    override fun decodeSnapshot(bytes: ByteArray): SnapshotNode =
        cbor.decodeFromByteArray(SnapshotNode.serializer(), unwrapCrc(bytes))

    // ── WAL ──

    override fun encodeWalEntry(entry: WalEntry): ByteArray =
        wrapWalRecord(cbor.encodeToByteArray(WalEntry.serializer(), entry))

    override fun encodeWalEntries(entries: List<WalEntry>): ByteArray {
        if (entries.isEmpty()) return ByteArray(0)
        var total = 0
        val records = entries.map { e ->
            val r = wrapWalRecord(cbor.encodeToByteArray(WalEntry.serializer(), e))
            total += r.size
            r
        }
        val out = ByteArray(total)
        var offset = 0
        for (r in records) {
            r.copyInto(out, offset)
            offset += r.size
        }
        return out
    }

    override fun decodeWalEntries(bytes: ByteArray): List<WalEntry> =
        decodeWalRecords(bytes) { payload ->
            cbor.decodeFromByteArray(WalEntry.serializer(), payload)
        }

    // ── Mounts ──

    override fun encodeMounts(mounts: List<MountInfo>): ByteArray =
        wrapCrc(cbor.encodeToByteArray(ListSerializer(MountInfo.serializer()), mounts))

    override fun decodeMounts(bytes: ByteArray): List<MountInfo> =
        cbor.decodeFromByteArray(ListSerializer(MountInfo.serializer()), unwrapCrc(bytes))

    // ── Versions ──

    override fun encodeVersionData(data: SnapshotVersionData): ByteArray =
        wrapCrc(cbor.encodeToByteArray(SnapshotVersionData.serializer(), data))

    override fun decodeVersionData(bytes: ByteArray): SnapshotVersionData =
        cbor.decodeFromByteArray(SnapshotVersionData.serializer(), unwrapCrc(bytes))

    // ── Trash ──

    override fun encodeTrashData(data: SnapshotTrashData): ByteArray =
        wrapCrc(cbor.encodeToByteArray(SnapshotTrashData.serializer(), data))

    override fun decodeTrashData(bytes: ByteArray): SnapshotTrashData =
        cbor.decodeFromByteArray(SnapshotTrashData.serializer(), unwrapCrc(bytes))
}
