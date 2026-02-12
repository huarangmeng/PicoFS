package com.hrm.fs.core.persistence

/**
 * 持久化编解码器接口。
 *
 * 抽象 Snapshot、WAL、Mounts、Versions、Trash 的编解码，
 * 底层可切换实现（CBOR / TLV）。
 */
internal interface VfsCodec {

    /** 编解码器名称（用于日志和调试）。 */
    val name: String

    // ── Snapshot ──

    fun encodeSnapshot(snapshot: SnapshotNode): ByteArray
    fun decodeSnapshot(bytes: ByteArray): SnapshotNode

    // ── WAL ──

    /** 编码单条 WAL entry（用于增量追加）。 */
    fun encodeWalEntry(entry: WalEntry): ByteArray

    /** 编码多条 WAL entries（用于快照后批量回写）。 */
    fun encodeWalEntries(entries: List<WalEntry>): ByteArray

    /** 解码 WAL 数据，返回所有可恢复的条目（损坏条目跳过）。 */
    fun decodeWalEntries(bytes: ByteArray): List<WalEntry>

    // ── Mounts ──

    fun encodeMounts(mounts: List<MountInfo>): ByteArray
    fun decodeMounts(bytes: ByteArray): List<MountInfo>

    // ── Versions ──

    fun encodeVersionData(data: SnapshotVersionData): ByteArray
    fun decodeVersionData(bytes: ByteArray): SnapshotVersionData

    // ── Trash ──

    fun encodeTrashData(data: SnapshotTrashData): ByteArray
    fun decodeTrashData(bytes: ByteArray): SnapshotTrashData
}

/** 编解码器类型枚举，用于 [PersistenceConfig] 配置。 */
internal enum class CodecType {
    CBOR, TLV
}

// ── 共享二进制工具函数 ──────────────────────────────────────

/** CRC32 查找表（标准多项式 0xEDB88320）。 */
private val CRC32_TABLE = IntArray(256) { n ->
    var c = n
    repeat(8) {
        c = if (c and 1 != 0) (c ushr 1) xor 0xEDB88320.toInt() else c ushr 1
    }
    c
}

/** 计算 CRC32 校验值，返回 Int（Big-Endian 写入 4 字节）。 */
internal fun crc32Bytes(data: ByteArray): Int {
    var crc = 0xFFFFFFFF.toInt()
    for (b in data) {
        crc = (crc ushr 8) xor CRC32_TABLE[(crc xor b.toInt()) and 0xFF]
    }
    return crc xor 0xFFFFFFFF.toInt()
}

/** Big-Endian 写入 4 字节 Int。 */
internal fun writeInt(buf: ByteArray, offset: Int, value: Int) {
    buf[offset] = (value ushr 24).toByte()
    buf[offset + 1] = (value ushr 16).toByte()
    buf[offset + 2] = (value ushr 8).toByte()
    buf[offset + 3] = value.toByte()
}

/** Big-Endian 读取 4 字节 Int。 */
internal fun readInt(buf: ByteArray, offset: Int): Int =
    ((buf[offset].toInt() and 0xFF) shl 24) or
    ((buf[offset + 1].toInt() and 0xFF) shl 16) or
    ((buf[offset + 2].toInt() and 0xFF) shl 8) or
    (buf[offset + 3].toInt() and 0xFF)

/** WAL 单条记录：[4B CRC32][4B length][payload]。 */
internal fun wrapWalRecord(payload: ByteArray): ByteArray {
    val crc = crc32Bytes(payload)
    val out = ByteArray(8 + payload.size)
    writeInt(out, 0, crc)
    writeInt(out, 4, payload.size)
    payload.copyInto(out, 8)
    return out
}

/** Snapshot/Mounts 等：[4B CRC32][payload]。 */
internal fun wrapCrc(payload: ByteArray): ByteArray {
    val crc = crc32Bytes(payload)
    val out = ByteArray(4 + payload.size)
    writeInt(out, 0, crc)
    payload.copyInto(out, 4)
    return out
}

/** 校验并解包 [4B CRC32][payload]，CRC 不匹配时抛异常。 */
internal fun unwrapCrc(bytes: ByteArray): ByteArray {
    if (bytes.size < 4) throw CorruptedDataException("Data too short")
    val storedCrc = readInt(bytes, 0)
    val payload = bytes.copyOfRange(4, bytes.size)
    val actualCrc = crc32Bytes(payload)
    if (storedCrc != actualCrc) throw CorruptedDataException("CRC mismatch")
    return payload
}

/** 解码 WAL 二进制流：逐条读取 [4B CRC32][4B length][payload]，CRC 失败则跳过。 */
internal fun decodeWalRecords(bytes: ByteArray, decodePayload: (ByteArray) -> WalEntry?): List<WalEntry> {
    if (bytes.isEmpty()) return emptyList()
    val entries = mutableListOf<WalEntry>()
    var offset = 0
    while (offset + 8 <= bytes.size) {
        val storedCrc = readInt(bytes, offset)
        val length = readInt(bytes, offset + 4)
        if (length < 0 || offset + 8 + length > bytes.size) break
        val payload = bytes.copyOfRange(offset + 8, offset + 8 + length)
        val actualCrc = crc32Bytes(payload)
        if (storedCrc == actualCrc) {
            try {
                decodePayload(payload)?.let { entries.add(it) }
            } catch (_: Exception) { }
        }
        offset += 8 + length
    }
    return entries
}
