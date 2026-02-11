package com.hrm.fs.api

/**
 * 纯 Kotlin 实现的归档编解码器（commonMain 可用，无平台依赖）。
 *
 * - **ZIP**：STORE 方式（无压缩），兼容标准 ZIP 规范。
 * - **TAR**：USTAR 格式，512 字节块对齐。
 *
 * 本类主要供 VFS 内存文件系统和 iOS 平台层使用。
 * JVM/Android 平台优先使用原生 `java.util.zip`。
 */
object ArchiveCodec {

    // ═══════════════════════════════════════════════════════════
    // 格式自动检测
    // ═══════════════════════════════════════════════════════════

    fun detectFormat(data: ByteArray): ArchiveFormat? {
        if (data.size >= 4 && data[0] == 0x50.toByte() && data[1] == 0x4B.toByte() &&
            data[2] == 0x03.toByte() && data[3] == 0x04.toByte()
        ) return ArchiveFormat.ZIP
        if (data.size >= 263) {
            val ustar = "ustar"
            val match = (0 until 5).all { data[257 + it] == ustar[it].code.toByte() }
            if (match) return ArchiveFormat.TAR
        }
        return null
    }

    // ═══════════════════════════════════════════════════════════
    // ZIP STORE 编解码
    // ═══════════════════════════════════════════════════════════

    data class ArchiveItem(
        val path: String,
        val type: FsType,
        val data: ByteArray,
        val modifiedAtMillis: Long = 0
    )

    fun zipEncode(items: List<ArchiveItem>): ByteArray {
        val buf = GrowableByteArray()
        val centralEntries = mutableListOf<ByteArray>()
        val offsets = mutableListOf<Int>()

        for (item in items) {
            offsets.add(buf.size)
            val entryName = if (item.type == FsType.DIRECTORY && !item.path.endsWith("/")) {
                item.path + "/"
            } else item.path
            val nameBytes = entryName.encodeToByteArray()
            val crc = crc32Int(item.data)
            val fileData = item.data

            buf.writeInt32LE(0x04034B50)
            buf.writeInt16LE(20)
            buf.writeInt16LE(0)
            buf.writeInt16LE(0)
            val (dosTime, dosDate) = toDosDateTime(item.modifiedAtMillis)
            buf.writeInt16LE(dosTime)
            buf.writeInt16LE(dosDate)
            buf.writeInt32LE(crc)
            buf.writeInt32LE(fileData.size)
            buf.writeInt32LE(fileData.size)
            buf.writeInt16LE(nameBytes.size)
            buf.writeInt16LE(0)
            buf.writeBytes(nameBytes)
            buf.writeBytes(fileData)

            val central = GrowableByteArray()
            central.writeInt32LE(0x02014B50)
            central.writeInt16LE(20)
            central.writeInt16LE(20)
            central.writeInt16LE(0)
            central.writeInt16LE(0)
            central.writeInt16LE(dosTime)
            central.writeInt16LE(dosDate)
            central.writeInt32LE(crc)
            central.writeInt32LE(fileData.size)
            central.writeInt32LE(fileData.size)
            central.writeInt16LE(nameBytes.size)
            central.writeInt16LE(0)
            central.writeInt16LE(0)
            central.writeInt16LE(0)
            central.writeInt16LE(0)
            central.writeInt32LE(if (item.type == FsType.DIRECTORY) 0x10 else 0)
            central.writeInt32LE(offsets[offsets.size - 1])
            central.writeBytes(nameBytes)
            centralEntries.add(central.toByteArray())
        }

        val centralStart = buf.size
        for (entry in centralEntries) buf.writeBytes(entry)
        val centralSize = buf.size - centralStart

        buf.writeInt32LE(0x06054B50)
        buf.writeInt16LE(0)
        buf.writeInt16LE(0)
        buf.writeInt16LE(items.size)
        buf.writeInt16LE(items.size)
        buf.writeInt32LE(centralSize)
        buf.writeInt32LE(centralStart)
        buf.writeInt16LE(0)

        return buf.toByteArray()
    }

    data class ZipDecodedEntry(
        val path: String,
        val data: ByteArray,
        val isDirectory: Boolean,
        val modifiedAtMillis: Long
    )

    fun zipDecode(archive: ByteArray): List<ZipDecodedEntry> {
        val entries = mutableListOf<ZipDecodedEntry>()
        var offset = 0
        while (offset + 4 <= archive.size) {
            val sig = readInt32LE(archive, offset)
            if (sig != 0x04034B50) break
            val compression = readInt16LE(archive, offset + 8)
            val dosTime = readInt16LE(archive, offset + 12)
            val dosDate = readInt16LE(archive, offset + 14)
            val compressedSize = readInt32LE(archive, offset + 18)
            val nameLen = readInt16LE(archive, offset + 26)
            val extraLen = readInt16LE(archive, offset + 28)
            val nameStart = offset + 30
            val name = archive.decodeToString(nameStart, nameStart + nameLen)
            val dataStart = nameStart + nameLen + extraLen
            if (compression != 0) {
                throw IllegalArgumentException("不支持的压缩方式: $compression（仅支持 STORE）")
            }
            val data = archive.copyOfRange(dataStart, dataStart + compressedSize)
            val isDir = name.endsWith("/")
            val modMillis = fromDosDateTime(dosTime, dosDate)
            entries.add(ZipDecodedEntry(name.trimEnd('/'), data, isDir, modMillis))
            offset = dataStart + compressedSize
        }
        return entries
    }

    fun zipList(archive: ByteArray): List<ArchiveEntry> {
        val entries = mutableListOf<ArchiveEntry>()
        var offset = 0
        while (offset + 4 <= archive.size) {
            val sig = readInt32LE(archive, offset)
            if (sig != 0x04034B50) break
            val dosTime = readInt16LE(archive, offset + 12)
            val dosDate = readInt16LE(archive, offset + 14)
            val uncompressedSize = readInt32LE(archive, offset + 22)
            val compressedSize = readInt32LE(archive, offset + 18)
            val nameLen = readInt16LE(archive, offset + 26)
            val extraLen = readInt16LE(archive, offset + 28)
            val nameStart = offset + 30
            val name = archive.decodeToString(nameStart, nameStart + nameLen)
            val isDir = name.endsWith("/")
            val modMillis = fromDosDateTime(dosTime, dosDate)
            entries.add(
                ArchiveEntry(
                    path = name.trimEnd('/'),
                    type = if (isDir) FsType.DIRECTORY else FsType.FILE,
                    size = uncompressedSize.toLong(),
                    modifiedAtMillis = modMillis
                )
            )
            offset = nameStart + nameLen + extraLen + compressedSize
        }
        return entries
    }

    // ═══════════════════════════════════════════════════════════
    // TAR USTAR 编解码
    // ═══════════════════════════════════════════════════════════

    fun tarEncode(items: List<ArchiveItem>): ByteArray {
        val buf = GrowableByteArray()
        for (item in items) {
            val header = buildTarHeader(item)
            buf.writeBytes(header)
            if (item.type == FsType.FILE && item.data.isNotEmpty()) {
                buf.writeBytes(item.data)
                val remainder = item.data.size % 512
                if (remainder != 0) buf.writeBytes(ByteArray(512 - remainder))
            }
        }
        buf.writeBytes(ByteArray(1024))
        return buf.toByteArray()
    }

    data class TarDecodedEntry(
        val path: String,
        val data: ByteArray,
        val isDirectory: Boolean,
        val modifiedAtMillis: Long
    )

    fun tarDecode(archive: ByteArray): List<TarDecodedEntry> {
        val entries = mutableListOf<TarDecodedEntry>()
        var offset = 0
        while (offset + 512 <= archive.size) {
            val allZero = (0 until 512).all { archive[offset + it] == 0.toByte() }
            if (allZero) break
            val name = readTarString(archive, offset, 100)
            val sizeOctal = readTarString(archive, offset + 124, 12)
            val mtimeOctal = readTarString(archive, offset + 136, 12)
            val typeFlag = archive[offset + 156]
            val size = parseOctal(sizeOctal)
            val mtime = parseOctal(mtimeOctal) * 1000
            val isDir = typeFlag == '5'.code.toByte() || name.endsWith("/")
            offset += 512
            val data = if (size > 0 && !isDir) {
                val d = archive.copyOfRange(offset, offset + size.toInt())
                offset += size.toInt()
                val remainder = (size % 512).toInt()
                if (remainder != 0) offset += 512 - remainder
                d
            } else ByteArray(0)
            entries.add(TarDecodedEntry(name.trimEnd('/'), data, isDir, mtime))
        }
        return entries
    }

    fun tarList(archive: ByteArray): List<ArchiveEntry> {
        val entries = mutableListOf<ArchiveEntry>()
        var offset = 0
        while (offset + 512 <= archive.size) {
            val allZero = (0 until 512).all { archive[offset + it] == 0.toByte() }
            if (allZero) break
            val name = readTarString(archive, offset, 100)
            val sizeOctal = readTarString(archive, offset + 124, 12)
            val mtimeOctal = readTarString(archive, offset + 136, 12)
            val typeFlag = archive[offset + 156]
            val size = parseOctal(sizeOctal)
            val mtime = parseOctal(mtimeOctal) * 1000
            val isDir = typeFlag == '5'.code.toByte() || name.endsWith("/")
            entries.add(
                ArchiveEntry(
                    path = name.trimEnd('/'),
                    type = if (isDir) FsType.DIRECTORY else FsType.FILE,
                    size = if (isDir) 0L else size,
                    modifiedAtMillis = mtime
                )
            )
            offset += 512
            if (size > 0 && !isDir) {
                offset += size.toInt()
                val remainder = (size % 512).toInt()
                if (remainder != 0) offset += 512 - remainder
            }
        }
        return entries
    }

    // ═══════════════════════════════════════════════════════════
    // 内部工具
    // ═══════════════════════════════════════════════════════════

    private fun buildTarHeader(item: ArchiveItem): ByteArray {
        val header = ByteArray(512)
        val name = if (item.type == FsType.DIRECTORY && !item.path.endsWith("/")) item.path + "/" else item.path
        val nameBytes = name.encodeToByteArray()
        nameBytes.copyInto(header, 0, 0, minOf(nameBytes.size, 100))
        writeOctal(header, 100, 8, if (item.type == FsType.DIRECTORY) 493 else 420)
        writeOctal(header, 108, 8, 0)
        writeOctal(header, 116, 8, 0)
        writeOctal(header, 124, 12, if (item.type == FsType.DIRECTORY) 0L else item.data.size.toLong())
        writeOctal(header, 136, 12, item.modifiedAtMillis / 1000)
        for (i in 148 until 156) header[i] = ' '.code.toByte()
        header[156] = if (item.type == FsType.DIRECTORY) '5'.code.toByte() else '0'.code.toByte()
        val ustar = "ustar\u000000"
        ustar.encodeToByteArray().copyInto(header, 257, 0, minOf(ustar.length, 8))
        var chksum = 0
        for (b in header) chksum += (b.toInt() and 0xFF)
        writeOctal(header, 148, 7, chksum.toLong())
        header[155] = ' '.code.toByte()
        return header
    }

    private fun crc32Int(data: ByteArray): Int {
        var crc = 0.inv()
        for (b in data) crc = (crc ushr 8) xor CRC32_TABLE[(crc xor b.toInt()) and 0xFF]
        return crc.inv()
    }

    private val CRC32_TABLE = IntArray(256) { n ->
        var c = n
        repeat(8) { c = if (c and 1 != 0) (c ushr 1) xor 0xEDB88320.toInt() else c ushr 1 }
        c
    }

    private fun toDosDateTime(millis: Long): Pair<Int, Int> {
        if (millis <= 0) return Pair(0, 0x0021)
        val totalSeconds = millis / 1000
        val days = (totalSeconds / 86400).toInt()
        val timeOfDay = (totalSeconds % 86400).toInt()
        val hours = timeOfDay / 3600
        val minutes = (timeOfDay % 3600) / 60
        val seconds = timeOfDay % 60
        var y = 1970; var remaining = days
        while (true) { val dy = if (isLeapYear(y)) 366 else 365; if (remaining < dy) break; remaining -= dy; y++ }
        val md = if (isLeapYear(y)) LEAP_MONTH_DAYS else NORMAL_MONTH_DAYS
        var m = 1; for (dm in md) { if (remaining < dm) break; remaining -= dm; m++ }
        val d = remaining + 1
        if (y < 1980) return Pair(0, 0x0021)
        return Pair((hours shl 11) or (minutes shl 5) or (seconds / 2), ((y - 1980) shl 9) or (m shl 5) or d)
    }

    private fun fromDosDateTime(dosTime: Int, dosDate: Int): Long {
        if (dosDate == 0 && dosTime == 0) return 0
        val year = ((dosDate shr 9) and 0x7F) + 1980
        val month = (dosDate shr 5) and 0x0F; val day = dosDate and 0x1F
        val hours = (dosTime shr 11) and 0x1F; val minutes = (dosTime shr 5) and 0x3F; val seconds = (dosTime and 0x1F) * 2
        var days = 0L; for (y in 1970 until year) days += if (isLeapYear(y)) 366 else 365
        val md = if (isLeapYear(year)) LEAP_MONTH_DAYS else NORMAL_MONTH_DAYS
        for (i in 0 until (month - 1).coerceIn(0, 11)) days += md[i]
        days += (day - 1).coerceAtLeast(0)
        return (days * 86400 + hours * 3600 + minutes * 60 + seconds) * 1000
    }

    private fun isLeapYear(y: Int): Boolean = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
    private val NORMAL_MONTH_DAYS = intArrayOf(31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
    private val LEAP_MONTH_DAYS = intArrayOf(31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)

    private fun readInt16LE(data: ByteArray, offset: Int): Int =
        (data[offset].toInt() and 0xFF) or ((data[offset + 1].toInt() and 0xFF) shl 8)

    private fun readInt32LE(data: ByteArray, offset: Int): Int =
        (data[offset].toInt() and 0xFF) or ((data[offset + 1].toInt() and 0xFF) shl 8) or
                ((data[offset + 2].toInt() and 0xFF) shl 16) or ((data[offset + 3].toInt() and 0xFF) shl 24)

    private fun readTarString(data: ByteArray, offset: Int, maxLen: Int): String {
        var end = offset; val limit = minOf(offset + maxLen, data.size)
        while (end < limit && data[end] != 0.toByte()) end++
        return data.decodeToString(offset, end)
    }

    private fun parseOctal(s: String): Long {
        val trimmed = s.trim().trimEnd('\u0000')
        if (trimmed.isEmpty()) return 0
        return trimmed.toLongOrNull(8) ?: 0
    }

    private fun writeOctal(header: ByteArray, offset: Int, fieldLen: Int, value: Long) {
        val padded = value.toString(8).padStart(fieldLen - 1, '0')
        val bytes = padded.encodeToByteArray()
        val copyLen = minOf(bytes.size, fieldLen - 1)
        bytes.copyInto(header, offset, bytes.size - copyLen, bytes.size)
        header[offset + fieldLen - 1] = 0
    }

    class GrowableByteArray {
        private var data = ByteArray(4096)
        var size: Int = 0; private set
        fun writeBytes(bytes: ByteArray) { ensureCapacity(size + bytes.size); bytes.copyInto(data, size); size += bytes.size }
        fun writeInt16LE(value: Int) { ensureCapacity(size + 2); data[size] = (value and 0xFF).toByte(); data[size + 1] = ((value shr 8) and 0xFF).toByte(); size += 2 }
        fun writeInt32LE(value: Int) { ensureCapacity(size + 4); data[size] = (value and 0xFF).toByte(); data[size + 1] = ((value shr 8) and 0xFF).toByte(); data[size + 2] = ((value shr 16) and 0xFF).toByte(); data[size + 3] = ((value shr 24) and 0xFF).toByte(); size += 4 }
        fun toByteArray(): ByteArray = data.copyOf(size)
        private fun ensureCapacity(required: Int) { if (required <= data.size) return; var ns = data.size; while (ns < required) ns *= 2; data = data.copyOf(ns) }
    }
}
