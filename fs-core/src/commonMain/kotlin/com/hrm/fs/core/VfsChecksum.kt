package com.hrm.fs.core

/**
 * 纯 Kotlin 实现的校验算法集合（commonMain 可用，无平台依赖）。
 */
internal object VfsChecksum {

    // ── CRC32 ───────────────────────────────────────────────────

    private val CRC32_TABLE = IntArray(256) { n ->
        var c = n
        repeat(8) {
            c = if (c and 1 != 0) (c ushr 1) xor 0xEDB88320.toInt() else c ushr 1
        }
        c
    }

    fun crc32(data: ByteArray): String {
        var crc = 0.inv()
        for (b in data) {
            crc = (crc ushr 8) xor CRC32_TABLE[(crc xor b.toInt()) and 0xFF]
        }
        return (crc.inv().toLong() and 0xFFFFFFFFL).toString(16).padStart(8, '0')
    }

    // ── SHA-256 ─────────────────────────────────────────────────

    private val SHA256_K = intArrayOf(
        0x428a2f98.toInt(), 0x71374491, 0xb5c0fbcf.toInt(), 0xe9b5dba5.toInt(),
        0x3956c25b, 0x59f111f1, 0x923f82a4.toInt(), 0xab1c5ed5.toInt(),
        0xd807aa98.toInt(), 0x12835b01, 0x243185be, 0x550c7dc3,
        0x72be5d74, 0x80deb1fe.toInt(), 0x9bdc06a7.toInt(), 0xc19bf174.toInt(),
        0xe49b69c1.toInt(), 0xefbe4786.toInt(), 0x0fc19dc6, 0x240ca1cc,
        0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
        0x983e5152.toInt(), 0xa831c66d.toInt(), 0xb00327c8.toInt(), 0xbf597fc7.toInt(),
        0xc6e00bf3.toInt(), 0xd5a79147.toInt(), 0x06ca6351, 0x14292967,
        0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
        0x650a7354, 0x766a0abb, 0x81c2c92e.toInt(), 0x92722c85.toInt(),
        0xa2bfe8a1.toInt(), 0xa81a664b.toInt(), 0xc24b8b70.toInt(), 0xc76c51a3.toInt(),
        0xd192e819.toInt(), 0xd6990624.toInt(), 0xf40e3585.toInt(), 0x106aa070,
        0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5,
        0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
        0x748f82ee, 0x78a5636f, 0x84c87814.toInt(), 0x8cc70208.toInt(),
        0x90befffa.toInt(), 0xa4506ceb.toInt(), 0xbef9a3f7.toInt(), 0xc67178f2.toInt()
    )

    fun sha256(data: ByteArray): String {
        val msgLen = data.size.toLong()
        // 填充
        val padded = padMessage(data, msgLen)
        // 初始哈希值
        var h0 = 0x6a09e667
        var h1 = 0xbb67ae85.toInt()
        var h2 = 0x3c6ef372
        var h3 = 0xa54ff53a.toInt()
        var h4 = 0x510e527f
        var h5 = 0x9b05688c.toInt()
        var h6 = 0x1f83d9ab
        var h7 = 0x5be0cd19

        val w = IntArray(64)

        var offset = 0
        while (offset < padded.size) {
            // 准备消息调度
            for (i in 0 until 16) {
                w[i] = ((padded[offset + i * 4].toInt() and 0xFF) shl 24) or
                        ((padded[offset + i * 4 + 1].toInt() and 0xFF) shl 16) or
                        ((padded[offset + i * 4 + 2].toInt() and 0xFF) shl 8) or
                        (padded[offset + i * 4 + 3].toInt() and 0xFF)
            }
            for (i in 16 until 64) {
                val s0 = rightRotate(w[i - 15], 7) xor rightRotate(w[i - 15], 18) xor (w[i - 15] ushr 3)
                val s1 = rightRotate(w[i - 2], 17) xor rightRotate(w[i - 2], 19) xor (w[i - 2] ushr 10)
                w[i] = w[i - 16] + s0 + w[i - 7] + s1
            }

            var a = h0; var b = h1; var c = h2; var d = h3
            var e = h4; var f = h5; var g = h6; var h = h7

            for (i in 0 until 64) {
                val s1 = rightRotate(e, 6) xor rightRotate(e, 11) xor rightRotate(e, 25)
                val ch = (e and f) xor (e.inv() and g)
                val temp1 = h + s1 + ch + SHA256_K[i] + w[i]
                val s0 = rightRotate(a, 2) xor rightRotate(a, 13) xor rightRotate(a, 22)
                val maj = (a and b) xor (a and c) xor (b and c)
                val temp2 = s0 + maj

                h = g; g = f; f = e; e = d + temp1
                d = c; c = b; b = a; a = temp1 + temp2
            }

            h0 += a; h1 += b; h2 += c; h3 += d
            h4 += e; h5 += f; h6 += g; h7 += h
            offset += 64
        }

        return intArrayOf(h0, h1, h2, h3, h4, h5, h6, h7).joinToString("") {
            (it.toLong() and 0xFFFFFFFFL).toString(16).padStart(8, '0')
        }
    }

    private fun rightRotate(value: Int, bits: Int): Int =
        (value ushr bits) or (value shl (32 - bits))

    private fun padMessage(data: ByteArray, msgLen: Long): ByteArray {
        val bitLen = msgLen * 8
        // 需要的填充长度：至少 1 byte (0x80) + 足够 0 + 8 bytes 长度
        val mod = ((msgLen + 1) % 64).toInt()
        val padLen = if (mod <= 56) (56 - mod) else (120 - mod)
        val totalLen = msgLen.toInt() + 1 + padLen + 8
        val padded = ByteArray(totalLen)
        data.copyInto(padded)
        padded[msgLen.toInt()] = 0x80.toByte()
        // 大端序写入比特长度
        for (i in 0 until 8) {
            padded[totalLen - 8 + i] = (bitLen ushr (56 - i * 8)).toByte()
        }
        return padded
    }
}
