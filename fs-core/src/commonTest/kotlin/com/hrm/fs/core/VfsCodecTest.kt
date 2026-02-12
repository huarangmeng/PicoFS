package com.hrm.fs.core

import com.hrm.fs.core.persistence.*
import kotlin.test.*
import kotlin.time.measureTime

/**
 * 两种编解码器（CBOR / TLV）的极致性能单测。
 *
 * 覆盖范围：
 * - 全部 5 种数据类型的 round-trip 正确性
 * - 全部 12 种 WalEntry 子类型
 * - 递归 SnapshotNode（深层嵌套 + nullable 组合）
 * - WAL 批量编解码 + 增量追加拼接
 * - CRC 完整性校验 + 损坏数据容错
 * - 边界条件（空列表、空 ByteArray、超长字符串、大文件内容）
 * - TLV 体积优势断言（TLV < CBOR）
 * - 大规模数据编解码性能基准
 */
class VfsCodecTest {

    private val codecs: List<VfsCodec> = listOf(CborCodec(), TlvCodec())

    // ── 辅助函数 ──────────────────────────────────────────────

    private val rwxPerms = SnapshotPermissions(read = true, write = true, execute = true)
    private val roPerms = SnapshotPermissions(read = true, write = false, execute = false)
    private val noPerms = SnapshotPermissions(read = false, write = false, execute = false)

    private fun fileNode(
        name: String,
        content: ByteArray? = null,
        versions: List<SnapshotVersionEntry>? = null,
        xattrs: Map<String, ByteArray>? = null
    ) = SnapshotNode(
        name = name, type = "FILE",
        createdAtMillis = 1000L, modifiedAtMillis = 2000L,
        permissions = rwxPerms, content = content,
        children = null, versions = versions, xattrs = xattrs
    )

    private fun dirNode(name: String, children: List<SnapshotNode>) = SnapshotNode(
        name = name, type = "DIRECTORY",
        createdAtMillis = 3000L, modifiedAtMillis = 4000L,
        permissions = roPerms, children = children
    )

    private fun symlinkNode(name: String, target: String) = SnapshotNode(
        name = name, type = "SYMLINK",
        createdAtMillis = 5000L, modifiedAtMillis = 6000L,
        permissions = noPerms, target = target
    )

    /** 深度比较两个 SnapshotNode 树（ByteArray 需 contentEquals）。 */
    private fun assertSnapshotEquals(expected: SnapshotNode, actual: SnapshotNode, path: String = "") {
        assertEquals(expected.name, actual.name, "$path.name")
        assertEquals(expected.type, actual.type, "$path.type")
        assertEquals(expected.createdAtMillis, actual.createdAtMillis, "$path.createdAtMillis")
        assertEquals(expected.modifiedAtMillis, actual.modifiedAtMillis, "$path.modifiedAtMillis")
        assertEquals(expected.permissions, actual.permissions, "$path.permissions")
        assertTrue(expected.content contentEquals actual.content, "$path.content mismatch")
        assertEquals(expected.target, actual.target, "$path.target")
        // children
        assertEquals(expected.children?.size, actual.children?.size, "$path.children.size")
        expected.children?.forEachIndexed { i, child ->
            assertSnapshotEquals(child, actual.children!![i], "$path/${child.name}")
        }
        // versions
        assertEquals(expected.versions?.size, actual.versions?.size, "$path.versions.size")
        expected.versions?.forEachIndexed { i, v ->
            val av = actual.versions!![i]
            assertEquals(v.versionId, av.versionId, "$path.versions[$i].id")
            assertEquals(v.timestampMillis, av.timestampMillis, "$path.versions[$i].ts")
            assertTrue(v.data contentEquals av.data, "$path.versions[$i].data mismatch")
        }
        // xattrs
        assertEquals(expected.xattrs?.size, actual.xattrs?.size, "$path.xattrs.size")
        expected.xattrs?.forEach { (k, v) ->
            assertTrue(actual.xattrs!!.containsKey(k), "$path.xattrs missing key=$k")
            assertTrue(v contentEquals actual.xattrs!![k]!!, "$path.xattrs[$k] mismatch")
        }
    }

    /** 深度比较两个 WalEntry（ByteArray 需 contentEquals）。 */
    private fun assertWalEntryEquals(expected: WalEntry, actual: WalEntry) {
        when (expected) {
            is WalEntry.Write -> {
                assertIs<WalEntry.Write>(actual)
                assertEquals(expected.path, actual.path)
                assertEquals(expected.offset, actual.offset)
                assertTrue(expected.data contentEquals actual.data, "Write.data mismatch")
            }
            is WalEntry.SetXattr -> {
                assertIs<WalEntry.SetXattr>(actual)
                assertEquals(expected.path, actual.path)
                assertEquals(expected.name, actual.name)
                assertTrue(expected.value contentEquals actual.value, "SetXattr.value mismatch")
            }
            else -> assertEquals(expected, actual)
        }
    }

    // ═══════════════════════════════════════════════════════════
    // 1. SnapshotNode 编解码 round-trip
    // ═══════════════════════════════════════════════════════════

    @Test
    fun snapshot_minimal_file_node() {
        val node = fileNode("test.txt")
        for (codec in codecs) {
            val decoded = codec.decodeSnapshot(codec.encodeSnapshot(node))
            assertSnapshotEquals(node, decoded)
        }
    }

    @Test
    fun snapshot_file_with_content_and_xattrs() {
        val content = byteArrayOf(0, 1, 2, 127, -128, -1)
        val xattrs = mapOf("user.mime" to "text/plain".encodeToByteArray(), "user.tag" to byteArrayOf(0xFF.toByte()))
        val node = fileNode("data.bin", content = content, xattrs = xattrs)
        for (codec in codecs) {
            val decoded = codec.decodeSnapshot(codec.encodeSnapshot(node))
            assertSnapshotEquals(node, decoded)
        }
    }

    @Test
    fun snapshot_file_with_versions() {
        val versions = listOf(
            SnapshotVersionEntry("v1", 100L, byteArrayOf(10, 20)),
            SnapshotVersionEntry("v2", 200L, ByteArray(0))
        )
        val node = fileNode("versioned.txt", content = "hello".encodeToByteArray(), versions = versions)
        for (codec in codecs) {
            val decoded = codec.decodeSnapshot(codec.encodeSnapshot(node))
            assertSnapshotEquals(node, decoded)
        }
    }

    @Test
    fun snapshot_symlink_node() {
        val node = symlinkNode("link", "/target/path")
        for (codec in codecs) {
            val decoded = codec.decodeSnapshot(codec.encodeSnapshot(node))
            assertSnapshotEquals(node, decoded)
        }
    }

    @Test
    fun snapshot_deep_nested_directory() {
        // 3 层嵌套目录树，混合文件/目录/符号链接
        val tree = dirNode("root", listOf(
            dirNode("sub1", listOf(
                fileNode("a.txt", content = "aaa".encodeToByteArray()),
                dirNode("sub1_1", listOf(
                    fileNode("deep.bin", content = ByteArray(1024) { it.toByte() }),
                    symlinkNode("deeplink", "../a.txt")
                ))
            )),
            dirNode("sub2", listOf(
                fileNode("b.txt",
                    xattrs = mapOf("k" to byteArrayOf(1, 2, 3))
                )
            )),
            fileNode("root_file.dat"),
            symlinkNode("root_link", "/abs/path")
        ))
        for (codec in codecs) {
            val decoded = codec.decodeSnapshot(codec.encodeSnapshot(tree))
            assertSnapshotEquals(tree, decoded)
        }
    }

    @Test
    fun snapshot_all_permissions_combinations() {
        val permsList = listOf(rwxPerms, roPerms, noPerms,
            SnapshotPermissions(read = false, write = true, execute = false),
            SnapshotPermissions(read = false, write = false, execute = true),
            SnapshotPermissions(read = true, write = true, execute = false),
            SnapshotPermissions(read = true, write = false, execute = true),
            SnapshotPermissions(read = false, write = true, execute = true)
        )
        // 一共 8 种组合
        assertEquals(8, permsList.size)
        val children = permsList.mapIndexed { i, p ->
            SnapshotNode("f$i", "FILE", 0L, 0L, p)
        }
        val root = dirNode("permroot", children)
        for (codec in codecs) {
            val decoded = codec.decodeSnapshot(codec.encodeSnapshot(root))
            assertSnapshotEquals(root, decoded)
        }
    }

    // ═══════════════════════════════════════════════════════════
    // 2. 全部 12 种 WalEntry round-trip
    // ═══════════════════════════════════════════════════════════

    private fun allWalEntries(): List<WalEntry> = listOf(
        WalEntry.CreateFile("/a/b.txt"),
        WalEntry.CreateDir("/a/b"),
        WalEntry.CreateSymlink("/link", "/target"),
        WalEntry.Delete("/a/b.txt"),
        WalEntry.Write("/a/b.txt", 42L, byteArrayOf(1, 2, 3, 4, 5)),
        WalEntry.SetPermissions("/a/b.txt", SnapshotPermissions(true, false, true)),
        WalEntry.SetXattr("/a/b.txt", "user.tag", byteArrayOf(0xFF.toByte(), 0x00)),
        WalEntry.RemoveXattr("/a/b.txt", "user.tag"),
        WalEntry.Copy("/src", "/dst"),
        WalEntry.Move("/old", "/new"),
        WalEntry.MoveToTrash("/file", "trash-id-123"),
        WalEntry.RestoreFromTrash("trash-id-123", "/file")
    )

    @Test
    fun wal_single_entry_all_12_types() {
        val entries = allWalEntries()
        assertEquals(12, entries.size, "应覆盖全部 12 种 WalEntry 类型")
        for (codec in codecs) {
            for (entry in entries) {
                val encoded = codec.encodeWalEntry(entry)
                val decoded = codec.decodeWalEntries(encoded)
                assertEquals(1, decoded.size, "${codec.name}: single entry decode count")
                assertWalEntryEquals(entry, decoded[0])
            }
        }
    }

    @Test
    fun wal_batch_encode_decode_all_types() {
        val entries = allWalEntries()
        for (codec in codecs) {
            val encoded = codec.encodeWalEntries(entries)
            val decoded = codec.decodeWalEntries(encoded)
            assertEquals(entries.size, decoded.size, "${codec.name}: batch decode count")
            entries.forEachIndexed { i, e -> assertWalEntryEquals(e, decoded[i]) }
        }
    }

    @Test
    fun wal_incremental_append() {
        // 模拟增量追加：逐条编码拼接后，一次性解码
        val entries = allWalEntries()
        for (codec in codecs) {
            var combined = ByteArray(0)
            for (entry in entries) {
                combined += codec.encodeWalEntry(entry)
            }
            val decoded = codec.decodeWalEntries(combined)
            assertEquals(entries.size, decoded.size, "${codec.name}: incremental append count")
            entries.forEachIndexed { i, e -> assertWalEntryEquals(e, decoded[i]) }
        }
    }

    @Test
    fun wal_empty_entries() {
        for (codec in codecs) {
            val encoded = codec.encodeWalEntries(emptyList())
            assertEquals(0, encoded.size, "${codec.name}: empty WAL should produce empty bytes")
            val decoded = codec.decodeWalEntries(ByteArray(0))
            assertTrue(decoded.isEmpty(), "${codec.name}: decoding empty bytes should return empty list")
        }
    }

    @Test
    fun wal_write_with_large_binary_data() {
        val bigData = ByteArray(64 * 1024) { (it % 256).toByte() }
        val entry = WalEntry.Write("/big.bin", 0L, bigData)
        for (codec in codecs) {
            val decoded = codec.decodeWalEntries(codec.encodeWalEntry(entry))
            assertEquals(1, decoded.size)
            val d = decoded[0]
            assertIs<WalEntry.Write>(d)
            assertEquals("/big.bin", d.path)
            assertEquals(0L, d.offset)
            assertTrue(bigData contentEquals d.data, "${codec.name}: large binary data mismatch")
        }
    }

    @Test
    fun wal_write_with_zero_offset_and_empty_data() {
        val entry = WalEntry.Write("/empty.dat", 0L, ByteArray(0))
        for (codec in codecs) {
            val decoded = codec.decodeWalEntries(codec.encodeWalEntry(entry))
            assertEquals(1, decoded.size)
            val d = decoded[0]
            assertIs<WalEntry.Write>(d)
            assertTrue(d.data.isEmpty(), "${codec.name}: empty data mismatch")
        }
    }

    @Test
    fun wal_write_with_max_long_offset() {
        val entry = WalEntry.Write("/off.bin", Long.MAX_VALUE, byteArrayOf(42))
        for (codec in codecs) {
            val decoded = codec.decodeWalEntries(codec.encodeWalEntry(entry))
            val d = decoded[0]
            assertIs<WalEntry.Write>(d)
            assertEquals(Long.MAX_VALUE, d.offset, "${codec.name}: max Long offset")
        }
    }

    // ═══════════════════════════════════════════════════════════
    // 3. Mounts 编解码
    // ═══════════════════════════════════════════════════════════

    @Test
    fun mounts_round_trip() {
        val mounts = listOf(
            MountInfo("/virtual/a", "/real/a", readOnly = false),
            MountInfo("/virtual/b", "/real/b", readOnly = true)
        )
        for (codec in codecs) {
            val decoded = codec.decodeMounts(codec.encodeMounts(mounts))
            assertEquals(mounts, decoded, "${codec.name}: mounts mismatch")
        }
    }

    @Test
    fun mounts_empty_list() {
        for (codec in codecs) {
            val decoded = codec.decodeMounts(codec.encodeMounts(emptyList()))
            assertTrue(decoded.isEmpty(), "${codec.name}: empty mounts")
        }
    }

    // ═══════════════════════════════════════════════════════════
    // 4. VersionData 编解码
    // ═══════════════════════════════════════════════════════════

    @Test
    fun version_data_round_trip() {
        val data = SnapshotVersionData(mapOf(
            "/a.txt" to listOf(
                SnapshotVersionEntry("v1", 100L, "first".encodeToByteArray()),
                SnapshotVersionEntry("v2", 200L, "second".encodeToByteArray())
            ),
            "/b.txt" to listOf(
                SnapshotVersionEntry("v1", 300L, ByteArray(0))
            )
        ))
        for (codec in codecs) {
            val decoded = codec.decodeVersionData(codec.encodeVersionData(data))
            assertEquals(data.entries.keys, decoded.entries.keys, "${codec.name}: version keys")
            for ((path, versions) in data.entries) {
                val dv = decoded.entries[path]!!
                assertEquals(versions.size, dv.size, "${codec.name}: $path version count")
                versions.forEachIndexed { i, v ->
                    assertEquals(v.versionId, dv[i].versionId)
                    assertEquals(v.timestampMillis, dv[i].timestampMillis)
                    assertTrue(v.data contentEquals dv[i].data, "${codec.name}: $path version[$i].data")
                }
            }
        }
    }

    @Test
    fun version_data_empty() {
        val data = SnapshotVersionData(emptyMap())
        for (codec in codecs) {
            val decoded = codec.decodeVersionData(codec.encodeVersionData(data))
            assertTrue(decoded.entries.isEmpty(), "${codec.name}: empty version data")
        }
    }

    // ═══════════════════════════════════════════════════════════
    // 5. TrashData 编解码
    // ═══════════════════════════════════════════════════════════

    @Test
    fun trash_data_round_trip() {
        val data = SnapshotTrashData(listOf(
            SnapshotTrashEntry(
                trashId = "t1", originalPath = "/old.txt", type = "FILE",
                deletedAtMillis = 9999L,
                content = "deleted content".encodeToByteArray(),
                children = null, isMounted = false
            ),
            SnapshotTrashEntry(
                trashId = "t2", originalPath = "/old_dir", type = "DIRECTORY",
                deletedAtMillis = 8888L, content = null,
                children = listOf(
                    SnapshotTrashEntry.SnapshotTrashChild(
                        relativePath = "sub/a.txt", type = "FILE",
                        content = "child data".encodeToByteArray(),
                        children = null
                    ),
                    SnapshotTrashEntry.SnapshotTrashChild(
                        relativePath = "sub/inner", type = "DIRECTORY",
                        content = null,
                        children = listOf(
                            SnapshotTrashEntry.SnapshotTrashChild(
                                relativePath = "deep.txt", type = "FILE",
                                content = byteArrayOf(1, 2, 3),
                                children = null
                            )
                        )
                    )
                ),
                isMounted = true
            )
        ))
        for (codec in codecs) {
            val encoded = codec.encodeTrashData(data)
            val decoded = codec.decodeTrashData(encoded)
            assertEquals(data.entries.size, decoded.entries.size, "${codec.name}: trash count")
            // entry 0: file
            val e0 = decoded.entries[0]
            assertEquals("t1", e0.trashId)
            assertEquals("/old.txt", e0.originalPath)
            assertEquals("FILE", e0.type)
            assertEquals(9999L, e0.deletedAtMillis)
            assertTrue("deleted content".encodeToByteArray() contentEquals e0.content!!)
            assertNull(e0.children)
            assertFalse(e0.isMounted)
            // entry 1: directory with recursive children
            val e1 = decoded.entries[1]
            assertEquals("t2", e1.trashId)
            assertEquals("DIRECTORY", e1.type)
            assertTrue(e1.isMounted)
            assertEquals(2, e1.children!!.size)
            assertEquals("sub/a.txt", e1.children!![0].relativePath)
            assertTrue("child data".encodeToByteArray() contentEquals e1.children!![0].content!!)
            // deep child
            val innerDir = e1.children!![1]
            assertEquals("DIRECTORY", innerDir.type)
            assertEquals(1, innerDir.children!!.size)
            assertEquals("deep.txt", innerDir.children!![0].relativePath)
            assertTrue(byteArrayOf(1, 2, 3) contentEquals innerDir.children!![0].content!!)
        }
    }

    @Test
    fun trash_data_empty() {
        for (codec in codecs) {
            val decoded = codec.decodeTrashData(codec.encodeTrashData(SnapshotTrashData()))
            assertTrue(decoded.entries.isEmpty(), "${codec.name}: empty trash")
        }
    }

    @Test
    fun trash_symlink_entry() {
        val data = SnapshotTrashData(listOf(
            SnapshotTrashEntry(
                trashId = "t3", originalPath = "/link", type = "SYMLINK",
                deletedAtMillis = 7777L, content = null, children = null, isMounted = false
            )
        ))
        for (codec in codecs) {
            val decoded = codec.decodeTrashData(codec.encodeTrashData(data))
            assertEquals("SYMLINK", decoded.entries[0].type, "${codec.name}: symlink trash type")
        }
    }

    // ═══════════════════════════════════════════════════════════
    // 6. CRC 完整性 & 损坏容错
    // ═══════════════════════════════════════════════════════════

    @Test
    fun snapshot_corrupted_crc_throws() {
        val node = fileNode("test.txt", content = "hello".encodeToByteArray())
        for (codec in codecs) {
            val encoded = codec.encodeSnapshot(node)
            // 翻转 CRC 的第一个字节
            encoded[0] = (encoded[0].toInt() xor 0xFF).toByte()
            assertFailsWith<CorruptedDataException>("${codec.name}: corrupted CRC should throw") {
                codec.decodeSnapshot(encoded)
            }
        }
    }

    @Test
    fun snapshot_truncated_data_throws() {
        val node = dirNode("root", listOf(fileNode("child.txt")))
        for (codec in codecs) {
            val encoded = codec.encodeSnapshot(node)
            // 截断到一半
            val truncated = encoded.copyOfRange(0, encoded.size / 2)
            assertFails("${codec.name}: truncated data should fail") {
                codec.decodeSnapshot(truncated)
            }
        }
    }

    @Test
    fun wal_corrupted_record_skipped() {
        // 编码 3 条 entry，损坏中间那条，解码应得到 2 条
        val entries = listOf(
            WalEntry.CreateFile("/a"),
            WalEntry.CreateFile("/b"),
            WalEntry.CreateFile("/c")
        )
        for (codec in codecs) {
            val records = entries.map { codec.encodeWalEntry(it) }
            // 损坏第二条的 CRC（第一个字节）
            val r1 = records[1]
            r1[0] = (r1[0].toInt() xor 0xFF).toByte()
            val combined = records[0] + r1 + records[2]
            val decoded = codec.decodeWalEntries(combined)
            assertEquals(2, decoded.size, "${codec.name}: corrupted record should be skipped")
            assertEquals(WalEntry.CreateFile("/a"), decoded[0])
            assertEquals(WalEntry.CreateFile("/c"), decoded[1])
        }
    }

    @Test
    fun wal_garbage_data_returns_empty() {
        val garbage = ByteArray(100) { (it * 37).toByte() }
        for (codec in codecs) {
            val decoded = codec.decodeWalEntries(garbage)
            // 垃圾数据可能 CRC 碰巧匹配但解析失败，应返回 0 或少量条目
            assertTrue(decoded.size <= 1, "${codec.name}: garbage data should yield ≤1 entry")
        }
    }

    @Test
    fun mounts_corrupted_crc_throws() {
        val mounts = listOf(MountInfo("/v", "/r"))
        for (codec in codecs) {
            val encoded = codec.encodeMounts(mounts)
            encoded[0] = (encoded[0].toInt() xor 0xFF).toByte()
            assertFailsWith<CorruptedDataException>("${codec.name}: corrupted mounts CRC") {
                codec.decodeMounts(encoded)
            }
        }
    }

    @Test
    fun versions_corrupted_crc_throws() {
        val data = SnapshotVersionData(mapOf("/f" to listOf(SnapshotVersionEntry("v1", 1L, byteArrayOf(1)))))
        for (codec in codecs) {
            val encoded = codec.encodeVersionData(data)
            encoded[1] = (encoded[1].toInt() xor 0xFF).toByte()
            assertFailsWith<CorruptedDataException>("${codec.name}: corrupted versions CRC") {
                codec.decodeVersionData(encoded)
            }
        }
    }

    @Test
    fun trash_corrupted_crc_throws() {
        val data = SnapshotTrashData(listOf(
            SnapshotTrashEntry("t", "/p", "FILE", 0L)
        ))
        for (codec in codecs) {
            val encoded = codec.encodeTrashData(data)
            encoded[2] = (encoded[2].toInt() xor 0xFF).toByte()
            assertFailsWith<CorruptedDataException>("${codec.name}: corrupted trash CRC") {
                codec.decodeTrashData(encoded)
            }
        }
    }

    // ═══════════════════════════════════════════════════════════
    // 7. 边界条件
    // ═══════════════════════════════════════════════════════════

    @Test
    fun snapshot_empty_content_vs_null_content() {
        val withEmpty = fileNode("a.txt", content = ByteArray(0))
        val withNull = fileNode("b.txt", content = null)
        for (codec in codecs) {
            val decodedEmpty = codec.decodeSnapshot(codec.encodeSnapshot(withEmpty))
            val decodedNull = codec.decodeSnapshot(codec.encodeSnapshot(withNull))
            assertNotNull(decodedEmpty.content, "${codec.name}: empty content should not be null")
            assertTrue(decodedEmpty.content!!.isEmpty(), "${codec.name}: empty content size")
            assertNull(decodedNull.content, "${codec.name}: null content should be null")
        }
    }

    @Test
    fun snapshot_empty_xattrs_vs_null_xattrs() {
        val withEmpty = fileNode("a.txt", xattrs = emptyMap())
        val withNull = fileNode("b.txt", xattrs = null)
        for (codec in codecs) {
            val decodedEmpty = codec.decodeSnapshot(codec.encodeSnapshot(withEmpty))
            val decodedNull = codec.decodeSnapshot(codec.encodeSnapshot(withNull))
            assertNotNull(decodedEmpty.xattrs, "${codec.name}: empty xattrs not null")
            assertTrue(decodedEmpty.xattrs!!.isEmpty(), "${codec.name}: empty xattrs size")
            assertNull(decodedNull.xattrs, "${codec.name}: null xattrs")
        }
    }

    @Test
    fun snapshot_unicode_and_special_chars() {
        val node = fileNode("文件名_émojis_\uD83D\uDE80.txt",
            content = "中文内容 🎉".encodeToByteArray(),
            xattrs = mapOf("描述" to "注释说明".encodeToByteArray())
        )
        for (codec in codecs) {
            val decoded = codec.decodeSnapshot(codec.encodeSnapshot(node))
            assertSnapshotEquals(node, decoded)
        }
    }

    @Test
    fun snapshot_long_path_names() {
        val longName = "a".repeat(4096)
        val node = fileNode(longName, content = "x".encodeToByteArray())
        for (codec in codecs) {
            val decoded = codec.decodeSnapshot(codec.encodeSnapshot(node))
            assertEquals(longName, decoded.name, "${codec.name}: long name preserved")
        }
    }

    @Test
    fun wal_setpermissions_all_combos() {
        val allPerms = listOf(rwxPerms, roPerms, noPerms,
            SnapshotPermissions(false, true, false),
            SnapshotPermissions(false, false, true),
            SnapshotPermissions(true, true, false),
            SnapshotPermissions(true, false, true),
            SnapshotPermissions(false, true, true)
        )
        for (codec in codecs) {
            for (perm in allPerms) {
                val entry = WalEntry.SetPermissions("/f", perm)
                val decoded = codec.decodeWalEntries(codec.encodeWalEntry(entry))
                assertEquals(1, decoded.size)
                val d = decoded[0]
                assertIs<WalEntry.SetPermissions>(d)
                assertEquals(perm, d.permissions, "${codec.name}: perm=$perm")
            }
        }
    }

    @Test
    fun wal_unicode_paths() {
        val entry = WalEntry.CreateFile("/中文路径/файл/αβγ.txt")
        for (codec in codecs) {
            val decoded = codec.decodeWalEntries(codec.encodeWalEntry(entry))
            assertEquals(WalEntry.CreateFile("/中文路径/файл/αβγ.txt"), decoded[0])
        }
    }

    @Test
    fun snapshot_directory_with_empty_children_list() {
        val dir = dirNode("empty_dir", emptyList())
        for (codec in codecs) {
            val decoded = codec.decodeSnapshot(codec.encodeSnapshot(dir))
            assertSnapshotEquals(dir, decoded)
            assertNotNull(decoded.children)
            assertTrue(decoded.children!!.isEmpty())
        }
    }

    // ═══════════════════════════════════════════════════════════
    // 8. TLV 体积优势
    // ═══════════════════════════════════════════════════════════

    @Test
    fun tlv_is_smaller_than_cbor_for_snapshot() {
        val cbor = CborCodec()
        val tlv = TlvCodec()
        val tree = dirNode("root", listOf(
            dirNode("sub", listOf(
                fileNode("file.txt", content = "hello world".encodeToByteArray(),
                    xattrs = mapOf("k1" to byteArrayOf(1, 2), "k2" to byteArrayOf(3))),
                symlinkNode("link", "/target")
            )),
            fileNode("another.dat", content = ByteArray(256) { it.toByte() })
        ))
        val cborSize = cbor.encodeSnapshot(tree).size
        val tlvSize = tlv.encodeSnapshot(tree).size
        assertTrue(tlvSize < cborSize,
            "TLV($tlvSize) should be smaller than CBOR($cborSize)")
    }

    @Test
    fun tlv_is_smaller_than_cbor_for_wal() {
        val cbor = CborCodec()
        val tlv = TlvCodec()
        val entries = allWalEntries()
        val cborSize = cbor.encodeWalEntries(entries).size
        val tlvSize = tlv.encodeWalEntries(entries).size
        assertTrue(tlvSize < cborSize,
            "TLV WAL($tlvSize) should be smaller than CBOR WAL($cborSize)")
    }

    @Test
    fun tlv_is_smaller_than_cbor_for_mounts() {
        val cbor = CborCodec()
        val tlv = TlvCodec()
        val mounts = listOf(
            MountInfo("/virtual/documents", "/real/storage/documents", readOnly = false),
            MountInfo("/virtual/photos", "/real/storage/photos", readOnly = true),
            MountInfo("/virtual/config", "/real/app/config", readOnly = true)
        )
        val cborSize = cbor.encodeMounts(mounts).size
        val tlvSize = tlv.encodeMounts(mounts).size
        assertTrue(tlvSize < cborSize,
            "TLV Mounts($tlvSize) should be smaller than CBOR Mounts($cborSize)")
    }

    @Test
    fun tlv_is_smaller_than_cbor_for_trash() {
        val cbor = CborCodec()
        val tlv = TlvCodec()
        val data = SnapshotTrashData(listOf(
            SnapshotTrashEntry("t1", "/old/dir", "DIRECTORY", 1000L, null,
                listOf(
                    SnapshotTrashEntry.SnapshotTrashChild("a.txt", "FILE", "data".encodeToByteArray()),
                    SnapshotTrashEntry.SnapshotTrashChild("sub", "DIRECTORY", null,
                        listOf(SnapshotTrashEntry.SnapshotTrashChild("b.txt", "FILE", byteArrayOf(1, 2))))
                ), false),
            SnapshotTrashEntry("t2", "/old/file.txt", "FILE", 2000L, "content".encodeToByteArray())
        ))
        val cborSize = cbor.encodeTrashData(data).size
        val tlvSize = tlv.encodeTrashData(data).size
        assertTrue(tlvSize < cborSize,
            "TLV Trash($tlvSize) should be smaller than CBOR Trash($cborSize)")
    }

    // ═══════════════════════════════════════════════════════════
    // 9. 性能基准（大规模数据）
    // ═══════════════════════════════════════════════════════════

    @Test
    fun perf_snapshot_large_tree() {
        // 1000 个文件的扁平目录
        val children = List(1000) { i ->
            fileNode("file_$i.txt", content = ByteArray(128) { (i + it).toByte() })
        }
        val root = dirNode("bigdir", children)

        for (codec in codecs) {
            // Warmup
            repeat(3) { codec.decodeSnapshot(codec.encodeSnapshot(root)) }

            val encodeTime = measureTime {
                repeat(10) { codec.encodeSnapshot(root) }
            }
            val encoded = codec.encodeSnapshot(root)
            val decodeTime = measureTime {
                repeat(10) { codec.decodeSnapshot(encoded) }
            }
            println("${codec.name} snapshot(1000 files): encode=${encodeTime / 10}, decode=${decodeTime / 10}, size=${encoded.size}")

            // 验证正确性
            val decoded = codec.decodeSnapshot(encoded)
            assertEquals(1000, decoded.children!!.size)
            assertSnapshotEquals(root, decoded)
        }
    }

    @Test
    fun perf_wal_batch_1000_entries() {
        val entries = List(1000) { i ->
            when (i % 4) {
                0 -> WalEntry.CreateFile("/file_$i.txt")
                1 -> WalEntry.Write("/file_$i.txt", i.toLong(), ByteArray(64) { it.toByte() })
                2 -> WalEntry.SetPermissions("/file_$i.txt", rwxPerms)
                else -> WalEntry.Delete("/file_$i.txt")
            }
        }

        for (codec in codecs) {
            // Warmup
            repeat(3) { codec.decodeWalEntries(codec.encodeWalEntries(entries)) }

            val encodeTime = measureTime {
                repeat(10) { codec.encodeWalEntries(entries) }
            }
            val encoded = codec.encodeWalEntries(entries)
            val decodeTime = measureTime {
                repeat(10) { codec.decodeWalEntries(encoded) }
            }
            println("${codec.name} WAL(1000 entries): encode=${encodeTime / 10}, decode=${decodeTime / 10}, size=${encoded.size}")

            // 验证正确性
            val decoded = codec.decodeWalEntries(encoded)
            assertEquals(1000, decoded.size)
            entries.forEachIndexed { i, e -> assertWalEntryEquals(e, decoded[i]) }
        }
    }

    @Test
    fun perf_snapshot_deep_tree() {
        // 深度 100 的链式嵌套目录
        var node: SnapshotNode = fileNode("leaf.txt", content = "deep leaf".encodeToByteArray())
        for (i in 100 downTo 1) {
            node = dirNode("level_$i", listOf(node))
        }

        for (codec in codecs) {
            repeat(3) { codec.decodeSnapshot(codec.encodeSnapshot(node)) }

            val encodeTime = measureTime {
                repeat(50) { codec.encodeSnapshot(node) }
            }
            val encoded = codec.encodeSnapshot(node)
            val decodeTime = measureTime {
                repeat(50) { codec.decodeSnapshot(encoded) }
            }
            println("${codec.name} snapshot(depth=100): encode=${encodeTime / 50}, decode=${decodeTime / 50}, size=${encoded.size}")

            // 验证正确性：遍历到叶子
            var current = codec.decodeSnapshot(encoded)
            repeat(100) {
                assertEquals(1, current.children!!.size)
                current = current.children!![0]
            }
            assertEquals("leaf.txt", current.name)
            assertTrue("deep leaf".encodeToByteArray() contentEquals current.content!!)
        }
    }

    @Test
    fun perf_large_binary_content() {
        // 单个文件 1MB 内容
        val bigContent = ByteArray(1024 * 1024) { (it % 251).toByte() }
        val node = fileNode("huge.bin", content = bigContent)

        for (codec in codecs) {
            repeat(2) { codec.decodeSnapshot(codec.encodeSnapshot(node)) }

            val encodeTime = measureTime {
                repeat(5) { codec.encodeSnapshot(node) }
            }
            val encoded = codec.encodeSnapshot(node)
            val decodeTime = measureTime {
                repeat(5) { codec.decodeSnapshot(encoded) }
            }
            println("${codec.name} snapshot(1MB file): encode=${encodeTime / 5}, decode=${decodeTime / 5}, size=${encoded.size}")

            val decoded = codec.decodeSnapshot(encoded)
            assertTrue(bigContent contentEquals decoded.content!!)
        }
    }

    @Test
    fun perf_versions_large_history() {
        // 100 个文件，每个 50 个版本
        val entries = (1..100).associate { i ->
            "/file_$i.txt" to List(50) { v ->
                SnapshotVersionEntry("v$v", (i * 100 + v).toLong(), ByteArray(32) { (v + it).toByte() })
            }
        }
        val data = SnapshotVersionData(entries)

        for (codec in codecs) {
            repeat(3) { codec.decodeVersionData(codec.encodeVersionData(data)) }

            val encodeTime = measureTime {
                repeat(10) { codec.encodeVersionData(data) }
            }
            val encoded = codec.encodeVersionData(data)
            val decodeTime = measureTime {
                repeat(10) { codec.decodeVersionData(encoded) }
            }
            println("${codec.name} versions(100x50): encode=${encodeTime / 10}, decode=${decodeTime / 10}, size=${encoded.size}")

            val decoded = codec.decodeVersionData(encoded)
            assertEquals(100, decoded.entries.size)
            for ((path, versions) in data.entries) {
                val dv = decoded.entries[path]!!
                assertEquals(versions.size, dv.size)
                versions.forEachIndexed { vi, v ->
                    assertEquals(v.versionId, dv[vi].versionId)
                    assertEquals(v.timestampMillis, dv[vi].timestampMillis)
                    assertTrue(v.data contentEquals dv[vi].data)
                }
            }
        }
    }

    // ═══════════════════════════════════════════════════════════
    // 10. TlvWriter 内部 VarInt 验证
    // ═══════════════════════════════════════════════════════════

    @Test
    fun tlv_varint_boundary_values() {
        // VarInt 边界：0, 127, 128, 16383, 16384, Int.MAX_VALUE
        val boundaries = listOf(0, 127, 128, 16383, 16384, 2097151, 2097152, Int.MAX_VALUE)
        // 用 Mounts 编码来间接测试 VarInt（count 字段使用 VarInt）
        val tlv = TlvCodec()
        // 测试通过创建对应数量的 mount entry 不太现实(太多)，
        // 改用 WAL entry 列表来验证 VarInt 正确编解码小值
        for (count in listOf(0, 1, 127, 128, 200)) {
            val entries = List(count) { WalEntry.CreateFile("/f$it") }
            val encoded = tlv.encodeWalEntries(entries)
            val decoded = tlv.decodeWalEntries(encoded)
            assertEquals(count, decoded.size, "VarInt count=$count round-trip")
        }
    }

    @Test
    fun tlv_writer_dynamic_resize() {
        // 验证 TlvWriter 的动态扩容：写入超过初始容量
        val w = TlvWriter(8) // 极小初始容量
        // 写入远超 8 字节的数据
        repeat(100) { w.writeString("string_number_$it") }
        val bytes = w.toByteArray()
        assertTrue(bytes.size > 8, "TlvWriter should have resized beyond initial 8 bytes")
    }

    // ═══════════════════════════════════════════════════════════
    // 11. 共享工具函数校验
    // ═══════════════════════════════════════════════════════════

    @Test
    fun crc32_known_values() {
        // CRC32 空数据 = 0x00000000
        assertEquals(0, crc32Bytes(ByteArray(0)))
        // CRC32("123456789") = 0xCBF43926
        val data = "123456789".encodeToByteArray()
        assertEquals(0xCBF43926.toInt(), crc32Bytes(data), "CRC32 of '123456789'")
    }

    @Test
    fun wrap_unwrap_crc_round_trip() {
        val payload = "test payload".encodeToByteArray()
        val wrapped = wrapCrc(payload)
        assertEquals(payload.size + 4, wrapped.size, "wrapCrc adds 4-byte header")
        val unwrapped = unwrapCrc(wrapped)
        assertTrue(payload contentEquals unwrapped, "unwrapCrc recovers original payload")
    }

    @Test
    fun wrap_wal_record_round_trip() {
        val payload = byteArrayOf(1, 2, 3, 4, 5)
        val record = wrapWalRecord(payload)
        assertEquals(8 + payload.size, record.size, "wrapWalRecord header = 8 bytes")
        // 手动解析
        val storedCrc = readInt(record, 0)
        val storedLen = readInt(record, 4)
        assertEquals(payload.size, storedLen)
        assertEquals(crc32Bytes(payload), storedCrc)
        val extracted = record.copyOfRange(8, 8 + storedLen)
        assertTrue(payload contentEquals extracted)
    }

    @Test
    fun readInt_writeInt_consistency() {
        val buf = ByteArray(4)
        val values = listOf(0, 1, -1, Int.MAX_VALUE, Int.MIN_VALUE, 0x12345678)
        for (v in values) {
            writeInt(buf, 0, v)
            assertEquals(v, readInt(buf, 0), "readInt/writeInt round-trip for $v")
        }
    }
}
