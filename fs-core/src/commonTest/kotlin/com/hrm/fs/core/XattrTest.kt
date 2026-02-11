package com.hrm.fs.core

import com.hrm.fs.api.*
import kotlinx.coroutines.test.runTest
import kotlin.test.*

class XattrTest {

    // ── 基本 set / get / list / remove ──────────────────────────

    @Test
    fun setAndGetXattr_on_file() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "hello".encodeToByteArray()).getOrThrow()

        fs.setXattr("/f.txt", "user.tag", "important".encodeToByteArray()).getOrThrow()
        val value = fs.getXattr("/f.txt", "user.tag").getOrThrow()
        assertEquals("important", value.decodeToString())
    }

    @Test
    fun setAndGetXattr_on_directory() = runTest {
        val fs = createFs()
        fs.createDir("/dir").getOrThrow()

        fs.setXattr("/dir", "color", "blue".encodeToByteArray()).getOrThrow()
        val value = fs.getXattr("/dir", "color").getOrThrow()
        assertEquals("blue", value.decodeToString())
    }

    @Test
    fun setAndGetXattr_on_symlink_follows_target() = runTest {
        val fs = createFs()
        fs.writeAll("/target.txt", "data".encodeToByteArray()).getOrThrow()
        fs.createSymlink("/link", "/target.txt").getOrThrow()

        // set via symlink → actually set on target
        fs.setXattr("/link", "tag", "v1".encodeToByteArray()).getOrThrow()
        val value = fs.getXattr("/target.txt", "tag").getOrThrow()
        assertEquals("v1", value.decodeToString())
    }

    @Test
    fun listXattrs_returns_all_keys() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "x".encodeToByteArray()).getOrThrow()

        fs.setXattr("/f.txt", "a", "1".encodeToByteArray()).getOrThrow()
        fs.setXattr("/f.txt", "b", "2".encodeToByteArray()).getOrThrow()
        fs.setXattr("/f.txt", "c", "3".encodeToByteArray()).getOrThrow()

        val keys = fs.listXattrs("/f.txt").getOrThrow()
        assertEquals(listOf("a", "b", "c"), keys)
    }

    @Test
    fun listXattrs_empty_when_no_attrs() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "x".encodeToByteArray()).getOrThrow()

        val keys = fs.listXattrs("/f.txt").getOrThrow()
        assertTrue(keys.isEmpty())
    }

    @Test
    fun removeXattr_succeeds() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "x".encodeToByteArray()).getOrThrow()
        fs.setXattr("/f.txt", "tag", "v".encodeToByteArray()).getOrThrow()

        fs.removeXattr("/f.txt", "tag").getOrThrow()
        val keys = fs.listXattrs("/f.txt").getOrThrow()
        assertTrue(keys.isEmpty())
    }

    @Test
    fun removeXattr_nonexistent_fails_notFound() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "x".encodeToByteArray()).getOrThrow()

        val result = fs.removeXattr("/f.txt", "no-such-key")
        assertTrue(result.isFailure)
        assertIs<FsError.NotFound>(result.exceptionOrNull())
    }

    @Test
    fun getXattr_nonexistent_fails_notFound() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "x".encodeToByteArray()).getOrThrow()

        val result = fs.getXattr("/f.txt", "no-such-key")
        assertTrue(result.isFailure)
        assertIs<FsError.NotFound>(result.exceptionOrNull())
    }

    // ── 覆盖 / 二进制值 ─────────────────────────────────────────

    @Test
    fun setXattr_overwrite_value() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "x".encodeToByteArray()).getOrThrow()

        fs.setXattr("/f.txt", "key", "old".encodeToByteArray()).getOrThrow()
        fs.setXattr("/f.txt", "key", "new".encodeToByteArray()).getOrThrow()

        val value = fs.getXattr("/f.txt", "key").getOrThrow()
        assertEquals("new", value.decodeToString())
    }

    @Test
    fun xattr_binary_value() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "x".encodeToByteArray()).getOrThrow()
        val binaryData = byteArrayOf(0, 1, 127, -128, -1)

        fs.setXattr("/f.txt", "bin", binaryData).getOrThrow()
        val value = fs.getXattr("/f.txt", "bin").getOrThrow()
        assertContentEquals(binaryData, value)
    }

    // ── 值隔离（防御性复制） ─────────────────────────────────────

    @Test
    fun xattr_value_is_defensively_copied() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "x".encodeToByteArray()).getOrThrow()

        val input = "abc".encodeToByteArray()
        fs.setXattr("/f.txt", "key", input).getOrThrow()
        // 修改原数组不应影响已存储的值
        input[0] = 'z'.code.toByte()

        val stored = fs.getXattr("/f.txt", "key").getOrThrow()
        assertEquals("abc", stored.decodeToString())

        // 修改返回数组不应影响存储
        stored[0] = 'z'.code.toByte()
        val stored2 = fs.getXattr("/f.txt", "key").getOrThrow()
        assertEquals("abc", stored2.decodeToString())
    }

    // ── 权限检查 ────────────────────────────────────────────────

    @Test
    fun setXattr_on_readonly_file_fails() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "x".encodeToByteArray()).getOrThrow()
        fs.setPermissions("/f.txt", FsPermissions.READ_ONLY).getOrThrow()

        val result = fs.setXattr("/f.txt", "key", "v".encodeToByteArray())
        assertTrue(result.isFailure)
        assertIs<FsError.PermissionDenied>(result.exceptionOrNull())
    }

    @Test
    fun removeXattr_on_readonly_file_fails() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "x".encodeToByteArray()).getOrThrow()
        fs.setXattr("/f.txt", "key", "v".encodeToByteArray()).getOrThrow()
        fs.setPermissions("/f.txt", FsPermissions.READ_ONLY).getOrThrow()

        val result = fs.removeXattr("/f.txt", "key")
        assertTrue(result.isFailure)
        assertIs<FsError.PermissionDenied>(result.exceptionOrNull())
    }

    @Test
    fun getXattr_on_noread_file_fails() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "x".encodeToByteArray()).getOrThrow()
        fs.setXattr("/f.txt", "key", "v".encodeToByteArray()).getOrThrow()
        fs.setPermissions("/f.txt", FsPermissions(read = false, write = true, execute = false)).getOrThrow()

        val result = fs.getXattr("/f.txt", "key")
        assertTrue(result.isFailure)
        assertIs<FsError.PermissionDenied>(result.exceptionOrNull())
    }

    @Test
    fun listXattrs_on_noread_file_fails() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "x".encodeToByteArray()).getOrThrow()
        fs.setXattr("/f.txt", "key", "v".encodeToByteArray()).getOrThrow()
        fs.setPermissions("/f.txt", FsPermissions(read = false, write = true, execute = false)).getOrThrow()

        val result = fs.listXattrs("/f.txt")
        assertTrue(result.isFailure)
        assertIs<FsError.PermissionDenied>(result.exceptionOrNull())
    }

    // ── 路径不存在 ──────────────────────────────────────────────

    @Test
    fun xattr_on_nonexistent_path_fails() = runTest {
        val fs = createFs()

        val result = fs.setXattr("/no-such-file", "key", "v".encodeToByteArray())
        assertTrue(result.isFailure)
        assertIs<FsError.NotFound>(result.exceptionOrNull())
    }

    // ── 持久化：Snapshot ────────────────────────────────────────

    @Test
    fun xattrs_survive_snapshot_restore() = runTest {
        val storage = InMemoryFsStorage()
        val fs1 = createFs(storage)
        fs1.writeAll("/f.txt", "data".encodeToByteArray()).getOrThrow()
        fs1.createDir("/dir").getOrThrow()
        fs1.setXattr("/f.txt", "tag", "file-tag".encodeToByteArray()).getOrThrow()
        fs1.setXattr("/dir", "color", "red".encodeToByteArray()).getOrThrow()

        // 触发足够多的 WAL 条目以强制快照
        repeat(20) { i ->
            fs1.writeAll("/tmp$i", "x".encodeToByteArray()).getOrThrow()
        }

        // 新实例从 storage 加载
        val fs2 = createFs(storage)
        val fileTag = fs2.getXattr("/f.txt", "tag").getOrThrow()
        assertEquals("file-tag", fileTag.decodeToString())
        val dirColor = fs2.getXattr("/dir", "color").getOrThrow()
        assertEquals("red", dirColor.decodeToString())
    }

    // ── 持久化：WAL ─────────────────────────────────────────────

    @Test
    fun xattr_set_persisted_via_wal() = runTest {
        val storage = InMemoryFsStorage()
        val fs1 = createFs(storage)
        fs1.writeAll("/f.txt", "data".encodeToByteArray()).getOrThrow()
        fs1.setXattr("/f.txt", "key", "value".encodeToByteArray()).getOrThrow()

        // 新实例从 WAL 回放
        val fs2 = createFs(storage)
        val v = fs2.getXattr("/f.txt", "key").getOrThrow()
        assertEquals("value", v.decodeToString())
    }

    @Test
    fun xattr_remove_persisted_via_wal() = runTest {
        val storage = InMemoryFsStorage()
        val fs1 = createFs(storage)
        fs1.writeAll("/f.txt", "data".encodeToByteArray()).getOrThrow()
        fs1.setXattr("/f.txt", "key", "value".encodeToByteArray()).getOrThrow()
        fs1.removeXattr("/f.txt", "key").getOrThrow()

        // 新实例从 WAL 回放
        val fs2 = createFs(storage)
        val result = fs2.getXattr("/f.txt", "key")
        assertTrue(result.isFailure)
        assertIs<FsError.NotFound>(result.exceptionOrNull())
    }

    // ── 删除节点后 xattr 消失 ───────────────────────────────────

    @Test
    fun xattrs_gone_after_file_delete() = runTest {
        val fs = createFs()
        fs.writeAll("/f.txt", "x".encodeToByteArray()).getOrThrow()
        fs.setXattr("/f.txt", "key", "v".encodeToByteArray()).getOrThrow()

        fs.delete("/f.txt").getOrThrow()

        val result = fs.getXattr("/f.txt", "key")
        assertTrue(result.isFailure)
        assertIs<FsError.NotFound>(result.exceptionOrNull())
    }

    // ── root 目录也能设置 xattr ─────────────────────────────────

    @Test
    fun xattr_on_root_directory() = runTest {
        val fs = createFs()
        fs.setXattr("/", "rootKey", "rootVal".encodeToByteArray()).getOrThrow()
        val v = fs.getXattr("/", "rootKey").getOrThrow()
        assertEquals("rootVal", v.decodeToString())
    }

    // ── 挂载点（真实文件系统）xattr ─────────────────────────────

    @Test
    fun xattr_on_mounted_file() = runTest {
        val fs = createFs()
        val diskOps = FakeDiskFileOperations()
        diskOps.files["/test.txt"] = "hello".encodeToByteArray()
        fs.mount("/mnt", diskOps).getOrThrow()

        fs.setXattr("/mnt/test.txt", "tag", "mounted".encodeToByteArray()).getOrThrow()
        val v = fs.getXattr("/mnt/test.txt", "tag").getOrThrow()
        assertEquals("mounted", v.decodeToString())

        val keys = fs.listXattrs("/mnt/test.txt").getOrThrow()
        assertEquals(listOf("tag"), keys)

        fs.removeXattr("/mnt/test.txt", "tag").getOrThrow()
        val keys2 = fs.listXattrs("/mnt/test.txt").getOrThrow()
        assertTrue(keys2.isEmpty())
    }

    @Test
    fun xattr_on_mounted_nonexistent_file_fails() = runTest {
        val fs = createFs()
        val diskOps = FakeDiskFileOperations()
        fs.mount("/mnt", diskOps).getOrThrow()

        val result = fs.setXattr("/mnt/no-such-file", "key", "v".encodeToByteArray())
        assertTrue(result.isFailure)
    }
}
