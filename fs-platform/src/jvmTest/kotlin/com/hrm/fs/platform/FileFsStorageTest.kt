package com.hrm.fs.platform

import kotlinx.coroutines.test.runTest
import java.io.File
import kotlin.test.*

class FileFsStorageTest {

    private lateinit var tempDir: File
    private lateinit var storage: FileFsStorage

    @BeforeTest
    fun setup() {
        tempDir = File(System.getProperty("java.io.tmpdir"), "picofs-storage-test-${System.nanoTime()}")
        tempDir.mkdirs()
        storage = FileFsStorage(tempDir.absolutePath)
    }

    @AfterTest
    fun cleanup() {
        tempDir.deleteRecursively()
    }

    @Test
    fun read_nonexistent_returns_null() = runTest {
        val result = storage.read("no-such-key")
        assertTrue(result.isSuccess)
        assertNull(result.getOrNull())
    }

    @Test
    fun write_then_read() = runTest {
        val data = "hello world".encodeToByteArray()
        storage.write("mykey", data).getOrThrow()

        val loaded = storage.read("mykey").getOrThrow()
        assertNotNull(loaded)
        assertContentEquals(data, loaded)
    }

    @Test
    fun write_overwrite() = runTest {
        storage.write("k", "v1".encodeToByteArray()).getOrThrow()
        storage.write("k", "v2".encodeToByteArray()).getOrThrow()

        val loaded = storage.read("k").getOrThrow()
        assertNotNull(loaded)
        assertEquals("v2", loaded.decodeToString())
    }

    @Test
    fun delete_removes_data() = runTest {
        storage.write("k", "data".encodeToByteArray()).getOrThrow()
        storage.delete("k").getOrThrow()

        val result = storage.read("k").getOrThrow()
        assertNull(result)
    }

    @Test
    fun delete_nonexistent_succeeds() = runTest {
        val result = storage.delete("no-such-key")
        assertTrue(result.isSuccess)
    }

    @Test
    fun special_characters_in_key() = runTest {
        val key = "vfs/snapshot:main.json"
        val data = "{\"test\": true}".encodeToByteArray()
        storage.write(key, data).getOrThrow()

        val loaded = storage.read(key).getOrThrow()
        assertNotNull(loaded)
        assertContentEquals(data, loaded)
    }

    @Test
    fun empty_data() = runTest {
        storage.write("empty", ByteArray(0)).getOrThrow()

        val loaded = storage.read("empty").getOrThrow()
        assertNotNull(loaded)
        assertEquals(0, loaded.size)
    }

    @Test
    fun large_data() = runTest {
        val data = ByteArray(1024 * 1024) { (it % 256).toByte() } // 1MB
        storage.write("large", data).getOrThrow()

        val loaded = storage.read("large").getOrThrow()
        assertNotNull(loaded)
        assertContentEquals(data, loaded)
    }

    @Test
    fun persistence_across_instances() = runTest {
        storage.write("persist", "value".encodeToByteArray()).getOrThrow()

        // 创建新实例指向同一目录
        val storage2 = FileFsStorage(tempDir.absolutePath)
        val loaded = storage2.read("persist").getOrThrow()
        assertNotNull(loaded)
        assertEquals("value", loaded.decodeToString())
    }

    @Test
    fun multiple_keys_independent() = runTest {
        storage.write("a", "1".encodeToByteArray()).getOrThrow()
        storage.write("b", "2".encodeToByteArray()).getOrThrow()
        storage.write("c", "3".encodeToByteArray()).getOrThrow()

        assertEquals("1", storage.read("a").getOrThrow()?.decodeToString())
        assertEquals("2", storage.read("b").getOrThrow()?.decodeToString())
        assertEquals("3", storage.read("c").getOrThrow()?.decodeToString())

        storage.delete("b").getOrThrow()
        assertNull(storage.read("b").getOrThrow())
        assertEquals("1", storage.read("a").getOrThrow()?.decodeToString())
        assertEquals("3", storage.read("c").getOrThrow()?.decodeToString())
    }
}
