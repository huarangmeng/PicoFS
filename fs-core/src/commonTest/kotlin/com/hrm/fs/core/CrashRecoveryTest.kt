package com.hrm.fs.core

import com.hrm.fs.api.*
import com.hrm.fs.core.persistence.PersistenceConfig
import kotlinx.coroutines.test.runTest
import kotlin.test.*

class CrashRecoveryTest {

    @Test
    fun corrupted_wal_falls_back_to_snapshot() = runTest {
        val storage = InMemoryFsStorage()
        val cfg = PersistenceConfig(autoSnapshotEvery = 1)
        val fs1 = InMemoryFileSystem(storage = storage, persistenceConfig = cfg)
        fs1.writeAll("/f.txt", "snapshot-data".encodeToByteArray()).getOrThrow()
        storage.write(cfg.walKey, "CORRUPTED_DATA{{{".encodeToByteArray())
        val fs2 = InMemoryFileSystem(storage = storage, persistenceConfig = cfg)
        assertEquals("snapshot-data", fs2.readAll("/f.txt").getOrThrow().decodeToString())
    }

    @Test
    fun corrupted_snapshot_with_valid_wal() = runTest {
        val storage = InMemoryFsStorage()
        val cfg = PersistenceConfig()
        val fs1 = InMemoryFileSystem(storage = storage, persistenceConfig = cfg)
        fs1.createDir("/data").getOrThrow()
        fs1.createFile("/data/file.txt").getOrThrow()
        storage.write(cfg.snapshotKey, "BROKEN!!!".encodeToByteArray())
        val fs2 = InMemoryFileSystem(storage = storage, persistenceConfig = cfg)
        assertEquals(FsType.FILE, fs2.stat("/data/file.txt").getOrThrow().type)
    }

    @Test
    fun both_corrupted_starts_empty() = runTest {
        val storage = InMemoryFsStorage()
        val cfg = PersistenceConfig()
        val fs1 = InMemoryFileSystem(storage = storage, persistenceConfig = cfg)
        fs1.writeAll("/f.txt", "data".encodeToByteArray()).getOrThrow()
        storage.write(cfg.snapshotKey, "BAD_SNAPSHOT".encodeToByteArray())
        storage.write(cfg.walKey, "BAD_WAL".encodeToByteArray())
        val fs2 = InMemoryFileSystem(storage = storage, persistenceConfig = cfg)
        assertTrue(fs2.stat("/f.txt").isFailure)
        fs2.writeAll("/new.txt", "fresh".encodeToByteArray()).getOrThrow()
        assertEquals("fresh", fs2.readAll("/new.txt").getOrThrow().decodeToString())
    }

    @Test
    fun corrupted_wal_crc_mismatch() = runTest {
        val storage = InMemoryFsStorage()
        val cfg = PersistenceConfig(autoSnapshotEvery = 1)
        val fs1 = InMemoryFileSystem(storage = storage, persistenceConfig = cfg)
        fs1.writeAll("/f.txt", "safe-data".encodeToByteArray()).getOrThrow()
        // Write binary garbage with fake CRC header (will fail CRC check)
        storage.write(cfg.walKey, byteArrayOf(0xDE.toByte(), 0xAD.toByte(), 0xBE.toByte(), 0xEF.toByte(), 0, 0, 0, 5, 1, 2, 3, 4, 5))
        val fs2 = InMemoryFileSystem(storage = storage, persistenceConfig = cfg)
        assertEquals("safe-data", fs2.readAll("/f.txt").getOrThrow().decodeToString())
        assertTrue(fs2.stat("/bad.txt").isFailure)
    }

    @Test
    fun snapshot_crc_mismatch_wal_valid() = runTest {
        val storage = InMemoryFsStorage()
        val cfg = PersistenceConfig()
        val fs1 = InMemoryFileSystem(storage = storage, persistenceConfig = cfg)
        fs1.createDir("/d").getOrThrow()
        fs1.createFile("/d/f.txt").getOrThrow()
        // Write binary garbage with wrong CRC for snapshot
        storage.write(cfg.snapshotKey, byteArrayOf(0, 0, 0, 0, 1, 2, 3))
        val fs2 = InMemoryFileSystem(storage = storage, persistenceConfig = cfg)
        assertEquals(FsType.FILE, fs2.stat("/d/f.txt").getOrThrow().type)
    }

    @Test
    fun corrupted_mounts_still_works() = runTest {
        val storage = InMemoryFsStorage()
        val cfg = PersistenceConfig(autoSnapshotEvery = 1)
        val fs1 = InMemoryFileSystem(storage = storage, persistenceConfig = cfg)
        fs1.writeAll("/f.txt", "data".encodeToByteArray()).getOrThrow()
        fs1.mounts.mount("/mnt", FakeDiskFileOperations()).getOrThrow()
        storage.write(cfg.mountsKey, "NOT_VALID!!!".encodeToByteArray())
        val fs2 = InMemoryFileSystem(storage = storage, persistenceConfig = cfg)
        assertEquals("data", fs2.readAll("/f.txt").getOrThrow().decodeToString())
        assertTrue(fs2.mounts.pending().isEmpty())
    }

    @Test
    fun corrupted_versions_still_works() = runTest {
        val storage = InMemoryFsStorage()
        val cfg = PersistenceConfig(autoSnapshotEvery = 1)
        val fs1 = InMemoryFileSystem(storage = storage, persistenceConfig = cfg)
        fs1.writeAll("/f.txt", "v1".encodeToByteArray()).getOrThrow()
        fs1.writeAll("/f.txt", "v2".encodeToByteArray()).getOrThrow()
        storage.write(cfg.versionsKey, "CORRUPT".encodeToByteArray())
        val fs2 = InMemoryFileSystem(storage = storage, persistenceConfig = cfg)
        assertEquals("v2", fs2.readAll("/f.txt").getOrThrow().decodeToString())
        assertTrue(fs2.versions.list("/f.txt").getOrThrow().isEmpty())
    }

    @Test
    fun wal_cleared_after_corruption() = runTest {
        val storage = InMemoryFsStorage()
        val cfg = PersistenceConfig()
        val fs1 = InMemoryFileSystem(storage = storage, persistenceConfig = cfg)
        fs1.createDir("/d").getOrThrow()
        storage.write(cfg.walKey, "BROKEN".encodeToByteArray())
        val fs2 = InMemoryFileSystem(storage = storage, persistenceConfig = cfg)
        fs2.stat("/").getOrThrow()
        fs2.createDir("/new_dir").getOrThrow()
        fs2.createFile("/new_dir/f.txt").getOrThrow()
        val fs3 = InMemoryFileSystem(storage = storage, persistenceConfig = cfg)
        assertEquals(FsType.FILE, fs3.stat("/new_dir/f.txt").getOrThrow().type)
    }

    @Test
    fun snapshot_after_wal_cleared() = runTest {
        val storage = InMemoryFsStorage()
        val cfg = PersistenceConfig(autoSnapshotEvery = 2)
        val fs1 = InMemoryFileSystem(storage = storage, persistenceConfig = cfg)
        fs1.createDir("/d").getOrThrow()
        fs1.createFile("/d/a.txt").getOrThrow()
        fs1.createFile("/d/b.txt").getOrThrow()
        storage.write(cfg.walKey, "PARTIAL_WRITE".encodeToByteArray())
        val fs2 = InMemoryFileSystem(storage = storage, persistenceConfig = cfg)
        assertTrue(fs2.stat("/d/a.txt").isSuccess)
        assertTrue(fs2.stat("/d/b.txt").isFailure)
    }

    @Test
    fun wal_crc_integrity_on_normal_operation() = runTest {
        val storage = InMemoryFsStorage()
        val cfg = PersistenceConfig(autoSnapshotEvery = 100)
        val fs1 = InMemoryFileSystem(storage = storage, persistenceConfig = cfg)
        fs1.createDir("/d").getOrThrow()
        fs1.createFile("/d/f.txt").getOrThrow()
        val walData = storage.read(cfg.walKey).getOrThrow()!!
        // Binary WAL format: starts with 4-byte CRC32 (not text "CRC:")
        assertTrue(walData.size >= 8, "WAL should have binary CRC + length header")
        val fs2 = InMemoryFileSystem(storage = storage, persistenceConfig = cfg)
        assertTrue(fs2.stat("/d/f.txt").isSuccess)
    }

    @Test
    fun snapshot_crc_integrity_on_normal_operation() = runTest {
        val storage = InMemoryFsStorage()
        val cfg = PersistenceConfig(autoSnapshotEvery = 1)
        val fs1 = InMemoryFileSystem(storage = storage, persistenceConfig = cfg)
        fs1.writeAll("/f.txt", "data".encodeToByteArray()).getOrThrow()
        val snapData = storage.read(cfg.snapshotKey).getOrThrow()!!
        // Binary snapshot format: starts with 4-byte CRC32
        assertTrue(snapData.size >= 4, "Snapshot should have binary CRC header")
        val fs2 = InMemoryFileSystem(storage = storage, persistenceConfig = cfg)
        assertEquals("data", fs2.readAll("/f.txt").getOrThrow().decodeToString())
    }

    @Test
    fun no_crc_header_treated_as_corrupted() = runTest {
        val storage = InMemoryFsStorage()
        val cfg = PersistenceConfig()
        // Write text data (invalid binary format) — should be treated as corrupted
        storage.write(cfg.walKey, "some random text data".encodeToByteArray())
        val fs = InMemoryFileSystem(storage = storage, persistenceConfig = cfg)
        assertTrue(fs.stat("/legacy").isFailure)
        assertTrue(fs.stat("/legacy/old.txt").isFailure)
        fs.writeAll("/new.txt", "ok".encodeToByteArray()).getOrThrow()
        assertEquals("ok", fs.readAll("/new.txt").getOrThrow().decodeToString())
    }
}
