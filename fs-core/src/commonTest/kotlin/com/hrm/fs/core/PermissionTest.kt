package com.hrm.fs.core

import com.hrm.fs.api.*
import kotlinx.coroutines.test.runTest
import kotlin.test.*

class PermissionTest {

    @Test
    fun setPermissions_readonly_blocks_write() = runTest {
        val fs = createFs()
        fs.createDir("/d").getOrThrow()
        fs.createFile("/d/f.txt").getOrThrow()
        fs.setPermissions("/d/f.txt", FsPermissions.READ_ONLY).getOrThrow()

        val handle = fs.open("/d/f.txt", OpenMode.READ).getOrThrow()
        val writeResult = handle.writeAt(0, "test".encodeToByteArray())
        assertTrue(writeResult.isFailure)
        handle.close()
    }

    @Test
    fun setPermissions_readonly_blocks_open_write() = runTest {
        val fs = createFs()
        fs.createDir("/d").getOrThrow()
        fs.createFile("/d/f.txt").getOrThrow()
        fs.setPermissions("/d/f.txt", FsPermissions.READ_ONLY).getOrThrow()

        val result = fs.open("/d/f.txt", OpenMode.WRITE)
        assertTrue(result.isFailure)
        assertIs<FsError.PermissionDenied>(result.exceptionOrNull())
    }

    @Test
    fun createFile_in_readonly_dir_fails() = runTest {
        val fs = createFs()
        fs.createDir("/d").getOrThrow()
        fs.setPermissions("/d", FsPermissions.READ_ONLY).getOrThrow()
        val result = fs.createFile("/d/f.txt")
        assertTrue(result.isFailure)
        assertIs<FsError.PermissionDenied>(result.exceptionOrNull())
    }
}
