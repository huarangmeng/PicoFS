package com.hrm.fs.core

import com.hrm.fs.api.PathUtils
import kotlin.test.*

class PathUtilsTest {

    @Test
    fun pathNormalize_basic() {
        assertEquals("/", PathUtils.normalize(""))
        assertEquals("/", PathUtils.normalize("/"))
        assertEquals("/a/b", PathUtils.normalize("/a/b"))
        assertEquals("/a/b", PathUtils.normalize("/a/b/"))
        assertEquals("/a/b", PathUtils.normalize("//a//b//"))
    }

    @Test
    fun pathNormalize_dots() {
        assertEquals("/a", PathUtils.normalize("/a/b/.."))
        assertEquals("/", PathUtils.normalize("/a/.."))
        assertEquals("/a/c", PathUtils.normalize("/a/./c"))
        assertEquals("/b", PathUtils.normalize("/a/../b"))
    }
}
