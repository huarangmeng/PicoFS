package com.hrm.fs.core

import com.hrm.fs.api.FsPermissions
import com.hrm.fs.api.FsType

internal sealed class VfsNode(
    val name: String,
    val type: FsType,
    val createdAtMillis: Long,
    var modifiedAtMillis: Long,
    var permissions: FsPermissions
)

internal class DirNode(
    name: String,
    createdAtMillis: Long,
    modifiedAtMillis: Long,
    permissions: FsPermissions
) : VfsNode(name, FsType.DIRECTORY, createdAtMillis, modifiedAtMillis, permissions) {
    val children: MutableMap<String, VfsNode> = LinkedHashMap()
}

internal class FileNode(
    name: String,
    createdAtMillis: Long,
    modifiedAtMillis: Long,
    permissions: FsPermissions
) : VfsNode(name, FsType.FILE, createdAtMillis, modifiedAtMillis, permissions) {
    var content: ByteArray = ByteArray(0)
    var size: Int = 0
}
