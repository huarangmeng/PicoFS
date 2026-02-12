package com.hrm.fs.core

import com.hrm.fs.api.FsPermissions
import com.hrm.fs.api.FsType
import com.hrm.fs.core.BlockStorage.Companion.BLOCK_SIZE

internal sealed class VfsNode(
    val name: String,
    val type: FsType,
    val createdAtMillis: Long,
    var modifiedAtMillis: Long,
    var permissions: FsPermissions
) {
    /** 节点级读写锁：保护节点内容（blocks/children/xattrs/metadata）。 */
    val nodeLock = CoroutineReadWriteMutex()

    /** 扩展属性（xattr）：name -> value。所有节点类型都可以有扩展属性。 */
    val xattrs: MutableMap<String, ByteArray> = LinkedHashMap()
}

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
    /** 分块存储，避免单一 ByteArray 占用连续大内存。 */
    internal val blocks = BlockStorage()

    /** 文件有效字节数。 */
    var size: Int
        get() = blocks.size
        set(value) {
            blocks.size = value
        }
}

internal class SymlinkNode(
    name: String,
    createdAtMillis: Long,
    modifiedAtMillis: Long,
    permissions: FsPermissions,
    /** 符号链接指向的目标路径（可以是绝对或相对路径）。 */
    val targetPath: String
) : VfsNode(name, FsType.SYMLINK, createdAtMillis, modifiedAtMillis, permissions)

// ── 分块存储 ─────────────────────────────────────────────────

/**
 * 基于固定大小 Block 的文件内容存储。
 *
 * 每个 Block 为 [BLOCK_SIZE] 字节的 ByteArray，按需分配。
 * 优势：
 * - 避免大文件分配单一连续内存（减少 OOM 风险）
 * - 扩容时只分配新 Block，不需要拷贝整个旧数组
 * - 稀疏写入时未使用的 Block 不分配内存
 */
internal class BlockStorage(
    /** 块大小，默认 64KB。 */
    val blockSize: Int = BLOCK_SIZE
) {
    companion object {
        /** 默认块大小：64KB。 */
        const val BLOCK_SIZE = 64 * 1024
    }

    /** 块列表，按序号索引。 */
    private val blockList = mutableListOf<ByteArray>()

    /** 文件有效字节数。 */
    var size: Int = 0

    /** 读取 [length] 字节从 [offset] 开始。超过 size 的部分不读取。 */
    fun read(offset: Int, length: Int): ByteArray {
        if (offset >= size || length <= 0) return ByteArray(0)
        val available = minOf(length, size - offset)
        val out = ByteArray(available)
        var remaining = available
        var srcOffset = offset
        var dstOffset = 0
        while (remaining > 0) {
            val blockIndex = srcOffset / blockSize
            val blockOffset = srcOffset % blockSize
            val block = if (blockIndex < blockList.size) blockList[blockIndex] else null
            val copyLen = minOf(remaining, blockSize - blockOffset)
            block?.copyInto(out, dstOffset, blockOffset, blockOffset + copyLen)
            // 如果 block 为 null（稀疏区域），out 中该段保持全零
            srcOffset += copyLen
            dstOffset += copyLen
            remaining -= copyLen
        }
        return out
    }

    /** 写入 [data] 到 [offset] 位置。自动扩展 Block 列表。 */
    fun write(offset: Int, data: ByteArray) {
        if (data.isEmpty()) return
        val end = offset + data.size
        ensureBlocks(end)
        var remaining = data.size
        var srcOffset = 0
        var dstOffset = offset
        while (remaining > 0) {
            val blockIndex = dstOffset / blockSize
            val blockOffset = dstOffset % blockSize
            val block = blockList[blockIndex]
            val copyLen = minOf(remaining, blockSize - blockOffset)
            data.copyInto(block, blockOffset, srcOffset, srcOffset + copyLen)
            srcOffset += copyLen
            dstOffset += copyLen
            remaining -= copyLen
        }
        if (end > size) size = end
    }

    /** 返回文件全部有效内容的拷贝。 */
    fun toByteArray(): ByteArray {
        if (size == 0) return ByteArray(0)
        return read(0, size)
    }

    /** 清空所有块。 */
    fun clear() {
        blockList.clear()
        size = 0
    }

    /** 当前分配的总内存字节数（含未使用的尾部空间）。 */
    val allocatedBytes: Int get() = blockList.size * blockSize

    /** 确保有足够的 Block 覆盖到 [endOffset] 位置。 */
    private fun ensureBlocks(endOffset: Int) {
        val blocksNeeded = (endOffset + blockSize - 1) / blockSize
        while (blockList.size < blocksNeeded) {
            blockList.add(ByteArray(blockSize))
        }
    }
}

// ── 版本快照 ──────────────────────────────────────────────────

/**
 * 文件的一个历史版本快照。
 *
 * @param versionId 唯一标识
 * @param timestampMillis 保存时间
 * @param data 该版本的数据（base 版本为完整内容，delta 版本为 XOR 差异）
 * @param isBase 是否为基准版本（完整数据）。false 表示 delta 编码。
 */
internal data class VersionSnapshot(
    val versionId: String,
    val timestampMillis: Long,
    val data: ByteArray,
    val isBase: Boolean = true
)
