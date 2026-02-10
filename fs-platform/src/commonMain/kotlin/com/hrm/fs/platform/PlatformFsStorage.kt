package com.hrm.fs.platform

import com.hrm.fs.api.FsStorage

/**
 * 创建基于平台文件系统的 [FsStorage] 实现。
 *
 * 每个 key 映射为 [dirPath] 下的一个文件，文件名经过安全编码。
 * 重启后数据不丢失。
 *
 * @param dirPath 存储目录的绝对路径
 */
expect fun createPlatformFsStorage(dirPath: String): FsStorage
