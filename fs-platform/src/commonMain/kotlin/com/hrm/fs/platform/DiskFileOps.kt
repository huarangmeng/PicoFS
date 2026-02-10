package com.hrm.fs.platform

import com.hrm.fs.api.DiskFileOperations

/**
 * 创建平台特定的磁盘文件操作实现。
 *
 * @param rootPath 真实磁盘根目录路径
 */
expect fun createDiskFileOperations(rootPath: String): DiskFileOperations
