## PicoFS（KMP）全虚拟文件系统（VFS）方案

### 目标与边界

- **目标**：在 Android / iOS / Desktop（JVM）上提供统一的“用户级虚拟文件系统”能力。
- **边界**：**不进行系统级挂载**，不依赖 FUSE/WinFsp 等驱动。所有能力在应用内完成。
- **优势**：核心逻辑使用 KMP 共享，平台侧只处理沙箱路径、权限与桥接。

### 架构分层

- **`fs-api`**：对外接口与模型（`FileSystem`、`FileHandle`、`FsMeta`、`ArchiveCodec` 等）。
- **`fs-core`**：VFS 核心逻辑（目录树、元数据、读写与路径规范化）。
- **`fs-platform`**：平台差异适配（平台识别、能力提示；后续可扩展沙箱路径策略）。

###  VFS 应该具备的能力

- **一致性优先**：跨平台语义一致（路径、错误码、权限），避免平台差异泄漏到业务层。
- **可恢复性**：支持写前日志（WAL）或原子提交，保证断电/崩溃后不破坏结构。
- **可扩展性**：目录树与元数据结构可扩展（属性、标签、权限位）。
- **性能可控**：分层缓存（元数据缓存、读写缓冲）与可调策略。
- **可观测性**：内置事件与日志，支持统计与调试（如操作耗时、命中率）。
- **安全性**：路径规范化与权限校验统一化，避免越权访问。

> 简单说：最好的 VFS 是“跨平台一致、可恢复、可扩展、可观测、性能可控”。

### 能力清单与实现状态（VFS）

- **基础能力**
  - **✅ 路径规范化**（`/a/../b`、重复斜杠）
  - **✅ 目录/文件创建与删除**（含递归 `mkdir -p` / `rm -rf`）
  - **✅ 随机读写**（`readAt`/`writeAt`）
  - **✅ 便捷读写**（`readAll`/`writeAll` 一次性读写）
  - **✅ 元数据查询**（大小、时间戳、权限）
  - **✅ 目录列举**
  - **✅ copy / move / rename**（支持跨挂载点）
- **流式 IO**
  - **✅ 流式读取**（`readStream` 分块 `Flow<ByteArray>`）
  - **✅ 流式写入**（`writeStream` 从 `Flow` 逐块写入）
- **挂载系统**
  - **✅ 虚拟路径挂载到磁盘目录**（`mount`/`unmount`/`listMounts`）
  - **✅ 只读挂载**（`MountOptions(readOnly = true)`）
  - **✅ 挂载持久化与恢复**（`pendingMounts` 机制）
  - **✅ 三端磁盘操作实现**（Android/iOS/JVM `DiskFileOperations`）
- **权限与错误模型**
  - **✅ 权限模型**（读/写/执行）
  - **✅ 统一错误码**（NotFound/AlreadyExists/PermissionDenied/NotMounted 等 8 种）
  - **⬜ 多用户/角色权限**（ACL/Owner/Group）
- **一致性与恢复**
  - **✅ WAL（写前日志）**（5 种操作类型，自动回放）
  - **✅ 快照（Snapshot）**（自动快照，每 20 次操作触发）
  - **✅ 崩溃恢复验证**（CRC32 完整性校验 + 断电场景测试覆盖）
  - **✅ WAL 原子写入保证**（CRC32 包装 + 平台层 tmp+rename 原子写入）
  - **✅ 损坏 WAL/Snapshot 容错处理**（独立降级：snapshot 损坏回退 WAL、WAL 损坏回退 snapshot、均损坏从空树启动）
- **持久化存储**
  - **✅ 抽象存储接口**（`FsStorage`）
  - **✅ 内存存储实现**（`InMemoryFsStorage`）
  - **✅ 映射真实目录**（Android/iOS/JVM）
  - **✅ 平台持久化 FsStorage 实现**（JVM/Android 基于 `java.io.File`、iOS 基于 `NSFileManager`，原子写入）
- **外部变更感知**
  - **✅ DiskFileWatcher 接口**（可选实现，mount 时自动桥接）
  - **✅ JVM WatchService 实现**（`java.nio.file.WatchService` 递归监听）
  - **✅ Android 分版本实现**（API 26+ WatchService / API 24-25 轮询）
  - **✅ iOS 轮询实现**（`NSFileManager` 快照对比）
  - **✅ 手动同步 `sync()`**（降级场景全量扫描）
- **事件系统**
  - **✅ 文件变更事件**（`watch()` 返回 `Flow<FsEvent>`，CREATED/MODIFIED/DELETED）
  - **✅ IO 统计与可观测性**（操作计数、成功/失败数、耗时统计、字节吞吐、`metrics()`/`resetMetrics()` API）
- **性能与扩展**
  - **✅ 缓存策略**（挂载点 stat/readDir 结果 LRU 缓存，自动失效）
  - **✅ 大文件分块**（BlockStorage 64KB 分块，按需分配，避免单一 ByteArray 占用连续大内存）
  - **✅ 磁盘空间配额**（`quotaBytes` 参数限制虚拟磁盘容量，`quotaInfo()` API 查询用量）
- **高级功能**
  - **✅ 符号链接**（symlink，创建/读取/透明跟随/链式解析/相对路径/持久化）
  - **✅ 文件锁**（flock，文件级并发控制，共享锁/独占锁/锁升级/挂起等待）
  - **✅ 搜索 / 查找**（按文件名/内容搜索，类 find/grep，支持 glob 通配符、内容 grep、类型过滤、深度限制、大小写控制，跨内存与挂载点统一搜索）
  - **✅ 文件扩展属性**（xattr / 自定义标签，set/get/remove/list，支持文件/目录/符号链接，WAL + Snapshot 持久化）
  - **✅ 压缩 / 解压**（ZIP/TAR 归档操作，compress/extract/list，支持自动格式检测，VFS 内存文件与挂载点统一操作，三端真实文件系统 JVM/Android/iOS 均支持）
  - **✅ 文件哈希 / 校验**（纯 Kotlin 实现 CRC32/SHA-256，`checksum()` API 支持内存文件与挂载点文件）
  - **✅ 回收站**（软删除 + 恢复机制，moveToTrash/restore/list/purge/purgeAll，支持文件/目录/符号链接，挂载点文件委托磁盘 `.trash` 目录管理，VFS 内存文件完整保存内容，自动修剪最旧条目，独立持久化）
  - **✅ 版本历史**（写入自动保存历史版本，`fileVersions()`/`readVersion()`/`restoreVersion()` API，支持内存文件与挂载点文件，独立持久化）

### 当前接口概览

```kotlin
interface FileSystem {
    // 基础 CRUD
    suspend fun createFile(path: String): Result<Unit>
    suspend fun createDir(path: String): Result<Unit>
    suspend fun open(path: String, mode: OpenMode): Result<FileHandle>
    suspend fun readDir(path: String): Result<List<FsEntry>>
    suspend fun stat(path: String): Result<FsMeta>
    suspend fun delete(path: String): Result<Unit>
    suspend fun setPermissions(path: String, permissions: FsPermissions): Result<Unit>

    // 递归操作
    suspend fun createDirRecursive(path: String): Result<Unit>
    suspend fun deleteRecursive(path: String): Result<Unit>

    // 便捷读写
    suspend fun readAll(path: String): Result<ByteArray>
    suspend fun writeAll(path: String, data: ByteArray): Result<Unit>

    // copy / move / rename
    suspend fun copy(srcPath: String, dstPath: String): Result<Unit>
    suspend fun move(srcPath: String, dstPath: String): Result<Unit>
    suspend fun rename(srcPath: String, dstPath: String): Result<Unit>

    // ─── 扩展能力（子接口） ─────────────────────────────────

    val mounts: FsMounts         // mount / unmount / sync / list / pending
    val versions: FsVersions     // list / read / restore
    val search: FsSearch         // find（glob + grep）
    val observe: FsObserve       // watch / metrics / resetMetrics / quotaInfo
    val streams: FsStreams       // read / write（Flow 分块流式 IO）
    val checksum: FsChecksum     // compute（CRC32 / SHA-256）
    val xattr: FsXattr           // set / get / remove / list
    val symlinks: FsSymlinks     // create / readLink
    val archive: FsArchive       // compress / extract / list（ZIP / TAR）
    val trash: FsTrash           // moveToTrash / restore / list / purge / purgeAll
}
```

> 说明：该接口在 `commonMain` 中定义，基础 CRUD 直接在 `FileSystem` 上调用，扩展能力通过 10 个子接口属性访问（如 `fs.mounts.mount(...)`、`fs.trash.moveToTrash(...)`）。平台侧通过 `expect/actual` 提供沙箱路径与能力适配。

### 平台实现思路（VFS）

- **Android**：使用应用沙箱目录（`context.filesDir` / `context.noBackupFilesDir`）。
- **iOS**：使用 `Documents/` 或 `Application Support/`。
- **Desktop (JVM)**：使用 `user.home` 下的应用目录。

### 真实文件映射（应用内根目录）

- **目的**：VFS 操作直接落盘到应用根目录，`readDir`/`stat` 能感知到外部新增文件。
- **实现**：由 `fs-core` 提供统一入口 `createFileSystem(FsConfig)`，通过参数选择 **虚拟** 或 **真实** 文件系统。
- **Android 初始化**：在 `App(backend = FsBackend.REAL, rootPath = filesDir.absolutePath)` 传入根目录。
- **外部变更感知**：平台层 `DiskFileOperations` 可选实现 `DiskFileWatcher` 接口，mount 时自动桥接磁盘变更到 VFS 事件流；也可调用 `sync()` 手动全量同步。

### 项目结构建议

- **`fs-api`**
  - `fs-api/src/commonMain/kotlin`：接口与模型
- **`fs-core`**
  - `fs/src/commonMain/kotlin`：VFS 核心逻辑
  - 依赖 `fs-api` + `fs-platform`
- **`fs-platform`**
  - `fs-platform/src/*Main`：平台能力适配
- **`composeApp`**
  - UI 与业务调用层，依赖 `fs-core`

### 开发与集成步骤

1. 在 `fs-api` 定义接口与模型。
2. 在 `fs-core` 实现 VFS 核心逻辑。
3. 在 `fs-platform` 实现平台能力适配。
4. `composeApp` 提供 UI + 操作入口（创建文件、列目录、读取/写入等）。

---

## Build and Run

### Android

- macOS/Linux
  ```shell
  ./gradlew :composeApp:assembleDebug
  ```
- Windows
  ```shell
  .\gradlew.bat :composeApp:assembleDebug
  ```

### Desktop (JVM)

- macOS/Linux
  ```shell
  ./gradlew :composeApp:run
  ```
- Windows
  ```shell
  .\gradlew.bat :composeApp:run
  ```

### iOS

打开 `iosApp` 目录，用 Xcode 运行。
