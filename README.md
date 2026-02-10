## PicoFS（KMP）全虚拟文件系统（VFS）方案

### 目标与边界

- **目标**：在 Android / iOS / Desktop（JVM）上提供统一的“用户级虚拟文件系统”能力。
- **边界**：**不进行系统级挂载**，不依赖 FUSE/WinFsp 等驱动。所有能力在应用内完成。
- **优势**：核心逻辑使用 KMP 共享，平台侧只处理沙箱路径、权限与桥接。

### 架构分层

- **`fs-api`**：对外接口与模型（`FileSystem`、`FileHandle`、`FsMeta` 等）。
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
  - **✅ 目录/文件创建与删除**
  - **✅ 随机读写**（`readAt`/`writeAt`）
  - **✅ 元数据查询**（大小、时间戳）
  - **✅ 目录列举**
- **权限与错误模型**
  - **✅ 权限模型**（读/写/执行）
  - **✅ 统一错误码**（NotFound/PermissionDenied 等）
  - **⬜ 多用户/角色权限**（ACL/Owner/Group）
- **一致性与恢复**
  - **✅ WAL（写前日志）**
  - **✅ 快照（Snapshot）**
  - **⚠️ 崩溃恢复验证**（需平台存储适配）
- **持久化存储**
  - **✅ 抽象存储接口**（`FsStorage`）
  - **✅ 映射真实目录**（Android/iOS/JVM）
  - **⚠️ 外部变更感知**（依赖重新读取目录/刷新）
- **可观测性**
  - **⬜ 事件总线/统计**（IO 次数、耗时、命中率）
- **性能与扩展**
  - **⬜ 缓存策略**（热目录缓存、读写缓冲）
  - **⬜ 大文件分块**（Block/Page）

### 最小接口（示意）

```kotlin
interface FileSystem {
    suspend fun createFile(path: String): Result<Unit>
    suspend fun createDir(path: String): Result<Unit>
    suspend fun open(path: String, mode: OpenMode): Result<FileHandle>
    suspend fun readDir(path: String): Result<List<FsEntry>>
    suspend fun stat(path: String): Result<FsMeta>
    suspend fun delete(path: String): Result<Unit>
    suspend fun setPermissions(path: String, permissions: FsPermissions): Result<Unit>
}
```

> 说明：该接口在 `commonMain` 中定义，平台侧通过 `expect/actual` 提供沙箱路径与能力适配；`FsMeta` 含权限信息。

### 平台实现思路（VFS）

- **Android**：使用应用沙箱目录（`context.filesDir` / `context.noBackupFilesDir`）。
- **iOS**：使用 `Documents/` 或 `Application Support/`。
- **Desktop (JVM)**：使用 `user.home` 下的应用目录。

### 真实文件映射（应用内根目录）

- **目的**：VFS 操作直接落盘到应用根目录，`readDir`/`stat` 能感知到外部新增文件。
- **实现**：由 `fs-core` 提供统一入口 `createFileSystem(FsConfig)`，通过参数选择 **虚拟** 或 **真实** 文件系统。
- **Android 初始化**：在 `App(backend = FsBackend.REAL, rootPath = filesDir.absolutePath)` 传入根目录。
- **外部变更感知**：当应用目录被外部写入后，调用 `readDir`/`stat` 将读取最新文件状态。

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
