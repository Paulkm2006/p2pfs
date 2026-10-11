# p2pfs Windows 客户端

基于 Wails 3 `v3.0.0-beta.28` 的中文桌面客户端。独立 Go module 通过 `replace ..` 使用仓库的真实客户端、RS 编码与 UDP 传输实现；根目录的 Linux 服务端测试无需安装 GUI 开发库。

## 构建与启动

当前构建：Windows x64，需安装 Microsoft Edge WebView2 Runtime，不进一步限定 Windows 版本。Windows 运行验证仍待完成，不将跨编译结果等同于平台验收。Go 版本遵循本模块 `go.mod`（1.26 或更新）；首次构建需要下载 Go modules。

在仓库 `desktop/` 内执行：

```powershell
.\scripts\build-windows.ps1
.\bin\p2pfs.exe
```

也可从 Linux/macOS 交叉编译：

```sh
cd desktop
./scripts/build-windows.sh
```

等价命令：

```sh
GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -tags production -ldflags='-s -w -H windowsgui' -o bin/p2pfs.exe .
```

前端为打包进可执行文件的本地 HTML/CSS/ES modules，无需 Node、npm、CDN 或前端构建步骤。`application.BundledAssetFileServer` 提供与锁定的 Wails 版本一致的 `/wails/runtime.js`。Node/npm 只用于开发测试（Node 24.15+，本次使用 24.19）。本版本没有安装器、自动更新、代码签名或发布操作。

## 使用流程

1. 启动后在“连接与存储”填写服务端 HTTPS 地址、设备名称和本机数据目录。STUN/TURN 配置由服务端下发。只在回环地址调试时启用“允许本机调试 HTTP”。
2. 连接成功后保存非敏感桌面配置至 `%APPDATA%\p2pfs\desktop.json`。默认数据目录为 `%APPDATA%\p2pfs\data`。设备凭据由核心客户端保存在数据目录，不传到前端。
3. 在“文件发现”发布本地文件（最大 1 GiB）。编码成功后分片最初只在本机；在其他设备选择“保留选定分片”才能建立多设备冗余。
4. 筛选文件名或完整版本 ID，选择文件，点击“下载完整文件”，使用 Windows 原生保存对话框选择新文件名。本客户端拒绝覆盖既有文件；即使系统对话框允许选择现有文件，后端仍会拒绝。
5. 在“下载任务”暂停、继续、取消或重试。仅校验完整分片计入可复用进度；取得 4 片后仍需恢复、整文件校验和成功写入，随后才显示“已完成”。
6. 选中文件可停止本机共享，另行勾选是否删除相关缓存。默认保留缓存，原始文件和下载结果不受此操作删除。核心客户端会拒绝删除仍在使用的缓存。
7. “连接与存储”可设置 1–6 的下载并发量和总上行速率上限（KiB/s，0 为无手动上限）。修改连接参数前先断开。
8. 任务页面可导出 JSON 诊断统计。导出包含任务保存路径、设备 UUID、实际分片传输记录与错误，不包含会话令牌或设备密钥。分享导出文件前检查其中的路径和设备信息。

## 状态的含义

- 候选来源来自服务端近期持有声明；在线声明不保证可达。
- 分片进度与网络接收总字节分别展示，不把传输字节达到文件大小视为完成。
- 路径、耗时、字节和平均速度来自已结束的实际分片传输记录。界面不虚构当前瞬时速率、正在连接的路径或尚未测得的 RTT。
- UI 每 1 秒读取本机任务状态、每 10 秒更新文件目录；手动刷新可以立即查询。此处为轮询间隔配置，实际响应时间仍待 Windows 验收。失败时明确显示列表为上次结果。
- 本机数据占用统计来自真实目录中的普通文件，包含任务元数据和缓存。目录统计不跟随符号链接。
- “重新统计”同时校验本机分片，可能需要读取较多磁盘数据。本机缓存详情标明为上次检查结果。
- 取消任务保留完整分片；停止共享时可单独选择清理。断开连接会取消正在进行的发布/保留操作，并停止共享和下载。
- 暂不自动连接或自动继续任务。重新启动后连接原服务端与原目录，在任务列表选择继续。

## 检查

```sh
# 服务层：不依赖 GUI，可在 Linux 运行
cd desktop
go test -race ./internal/app

# 前端纯函数、桥接名称一致性与 DOM 交互（mock bridge）
npm ci --ignore-scripts
npm test

# 可选：真正的浏览器渲染/交互，仍然使用 mock bridge
npx playwright install chromium
npm run test:ui
```

可以用 `CHROMIUM_PATH` 指定已安装的 Chromium。浏览器检查截图写入已忽略的 `bin/qa/`，测试数据只存在于检查脚本中，不会进入应用的前端目录。

2026-10-11 开发验证：

- Linux 上 Go 服务层/文件名测试通过，含 race detector。
- Node 纯函数和 DOM 模拟交互测试通过，覆盖重复点击、对话框取消、保留索引、任务操作与断开后的陈旧内容清理。
- Windows amd64、CGO 关闭的 production 交叉编译通过（实际构建使用 Go 1.27.2）。
- Chromium 渲染检查尝试后被环境的本地 socket 权限限制阻止，没有宣称通过。
- Windows WebView2 启动、真实原生对话框、布局视觉检查和 GUI 跨设备实际传输尚未验收。参见 [Windows 验收清单](WINDOWS-ACCEPTANCE.md)。核心客户端的网络测试结果另见仓库测试报告。

## 实现依据

- [Wails v3 方法绑定](https://v3.wails.io/features/bindings/methods/)：`Call.ByName` 与 Go service。
- [Wails v3 前端运行时](https://v3.wails.io/reference/frontend-runtime/)：使用版本对应的 bundled runtime。
- [Wails v3 原生文件对话框](https://v3.wails.io/features/dialogs/file/)：打开、保存与目录选择。
- [beta.28 对话框源码](https://github.com/wailsapp/wails/blob/v3.0.0-beta.28/v3/pkg/application/dialogs.go)：此版本 SaveFile 没有 SetTitle 链式方法，使用 `SaveFileWithOptions` 指定标题，已以真实 Windows 编译确认。
