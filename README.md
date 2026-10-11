# p2pfs

软件工程实践项目：使用 Reed–Solomon 纠删码的多节点文件下载工具。

- Linux 服务端：Go、Echo v5、SQLite，维护设备/清单/分片位置并提供 UDP 中转。
- Windows 客户端：Go、Wails 3，中文桌面操作；另有用于实验的无界面客户端。
- 文件数据：Pion WebRTC DataChannel，SCTP/DTLS **仅经 UDP**；先 ICE/STUN 直连，连接失败后通过同一服务端的 TURN/UDP 中转。HTTP(S) 只承载元数据与信令。
- 文件上限 1 GiB，固定 k=4、m=2；任意四个有效独立分片可恢复。
- 无账号：服务端分配 UUID，客户端本地持久化并复用。UUID 不是凭据；独立设备恢复秘密和短期会话用于绑定。

这是开发中的 MVP 草稿，尚未合并或部署。实际验证范围、未完成的性能验收和 Windows GUI 限制见[测试记录](docs/testing.md)。不将本地 UDP 测试当作公网 NAT 兼容性证明。

## 本机快速运行

已锁定 Go 1.27.2；依赖版本见 go.mod/go.sum。以下命令仅用于同一机器的回环测试，生产环境必须使用 HTTPS 和受控协作网络。

```sh
go test ./...
go run ./cmd/server -listen 127.0.0.1:8080 -db ./server-data/p2pfs.db -insecure-loopback \
  -turn-listen 127.0.0.1:3478 -turn-public-ip 127.0.0.1 -turn-allow-loopback
```

另开终端发布文件，输出清单 ID 后进程继续作为来源：

```sh
go run ./cmd/peer -server http://127.0.0.1:8080 -insecure-local -data ./alice-data -publish ./example.bin
```

再开终端查看列表、下载。将 `<ID>` 替换成发布输出里的完整 id：

```sh
go run ./cmd/peer -server http://127.0.0.1:8080 -insecure-local -data ./bob-data -list
go run ./cmd/peer -server http://127.0.0.1:8080 -insecure-local -data ./bob-data -download '<ID>' -out ./recovered.bin
```

下载文件必须使用尚不存在的路径，程序不会覆盖既有文件。每个同时运行的 Peer 使用独立数据目录；程序用操作系统文件锁拒绝重复使用同一目录。强制测试 UDP 中转时在下载客户端追加 `-relay`。

### 建立真实分片分布

发布者运行时，启动三台来源（或三个独立目录的进程）：

```sh
go run ./cmd/peer -server http://127.0.0.1:8080 -insecure-local -data ./p1-data -retain '<ID>' -indices 0,1
go run ./cmd/peer -server http://127.0.0.1:8080 -insecure-local -data ./p2-data -retain '<ID>' -indices 2,3
go run ./cmd/peer -server http://127.0.0.1:8080 -insecure-local -data ./p3-data -retain '<ID>' -indices 4,5
```

每个命令完成校验后继续提供分片。随后可关闭原发布者，等待其在线状态过期，再用第四个客户端下载。中转服务器不算一个分片副本。

## Windows 桌面

```powershell
cd desktop
go build -tags production -ldflags="-H windowsgui" -o p2pfs.exe .
.\p2pfs.exe
```

需要 Microsoft WebView2 Runtime。桌面源码与独立模块位于 [desktop](desktop/README.md)；界面包括连接、选择文件发布、搜索、选片保留、选择新文件名下载、暂停/继续/取消、停止共享与实际传输路径记录。

## 服务器及安全边界

- 公网/局域网使用有效 TLS 证书：`-tls-cert`、`-tls-key`；程序不会关闭证书验证。
- 一个受控协作空间内的已连接设备能读取空间清单与共享文件。**没有账号、邀请审批或按文件 ACL**，不要直接作为匿名公共文件服务开放。用防火墙/受控网络限制参与者。
- TURN 开启 `-turn-listen 0.0.0.0:3478 -turn-public-ip <PUBLIC_IPV4>`；开放 UDP 3478 及默认分配端口 49160–49287。双方中转候选均须属于本服务端的活动会话。
- `-stun stun:<STUN_HOST>:3478` 可配置 STUN；未配置时仍尝试本地候选，失败走已配置中转。UDP 全面阻断时失败，不回退 TCP 文件传输。
- 本地 config.json 含恢复秘密。不要提交、复制给其他人或粘贴到日志；丢失它会失去旧 UUID 的恢复能力。Unix 文件权限为0600；Windows 应使用仅本人可访问的数据目录。
- 所有正式性能目标仍按需求文档保留，未测的目标不宣称达标。发布、合并和部署需另行确认。

## 文档

- [需求规格](docs/requirements.md)
- [架构和协议](docs/architecture.md)
- [测试与已知限制](docs/testing.md)
- [存储模块 1 GiB 实测](docs/storage-test-results.md)
- [协调服务10分钟负载](docs/control-load-results.md)
- [100MiB调度配对对照](docs/scheduler-results.md)
- [MIT License](LICENSE)
