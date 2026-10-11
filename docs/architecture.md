# 实现架构与协议 v1

## 组件与依赖

一个 Go 根模块提供 `cmd/server`、`cmd/peer`、`internal/model`、`codec`、`coordinator`、`transport`、`client`。`desktop` 为单独 Wails 3 模块，复用根模块客户端，避免 Linux 服务端构建依赖桌面组件。

依赖锁定于 go.mod/go.sum：Echo v5、modernc SQLite、klauspost/reedsolomon、Pion WebRTC v4 和 TURN v4。Wails 版本由 desktop/go.mod 锁定。框架 API 按官方源码和文档核实。

## 身份、连接和信任

1. 首次 `/v1/register` 由服务端生成 UUID 与随机设备恢复秘密；SQLite 仅保存恢复秘密 SHA-256。
2. 客户端把 UUID 和恢复秘密写入本地 config.json。重连时提交二者证明恢复权限；UUID 本身不授予权限。
3. 每次注册生成新的随机会话 Bearer token，旧会话、信令和持有关系失效。会话 token 不持久化。
4. 所有元数据与 SDP 指纹通过 HTTPS；本机测试仅显式允许回环 HTTP。控制面不提供跳过 TLS 校验选项。
5. 对端身份以服务端认证后的信令发送者绑定；DTLS 使用 SDP 指纹验证。DTLS/SCTP 负责成熟的加密、重放保护、可靠交付和拥塞控制，不自行设计密码协议。

这是一个受控协作空间，没有账号/按文件 ACL/邀请审批。能注册的参与者可发现并下载该空间共享文件。部署者必须限制协调 API 接入范围；UUID 方案不等于匿名公网资源授权系统。文件摘要提供与可信清单的一致性，不构成作者签名。

## 控制 API

除注册与健康检查外都需要 `Authorization: Bearer <session>`：

| 方法 | 路径 | 作用 |
| --- | --- | --- |
| GET | /healthz | 健康检查 |
| POST | /v1/register | 分配/恢复 UUID，返回会话与 ICE 配置 |
| POST | /v1/heartbeat | 更新会话在线状态 |
| POST | /v1/manifests | 发布经验证的不可变清单 |
| GET | /v1/files | 文件与在线来源列表 |
| GET | /v1/files/:id | 指定版本及在线来源 |
| PUT | /v1/holdings | 替换当前会话对指定版本的持有索引；空数组撤销 |
| POST | /v1/signals | 发送 offer/answer；服务端覆盖 From |
| GET | /v1/signals | 取出当前会话的待处理信令 |

心跳 5 秒，在线过期 15 秒；来源查询实时过滤过期/旧会话。服务端重启会清空活跃会话，客户端自动注册恢复并重新验证、上报本地分片。SQLite 不保存文件内容。

## 文件与 RS

清单格式固定 `format=1`，编码 `rs-vandermonde-gf256-k4m2-v1`。四个连续数据分片，各长 `max(1, ceil(size/4))`，末尾补零；另有两个校验分片。一个文件就是一个 RS 组，传输帧不是独立 RS 分片。

清单绑定显示名称、原长、整文件 SHA-256、编码参数和六个分片的索引/长度/SHA-256；ID 为去掉 ID 字段后的规范 JSON SHA-256。结构、参数、名称、索引、大小和摘要必须通过严格验证。名称仅用于显示，不参与缓存路径。

编码与重建采用流式 256 KiB block；缓存位于 `cache/<manifestID>/<index>.shard`。接收时写随机临时文件，完整分片经长度与摘要验证才发布到缓存。重建忽略坏片，至少四个不同有效索引才继续；最后再次验证文件长度和摘要，以不可覆盖方式创建目标。原文件不会被删除。

## UDP 数据路径

- 网络仅允许 UDP4/UDP6 ICE 候选；禁用 ICE TCP。
- 先建立不含 TURN 的 direct-only ICE 连接，可使用 STUN 映射地址。
- 连接在默认 8 秒内失败则重新建立 relay-only 连接；offer/answer 两端均强制服务端 UDP TURN。
- 只在数据写入前进行直连到中转的连接回退。中途传输失败时丢弃未完成片，重新发现/换源；不会把部分直连数据拼到新中转传输。
- 每个分片使用可靠有序 DataChannel；单消息 16 KiB，发送与接收缓冲有界，接收背压传到 SCTP。完整消息会被 SCTP/DTLS 分段为 UDP 数据报，16 KiB 不是 UDP 单报文大小。
- 协议交换请求、声明长度、二进制块和结束帧。超长/不足/错误帧拒绝；上层最终按清单校验。
- 实际 selected ICE candidate pair 决定界面与日志的 direct/relay 标记，不从“已配置中转”推断路径。
- 默认32个并发会话、每 Peer 最多8个、独立传输库默认会话最长10分钟；客户端为慢速大文件设为24小时并另加30秒无载荷进展超时；上传负载可设置 bytes/s。取消与停止共享会关闭对应传输，重复 offer 有有界重放记录。

## 服务端 TURN 边界

TURN 身份绑定协调活动会话；只有本服务端上仍活跃的分配地址之间可以互转。每个 UDP 目标的 IP **和端口**均核验，不允许对任意互联网/内网 UDP 地址开放代理。默认128分配、每会话8个、全局64MiB/s、每会话8MiB/s和90秒空闲过期。中转仅处理端到端 DTLS 密文，不把中转当作分片持有者。

必须开放 TURN 监听 UDP 端口和分配端口范围。双端 UDP 全面受限时不能完成；无文件 TCP 回退。公网 NAT 类型、实际丢包环境及真实设备验证仍是单独验收。

## 调度、任务与磁盘

下载优先缺失且稀缺的独立索引，尽量分散到不同 Peer，再依据成功吞吐 EWMA 与近期失败惩罚选择来源。`-fixed` 禁用稀缺度/网络评分供对照。最多同时获取配置数量的分片，轮次结束后刷新来源；不足时保留任务等待，每两秒重新发现。当前策略不是最优调度证明，仍需按 NFR-004 五轮实测。

数据目录通过操作系统文件锁保证单实例使用，关闭幂等且等待已接受的操作完成后释放锁。任务状态与目标、有效片记录持久化；重启恢复为暂停，继续时重新校验。暂停、取消可停止新任务与活跃网络；已校验完整片保留，半片不复用。开始输出检查已有路径与写权限，按所需额外缓存、恢复临时片和输出进行保守空间检查。磁盘空间不是预留，其他进程仍可能耗尽空间，实际写入错误必须保留为失败。

停止共享先禁用本地服务，再撤销持有关系；删除缓存拒绝活动任务和并行发布/保留操作，并拒绝符号链接目录。文件操作只面向私有数据目录，操作系统权限仍应限制其他用户修改该目录。

## 日志与指标口径

JSON CLI 日志包含任务/文件ID、分片索引、来源UUID、决策分数、direct/relay路径、载荷字节、耗时和结果。`Task.Bytes` 是应用实际收到的分片载荷（含最终失败尝试），不是 UDP/SCTP 重传总字节；`CompletedShards` 是经完整校验的进度。现有统计不替代抓包、链路重传统计或服务端全量性能测量。

## 官方参考

- [Echo v5](https://echo.labstack.com/docs/quick-start)
- [Wails 3](https://v3.wails.io/reference/application/)
- [Pion WebRTC](https://github.com/pion/webrtc)
- [Pion TURN](https://github.com/pion/turn)
- [Reed–Solomon streaming API](https://github.com/klauspost/reedsolomon#streaming-encodingdecoding)
