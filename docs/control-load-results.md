# 控制面十分钟负载验收记录

需求：NFR-003，保留已接受目标：50 个已分配 UUID 的设备、500 个文件清单、5,000 条持有关系、10 请求/秒混合查询/上报、服务端处理耗时 p95 ≤500 毫秒，连续 10 分钟无未恢复写入错误。

测试对象为 `feat/coordinator-load-test` 分支上的 `internal/coordinator`，基础实现来自 `e1eb669`，十分钟用例编译自测试提交 `05ef702f036abcc8298e4932e84b92d37b40db33`。后续 `a8901c2` 仅将独立快速校验从 100 个请求调整为一个完整的十请求周期；十分钟用例及其共用 fixture/计时逻辑未变化。测试日期：2026-10-11 UTC。

## 真实环境和测量边界

- Debian GNU/Linux 13.6 (trixie)，Linux 6.18.44，linux/amd64 云容器
- Intel Xeon Platinum 8573C；Go 与系统均报告 9 个逻辑 CPU，`GOMAXPROCS=9`
- `/proc/meminfo` 报告总内存 10,206,504 KiB；cgroup CPU/内存配额文件不可读取，不能据此宣称独占资源或 2 核/2 GiB 配额
- Go 实际工具链为 1.27.2；模块声明最低版本为 1.26.0，本次没有使用 Go 1.26 执行
- Echo v5.4.0、modernc.org/sqlite v1.60.1；所有依赖以该提交的 `go.mod` / `go.sum` 为准
- SQLite 临时数据库放在 `/workspace/shared/tmp-control` 下的 overlay 文件系统；没有放入 `/tmp` tmpfs。单实例、一个数据库连接、WAL、foreign_keys=ON、busy_timeout=5000，均为实现默认配置
- HTTP 客户端、Echo 服务端和 SQLite 在同一测试进程，走真实 localhost TCP/HTTP。共 50 个逻辑设备，并非 50 台独立物理设备
- 不使用 TLS、公网、NAT、TURN 或文件传输；该测试不替代独立网络/Windows/中转验收。共享执行器在测量期间同时运行其他模块的编译/测试，并与约 05:49:44 UTC 启动的 1 GiB 后端往返试验重叠；所有受影响样本仍保留，没有声明隔离主机

本次结果只代表上述真实环境。需求中的 2 核、2 GiB Linux 参考环境没有被伪装为本次实测配置；不能将结果直接外推为该配置或生产部署的保证。

## 数据准备与负载构成

50 个设备通过实际 `POST /v1/register` 获得不同 UUID、设备密钥和会话令牌。之后每个请求都使用相应的 Bearer 令牌，不用 UUID 冒充凭据。保留生产默认的 64 会话上限、500 请求/秒全局限流、100 请求/秒每会话限流和 120 次/分钟注册限流。

测试编码固定四字节 `13 27 42 99` 的合成内容，用其真实编码结果生成 500 个不同名称和 ID 的合法清单；清单均经 `codec.ValidateManifest` 验证。它们描述同样的合成内容，并不是 500 份真实用户文件。每个清单有两个持有者，每个持有者初始报告五个分片，即恰好 5,000 条 SQLite 持有关系。各设备初始各有 100 条关系。

为避免准备阶段撞到生产 API 限流，500 个清单和 5,000 条关系由同包测试通过一个 SQLite 事务直接写入；会话哈希使用实际注册令牌生成。该准备事务、注册和初始心跳都不计入测量区间。测量期间没有直接数据库写入、时钟替换、延长在线期限或绕开认证/限流。

主负载固定每 100 毫秒调度一个请求，使用独立于响应完成时间的开放式到达计划，持续整整 600 秒，共 6,000 个请求：

| 主负载操作 | 占比 | 请求数 | 验证内容 |
| --- | ---: | ---: | --- |
| GET /v1/files | 20% | 1,200 | 全量读取 500 个清单、每个清单的两位在线持有者及其各五条关系 |
| GET /v1/files/:id | 30% | 1,800 | 检查返回清单 ID 和在线持有关系 |
| PUT /v1/holdings | 30% | 1,800 | 实际执行 DELETE/INSERT/COMMIT，交替更换五片集合，持有行数不变 |
| POST /v1/manifests | 10% | 600 | 幂等重发既有不可变清单，预期 204；不冒充新增清单写入 |
| POST /v1/heartbeat | 10% | 600 | 经认证刷新实际在线状态 |

另外每 100 毫秒调度一个维护心跳，轮询所有 50 个设备，使每个设备每五秒收到一次心跳。维护心跳与主请求相隔 50 毫秒，一共 6,000 个。维护心跳**不计入主负载的 10 请求/秒**，因此服务端实际承受合计 20 请求/秒，分别报告主负载和合并耗时。这样保留五秒心跳和十五秒过期的默认语义，同时完整满足混合业务负载，避免通过拉长在线期限降低负载。

每秒在服务器会话锁内检查所有设备在线状态，同时所有查询响应逐一检查所需持有者仍在线。测量前后都核对设备/清单/关系总数为 50/500/5,000；结束后执行 SQLite `integrity_check`。

## 计时与错误口径

- 从 HTTP 服务端进入 `Server.Handler().ServeHTTP` 到其返回，用 Go 单调时钟测量；包含 Echo 路由、认证、限流、锁等待、SQLite 操作和响应写出，不包含客户端 JSON 解析或公网 RTT
- 每类请求、主负载合计和含维护心跳的合计均记录服务端耗时；p95 使用 nearest-rank，即排序后第 `ceil(0.95*N)` 个值
- 记录每类 HTTP 状态码、调度最大迟到、最少在线设备数以及每秒抽样的最大心跳年龄
- 所有非预期状态、传输失败和响应校验错误均算错误；实际持有关系写入失败另计。测试不重试失败，所以任何失败写入都会算未恢复写入错误
- 为防止生成器用少发请求掩盖慢服务，断言 6,000 个主请求、12,000 个总请求、至少 600 秒且最多 601 秒完成，以及最大调度迟到小于 100 毫秒
- 测量未开启 race 检测；同一 fixture 的快速校验另行运行 race。初版快速校验在 race 下重复 100 个请求耗时 15.43 秒，因没有维护心跳触发真实在线过期而失败；改为覆盖整个十请求混合周期后通过，未修改十分钟验收函数、生产 TTL 或负载。进程峰值 RSS 含服务端和负载生成器，不是独立服务端峰值

## 复现命令

普通测试默认跳过十分钟用例。环境变量只控制是否执行，没有可缩短验收时长的开关。

```sh
export PATH=/workspace/shared/toolchains/go/bin:$PATH
export GOMODCACHE=/workspace/shared/gomodcache
export GOPATH=/workspace/shared/gopath
export GOCACHE=/workspace/shared/gocache

go test ./internal/coordinator -run 'TestControlPlaneLoad' -count=1 -v
go test -race ./internal/coordinator -run '^TestControlPlaneLoadFixture$' -count=1
go vet ./internal/coordinator

go test -c -o /tmp/p2pfs-control-load.test ./internal/coordinator
mkdir -p /workspace/shared/tmp-control
python3 -u - <<'PY'
import os, subprocess, resource, time
start = time.monotonic()
p = subprocess.run(
    ['/tmp/p2pfs-control-load.test',
     '-test.run', '^TestControlPlaneLoadAcceptance$',
     '-test.timeout', '12m', '-test.v'],
    env=dict(os.environ, P2PFS_CONTROL_LOAD='1',
             TMPDIR='/workspace/shared/tmp-control'))
print('exit_code:', p.returncode)
print('combined_server_and_generator_peak_rss_kib:',
      resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)
print('process_wall_seconds:', round(time.monotonic() - start, 3))
raise SystemExit(p.returncode)
PY
```

路径可按实际环境调整，但必须记录文件系统/硬件；不要移入 tmpfs 后仍宣称测试了磁盘 SQLite。

## 实际结果

**通过本次真实环境下的 NFR-003 负载验收。** 测量从 2026-10-11 05:41:46.163928511 UTC 开始，实际等待/完成时间为 600.001084596 秒；HTTP 结束后的统计输出时刻为 05:51:46.171257773 UTC。主负载 6,000 请求，维护心跳 6,000 请求；主负载实际平均 9.999982 请求/秒，微小差异来自末尾约 1.08 毫秒计时/关闭开销。

- 主负载服务端 p95：**27.071950 毫秒**，低于 500 毫秒目标
- 含额外维护心跳的全部请求 p95：25.567245 毫秒
- 请求/响应校验错误：0；未恢复写入错误：0；实际持有关系事务：1,800 次
- 最少在线设备：50；每秒抽样的最大心跳年龄：4.952604981 秒
- 最大调度迟到：39.661207 毫秒；没有丢弃计划中的请求
- 结束设备/清单/持有关系：50 / 500 / 5,000；SQLite integrity_check：ok
- 服务端与生成器合计进程峰值 RSS：30,292 KiB（约 29.58 MiB）；不含 Go 编译器，但不能解读为纯服务端 RSS
- 测试进程墙钟：600.115 秒；退出码：0

按操作统计，单位均为毫秒：

| 操作 | 请求数 | p50 | p95 | p99 | 最大 | HTTP 状态 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| 主负载 GET /v1/files | 1,200 | 25.566694 | 33.474116 | 46.650216 | 78.908729 | 200 × 1,200 |
| 主负载 GET /v1/files/:id | 1,800 | 0.235724 | 0.533416 | 1.139579 | 7.927873 | 200 × 1,800 |
| 主负载 POST /v1/heartbeat | 600 | 0.021306 | 0.049785 | 0.123102 | 0.383454 | 200 × 600 |
| 主负载 POST /v1/manifests | 600 | 0.166567 | 0.313527 | 0.718713 | 1.345234 | 204 × 600 |
| 主负载 PUT /v1/holdings | 1,800 | 0.361041 | 0.802157 | 1.575585 | 9.149768 | 204 × 1,800 |
| 额外维护心跳 | 6,000 | 0.021714 | 0.067672 | 0.238311 | 30.960935 | 200 × 6,000 |

最慢的全量文件列表需要读取 500 个清单及其关系，其最大服务端耗时仍为 78.91 毫秒。本次没有主动注入磁盘满、数据库损坏或 SQLite BUSY 故障；零错误说明此负载下没有出现写入失败，并不证明任何故障都已具备恢复能力。

### 追加检查

在快速校验修正后的最终测试代码上执行：

```text
go test ./internal/coordinator -count=1
ok github.com/Paulkm2006/p2pfs/internal/coordinator 0.378s

go test -race ./internal/coordinator -count=1
ok github.com/Paulkm2006/p2pfs/internal/coordinator 5.231s

go vet ./internal/coordinator
通过，无输出

git diff --check
通过，无输出
```

常规/race 整包检查均默认跳过十分钟用例；只有上文明确设置环境变量的独立进程执行了完整 600 秒测量。没有将短测试结果算作另一次负载验收。

### 原始十分钟输出

```text
test_fixture_commit: 05ef702f036abcc8298e4932e84b92d37b40db33
=== RUN   TestControlPlaneLoadAcceptance
    load_test.go:36: environment: go=go1.27.2 os=linux arch=amd64 logical_cpus=9 gomaxprocs=9 database=/workspace/shared/tmp-control/TestControlPlaneLoadAcceptance977743621/001/metadata.db
    load_test.go:42: environment: /proc/meminfo: MemTotal:       10206504 kB
    load_test.go:40: environment: /sys/fs/cgroup/cpu.max unavailable: open /sys/fs/cgroup/cpu.max: no such file or directory
    load_test.go:40: environment: /sys/fs/cgroup/memory.max unavailable: open /sys/fs/cgroup/memory.max: no such file or directory
    load_test.go:60: measurement_start_utc=2026-10-11T05:41:46.163928511Z duration=10m0s main_rps=10 maintenance_heartbeat_rps=10
    load_test.go:112: progress: elapsed=1m0.001s main_scheduled=601 maintenance_scheduled=600 min_online=50
    load_test.go:112: progress: elapsed=2m0s main_scheduled=1201 maintenance_scheduled=1200 min_online=50
    load_test.go:112: progress: elapsed=3m0s main_scheduled=1801 maintenance_scheduled=1800 min_online=50
    load_test.go:112: progress: elapsed=4m0s main_scheduled=2401 maintenance_scheduled=2400 min_online=50
    load_test.go:112: progress: elapsed=5m0.001s main_scheduled=3001 maintenance_scheduled=3000 min_online=50
    load_test.go:112: progress: elapsed=6m0.001s main_scheduled=3601 maintenance_scheduled=3600 min_online=50
    load_test.go:112: progress: elapsed=7m0.001s main_scheduled=4201 maintenance_scheduled=4200 min_online=50
    load_test.go:112: progress: elapsed=8m0s main_scheduled=4801 maintenance_scheduled=4800 min_online=50
    load_test.go:112: progress: elapsed=9m0.001s main_scheduled=5401 maintenance_scheduled=5400 min_online=50
    load_test.go:136: server main GET /v1/files: requests=1200 p50=25.566694ms p95=33.474116ms p99=46.650216ms max=78.908729ms statuses=map[200:1200]
    load_test.go:136: server main GET /v1/files/:id: requests=1800 p50=235.724µs p95=533.416µs p99=1.139579ms max=7.927873ms statuses=map[200:1800]
    load_test.go:136: server main POST /v1/heartbeat: requests=600 p50=21.306µs p95=49.785µs p99=123.102µs max=383.454µs statuses=map[200:600]
    load_test.go:136: server main POST /v1/manifests: requests=600 p50=166.567µs p95=313.527µs p99=718.713µs max=1.345234ms statuses=map[204:600]
    load_test.go:136: server main PUT /v1/holdings: requests=1800 p50=361.041µs p95=802.157µs p99=1.575585ms max=9.149768ms statuses=map[204:1800]
    load_test.go:136: server maintenance POST /v1/heartbeat: requests=6000 p50=21.714µs p95=67.672µs p99=238.311µs max=30.960935ms statuses=map[200:6000]
    load_test.go:139: measurement_end_utc=2026-10-11T05:51:46.171257773Z elapsed=10m0.001084596s main_requests=6000 maintenance_requests=6000 main_achieved_rps=9.999982
    load_test.go:140: summary: main_server_p95=27.07195ms combined_server_p95=25.567245ms errors=0 unrecovered_write_errors=0 min_online_peers=50 max_observed_heartbeat_age=4.952604981s max_schedule_lag=39.661207ms
    load_test.go:157: SQLite integrity_check=ok
--- PASS: TestControlPlaneLoadAcceptance (600.11s)
PASS
exit_code: 0
combined_server_and_generator_peak_rss_kib: 30292
process_wall_seconds: 600.115
```
