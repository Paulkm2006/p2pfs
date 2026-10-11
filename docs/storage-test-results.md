# RS 存储测试记录

测试日期：2026-10-11，约 05:21–05:28 UTC。初次执行对象为 `feat/mvp` 工作树中的 `internal/codec`；随后转入独立 `feat/rs-storage` 分支，补充缓存根目录符号链接检查并重新执行包测试。本记录仅覆盖存储组件，不能当作完整客户端、网络传输、Windows 实机或全部 NFR 的验收结论。

## 环境和版本

- Debian GNU/Linux 13.6 (trixie)，Linux 6.18.44，linux/amd64 云容器
- 实际执行工具链：`go version go1.27.2 linux/amd64`；模块声明的最低 Go 版本为 1.26.0。两者不是同一个版本，未以 Go 1.26 实测替代记录
- `/proc/cpuinfo` 报告 Intel Xeon Platinum 8573C、9 个逻辑处理器；容器未提供完整 sysfs CPU 拓扑，因此不推断独占 CPU 配额
- `/proc/meminfo` 报告总内存 10,206,504 KiB；该数值不代表独占的容器内存承诺
- 输入、缓存、恢复临时分片和输出位于同一 overlay 文件系统；测试前该卷约有 29 GiB 可用空间
- RS 依赖：`github.com/klauspost/reedsolomon v1.14.1`；`github.com/klauspost/cpuid/v2 v2.3.0`
- 编码固定为 GF(256) Vandermonde，k=4、m=2；流式块大小 256 KiB；每片长度 `max(1, ceil(原文件长度/4))`，末尾补零
- 格式标识 `rs-vandermonde-gf256-k4m2-v1`；文件、每个完整分片和不含 ID 字段的规范清单均采用 SHA-256

## 已执行检查

以下命令全部通过。设置仅反映本次容器路径；其他环境可以替换工具链和缓存路径。

```sh
export PATH=/workspace/shared/toolchains/go/bin:$PATH
export GOMODCACHE=/workspace/shared/gomodcache
export GOPATH=/workspace/shared/gopath
export GOCACHE=/workspace/shared/gocache
go test ./internal/codec -count=1 -v
go test -race ./internal/codec -count=1
go vet ./internal/codec
GOOS=windows GOARCH=amd64 go test -c \
  -o /tmp/p2pfs-codec-windows.test.exe ./internal/codec
```

独立分支上的存储 race 复验输出为 `ok github.com/Paulkm2006/p2pfs/internal/codec 1.755s`。Windows 命令只验证交叉编译，不表示已在 Windows 上运行。普通测试默认跳过 1 GiB 用例；该用例已经按下一节单独启用执行。

测试清单：

- `TestEveryFourShardSubset`：0 字节、1 字节、17 字节、1,048,589 字节，分别遍历全部 15 种不同四片组合，共 60 次逐字节恢复检查；覆盖跨多个流式块的数据
- `TestCorruptAndPartialShardsDoNotCount`：同长度损坏、截短分片排除；其余四片可恢复，只有三片时失败且不生成最终文件
- `TestManifestValidation`：格式、k/m、大小、摘要、分片顺序/重复/越界、补齐长度、不可变 ID、路径穿越和 Windows 保留名
- `TestDeterministicEncodingAndVersionIsolation`：重复编码稳定；同名异内容版本隔离；不覆盖既有损坏缓存条目
- `TestNoOverwriteAndFinalDigestCheck`：目标冲突不覆盖；整文件摘要错误时不提交输出且清理临时文件
- `TestCancellationAndSourceMutation`：启动前/流中取消；清理未提交分片；源文件大小和 mtime 保持不变时仍由第二次内容散列发现修改
- `TestConcurrentDestinationCreationDoesNotOverwrite`：在最终原子提交前由另一操作创建目标文件，原内容保留
- `TestEmptyManifestRequiresDeterministicPadding`：拒绝不符合确定性零补齐的空文件清单
- `TestOversizeAndNonregularSource`：大于 1 GiB 和非普通文件拒绝
- `TestSymlinkedShardAndCacheRejected`：符号链接分片、版本缓存目录和缓存根目录不计入有效来源；编码拒绝符号链接缓存根目录
- `TestLargeFileRoundTrip`：独立启用的 1 GiB 四片恢复及整文件散列/长度复核

与需求的主要对应：FR-005/006/007/011/016/017/020，AT-02/04/10/11，以及 NFR-001、NFR-005 的存储组件证据。磁盘满故障注入、完整客户端峰值、网络故障和 Windows 实机验收未由这些测试覆盖。

## 1 GiB 资源测量

输入长度恰为 1,073,741,824 字节，通过稀疏文件创建。内容主要是零，在偏移 0、268,435,456、536,870,912、805,306,368 和 1,073,741,820 写入 `01 25 a9 ff` 四字节标记。编码得到六个完整分片后删除索引 1 和 3，从索引 0、2、4、5 恢复。最后再次读取输出并独立检查长度与 SHA-256。

稀疏零输入的读取成本和可压缩性可能影响耗时，不能将本次时间外推成随机内容、机械磁盘、实际网络或 Windows 的吞吐承诺。程序没有按输入文件大小分配内存，输出/分片由普通顺序写入产生；这些数字主要用于验证存储流程的内存量级。

为避免把 Go 编译器的内存算入被测进程，先生成测试可执行文件，再启动它。测试临时文件放在 overlay 卷，而非 `/tmp` 的 tmpfs。实际测量命令如下：

```sh
export PATH=/workspace/shared/toolchains/go/bin:$PATH
export GOMODCACHE=/workspace/shared/gomodcache
export GOPATH=/workspace/shared/gopath
export GOCACHE=/workspace/shared/gocache
go test -c -o /tmp/p2pfs-codec.test ./internal/codec
mkdir -p /workspace/shared/tmp-rs
python3 - <<'PY'
import os, subprocess, resource, time
start = time.monotonic()
p = subprocess.run(
    ['/tmp/p2pfs-codec.test', '-test.run', '^TestLargeFileRoundTrip$', '-test.v'],
    env=dict(os.environ, P2PFS_LARGE_TEST='1', TMPDIR='/workspace/shared/tmp-rs'))
print('exit_code:', p.returncode)
print('storage_process_peak_rss_kib:',
      resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss)
print('wall_seconds:', round(time.monotonic() - start, 3))
raise SystemExit(p.returncode)
PY
```

原始结果：

```text
=== RUN   TestLargeFileRoundTrip
    resource_test.go:51: encode 1 GiB: 4.306189022s
    resource_test.go:62: restore from 0,2,4,5: 3.042574827s
--- PASS: TestLargeFileRoundTrip (8.38s)
PASS
exit_code: 0
storage_process_peak_rss_kib: 11716
wall_seconds: 8.383
```

Linux 的 `ru_maxrss` 单位是 KiB；11,716 KiB 约为 11.44 MiB，是整个存储测试进程生命周期的峰值，不是分别采样的编码/恢复峰值。Python 测量进程和 Go 编译进程不包含在该值内。单次通过不是统计分布，也不证明完整客户端在网络/UI/并发任务下满足 ≤512 MiB 的 NFR-005。

05:28 UTC 在独立 `feat/rs-storage` 分支、实现提交 `1d521b5` 上使用相同命令重新编译和测量，结果如下。该复验包括新增的缓存根目录检查；测试数据和测量限制不变。这两次存储检查不是要求的五轮网络性能对照实验。

```text
=== RUN   TestLargeFileRoundTrip
    resource_test.go:51: encode 1 GiB: 4.105724145s
    resource_test.go:62: restore from 0,2,4,5: 3.456300524s
--- PASS: TestLargeFileRoundTrip (8.61s)
PASS
exit_code: 0
storage_process_peak_rss_kib: 11564
wall_seconds: 8.62
```

## 已知边界

- 输出使用同文件系统硬链接原子提交，已有目标永不覆盖。目标文件系统必须支持硬链接；不支持时明确报错，不降级为可能覆盖目标的重命名
- 源文件检查结合文件身份、长度、mtime 和第二次 SHA-256；这是正常并发修改检测，不提供对恶意本机进程不断篡改再还原内容的文件系统快照隔离
- 清单必须仍来自经验证的可信控制通道。摘要校验不能独立证明发布者身份
- 缓存和目标目录是用户授权的本机位置。不能把目录本身可被恶意本机进程替换的环境当作隔离沙箱
