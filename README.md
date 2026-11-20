# gosteal — 工作窃取任务并行运行时

一个不依赖 Go 自带协程调度、自行管理工作线程与任务队列的工作窃取（work-stealing）
运行时，配套一个实时展示调度过程的网页。最初的动机是围棋道场复盘软件：一盘棋每
一手都要做胜率分析，单线程要等十几分钟，而机器的八个核只用了一个。

## 核心设计（对照需求）

| # | 需求 | 实现 |
|---|------|------|
| 1 | 每线程一个双端队列，自己队尾取、别人队头偷，Chase-Lev 无锁算法 | `internal/ws/deque.go`：每个 `Worker` 一个 deque，owner `push/pop` 走 bottom，窃取者走 top，全部用原子操作，热路径无锁 |
| 2 | 队列满自动扩容，扩容时并发窃取不出错 | **分段稳定槽位存储**：扩容只追加新 segment，永不复制/搬移现有任务指针，逻辑索引到槽位地址的映射终生不变，从根上消除复制式扩容的双数组读窗口 |
| 3 | 任务派生子任务并等待，等待时执行别的任务而非干等 | `TaskCtx.Spawn` / `Wait` / `WaitChildren`（`task.go`）：等待走 help-forward，先本地 pop 再去偷，实在没有才 park |
| 4 | 窃取对象随机选，连续偷不到逐步退避 | `pickVictim` 均匀随机选其它线程；`trySteal` 失败先 `procyield` 自旋再 `Gosched`，纯用户态退避，park 只在外层 |
| 5 | 一次偷一半 vs 一次偷一个，可对比 | `StealHalf` / `StealOne` 两种策略，运行时可切换，基准与页面都能对比 |
| 6 | 统计每线程忙碌/窃取/空闲时间占比 | `WorkerStats` 的 `BusyNanos/StealNanos/IdleNanos`，页面用堆叠条展示 |
| 7 | 并行循环按区间递归二分派生，粒度可调 | `TaskCtx.ParallelFor(lo, hi, grain, ...)`，区间递归二分，叶子不超过 grain |
| 8 | 任务错误回传给等待者 | 错误沿父子链记录，panic 被包成 `PanicError`，`Wait` 返回链上第一个错误 |

### 关于「不依赖 Go 协程调度」

工作线程是常驻 goroutine，但**任务的入队、出队、负载均衡、等待与唤醒全部由本包
实现**，Go 运行时只负责把 goroutine 映射到 OS 线程。最初用 `runtime.LockOSThread`
绑死线程，实测会导致 Go 运行时反复创建 M、严重抖动，因此移除——自己管任务调度并
不需要锁 OS 线程。

### 为什么不是经典的复制式扩容

严格按 Chase-Lev 论文实现复制式环形数组后，`go test -race` 的高强度交错测试会偶发
丢任务：owner 基于旧数组快照读槽位时，扩容把元素复制到了新数组，旧槽位被清空，
形成两个数组的读窗口。最终改为**只追加、不搬移的分段槽位**：segment 表通过 CAS 追加，
任何逻辑索引在任何时刻只有一个稳定地址，窃取者与 owner 不可能读到同一份任务的两个副本。

## 目录结构

```
internal/ws/        运行时核心
  deque.go          Chase-Lev 工作窃取双端队列（分段稳定槽位）
  task.go           Task / TaskCtx、Spawn/Wait、ParallelFor
  pool.go           Pool、工作线程、窃取策略、退避、停车唤醒、统计
  monitor.go        任务树、父子关系、窃取记录环形缓冲
  procyield_*.s     arm64/amd64 的 PAUSE/YIELD 自旋原语
internal/bench/     三个负载 + 正确性/加速比扫描
  go_board.go       9x9 围棋：落子、提子、禁入点、劫、计分
  go_search.go      每手随机模拟（playout）胜率分析
  mergesort.go      并行归并排序
  matmul.go         矩阵乘
  sweep.go          1..N 线程 × 两种窃取策略，逐结果与单线程比对
internal/web/       仪表盘 HTTP 服务与内嵌静态页
cmd/dashboard/      可视化页面入口
internal/bench/cmd/bench/  命令行基准入口
```

## 使用

### 实时调度页面

```bash
go run ./cmd/dashboard -workers 8 -addr :8080
# 浏览器打开 http://localhost:8080
```

页面分五块，每 500ms 轮询 `/api/state`：

1. **工作线程**：状态、本队队列长度、忙碌/窃取/空闲堆叠占比、执行/偷来/被偷计数、自旋与停车次数
2. **队列长度**：每个 deque 的实时长度条形图
3. **窃取记录**：受害者、小偷、批量大小、任务 ID 的滚动日志
4. **加速比**：实测加速比曲线叠加理想线性参考线（SVG 手绘，无前端依赖）
5. **任务树**：Spawn 派生树，标注运行状态、执行线程、是否被窃取

页面上可切换负载（围棋/归并/矩阵乘）与窃取策略（偷一半/偷一个），并能现场跑 1..N
加速比测量。

### 命令行基准

```bash
go run ./internal/bench/cmd/bench -workers 8            # 三个负载全跑
go run ./internal/bench/cmd/bench -workload matmul -json
go run ./internal/bench/cmd/bench -workload go -out result.json
```

输出每个负载在 1..8 线程、两种窃取策略下的耗时、加速比、并行效率，并对每次并行结果
与单线程参考实现做**逐元素精确比对**，任一不一致即以非零码退出。

## 实测结果（8 核，darwin/arm64）

矩阵乘（CPU 密集、任务均衡）近线性：

```
workers  steal   elapsed(ms)  speedup  efficiency  correct
1        half        23.02     0.98x      98.3%    true
2        half        11.70     1.93x      96.7%    true
4        half         6.64     3.41x      85.3%    true
8        half         3.66     6.19x      77.3%    true
```

围棋搜索与归并排序因任务粒度/串行归并段占比，8 线程约 2.3x–3.5x，全部 `correct=true`。
两种窃取策略结果完全一致，差异只在调度开销。

## 正确性与竞态保证

```bash
go test ./...                 # 功能与结果一致性
go test -race ./...           # 竞态检测器，全部通过、无报告
```

- 三个负载都有确定性的串行参考实现；并行结果在 1/2/4/8 线程、两种策略下逐元素比对，
  必须完全相同（`parity_test.go`、`sweep.go`）。
- 围棋的随机模拟用 `(seed, 手数, 模拟序号)` 派生 RNG，结果只取决于输入，与调度顺序、
  goroutine 身份无关，重复运行结果逐字节一致。
- deque 级测试覆盖：顺序 LIFO、跨多个 segment 的扩容、6 窃取者并发消费不重不漏、
  owner 与窃取者高速交错、批量窃取。
