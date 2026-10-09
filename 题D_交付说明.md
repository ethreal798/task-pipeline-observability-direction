# 题 D：任务管线可观测 —— 交付说明

> 场景：一段动画素材的预处理任务，含多个耗时步骤（`Ingest → Transcode → FrameExtract
> → OCR/字幕 → Vectorize → RenderPreview`，含分支）。真实计算全部 MOCK，专注
> **长任务执行 / 状态管理 / 失败恢复 / 可观察性 / 结果复用**。
> 技术底座：FastAPI + Celery(Rabbit/Redis) + PostgreSQL + SQLAlchemy。
> 关键取舍：核心编排与 Celery **解耦**（`Backend` 抽象），因此失败恢复逻辑可被
> 单元测试直接验证，也更「简单、可控、可解释」。

---

## 1. 实现方式与时间规划

**总体思路**：不写通用工作流引擎，只做「任务 = DAG of steps」这一件事；用一个
`Backend` 抽象把「调度逻辑」与「真正到哪执行」分开——

- **CeleryBackend**：生产，step 投递给 Celery worker 异步执行，Beat 跑 reaper；
- **SyncBackend**：测试/演示，进程内顺序执行（同一份 `scheduler` 代码）。

这带来两个免费收益：(a) 失败恢复逻辑可用 pytest 直接验证，不依赖 broker/worker 起没起；
(b) 换执行后端（如改插队/加并发限制）只改一个文件。

| 阶段     | 内容 |
|--------|---|
| 0.5 天  | 架构确认：状态机、失败矩阵、数据模型、调度模型 |
| 0.5 天  | scheduler 核心（DAG 推进/重试/超时/断点续跑/复用）+ MOCK 处理器 |
| 0.5 天  | 失败场景单测（9 用例全绿） |
| 0.5 天  | FastAPI + history 查询 + 极简观测页 |
| 0.25 天 | Docker 编排 + README + 交付说明 + 公网部署 |

---

## 2. 长时间任务执行 / 状态管理 / 失败恢复 —— 设计考量

### 状态建模
- **Task**：`PENDING → RUNNING → SUCCEEDED / FAILED / CANCELLED`
- **Step**：`PENDING → RUNNING → SUCCEEDED / FAILED / RETRYING / SKIPPED / RECOVERED`

集中在 `state_machine.py` 一张转移表里，任何非法转移（如 `SUCCEEDED→RUNNING`）直接
`raise`——这是「可解释正确性」的基石。

### 失败场景矩阵（步骤级重试 / 恢复 / 复用）

| 场景 | 触发点 | 恢复行为 |
|---|---|---|
| 单步业务失败 | step 抛异常 | 指数退避重试至 `max_attempts`，过程中 `RETRYING` |
| 重试耗尽 | `attempt == max_attempts` | step `FAILED`，task `FAILED`，error 精确定位到 step |
| 步骤超时 | 处理器超时注入 | 走失败→重试；重试从 `resume_from` 续传而非整体重来（见失败案例 5） |
| **进程中断** | 心跳 `heartbeat_at` 过期 → stale | reaper 巡检：已完成兄弟 step 保持并复用，悬挂 step 重新调度续跑 |
| worker 崩溃 | 当前 step 悬挂 | 重新投递 at-least-once，靠幂等/结果复用消化重复执行 |
| 依赖步骤失败 | 上游 FAILED | 下游保持 `PENDING` 不推进（可解释的阻塞） |
| 重跑/恢复 | `/replay` | 输入(参数+deps哈希+版本)未变的已完成 step 直接 `SKIPPED` 复用历史结果 |

### 结果复用的关键设计
`result_hash = sha256(step_key + params + 依赖结果哈希 + 处理器版本)`。只要这三个
输入没变，历史结果就可复用 —— 这是**断点续跑**与**重放**都能做到「不白跑」的原因。

### 断点续跑（进程中断）
每步「成功落库」即天然 checkpoint。`recover_stale`（Beat 每 N 秒）只做两件事：
已完成兄弟 step 补一条 `RECOVER` 事件确认复用；把 stale 的 step 置回 `PENDING`
重新调度。**不重头跑**，只从断点继续。

### 长任务的设计侧重：进度上报 + 心跳续租 + 增量 checkpoint（断点续传）
时长任务如果只是"开始/成功/失败"三个瞬间打点，reaper 会把"还在跑的慢 step"误判成
崩溃（见失败案例 6）。因此对长任务是加法**侧重**，核心是"执行中持续可见 + 从断点继续"：

- **分片执行**：MOCK 处理器把 `duration_ms` 按 `_PROGRESS_SLICE_MS=100ms` 切成 N 片循环，
  每完成一片回调 `report_progress(done, total)`；参数 `params.duration_scale≥1` 可整体放大
  时长，让进度条/续传在演示里可见（长任务演示入口）。
- **心跳续租**：`execute_step` 注入的 `report_progress` 用**独立 SessionLocal** 写库，每次
  回调必刷 `heartbeat_at` —— 执行全程心跳持续刷新，reaper 不再把"还活着"的长任务当 stale
- **断点续传（增量 checkpoint）**：`execute_step` 开头取 `resume_from = step.progress`，处理器
  直接从已做的 `done` 片继续（`duration_ms` 按已做比例回减），跳过已消耗时间。统一覆盖
  「单步失败重试」与「进程中断恢复」两条路径；`replay` 场景在 `replay_task` 清空 `progress`，
  避免"重跑已变分支却从终点续传"。相比"每步成功落库即天然 checkpoint"的粗粒度，这是
  **step 内部更细的 checkpoint**。
- 演进方向（秒级→小时级，MVP 未实现）：细 step 需更粗聚合、heartbeat 独立线程、stale 窗口
  拉长、取消语义、GPU 资源池 / `QUEUED` 状态（见失败案例·GPU）。

**实测验证（单测 + 公网）**：新增 4 个单测（进度落地 / duration_scale 放大 / resume_from 续传 /
replay 清进度），全套 **14 passed**（`d\.venv\Scripts\python.exe -m pytest tests/ -q`）；公网
`steps` 表 `ALTER TABLE ADD COLUMN progress JSONB` 后 POST `{params:{duration_scale:8}}` 的
`animate_prep` 长任务，整单 `SUCCEEDED`，每 step 进度落库（如 `frame_extract {done:48,total:48}`、
`duration_ms≈4998`）、心跳续租生效、`frame_extract` 的 `attempt=2` 重试保留，全程无报错。

---

## 3. 如何定义任务步骤与依赖

- 每个 step 有唯一 `step_key` 与显式的 `parent_keys`（DAG 的边），在 `graph.py`
  里以模板声明（如 `frame_extract` 依赖 `transcode`；`render_preview` 和
  `ocr_subtitle` 都依赖 `frame_extract`，形成分支）。
- `schedule_ready` 按拓扑逐层推进：仅当某 step 的**所有依赖**处于成功态
  （`SUCCEEDED / RECOVERED / SKIPPED`）才把它投递执行；任一依赖失败则它保持
  `PENDING` 不推进。
- 选显式依赖而非通用 DAG 引擎，理由：每个 step 边界清晰、可单独暂停/重跑/复用，
  也便于在文档里论证「哪些动画管线步骤适合独立成可恢复任务」。

**哪些动画步骤适合独立成可恢复任务**：耗时且可分割、结果可缓存、天然可断点续传者
——例如 转码、抽帧、OCR、向量化。这些步骤执行分钟级、失败点局部，一旦独立成 step
并缓存结果，断点续跑和复用价值最大。反之，像「渲染成片」这种强耦合、失败影响全局
的，宜作为一个大 step 并配粗 checkpoint。

---

## 4. 持久化了哪些状态，为什么

| 内容 | 落库原因 |
|---|---|
| Task.status / 生命周期时间 | 上游随时可见整条任务走到哪 |
| Step.status / attempt / max_attempts | 单步进度、重试到哪一步 |
| Step.parent_keys | 依赖关系（可解释 + 供调度） |
| Step.result_ref / result_hash | **结果复用与断点续跑的数据基础**，不重跑 |
| Step.progress (`{done,total,phase}`) | **执行中进度快照**：既供上游观测(进度条/当前完成度)，也是**断点续传的恢复起点**(resume_from) |
| Step.heartbeat_at | stale 判定的唯一依据 |
| StepRun(每 attempt 一条事件) | 状态变更流水 → `/history`，可追溯 |
| error_info | 失败定位 |

**为何全部放 PG 而非只有 Redis**：任务需要「崩溃后仍在」的权威状态；Redis 队列只在
途消息，重启/崩溃会丢，无法单独充当恢复依据。PG 是真相源，Redis 只当 broker。

---

## 5. AI coding 工具 / token 用量 / 最大开销点

- **AI coding 工具**：TRAE（TraeCode / 本 IDE 内建 Agent，主力 `DeepSeek-V4-Flash`）。
- **Token 用量（按本次对话从架构到交付的完整会话估算）**：
  TRAE 侧无法精确导出会话 token，这里按**分环节工作量估数量级**：

  | 环节 | 估算 token 占比 | 说明 |
  |---|---|---|
  | 架构设计 + 多次 AskUserQuestion 澄清（长任务恢复/可观测方案） | ~20% | 状态机、失败矩阵、进度事件方案的反复确认 |
  | scheduler/processor/API 核心代码生成与迭代 | ~30% | DAG 推进/复用/重试 + 进度上报/续传/续租 |
  | **生产排错——`schedule_ready` 异步死循环熔断管线** | ~25% | 本地全绿→生产必现，redis 队列 0→100129 的远程排查与修复 |
  | 断点续传 + 进度条 + 长任务演示（duration_scale）新增 | ~15% | 增量实现 + 4 个新单测 |
  | Docker 编排 + 公网部署 + 迁移 + 交付说明 | ~10% | beat OOM 处理、steps 加 progress 列、文档 |
  | **合计** | **~1.2M tokens 量级** | 整段会话含 14 个单测全绿 + 线上长任务实测 |

- **最大开销点**：仍是 **生产部署后复现的 `schedule_ready` 异步死循环**（见失败案例 3）——
  它与架构澄清并列为两个 token 高峰。首次是"本地绿 ≠ 生产对"的执行模型不一致，反复推理、
  复现、修复、回归消耗最大；其次是"超卖式"的架构澄清轮次（进度事件方案、续传语义、GPU）。
- 相对 A 题（retrieve 调优/网关排错为主），D 题 token 更多花在**「异步执行模型的正确性」**
  与**「进度/续传新机制的落地」**上，排错主题从"数据/网关"转向"并发正确性"。

---

## 6. 个人时间花最多的部分（不算 LLM 直接做的部分）

个人精力主要集中在两大块：

1. **Spec / 方案设计——与 AI 交互后对架构、实现难点、细节反复考究**（约 55%）：
   长任务"恢复/可观测"方案的取舍（进度事件 vs 真实 checkpoint、续传起点语义、心跳 vs stale
   判定、进度节流与事件流水）、状态机与失败矩阵如何落到状态图里、`schedule_ready` 的推进/复用
   拓扑、"哪些步骤该独立成可恢复 step"。这些不是"直接生成代码"，而是先想清楚边界再让 AI 落地，
   占了最大人力。
2. **代码细节审查 review + 跑通全流程**（约 40%）：
   对 AI 生成的多处实现细节逐一 review（幂等保护、`submitted` 集合防死循环、`expire_all`
   重扫、`resume_from` 覆盖失败重试/崩溃恢复两条路径、replay 清 progress 防续传残留），
   并在本地跑通单测 → 公网跑通长任务全流程做实证，而不是只信代码"看起来对"。
3. **Docker 编排与部署联调**（约 5%）：数据库健康检查、beat/worker 环境变量、OOM 处理、
   steps 加 `progress` 列迁移

---

## 7. 失败案例分析：做错 / 做不了

1. **陈旧 session 重复提交**：`SyncBackend` 用独立 session 改库，外层调度器读到的 step
   还是旧 `PENDING`，导致同一 step 重复投递、报非法状态转移。→ 每次投递后
   `expire_all()` 与 DB 重同步再重扫。

2. **异步后端下 `schedule_ready` 死循环，熔断整条生产管线**（本次最伤的教训）：
   - **现象**：公网部署后开局即卡死，redis 队列从 0 涨到 **10 万**，solo worker CPU 恒 77% 空转。
   - **根因**：`backend.submit(step)` **不改 DB 里的 PENDING 状态**（worker 异步才接手），所以
     `schedule_ready` 的 while 重扫发现同一步仍是 PENDING 又再 submit → 无限 re-submit。
   - **为什么本地没测出来**：单测全走 `SyncBackend`，submit 时同步执行、立刻变 SUCCEEDED，
     重扫时已非 PENDING。**测试模型(同步) ≠ 生产模型(异步)**，10 个单测全绿但生产必现。
     教训：**「本地绿 ≠ 生产对」，回归用例必须用与生产一致的执行模型**。
   - **修复**：本轮用 `submitted` 集合记录已投递的 step，重扫跳过；并补一个「假异步后端」单测锁定只投一次。

3. **两个明确"做不了"**：
   - **GPU 任务完全没做**：`exec_meta` 的资源字段（GPU id / 排队时长 / 显存）仅设计预留，无真实 GPU 任务与资源池调度。
   - **线上「进程崩溃 → 自动恢复」没有真实验证**：该链路靠 beat 定时驱动 `recover_stale`，而服务器因 OOM 停用了 beat；
     所以线上只实测了正常长任务 + 心跳续租，没实测真 kill worker 后能否自动 recover（只在单测里手动调过期心跳模拟）。

---

## 8. 任务执行时间从「秒级」扩展到「小时级」后，系统设计需要发生哪些变化

> 推演章节：MVP 以秒级任务为基准设计并实测。一旦动画预处理来到真实**小时级**
> （如整部番的转码、逐帧 OCR、长镜头向量化），当前的「进度回调 + 心跳 + 续传」机制
> 有几处会从「够用」变成「失配」。这里逐条列出**必须发生的设计变化**，并标注 MVP
> 已具备 / 需要新增。

### 8.1 核心矛盾：从「秒级够用」到「小时级失配」的三条主线

| 维度 | 秒级（现有 MVP） | 小时级（必须变化） |
|---|---|---|
| **DB 写入频率** | 每次 `report_progress` 一条进度；秒级总量小，可接受 | 小时级若持续高频打点，会写入数十万条，把 PG 当 event log 打爆 |
| **心跳来源** | `report_progress` 回调同步刷 `heartbeat_at`，与处理进度强耦合 | 若某个计算片段**长时间不调回调**（GPU 单算子跑几十分钟），心跳会停 → 被 reaper 误杀。心跳必须与计算进度**解耦** |
| **断点粒度** | `{done,total}` 只记「第几片」，中间产物在内存，restart 进程后要回放 | 小时级重启进程是常态，中间产物必须**落盘**，resume 从磁盘恢复而非重跑已做片 |

### 8.2 与 8.1 三个维度一一对应的设计变化

**① 心跳来源 → 心跳与进度解耦为独立心跳线程（对应「心跳来源」）**
- **现状**：`execute_step` 注入的 `report_progress` 每次回调刷 `heartbeat_at`（[scheduler.py](file:///c:/Users/xie/Desktop/AI应用笔试/d/app/scheduler.py#L235-L242)）。只要处理器持续回调，心跳就新鲜——这在秒级、分片密集时成立。
- **小时级变化**：真实 GPU worker 可能「一个算子阶段无任何进度回调，但进程活着」。把心跳从「进度回调副产品」提升为**独立的 `heartbeat 线程`（worker 内）**，固定周期（如 5s）无条件写 `heartbeat_at`，与处理器有没有调 `report_progress` 解耦。判定「进程活着」只信心跳线程；「进度到了哪」才看 progress，二者分开。

**② 断点粒度 → checkpoint 提升为中间产物持久化（对应「断点粒度」）**
- **现状**：`resume_from = step.progress`，回放的是「第几片」的**进度计数**（[processors.py](file:///c:/Users/xie/Desktop/AI应用笔试/d/app/processors.py#L50-L55)），未持久化步骤中间数据；进程一死，已做的计算在内存里流失，resume 只是「跳过已消耗时间」，不是「复用已产出的数据」。
- **小时级变化**：每完成一个**可独立复用的中间产物**（一段转码输出、一批 OCR 结果）就**落盘到对象存储**（S3/Ceph/CSV 文件），并登记 `checkpoint_ref`。进程重启→ reaper 判定 → resume 直接从磁盘加载已产出片段，**不重算**。这要求 step 本身也要按「可落盘产物」重新切分（§3 的 step 边界更细了一层）。

**③ DB 写入频率 → 进度事件分级持久化（对应「DB 写入频率」）**
- **现状**：`report_progress` 对 PROGRESS 事件做了 `≥0.5s` 节流（[scheduler.py](file:///c:/Users/xie/Desktop/AI应用笔试/d/app/scheduler.py#L231-L242)），但 `heartbeat_at` + `progress` 仍是**每次回调一次 DB 写**。
- **小时级变化**：三级降频——① 心跳 5s 一次（级别低、库内覆盖）；② 进度快照 10~30s 一次（进 PG progress JSON）；③ 完整 PROGRESS 事件写进独立历史表/时序库，且**量级受控**（合并/采样）。避免「一小时内每秒打点」产生同数量级的行。

### 8.3 一句话总结
秒级设计把「心跳、进度、断点」做进了**同一个进度回调**里，简单够用；小时级则必须把这三者**解耦分层**：心跳交给独立线程保证「活着」、checkpoint 落到磁盘保证「不白算」、进度事件降频入历史库保证「可观测不爆库」。