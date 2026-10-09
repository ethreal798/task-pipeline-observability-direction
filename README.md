# Pipeline D — 可观测 AI 任务执行管线（MVP）

题 D「任务管线可观测」的实现。以「一段动画素材的预处理」为例，把任务拆成多个耗时
步骤，重点在：**长时间任务执行、任务/步骤状态、失败恢复机制、可观察性与结果复用**。
真实 AI 计算均以 MOCK 替代，专注调度本身 —— 更简单、可控、可解释。

## 快速开始（本地，同步模式，无需 Redis / Celery）

```powershell
# Windows PowerShell
python -m venv .venv
.venv\Scripts\python.exe -m pip install -r requirements.txt
$env:PIPELINE_RUNNER="sync"
.venv\Scripts\python.exe -m uvicorn app.main:app --port 8000
```

```bash
# Linux / macOS
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
PIPELINE_RUNNER=sync .venv/bin/python -m uvicorn app.main:app --port 8000
```

同步模式下 step 在进程内顺序执行（SQLite 存储，无需 broker/worker），便于体验/调试；
生产走下方 Docker 异步集群（Celery + Redis + Postgres）。

## 用 Docker Compose 起完整集群（Postgres + Redis + API + Worker + Beat）

```bash
docker compose up --build
```

- 极简观测页 http://localhost:18000/
- 数据库：PostgreSQL（状态真相源）；队列：Redis
- API 创建任务时把 step 投递给 Celery worker，beat 每 `REAPER_INTERVAL_SECONDS`
  巡检一次悬挂 step 并断点续跑。

## 跑测试（失败场景 / 恢复 / 复用全覆盖）

```bash
.venv\Scripts\python -m pytest tests -q
```

## 核心接口

| 方法 | 路径 | 作用 |
|---|---|---|
| POST | `/api/tasks` | 创建并启动任务（body: `{"pipeline": "animate_prep"}`） |
| GET | `/api/tasks` | 分页/过滤任务列表 |
| GET | `/api/tasks/{id}` | 任务 + 各 step 状态/依赖/重试次数/耗时/结果 |
| GET | `/api/tasks/{id}/history` | task/step 状态变更流水（事件流） |
| POST | `/api/tasks/{id}/replay` | 重放任务：输入未变的已完成 step 直接**复用**（SKIPPED） |
| POST | `/api/tasks/{id}/start` | 启动（用于延迟启动场景） |
| GET | `/api/steps/{id}` | 步骤级详情 |

预置 pipeline：`animate_prep`（6 步 DAG，含分支）与 `simple_linear`（3 步线性）。

## 结构

```
app/
  models.py        # Task / Step / StepRun(历史) 数据模型
  state_machine.py # 任务与步骤状态机（非法转移一律 reject）
  scheduler.py     # 核心编排：DAG 推进 / 重试 / 超时 / 断点续跑 / 结果复用
  processors.py    # MOCK 处理器（耗时 sleep / 失败注入 / 超时注入）
  graph.py         # pipeline Step DAG（显式 parent_keys）
  backends.py      # 执行后端抽象：CeleryBackend(生产) / SyncBackend(测试)
  tasks.py         # Celery worker 入口
  reaper.py        # Celery Beat：悬挂 step 巡检与恢复
  api.py           # FastAPI 路由 + 极简观测页
tests/             # 失败场景矩阵全部单测
```

## 可配置项（环境变量）

`DATABASE_URL`、`CELERY_BROKER_URL`、`CELERY_RESULT_BACKEND`、
`PIPELINE_RUNNER`(celery|sync)、`STALE_TIMEOUT_SECONDS`、`REAPER_INTERVAL_SECONDS`、
`DEFAULT_MAX_ATTEMPTS`。