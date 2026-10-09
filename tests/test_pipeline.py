"""核心场景测试：DAG 顺序 / 重试 / 结果复用 / 依赖阻塞 / 断点恢复 / 超时 / 进度与续传。

每个用例的注释都遵循「测什么 → 怎么断言」：先在 docstring 说明验证点，
再在断言行写清期望。运行 `pytest -q` 后，测试结果摘要会自动写入 reports/ 下的
markdown 报告（见 conftest.py 的 pytest_sessionfinish 钩子），便于离线查看。
"""

from datetime import datetime, timedelta

from app import scheduler
from app.models import Step, StepRun

# 视为“已完成且结果可用”的所有状态（成功 / 恢复成功 / 复用跳过）
SUCCESS = {"SUCCEEDED", "RECOVERED", "SKIPPED"}


def _create(db, backend, pipeline="simple_linear", params=None, name=None):
    """创建并立即启动一个任务，返回已刷新到最新状态的任务对象。"""
    task = scheduler.create_and_start(db, backend, pipeline, params, name)
    db.refresh(task)
    for s in task.steps:
        db.refresh(s)
    return task


def _step(task, key):
    """按 step_key 取出任务里的某个步骤。"""
    return next(s for s in task.steps if s.step_key == key)


# ------------------------------------------------------------------ DAG 主流程
def test_linear_success(db, backend):
    """测什么：simple_linear 整条链路正常执行并成功。

    断言：
    - 整单 SUCCEEDED，所有 step 进入成功态；
    - finished_at 已落；
    - DAG 依赖顺序成立：首发 step_a 的启动时间早于末位 step_c。
    """
    task = _create(db, backend)
    assert task.status == "SUCCEEDED"
    assert all(s.status in SUCCESS for s in task.steps)
    assert task.finished_at is not None
    # DAG 顺序保证：c 在 b 后，b 在 a 后
    a = _step(task, "step_a")
    c = _step(task, "step_c")
    assert a.started_at <= c.started_at


# ------------------------------------------------------------------ 重试
def test_retry_then_success(db, backend):
    """测什么：step 首次失败后能自动重试并最终成功。

    animate_prep 的 frame_extract 配置为前 1 次失败（fail_until_attempt=1），
    期望重试后第 2 次才成功，attempt 停在 2。
    断言：整单 SUCCEEDED，frame_extract SUCCEEDED 且 attempt == 2。
    """
    task = _create(db, backend, pipeline="animate_prep")
    assert task.status == "SUCCEEDED"
    fe = _step(task, "frame_extract")
    assert fe.status == "SUCCEEDED"
    assert fe.attempt == 2  # 第1次失败，第2次成功


def test_retry_exhausted_task_fails(db, backend):
    """测什么：重试次数耗尽后，失败会连坐整单并精确定位。

    step_a 配失败且 max_attempts=1 → 首次即耗尽。
    断言：
    - step_a FAILED，task FAILED；
    - error_info 精确到 step_a；
    - 下游 step_b / step_c 保持 PENDING（不推进）。
    """
    task = scheduler.create_task(db, "simple_linear", params=None, name="x")
    step_a = _step(task, "step_a")
    step_a.max_attempts = 1
    step_a.config = {"duration_ms": 10, "fail_until_attempt": 10}
    db.commit()
    scheduler.start_task(db, backend, task.id)
    db.refresh(task)
    for s in task.steps:
        db.refresh(s)
    assert _step(task, "step_a").status == "FAILED"
    assert task.status == "FAILED"
    assert task.error_info["step"] == "step_a"
    # 下游被阻塞，不推进
    assert _step(task, "step_b").status == "PENDING"
    assert _step(task, "step_c").status == "PENDING"


# ------------------------------------------------------------------ 复用 / 重放
def test_replay_reuses_results(db, backend):
    """测什么：replay 在“输入未变”时复用历史结果，不真跑。

    断言：
    - 重放后整单 SUCCEEDED，所有 step 为 SKIPPED（复用）；
    - 结果 result_ref 仍保留；
    - 至少一个 step 有 REUSE 事件。
    """
    task = _create(db, backend)
    assert task.status == "SUCCEEDED"
    after = scheduler.replay_task(db, backend, task.id)
    db.refresh(after)
    for s in after.steps:
        db.refresh(s)
    assert after.status == "SUCCEEDED"
    # 输入未变 → 全部复用(SKIPPED)
    assert all(s.status == "SKIPPED" for s in after.steps)
    # 结果仍保留
    assert _step(after, "step_c").result_ref is not None
    reuse_events = [
        s for s in after.steps
        if any(r.event == "REUSE" for r in s.runs)
    ]
    assert len(reuse_events) >= 1


def test_dependency_blocks_downstream(db, backend):
    """测什么：前置失败会阻塞下游推进（依赖拓扑正确）。

    断言：step_a 失败致整单 FAILED，step_b 保持 PENDING。
    """
    task = scheduler.create_task(db, "simple_linear")
    _step(task, "step_a").max_attempts = 1
    _step(task, "step_a").config = {"duration_ms": 10, "fail_until_attempt": 10}
    db.commit()
    scheduler.start_task(db, backend, task.id)
    db.refresh(task)
    assert task.status == "FAILED"
    assert _step(task, "step_b").status == "PENDING"


# ------------------------------------------------------------------ 断点恢复（reaper）
def test_recover_stale_step(db, backend):
    """测什么：recover_stale 能把“心跳过期但任务还未真死”的步骤续跑。

    手动构造悬挂：step_a 已完成(SUCCEEDED+result)，step_b 标记为 RUNNING 且
    heartbeat_at 拨回 5 分钟前（模拟 worker 掉线后心跳停更）。
    断言：
    - recover_stale 检出 ≥1 个悬挂；
    - step_b 被重置并续跑到 SUCCEEDED；
    - step_a 下发 RECOVER(reused) 事件，确认兄弟步骤被复用。
    """
    task = scheduler.create_task(db, "simple_linear")
    scheduler.start_task(db, backend, task.id)
    db.refresh(task)
    # 一件完整跑完后，改为模拟制造悬挂：
    task2 = scheduler.create_task(db, "simple_linear")
    db.commit()
    t = task2
    a = _step(t, "step_a")
    # 手动将 step_a 标记为成功（作为“已完成兄弟”），step_b 设为 stale RUNNING
    a.status = "SUCCEEDED"
    a.result_ref = {"ok": True, "input": "raw.mkv", "size_mb": 480}
    a.result_hash = "h1"
    a.finished_at = datetime.utcnow()
    b = _step(t, "step_b")
    b.status = "RUNNING"
    b.heartbeat_at = datetime.utcnow() - timedelta(minutes=5)
    b.max_attempts = 3
    t.status = "RUNNING"
    db.commit()

    recovered = scheduler.recover_stale(db, backend)
    assert recovered >= 1
    db.refresh(t)
    for s in t.steps:
        db.refresh(s)
    # step_b 被续跑并最终成功
    assert _step(t, "step_b").status == "SUCCEEDED"
    # step_a 被确认复用（有 RECOVER 事件）
    a_events = [r.event for r in _step(t, "step_a").runs]
    assert "RECOVER" in a_events


# ------------------------------------------------------------------ 超时
def test_timeout_treated_as_failure(db, backend):
    """测什么：步骤超时被判为“可重试失败”，走失败/重试语义。

    给 step_a 配 duration_ms=200 > timeout_ms=10，max_attempts=1 → 直接超时耗尽。
    断言：step_a FAILED，错误 reason 含“超时”。
    """
    task = scheduler.create_task(db, "simple_linear")
    sa = _step(task, "step_a")
    sa.max_attempts = 1
    sa.config = {"duration_ms": 200, "timeout_ms": 10}
    db.commit()
    scheduler.start_task(db, backend, task.id)
    db.refresh(task)
    assert _step(task, "step_a").status == "FAILED"
    assert "超时" in (_step(task, "step_a").error_info or {}).get("reason", "")


# ------------------------------------------------------------------ 异步防死循环回归
def test_schedule_ready_submits_each_ready_step_once(db):
    """测什么：异步后端下 schedule_ready 不会死循环重复投递（回归）。

    背景：异步(Celery) submit 不改 DB 状态，while 重扫会在“父成功+子就绪”时把
    同一就绪 step 无限 re-submit，曾导致 redis 队列被灌满、worker 死循环卡死。
    用 FakeAsyncBackend（submit 只记录不改库）模拟该异步语义。
    断言：就绪的 step_b 单次只投 1 次；依赖未满足的 step_c 不投；已成功的 step_a 不投。
    """
    submitted = []

    class FakeAsyncBackend:
        # 模拟 Celery：submit 仅记录，不改 DB 状态（worker 异步才接手）
        def submit(self, step_id):
            submitted.append(step_id)

        def retry_later(self, step_id, delay):
            raise AssertionError("本用例不应触发重试")

    task = scheduler.create_task(db, "simple_linear")
    a = _step(task, "step_a")
    a.status = "SUCCEEDED"
    a.result_ref = {"ok": True}
    a.result_hash = "h-a"
    a.finished_at = datetime.utcnow()
    task.status = "RUNNING"
    db.commit()

    # 父 step_a 已成功 → step_b 就绪；都不变再变故，step_c 依赖 b 仍阻塞。
    scheduler.schedule_ready(db, FakeAsyncBackend(), task)
    b = _step(task, "step_b")
    c = _step(task, "step_c")
    assert submitted.count(b.id) == 1  # 单次调用内只投一次；若死循环会无限次
    assert c.id not in submitted  # 依赖未满足，不投递
    assert submitted.count(a.id) == 0  # a 已成功，不重复投递


# ------------------------------------------------------------------ 状态机
def test_task_transition_invalid_rejected():
    """测什么：状态机拒绝非法状态转移。

    断言：SUCCEEDED→RUNNING 的 step 转移抛 ValueError；PENDING→RUNNING 的 task 转移合法。
    """
    from app.state_machine import assert_task_transition, assert_step_transition
    import pytest as _p

    with _p.raises(ValueError):
        assert_step_transition("SUCCEEDED", "RUNNING")
    assert assert_task_transition("PENDING", "RUNNING") is None


# ------------------------------------------------------------------ API 层
def test_api_flow(client, backend):
    """测什么：FastAPI 端点整链路可用（创建/详情/历史/重放），且为同步模式。

    断言：
    - POST /api/tasks 返回 201；
    - GET /api/tasks/{id} 整单 SUCCEEDED；
    - GET /api/tasks/{id}/history 含 SUCCESS 事件；
    - POST /api/tasks/{id}/replay 后所有 step 为 SKIPPED（复用）。
    """
    resp = client.post("/api/tasks", json={"pipeline": "simple_linear"})
    assert resp.status_code == 201
    tid = resp.json()["id"]
    detail = client.get(f"/api/tasks/{tid}")
    assert detail.status_code == 200
    assert detail.json()["status"] == "SUCCEEDED"
    hist = client.get(f"/api/tasks/{tid}/history")
    assert hist.status_code == 200
    events = hist.json()["events"]
    assert any(e["event"] == "SUCCESS" for e in events)
    replay = client.post(f"/api/tasks/{tid}/replay")
    assert replay.status_code == 200
    assert all(
        s["status"] == "SKIPPED" for s in replay.json()["steps"]
    )


# ------------------------------------------------------------------ 长任务：进度 / 放大 / 断点续传
def test_progress_persisted_on_success(db, backend):
    """测什么：长任务成功步骤应落库进度快照 {done,total}。

    进度快照同时是页面进度条的数据源和断点续传的起点。
    断言：每个成功步骤 progress 非空、total>0、done 收敛到 total。
    """
    task = _create(db, backend, pipeline="animate_prep")
    assert task.status == "SUCCEEDED"
    for s in task.steps:
        if s.status in SUCCESS:
            assert s.progress is not None
            assert s.progress["total"] > 0
            assert s.progress["done"] == s.progress["total"]  # 完成后 done 收敛到 total


def test_duration_scale_amplifies_work(monkeypatch):
    """测什么：params.duration_scale 放大 step 耗时（分片数按比例增）。

    用于长任务演示：拉长每步时长让进度条/续传可见。
    断言：1000ms*scale4 按 100ms 分片 → 40 片 sleep。
    """
    import app.processors as P

    sleeps = []
    monkeypatch.setattr(P.time, "sleep", lambda s: sleeps.append(s))

    class T:
        params = {"duration_scale": 4.0}

    class S:
        step_key = "ingest"
        config = {"duration_ms": 1000}
        task = T()

    P.mock_processor(S, {}, 0)
    # 1000ms * 4 / 100ms 每片 = 40 片
    assert len(sleeps) == 40


def test_progress_resume_from_skips_slices(monkeypatch):
    """测什么：断点续传 resume_from 从已完成片继续，不从头重复。

    断言：1000ms(10 片) 携带 resume_from={done:3} → 只再 sleep 7 片。
    """
    import app.processors as P

    sleeps = []
    monkeypatch.setattr(P.time, "sleep", lambda s: sleeps.append(s))

    class T:
        params = {}

    class S:
        step_key = "ingest"
        config = {"duration_ms": 1000}  # 10 片
        task = T()

    P.mock_processor(S, {}, 0, resume_from={"done": 3, "total": 10})
    # 从第 3 片续 → 只剩 7 片
    assert len(sleeps) == 7


def test_replay_ok_after_residual_progress(db, backend):
    """测什么：replay 不能被残留的旧 progress 带偏（继续续传）。

    replay 会清空 progress，否则「已变分支」会带着旧 done 直接从终点续传、
    被误判为已完成。人为在每个 step 注入 {done:1,total:1} 验证清零。
    断言：replay 后任何 step 的 progress 都不再是注入的 {1,1}。
    """
    task = _create(db, backend, pipeline="animate_prep")
    for s in task.steps:
        s.progress = {"done": 1, "total": 1}  # 人为制造“旧进度残留”
    db.commit()
    replay = scheduler.replay_task(db, backend, task.id)
    db.refresh(replay)
    for s in replay.steps:
        db.refresh(s)
    assert replay.status in ("SUCCEEDED", "RUNNING")
    for s in replay.steps:
        # 任何步骤都不允许残留注入的旧进度 → 证明续传起点已被清零
        if s.progress is not None:
            assert not (s.progress.get("done") == 1 and s.progress.get("total") == 1)