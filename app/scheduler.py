"""核心编排器：与后端(Celery/同步)解耦，纯函数式操作 DB + 通过 Backend 投递执行。

职责：
- create_task / start_task：建任务并推进
- schedule_ready：按 DAG 依赖把「可执行」的 step 投递（含结果复用判定）
- execute_step：真正执行一个 step（成功/失败/重试/超时）
- recover_stale：reaper 用，处理进程中断后的悬挂 step 与断点续跑
"""

import hashlib
import json
import os
import random
import time
from datetime import datetime, timedelta

from . import graph, processors
from .config import settings
from .db import SessionLocal
from .models import Step, StepRun, Task
from .state_machine import (
    assert_step_transition,
    assert_task_transition,
)

_DEBUG = os.getenv("PIPELINE_DEBUG") == "1"


def _log(*a):
    if _DEBUG:
        print("[DBG]", *a, flush=True)


# 成功态：这些状态都视为“已完成且结果为可用”
SUCCESS_STATES = {"SUCCEEDED", "RECOVERED", "SKIPPED"}


# ------------------------------------------------------------------ helpers

def _add_run(db, step: Step, event: str, detail=None) -> None:
    db.add(
        StepRun(
            step_id=step.id,
            attempt=step.attempt,
            event=event,
            detail=detail,
        )
    )


def _dep_hashes(step: Step, steps_by_key: dict) -> dict:
    return {
        key: steps_by_key[key].result_hash for key in step.parent_keys
    }


def compute_expected_hash(task: Task, step: Step, dep_hashes: dict) -> str:
    payload = {
        "step": step.step_key,
        "params": task.params or {},
        "deps": {k: dep_hashes[k] for k in sorted(dep_hashes)},
        "version": processors.get_version(step.step_key),
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:24]


def backoff_seconds(attempt: int) -> float:
    # 指数退避，基准 0.5s
    return 0.5 * (2 ** max(attempt - 1, 0))


# ------------------------------------------------------------------ task 管理

def create_task(db, pipeline_name: str, params=None, name=None) -> Task:
    """创建任务 + 步骤 DAG（初始 PENDING）。不自动启动。"""
    defs = graph.step_defs(pipeline_name)
    task = Task(
        name=name or pipeline_name,
        pipeline=pipeline_name,
        params=params or {},
        status="PENDING",
    )
    db.add(task)
    db.flush()  # 拿到 id

    for d in defs:
        step = Step(
            task_id=task.id,
            step_key=d["key"],
            status="PENDING",
            parent_keys=d.get("parents", []),
            max_attempts=d.get("max_attempts", settings.default_max_attempts),
            config=d.get("config", {}),
            attempt=0,
        )
        db.add(step)
    db.commit()
    db.refresh(task)
    return task


def start_task(db, backend, task_id: str) -> Task:
    task = db.get(Task, task_id)
    if task is None:
        raise KeyError(task_id)
    assert_task_transition(task.status, "RUNNING")
    task.status = "RUNNING"
    task.started_at = task.started_at or datetime.utcnow()
    db.commit()
    schedule_ready(db, backend, task)
    return task


# ------------------------------------------------------------------ 调度

def _needs_reuse(task: Task, step: Step, steps_by_key: dict) -> bool:
    """该 step 已有结果且输入(deps/参数/版本)未变 → 可直接复用，不真实执行。
        两个条件同时成立：
        - 有历史结果（`result_ref` 非空）；
        - 用 当前时刻 的输入算出来的指纹`compute_expected_hash` ，和上次存下的`result_hash` 相等 。
    """
    if not step.result_ref or step.result_hash is None:
        return False
    dep_hashes = _dep_hashes(step, steps_by_key)
    return step.result_hash == compute_expected_hash(task, step, dep_hashes)


def schedule_ready(db, backend, task: Task) -> None:
    """按依赖拓扑逐层把「可执行/可复用」的 PENDING step 推进。

    - 可复用(结果命中) → SKIPPED，直接用历史结果；
    - 依赖已就绪且不可复用 → 投递给 backend 执行。
    用 while 循环保证多级复用在 replay/断点续跑时能层层传播，而不是只扫一次。
    """
    if task.status not in ("RUNNING",):
        return
    # 本轮调用内已投递过的 step，避免 while 重扫因为“submit 不改 PENDING 状态”
    # 而把同一 step 无限重复投递（worker 未接手前其状态仍是 PENDING）。
    submitted: set = set()
    progressed = True
    while progressed:
        progressed = False
        # backend(可能是独立 session) 已写库，强制重新加载，避免陈旧对象重复提交
        db.expire_all()
        # {stea_a: a, step_b: b, step_c:c}
        steps_by_key = {s.step_key: s for s in task.steps}
        for step in task.steps:
            if step.status != "PENDING":
                continue
            # 前置依赖
            parents = [steps_by_key[p] for p in step.parent_keys]
            # 任一前置任务未完成直接跳过该任务
            if not all(p.status in SUCCESS_STATES for p in parents):
                continue

            if _needs_reuse(task, step, steps_by_key):
                # 断点续跑/重放：输入未变 → 直接复用历史结果(本 session 内同步，可继续扫描)
                assert_step_transition(step.status, "SKIPPED")
                step.status = "SKIPPED"
                step.finished_at = datetime.utcnow()
                _add_run(db, step, "REUSE", {"result_ref": step.result_ref})
                db.commit()
                maybe_finish_task(db, backend, task)
                progressed = True
                continue

            if step.id in submitted:
                continue  # 已投递待 worker 接手，跳过避免重复
            backend.submit(step.id)
            submitted.add(step.id)
            progressed = True
            break  # 提交后重扫，避免陈旧对象把同一 step 重复投递


# ------------------------------------------------------------------ 执行

def execute_step_from_id(db, backend, step_id: str) -> None:
    step = db.get(Step, step_id)
    if step is None:
        return
    execute_step(db, backend, step)


def execute_step(db, backend, step: Step) -> None:
    task = step.task
    if task.status == "CANCELLED":
        return
    if step.status not in ("PENDING", "RETRYING"):
        # 并发场景下同一 step 可能被重复投递：仅当已被接手(RUNNING)或已完成时跳过；
        # PENDING=正常首拉，RETRYING=重试重新执行，二者都必须放行
        return

    assert_step_transition(step.status, "RUNNING")
    step.status = "RUNNING"
    step.started_at = step.started_at or datetime.utcnow()
    step.heartbeat_at = datetime.utcnow()
    _add_run(db, step, "START")
    if task.status == "PENDING":
        assert_task_transition(task.status, "RUNNING")
        task.status = "RUNNING"
        task.started_at = task.started_at or datetime.utcnow()
    db.commit()

    # {"step_a":a, "step_b":b}
    steps_by_key = {s.step_key: s for s in task.steps}
    # {"step_b": {"ok": true, "step": "step_a", "consumed_ms": 50}}
    deps_result = {
        key: (steps_by_key[key].result_ref or {}) for key in step.parent_keys
    }

    # 断点续传起点：之前已在 {done,total} 取得进展 → 从 done 片继续，跳过已消耗时间。
    # 统一覆盖「单步失败重试」与「进程中断恢复」两条路径；replay 场景在 replay_task 里清空
    # progress，避免「重跑已变分支却从终点续传」的错误。
    resume_from = step.progress if (step.progress or {}).get("done") else None

    # 进度回调：由处理器分片循环驱动。用独立 session 做轻量写，避免干扰外层状态机事务。
    #  - 每次回调必做：刷 heartbeat（续租，让 reaper 不误杀长任务）+ 存 progress 快照（观测/续传起点）
    #  - 节流(≥0.5s)写一条 PROGRESS 事件：保留事件流水但不刷爆 /history
    _last_progress_event = {"t": 0.0}

    def report_progress(done: int, total: int, phase: str | None = None) -> None:
        progress = {"done": done, "total": total, "phase": phase}
        touch = datetime.utcnow()
        with SessionLocal() as s:
            s.query(Step).filter(Step.id == step.id).update(
                {Step.heartbeat_at: touch, Step.progress: progress}
            )
            s.commit()
        now = time.time()
        if now - _last_progress_event["t"] >= 0.5:
            _last_progress_event["t"] = now
            with SessionLocal() as s:
                s.add(
                    StepRun(
                        step_id=step.id,
                        attempt=step.attempt,
                        event="PROGRESS",
                        detail={"progress": progress},
                    )
                )
                s.commit()

    started = time.time()
    try:
        result = processors.mock_processor(
            step,
            deps_result,
            step.attempt,
            report_progress=report_progress,
            resume_from=resume_from,
        )
    except processors.StepError as exc:
        db.refresh(step)
        step.heartbeat_at = datetime.utcnow()
        db.commit()
        _handle_failure(db, backend, step, str(exc))
        return
    except Exception as exc:  # 未知异常同样按失败处理
        db.refresh(step)
        _handle_failure(db, backend, step, f"未预期异常: {exc}")
        return

    # 成功
    dep_hashes = _dep_hashes(step, steps_by_key)
    db.refresh(step)
    step.status = "SUCCEEDED"
    step.finished_at = datetime.utcnow()
    step.duration_ms = round((time.time() - started) * 1000, 1)
    step.result_ref = result
    step.result_hash = compute_expected_hash(task, step, dep_hashes)
    step.error_info = None
    step.heartbeat_at = datetime.utcnow()
    _add_run(db, step, "SUCCESS", {"duration_ms": step.duration_ms})
    db.commit()

    maybe_finish_task(db, backend, task)  # 检查整张单能否收工
    schedule_ready(db, backend, task)  # 继续驱动下游


def _handle_failure(db, backend, step: Step, reason: str) -> None:
    task = step.task
    if step.attempt < step.max_attempts:
        step.attempt += 1
        assert_step_transition(step.status, "RETRYING")
        step.status = "RETRYING"
        step.error_info = {"reason": reason, "attempt": step.attempt}
        _add_run(db, step, "RETRY", {"reason": reason, "next_attempt": step.attempt})
        db.commit()
        backend.retry_later(step.id, backoff_seconds(step.attempt))
        return

    # 重试耗尽 → 终结失败
    assert_step_transition(step.status, "FAILED")
    step.status = "FAILED"
    step.finished_at = datetime.utcnow()
    step.error_info = {"reason": reason, "attempt": step.attempt}
    _add_run(db, step, "FAIL", {"reason": reason})
    assert_task_transition(task.status, "FAILED")
    task.status = "FAILED"
    task.finished_at = datetime.utcnow()
    task.error_info = {"step": step.step_key, "reason": reason}
    db.commit()
    # 失败后不再推进下游；其余 PENDING step 保持 pending（不调度）
    _fail_pending_downstream(db, backend, task)


def _fail_pending_downstream(db, backend, task: Task) -> None:
    """被失败阻塞的下游保持 PENDING；这里不置 FAILED，方便恢复时重置。留白更可解释。"""
    # 不再调度任何新 step
    return


def maybe_finish_task(db, backend, task: Task) -> None:
    if not all(s.status in SUCCESS_STATES for s in task.steps):
        return
    assert_task_transition(task.status, "SUCCEEDED")
    task.status = "SUCCEEDED"
    task.finished_at = task.finished_at or datetime.utcnow()
    task.error_info = None
    db.commit()


# ------------------------------------------------------------------ 断点续跑 / 复用

def replay_task(db, backend, task_id: str) -> Task:
    """把任务所有 step 重置，重排一次；依赖与参数未变的已完成 step 会被复用(SKIPPED)。"""
    task = db.get(Task, task_id)
    if task is None:
        raise KeyError(task_id)
    for s in task.steps:
        if s.status in SUCCESS_STATES:
            s.status = "PENDING"
            s.finished_at = None
        # 重放语义 = 从头上重跑→必须清掉断点续传用到的进度起点，
        # 否则「变了的分支」会带着旧 progress.done 直接从终点续传→被误判为已完成。
        s.progress = None
    task.status = "PENDING"
    task.finished_at = None
    task.error_info = None
    db.commit()
    return start_task(db, backend, task_id)


def recover_stale(db, backend) -> int:
    """reaper：找出心跳过期的 RUNNING step → 断点续跑。

    行为：
    - 已完成(SUCCEEDED)的兄弟 step 保持结果，写一条 RECOVER 记录确认复用；
    - 悬挂的 RUNNING step 重置为 PENDING 重新调度；
    - 若某 step 已重试耗尽则让其失败并终结任务。
    """
    cutoff = datetime.utcnow() - timedelta(seconds=settings.stale_timeout_seconds)
    stale = (
        db.query(Step)
        .filter(Step.status == "RUNNING", Step.heartbeat_at < cutoff)
        .all()
    )
    for step in stale:
        task = step.task
        # (a) 已完成兄弟 step：确认复用，写 RECOVER 事件
        for s in task.steps:
            if s.status == "SUCCEEDED" and s.result_ref is not None:
                _add_run(db, s, "RECOVER", {"reused": True})

        # (b) 悬挂 step：允许再试则重置重排，否则终结失败
        if step.attempt < step.max_attempts:
            step.status = "PENDING"
            step.heartbeat_at = datetime.utcnow()
            _add_run(db, step, "RECOVER", {"recovered": True, "from": "stale"})
        else:
            if step.status == "RUNNING":
                step.status = "FAILED"
                step.finished_at = datetime.utcnow()
                step.error_info = {"reason": "stale worker(重试耗尽)", "attempt": step.attempt}
                _add_run(db, step, "FAIL", {"reason": "stale worker"})
                task.status = "FAILED"
                task.finished_at = datetime.utcnow()
                task.error_info = {"step": step.step_key, "reason": "stale worker"}
                continue

        if task.status == "PENDING":
            task.status = "RUNNING"
            task.started_at = task.started_at or datetime.utcnow()
        db.commit()
        schedule_ready(db, backend, task)

    db.commit()
    return len(stale)


def create_and_start(db, backend, pipeline_name: str, params=None, name=None) -> Task:
    task = create_task(db, pipeline_name, params, name)
    return start_task(db, backend, task.id)
