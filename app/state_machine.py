"""任务/步骤状态机：集中定义合法状态与转移，杜绝非法跳转。

注意：判定和写库不会在同一事务里强制锁，MVP 用“转移发生在 commit 前” +
“reaper 只处理明显 stale 的 step”来保证安全；事务级并发控制见设计稿演进章节。
"""

TASK_STATUSES = {"PENDING", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED"}
STEP_STATUSES = {
    "PENDING",
    "RUNNING",
    "RETRYING",
    "SUCCEEDED",
    "FAILED",
    "SKIPPED",
    "RECOVERED",
}

# 任务级合法转移
TASK_TRANSITIONS = {
    "PENDING": {"RUNNING"},
    "RUNNING": {"SUCCEEDED", "FAILED", "CANCELLED"},
    "SUCCEEDED": set(),  # 空集 = 死路 = 终态
    "FAILED": set(),
    "CANCELLED": set(),
}

# 步骤级合法转移
STEP_TRANSITIONS = {
    "PENDING": {"RUNNING", "SKIPPED", "RECOVERED"},  # RECOVERED=恢复确认复用
    "RUNNING": {"SUCCEEDED", "FAILED", "RETRYING", "RECOVERED"},
    "RETRYING": {"RUNNING", "FAILED"},
    "SUCCEEDED": set(),
    "FAILED": set(),
    "SKIPPED": set(),
    "RECOVERED": set(),
}


def validate_task_transition(current: str, new: str) -> bool:
    return new in TASK_TRANSITIONS.get(current, set())


def validate_step_transition(current: str, new: str) -> bool:
    return new in STEP_TRANSITIONS.get(current, set())


def assert_task_transition(current: str, new: str) -> None:
    if not validate_task_transition(current, new):
        raise ValueError(f"非法任务状态转移: {current} -> {new}")


def assert_step_transition(current: str, new: str) -> None:
    if not validate_step_transition(current, new):
        raise ValueError(f"非法步骤状态转移: {current} -> {new}")
