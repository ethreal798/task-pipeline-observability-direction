"""执行后端抽象：核心调度逻辑与 Celery 解耦，便于单元测试（SyncBackend）+ 生产(Celery)。"""

from abc import ABC, abstractmethod


class Backend(ABC):
    @abstractmethod
    def submit(self, step_id: str) -> None: ...

    @abstractmethod
    def retry_later(self, step_id: str, delay: float) -> None: ...


class CeleryBackend(Backend):
    """生产用：把 step 投递给 Celery worker 异步执行。"""

    def submit(self, step_id: str) -> None:
        from .tasks import run_step

        run_step.delay(step_id)

    def retry_later(self, step_id: str, delay: float) -> None:
        from .tasks import run_step

        run_step.apply_async(args=[step_id], countdown=max(int(delay), 0))


class SyncBackend(Backend):
    """测试/演示用：同步执行，不依赖 broker/worker。"""

    def submit(self, step_id: str) -> None:
        from .scheduler import execute_step_from_id
        from .db import SessionLocal

        with SessionLocal() as db:
            execute_step_from_id(db, self, step_id)

    def retry_later(self, step_id: str, delay: float) -> None:
        # 同步模式下立即重试（用 config 的 fail_until_attempt 控制尝试次数）
        from .scheduler import execute_step_from_id
        from .db import SessionLocal

        with SessionLocal() as db:
            execute_step_from_id(db, self, step_id)