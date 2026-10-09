import os
from dataclasses import dataclass


@dataclass
class Settings:
    database_url: str = os.getenv("DATABASE_URL", "sqlite:///./pipeline.db")
    broker_url: str = os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0")
    result_backend: str = os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/1")
    # 一个 step 超过多久没有心跳，判定为“悬挂(stale)”，可被 reaper 恢复
    stale_timeout_seconds: int = int(os.getenv("STALE_TIMEOUT_SECONDS", "30"))
    reaper_interval_seconds: int = int(os.getenv("REAPER_INTERVAL_SECONDS", "10"))
    default_max_attempts: int = int(os.getenv("DEFAULT_MAX_ATTEMPTS", "3"))
    # celery=worker 异步执行；sync=进程内同步执行(演示/测试，无需 broker)
    run_mode: str = os.getenv("PIPELINE_RUNNER", "celery")


settings = Settings()
