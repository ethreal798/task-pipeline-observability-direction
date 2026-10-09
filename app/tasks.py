"""Celery tasks：worker 执行单个 step + beat 巡检 reaper。"""

from celery import Celery

from .backends import CeleryBackend
from .config import settings

celery_app = Celery(
    "pipeline_d",
    broker=settings.broker_url,
    backend=settings.result_backend,
    include=["app.tasks", "app.reaper"],
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    task_track_started=True,
)

BACKEND = CeleryBackend()


@celery_app.task(name="run_step")
def run_step(step_id: str):
    """worker 执行一个 step；核心逻辑在 scheduler（Celery 只做入口与推进）。"""
    from .db import SessionLocal
    from .scheduler import execute_step_from_id

    with SessionLocal() as db:
        execute_step_from_id(db, BACKEND, step_id)
