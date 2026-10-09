"""Celery Beat 周期任务：reaper 巡检悬挂 step 并断点续跑。"""

from celery.schedules import crontab

from .config import settings
from .tasks import celery_app


@celery_app.task(name="recover_stale")
def recover_stale():
    from .backends import CeleryBackend
    from .db import SessionLocal
    from .scheduler import recover_stale as do_recover

    with SessionLocal() as db:
        return do_recover(db, CeleryBackend())


celery_app.conf.beat_schedule = {
    "recover-stale-every-Ns": {
        "task": "recover_stale",
        "schedule": settings.reaper_interval_seconds,
    }
}