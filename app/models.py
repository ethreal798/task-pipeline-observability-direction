import uuid
from datetime import datetime

from sqlalchemy import JSON, Column, DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from .db import Base


def uuid4() -> str:
    return str(uuid.uuid4())


class Task(Base):
    __tablename__ = "tasks"

    id = Column(String, primary_key=True, default=uuid4)
    name = Column(String, nullable=False)
    pipeline = Column(String, nullable=False)
    params = Column(JSON, nullable=True)
    status = Column(String, nullable=False, default="PENDING")
    created_at = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    error_info = Column(JSON, nullable=True)

    steps = relationship("Step", back_populates="task", cascade="all, delete-orphan")


class Step(Base):
    __tablename__ = "steps"

    id = Column(String, primary_key=True, default=uuid4)
    task_id = Column(String, ForeignKey("tasks.id"), nullable=False)
    step_key = Column(String, nullable=False)
    status = Column(String, nullable=False, default="PENDING")
    parent_keys = Column(JSON, nullable=False, default=list)
    attempt = Column(Integer, nullable=False, default=0)
    max_attempts = Column(Integer, nullable=False, default=3)
    config = Column(JSON, nullable=True)  # 传给 processor：mock 时长/失败注入等
    started_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    duration_ms = Column(Float, nullable=True)
    heartbeat_at = Column(DateTime, nullable=True)
    # 最新进度快照：既供上游观测(当前完成度)，也是断点续传的恢复起点。
    # 例：{"done": 120, "total": 240, "phase": "embed"} —— 处理器崩溃后可据此从 120 接着跑。
    progress = Column(JSON, nullable=True, default=None)
    result_ref = Column(JSON, nullable=True)
    result_hash = Column(String, nullable=True)
    error_info = Column(JSON, nullable=True)

    task = relationship("Task", back_populates="steps")
    runs = relationship("StepRun", back_populates="step", cascade="all, delete-orphan")


class StepRun(Base):
    """每次 attempt/恢复动作落一条事件记录，用于 /history 状态变更流水"""

    __tablename__ = "step_runs"

    id = Column(String, primary_key=True, default=uuid4)
    step_id = Column(String, ForeignKey("steps.id"), nullable=False)
    attempt = Column(Integer, nullable=False)
    event = Column(String, nullable=False)  # START/RETRY/SUCCESS/FAIL/REUSE/RECOVER/SKIP
    detail = Column(JSON, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    step = relationship("Step", back_populates="runs")