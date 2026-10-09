"""应用入口：建表 + 挂路由。"""

from fastapi import FastAPI

from .api import router
from .db import Base, engine


def create_app() -> FastAPI:
    Base.metadata.create_all(bind=engine)
    app = FastAPI(title="Pipeline D — 可观测 AI 任务管线", version="0.1.0")
    app.include_router(router)
    return app


app = create_app()
