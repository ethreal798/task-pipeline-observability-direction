"""测试环境：同步执行(SyncBackend) + 独立 SQLite，不依赖 Redis/Celery worker。

另外提供「测试结果持久化」：跑完 pytest 后自动把每个用例结果写成 markdown，
存到 reports/latest.md（另存一份带时间戳的历史），便于离线查看 / 归档。
"""

import os
import sys
from pathlib import Path

# 让 `app` 可导入
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# 必须在 import app 前设好环境
os.environ.setdefault("DATABASE_URL", "sqlite:///./test_pipeline.db")
os.environ.setdefault("PIPELINE_RUNNER", "sync")
os.environ.setdefault("STALE_TIMEOUT_SECONDS", "30")

import pytest
from fastapi.testclient import TestClient

from app.backends import SyncBackend


@pytest.fixture(scope="session", autouse=True)
def _reset_db():
    from app.db import Base, engine

    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
    yield


@pytest.fixture()
def db():
    from app.db import SessionLocal
    from app.models import Step, StepRun, Task

    s = SessionLocal()
    yield s
    s.commit()
    s.query(StepRun).delete()
    s.query(Step).delete()
    s.query(Task).delete()
    s.commit()
    s.close()


@pytest.fixture()
def backend():
    return SyncBackend()


@pytest.fixture()
def client():
    from app.main import app

    return TestClient(app)


# ----------------------------------------------------------------------------
# 测试结果持久化：收集每个用例 call 阶段结果 → sessionfinish 写 markdown
# ----------------------------------------------------------------------------

OUTCOME_MARK = {
    "passed": "✅ PASS",
    "failed": "❌ FAIL",
    "error": "⚠️ ERROR",
    "skipped": "⏭️ SKIP",
    "xfailed": "➖ XFAIL",
    "xpassed": "🚩 XPASS",
}


def _fmt_mark(report) -> str:
    return OUTCOME_MARK.get(report.outcome, report.outcome)


@pytest.hookimpl()
def pytest_sessionfinish(session, exitstatus):
    """会话结束：把用例结果汇总为 markdown，写入 reports/。"""
    import time as _t

    reports_dir = ROOT / "reports"
    reports_dir.mkdir(exist_ok=True)

    # 用 pytest 内置 terminal reporter 的 stats：{outcome: [TestReport...]}，
    # 比自维护列表更可靠（涵盖 setup/call/teardown 聚合后的最终结果）。
    term = session.config.pluginmanager.get_plugin("terminalreporter")
    rows = []
    passed = 0
    failed = 0
    for outcome, reports in (term.stats if term else {}).items():
        if outcome not in OUTCOME_MARK:
            continue
        for r in reports:
            mark = _fmt_mark(r)
            dur = f"{r.duration:.2f}s" if getattr(r, "duration", None) else "-"
            line = f"| {mark} | `{r.nodeid}` | {dur} |"
            if getattr(r, "failed", False) and getattr(r, "longrepr", None):
                first = str(r.longrepr).strip().splitlines()
                line = f"{line}\n|  | 📋 `{first[0] if first else ''}` | |"
            rows.append(line)
            if outcome == "passed":
                passed += 1
            elif outcome in ("failed", "error"):
                failed += 1

    stamp = _t.strftime("%Y-%m-%d %H:%M:%S")
    total = len(rows)
    md = f"""# Pipeline D · 测试结果报告

- 生成时间：{stamp}
- 用例总数：{total}｜通过：{passed}｜失败：{failed}
- 退出码（0=全通过）：`{exitstatus}`
- 执行模型：`PIPELINE_RUNNER={os.getenv('PIPELINE_RUNNER')}` / `DATABASE_URL={os.getenv('DATABASE_URL')}`
- 用例文件：`tests/test_pipeline.py`

| 结果 | 用例 | 耗时 |
|---|---|---|
{chr(10).join(rows)}

> 由 conftest.pytest_sessionfinish 自动生成，勿手改。
"""
    (reports_dir / "latest.md").write_text(md, encoding="utf-8")
    history = reports_dir / f"report_{_t.strftime('%Y%m%d_%H%M%S')}.md"
    history.write_text(md, encoding="utf-8")
    print(f"\n[reports] 测试结果已持久化 → {reports_dir / 'latest.md'}（{total} 个用例）")