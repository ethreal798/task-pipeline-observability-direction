"""MOCK 处理器：专注调度/恢复，不做真实计算。

每个 step 的耗时 sleep、失败注入、超时都以 config(JSON) 驱动。
"""

import random
import time


class StepError(Exception):
    """步骤业务失败（可重试）。"""


class StepTimeoutError(StepError):
    """步骤超时。"""


# 处理器版本：process 逻辑变化时应递增，用于结果复用判定(result_hash 的一部分)
PROCESSOR_VERSIONS = {
    "ingest": "1",
    "transcode": "1",
    "frame_extract": "1",
    "ocr_subtitle": "1",
    "vectorize": "1",
    "render_preview": "1",
}

# 每个 step 在“成功”时产出的 MOCK 结果（演示结果复用/断点续跑用）
MOCK_RESULTS = {
    "ingest": {"ok": True, "input": "raw.mkv", "size_mb": 480},
    "transcode": {"ok": True, "codec": "h264", "ratio": 3.2},
    "frame_extract": {"ok": True, "frames": 120, "fps": 24},
    "ocr_subtitle": {"ok": True, "subtitle_lines": 238},
    "vectorize": {"ok": True, "embeddings": 238, "dims": 768},
    "render_preview": {"ok": True, "preview_url": "preview.mp4"},
}


# 每个耗时片段的粒度(ms)：用于分片上报进度 + 断点续传的最小单位
_PROGRESS_SLICE_MS = 100


def mock_processor(
    step,
    deps_result,
    attempt: int,
    report_progress=None,
    resume_from: dict | None = None,
) -> dict:
    """根据 step.config 模拟：耗时、注入失败、注入超时。

    - 耗时按 `_PROGRESS_SLICE_MS` 分片循环执行，每完成一片回调 report_progress(done,total)
      （由 execute_step 注入，用于心跳续租 + 进度存档 + 观测）。
    - resume_from（如 {"done": 120, "total": 240}）表示此前已跑到 done 片：
      处理器直接从 done 起继续，模拟“断点续传”，不再重复前面已消耗的时间。
    """
    cfg = step.config or {}
    duration_ms = int(cfg.get("duration_ms", 100))
    # 长任务演示：task.params.duration_scale 整体放大耗时(≥1)，让进度条/续传可见。
    try:
        scale = float((step.task.params or {}).get("duration_scale", 1.0))
        if scale >= 1.0:
            duration_ms = int(duration_ms * scale)
    except (TypeError, ValueError):
        pass
    fail_until_attempt = int(cfg.get("fail_until_attempt", 0))
    fail_rate = float(cfg.get("fail_rate", 0.0))
    timeout_ms = int(cfg.get("timeout_ms", 0))

    if fail_rate and random.random() < fail_rate:
        raise StepError(f"mock 随机失败(rate={fail_rate})")
    if fail_until_attempt and attempt <= fail_until_attempt:
        raise StepError(f"前 {fail_until_attempt} 次注入失败 (attempt={attempt})")

    # 若有处理时长，模拟分片执行，支持进度上报与断点续传
    if duration_ms:
        total = max(int(duration_ms / _PROGRESS_SLICE_MS), 1)
        start = int((resume_from or {}).get("done", 0))
        start = max(min(start, total), 0)  # 夹在 [0, total] 内
        for i in range(start, total):
            time.sleep(_PROGRESS_SLICE_MS / 1000.0)
            if report_progress:
                report_progress(i + 1, total)
        # 超时语义：若配了 timeout_ms 且总时长超过，则判定超时
        if timeout_ms and duration_ms > timeout_ms:
            raise StepTimeoutError(
                f"步骤超时: duration={duration_ms}ms > timeout={timeout_ms}ms"
            )
    elif report_progress:
        # 无耗时也要上报一次“完成”，保证进度快照非空、续传起点明确
        report_progress(0, 0)

    return MOCK_RESULTS.get(
        step.step_key, {"ok": True, "step": step.step_key, "consumed_ms": duration_ms}
    )


def get_version(step_key: str) -> str:
    return PROCESSOR_VERSIONS.get(step_key, "0")
