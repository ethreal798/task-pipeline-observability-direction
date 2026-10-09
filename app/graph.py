"""Pipeline 定义：显式声明步骤 DAG（用 parent_keys 表达依赖）。

选用显式依赖而非通用 DAG 引擎：
- 每个 step 边界清晰、可单独暂停/重跑/复用；
- 便于在文档里论证“哪些动画管线步骤适合独立成可恢复任务”。
"""

# pipeline 名 -> 步骤定义列表
# step: {key, parents[], max_attempts, config{}}
PIPELINES = {
    "animate_prep": {
        "steps": [
            {"key": "ingest", "parents": [], "max_attempts": 3,
             "config": {"duration_ms": 120}},
            {"key": "transcode", "parents": ["ingest"], "max_attempts": 3,
             "config": {"duration_ms": 800}},
            {"key": "frame_extract", "parents": ["transcode"], "max_attempts": 3,
             "config": {"duration_ms": 600, "fail_until_attempt": 1}},
            {"key": "ocr_subtitle", "parents": ["frame_extract"], "max_attempts": 3,
             "config": {"duration_ms": 400}},
            {"key": "render_preview", "parents": ["frame_extract"], "max_attempts": 2,
             "config": {"duration_ms": 900}},
            {"key": "vectorize", "parents": ["ocr_subtitle"], "max_attempts": 3,
             "config": {"duration_ms": 500}},
        ]
    },
    "simple_linear": {
        "steps": [
            {"key": "step_a", "parents": [], "max_attempts": 3,
             "config": {"duration_ms": 50}},
            {"key": "step_b", "parents": ["step_a"], "max_attempts": 3,
             "config": {"duration_ms": 50}},
            {"key": "step_c", "parents": ["step_b"], "max_attempts": 3,
             "config": {"duration_ms": 50}},
        ]
    },
}


def step_defs(pipeline_name: str) -> list:
    if pipeline_name not in PIPELINES:
        raise ValueError(f"未知 pipeline: {pipeline_name}")
    # 深拷贝，避免不同任务共享同一 dict 被改
    import copy

    return copy.deepcopy(PIPELINES[pipeline_name]["steps"])
