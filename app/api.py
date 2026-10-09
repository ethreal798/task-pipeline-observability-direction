"""FastAPI 路由：任务/步骤创建、查询、历史、恢复与极简展示页。"""

import html

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session

from . import scheduler
from .backends import CeleryBackend, SyncBackend
from .db import get_db
from .models import Step, Task

router = APIRouter()


def _backend():
    from .config import settings

    mode = getattr(settings, "run_mode", "celery")
    return SyncBackend() if mode == "sync" else CeleryBackend()


class CreateTask(BaseModel):
    pipeline: str
    name: str | None = None
    params: dict | None = None
    # True=创建并立即执行(默认，保持原有语义)；False=仅创建不启动
    auto_start: bool = True


# ------------------------------------------------------------------ serializer

def step_dict(s: Step) -> dict:
    return {
        "id": s.id,
        "step_key": s.step_key,
        "status": s.status,
        "parents": s.parent_keys,
        "attempt": s.attempt,
        "max_attempts": s.max_attempts,
        "started_at": s.started_at.isoformat() if s.started_at else None,
        "finished_at": s.finished_at.isoformat() if s.finished_at else None,
        "duration_ms": s.duration_ms,
        "heartbeat_at": s.heartbeat_at.isoformat() if s.heartbeat_at else None,
        "progress": s.progress,
        "result_ref": s.result_ref,
        "result_hash": s.result_hash,
        "error_info": s.error_info,
    }


def task_dict(t: Task, with_steps: bool = True) -> dict:
    data = {
        "id": t.id,
        "name": t.name,
        "pipeline": t.pipeline,
        "params": t.params,
        "status": t.status,
        "created_at": t.created_at.isoformat() if t.created_at else None,
        "started_at": t.started_at.isoformat() if t.started_at else None,
        "finished_at": t.finished_at.isoformat() if t.finished_at else None,
        "error_info": t.error_info,
    }
    if with_steps:
        data["steps"] = [step_dict(s) for s in sorted(t.steps, key=lambda x: x.step_key)]
    return data


def hist_dict(task: Task) -> dict:
    events = []
    for s in sorted(task.steps, key=lambda x: x.step_key):
        for r in sorted(s.runs, key=lambda x: x.created_at or ""):
            events.append(
                {
                    "step_key": s.step_key,
                    "attempt": r.attempt,
                    "event": r.event,
                    "detail": r.detail,
                    "created_at": r.created_at.isoformat() if r.created_at else None,
                }
            )
    return {"events": events}


# ------------------------------------------------------------------ routes

@router.post("/api/tasks", status_code=201)
def create_and_run(body: CreateTask, db: Session = Depends(get_db)):
    try:
        if body.auto_start:
            task = scheduler.create_and_start(
                db, _backend(), body.pipeline, body.params, body.name
            )
        else:
            task = scheduler.create_task(db, body.pipeline, body.params, body.name)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return task_dict(task)


@router.get("/api/tasks")
def list_tasks(db: Session = Depends(get_db)):
    tasks = db.query(Task).order_by(Task.created_at.desc()).all()
    return {"tasks": [task_dict(t, with_steps=False) for t in tasks]}


@router.get("/api/tasks/{task_id}")
def get_task(task_id: str, db: Session = Depends(get_db)):
    t = db.get(Task, task_id)
    if t is None:
        raise HTTPException(status_code=404, detail="task not found")
    return task_dict(t)


@router.get("/api/tasks/{task_id}/history")
def get_history(task_id: str, db: Session = Depends(get_db)):
    t = db.get(Task, task_id)
    if t is None:
        raise HTTPException(status_code=404, detail="task not found")
    return hist_dict(t)


@router.post("/api/tasks/{task_id}/start")
def start_task(task_id: str, db: Session = Depends(get_db)):
    try:
        task = scheduler.start_task(db, _backend(), task_id)
    except (KeyError, ValueError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    return task_dict(task)


@router.post("/api/tasks/{task_id}/replay")
def replay_task(task_id: str, db: Session = Depends(get_db)):
    try:
        task = scheduler.replay_task(db, _backend(), task_id)
    except (KeyError, ValueError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    return task_dict(task)


@router.get("/api/steps/{step_id}")
def get_step(step_id: str, db: Session = Depends(get_db)):
    s = db.get(Step, step_id)
    if s is None:
        raise HTTPException(status_code=404, detail="step not found")
    return step_dict(s)


@router.get("/")
def dashboard():
    """可观测观测页：单页自包含 HTML（内联 CSS/JS，无外部依赖）。
    仅调用现有 /api/* JSON 端点；不新增后端路由。
    """
    from fastapi.responses import HTMLResponse

    return HTMLResponse(_OBSERVABILITY_PAGE)


_OBSERVABILITY_PAGE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Pipeline D · 任务观测</title>
<script>
/*** 主题切换（默认跟随系统，可手动切换，存 localStorage） ***/
const saved = localStorage.getItem("obs_theme");
if (saved === "light" || saved === "dark") {
  document.documentElement.setAttribute("data-theme", saved);
} else if (window.matchMedia && matchMedia("(prefers-color-scheme: dark)").matches) {
  document.documentElement.setAttribute("data-theme", "dark");
}
</script>
<style>
:root{
  --bg:#f6f7f9; --panel:#ffffff; --border:#e2e6ec; --text:#1f2430;
  --muted:#6b7280; --accent:#2563eb; --hover:#eef2f9;
  --green:#16a34a; --blue:#2563eb; --orange:#d97706;
  --red:#dc2626; --grey:#737a86;
}
[data-theme="dark"]{
  --bg:#0f1216; --panel:#171b22; --border:#2a3140; --text:#e6e9ef;
  --muted:#8a93a5; --accent:#5b8cff; --hover:#1e2530;
  --green:#3ecf7e; --blue:#5b8cff; --orange:#f0a23a;
  --red:#f06a6a; --grey:#8a93a5;
}
*{box-sizing:border-box}
body{margin:0;background:var(--bg);color:var(--text);
  font-family:system-ui,-apple-system,"Segoe UI",Roboto,"PingFang SC",sans-serif;
  font-size:14px;line-height:1.5}
header{position:sticky;top:0;z-index:5;background:var(--panel);border-bottom:1px solid var(--border);
  display:flex;align-items:center;gap:16px;padding:12px 20px;flex-wrap:wrap}
header h1{font-size:17px;margin:0;font-weight:600}
header .hint{color:var(--muted);font-size:12px}
.spacer{flex:1}
button,.btn{cursor:pointer;border:1px solid var(--border);background:var(--panel);
  color:var(--text);border-radius:8px;padding:6px 12px;font-size:13px;transition:background .15s}
button:hover{background:var(--hover)}
button[disabled]{opacity:.5;cursor:not-allowed}
.btn.primary{background:var(--accent);border-color:var(--accent);color:#fff}
.btn.danger{border-color:var(--red);color:var(--red)}
.btn.small{padding:3px 10px;font-size:12px}
select,input{border:1px solid var(--border);background:var(--panel);color:var(--text);
  border-radius:8px;padding:6px 10px;font-size:13px}
main{padding:20px;max-width:1280px;margin:0 auto}
.card{background:var(--panel);border:1px solid var(--border);border-radius:12px;
  padding:16px 20px;margin-bottom:20px}
.card h2{margin:0 0 12px;font-size:15px;font-weight:600}
table{width:100%;border-collapse:collapse}
th,td{text-align:left;padding:9px 10px;border-bottom:1px solid var(--border);font-size:13px}
th{color:var(--muted);font-weight:500;cursor:pointer;user-select:none}
tr.selectable{cursor:pointer}
tr.selectable:hover{background:var(--hover)}
tr.row-selected{background:var(--hover);box-shadow:inset 3px 0 0 var(--accent)}
.badge{display:inline-flex;align-items:center;gap:4px;border-radius:999px;
  padding:1px 9px;font-size:12px;font-weight:500;color:#fff}
.mono{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
.grey{color:var(--muted)}
.legend{display:flex;gap:14px;flex-wrap:wrap;color:var(--muted);font-size:12px;margin-top:6px}
.legend .badge{font-size:11px}
#detail{display:none}
#steps{display:flex;flex-direction:column;gap:8px;margin-top:10px}
.step-card{border:1px solid var(--border);border-radius:10px;padding:10px 12px}
.step-head{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.step-key{font-family:ui-monospace,Menlo,Consolas,monospace;font-weight:600;font-size:13px}
.step-meta{color:var(--muted);font-size:12px;display:flex;gap:12px;flex-wrap:wrap}
.step-parents{font-size:12px;color:var(--muted);margin-top:4px}
.step-parents .arrow{color:var(--muted)}
.err{background:color-mix(in srgb, var(--red) 10%, transparent);
  border:1px solid color-mix(in srgb, var(--red) 40%, transparent);color:var(--red);
  border-radius:8px;padding:10px 12px;font-size:13px;margin-top:8px;
  font-family:ui-monospace,Menlo,Consolas,monospace;white-space:pre-wrap}
.kv{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:6px;
  margin-top:8px;font-size:13px}
.kv b{color:var(--muted);font-weight:500;display:block;font-size:12px}
#history{list-style:none;padding:0;margin:12px 0 0;max-height:360px;overflow:auto}
#history li{display:flex;gap:10px;align-items:baseline;padding:6px 8px;border-bottom:1px solid var(--border);font-size:13px}
#history .h-time{color:var(--muted);font-size:12px;white-space:nowrap}
#history .h-step{font-family:ui-monospace,Menlo,Consolas,monospace;font-weight:600;min-width:120px}
#history .h-detail{color:var(--muted);font-size:12px;white-space:pre-wrap}
#toast{position:fixed;bottom:20px;left:50%;transform:translateX(-50%);background:#111;color:#fff;
  padding:10px 18px;border-radius:8px;font-size:13px;opacity:0;transition:opacity .2s;z-index:10;max-width:80vw}
#toast.show{opacity:1}
.empty{padding:20px;color:var(--muted);text-align:center}
.detail-title{display:flex;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:4px}
.detail-title h2{margin:0;font-size:16px}
.actions{display:flex;gap:10px;margin-top:12px;flex-wrap:wrap}
.row-tools{display:flex;gap:6px}
/* 创建弹窗 */
.modal-backdrop{position:fixed;inset:0;background:rgba(0,0,0,.45);display:none;
  align-items:center;justify-content:center;z-index:20}
.modal-backdrop.show{display:flex}
.modal{background:var(--panel);border:1px solid var(--border);border-radius:14px;
  width:min(560px,92vw);box-shadow:0 12px 40px rgba(0,0,0,.25)}
.modal-head{display:flex;align-items:center;justify-content:space-between;padding:14px 18px;
  border-bottom:1px solid var(--border)}
.modal-head h3{margin:0;font-size:15px;font-weight:600}
.modal-x{background:transparent;border:none;font-size:22px;line-height:1;color:var(--muted);
  cursor:pointer;padding:0 4px}
.modal-x:hover{color:var(--text)}
.modal-body{padding:16px 18px;display:flex;flex-direction:column;gap:14px}
.field{display:flex;flex-direction:column;gap:6px}
.field label{font-size:12px;color:var(--muted);font-weight:500}
.modal-foot{display:flex;justify-content:space-between;gap:10px;padding:14px 18px;
  border-top:1px solid var(--border);flex-wrap:wrap}
.ov-step{display:flex;align-items:center;gap:8px;padding:6px 8px;border:1px solid var(--border);
  border-radius:8px;margin-bottom:6px;font-size:13px;flex-wrap:wrap}
.ov-key{font-family:ui-monospace,Menlo,Consolas,monospace;font-weight:600}
.ov-dep{color:var(--muted);font-size:12px}
.ov-dep .arr{color:var(--muted)}
.ov-chip{font-size:11px;border:1px solid var(--border);border-radius:999px;padding:0 8px;color:var(--muted)}
</style>
</head>
<body>
<header>
  <h1>可观测 AI 任务管线 <span class="grey" style="font-weight:400">(D)</span></h1>
  <span class="hint">Observed via /api/*</span>
  <div class="spacer"></div>
  <button id="themeBtn" title="切换深浅色">主题</button>
</header>

<main>
  <section class="card">
    <h2>任务列表</h2>
    <div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:14px" id="createForm">
      <button class="btn primary" id="openCreateBtn">创建任务</button>
      <span class="grey" style="align-self:center;font-size:12px">点击行查看详情</span>
    </div>
    <div style="overflow-x:auto">
      <table id="taskTable">
        <thead><tr>
          <th data-sort="id">任务</th><th data-sort="pipeline">管线</th>
          <th data-sort="status">状态</th>
          <th data-sort="created_at">创建时间</th>
          <th data-sort="finished_at">完成时间</th>
          <th data-sort="name">名称</th><th>操作</th>
        </tr></thead>
        <tbody id="taskRows"></tbody>
      </table>
    </div>
    <div class="legend">
      <span>状态图例：</span>
      <span class="badge" style="background:var(--green)">成功</span>
      <span class="badge" style="background:var(--blue)">运行中</span>
      <span class="badge" style="background:var(--orange)">重试中</span>
      <span class="badge" style="background:var(--red)">失败</span>
      <span class="badge" style="background:var(--grey)">待启动</span>
      <span class="badge" style="background:var(--green)">已跳过（复用）</span>
      <span class="badge" style="background:var(--green)">已恢复</span>
    </div>
  </section>

  <section class="card" id="detail">
    <div class="detail-title">
      <h2 id="dTitle">任务详情</h2>
      <span id="dStatus" class="badge"></span>
    </div>
    <div id="dMeta" class="kv"></div>
    <div id="dError" class="err" style="display:none"></div>

    <h2 style="margin-top:18px">步骤可视化（DAG）</h2>
    <div id="steps"></div>

    <h2 style="margin-top:18px">执行历史流水线</h2>
    <ul id="history"></ul>

    <div class="actions">
      <button class="btn primary" id="replayBtn">重新执行</button>
      <button class="btn" id="startBtn" disabled>启动（PENDING）</button>
      <button class="btn" id="closeBtn">收起详情</button>
    </div>
  </section>
</main>

<div id="createModal" class="modal-backdrop">
  <div class="modal" role="dialog" aria-modal="true">
    <div class="modal-head">
      <h3>创建任务</h3>
      <button id="closeModalBtn" class="modal-x" title="取消创建" aria-label="关闭">&times;</button>
    </div>
    <div class="modal-body">
      <div class="field">
        <label>管线 pipeline</label>
        <select id="modalPipeline">
          <option value="animate_prep">animate_prep</option>
          <option value="simple_linear">simple_linear</option>
        </select>
      </div>
      <div class="field">
        <label>名称（可选）</label>
        <input id="modalName" placeholder="任务名称" size="24">
      </div>
      <div class="field">
        <label>时长放大 duration_scale（长任务演示，默认1）</label>
        <input id="modalScale" type="number" min="1" step="1" value="1" size="6">
      </div>
      <div class="field">
        <label>步骤概览与依赖</label>
        <div id="modalOverview"></div>
      </div>
    </div>
    <div class="modal-foot">
      <button class="btn" id="confirmCreateBtn">确认创建（仅创建，不执行）</button>
      <button class="btn primary" id="createRunBtn">创建并执行</button>
    </div>
  </div>
</div>

<div id="toast"></div>

<script>
/*** ---------- 小工具 ---------- ***/
const $ = (s)=>document.querySelector(s);
let currentId = null;
let listTimer = null, detailTimer = null;
let sortKey = "created_at", sortAsc = false;

function esc(s){
  return String(s==null?"":s).replace(/[&<>"']/g, c=>
    ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
}
function fmtIso(v){ if(!v) return "-"; const d=new Date(v); if(isNaN(d)) return esc(v);
  const p=n=>String(n).padStart(2,"0");
  return d.getFullYear()+"-"+p(d.getMonth()+1)+"-"+p(d.getDate())+" "+p(d.getHours())+":"+p(d.getMinutes())+":"+p(d.getSeconds());
}
function fmtMs(ms){ if(ms==null) return "-"; if(ms<1000) return Math.round(ms)+" ms"; return (ms/1000).toFixed(2)+" s"; }
function toast(msg){ const t=$("#toast"); t.textContent=msg; t.classList.add("show");
  clearTimeout(toast._t); toast._t=setTimeout(()=>t.classList.remove("show"),2600); }
const KIND_COLORS = {
  SUCCEEDED:"var(--green)", SKIPPED:"var(--green)", RECOVERED:"var(--green)",
  RUNNING:"var(--blue)", START:"var(--blue)",
  RETRYING:"var(--orange)", RETRY:"var(--orange)",
  FAILED:"var(--red)", FAIL:"var(--red)", CANCELLED:"var(--grey)",
  PENDING:"var(--grey)", SUCCESS:"var(--green)", RECOVER:"var(--green)",
  REUSE:"var(--green)", SKIP:"var(--green)"
};
const STATUS_CN = {
  SUCCEEDED:"成功", RUNNING:"运行中", RETRYING:"重试中", FAILED:"失败",
  PENDING:"待启动", SKIPPED:"已跳过（复用）", RECOVERED:"已恢复", CANCELLED:"已取消",
  START:"开始", SUCCESS:"成功", RETRY:"重试", FAIL:"失败", RECOVER:"恢复", REUSE:"复用", SKIP:"跳过"
};
function badge(kind,label){ const c=KIND_COLORS[kind]||"var(--grey)";
  return '<span class="badge" style="background:'+c+'">'+esc(label||STATUS_CN[kind]||kind)+"</span>"; }

/*** ---------- 列表 ---------- ***/
async function refreshList(){
  try{
    const r=await fetch("/api/tasks"); if(!r.ok) throw new Error(r.status);
    const data=await r.json(); const tasks=(data.tasks||[]).slice();
    tasks.sort((a,b)=>{ const av=a[sortKey]||"", bv=b[sortKey]||"";
      if(av<bv) return sortAsc?-1:1; if(av>bv) return sortAsc?1:-1; return 0; });
    $("#taskRows").innerHTML = tasks.map(t=>{
      const short=t.id.slice(0,8);
      const pending=t.status==="PENDING";
      return '<tr class="selectable'+(currentId===t.id?" row-selected":"")+'" data-id="'+esc(t.id)+'">'
        +"<td><span class='mono'>"+esc(short)+"</span></td>"
        +"<td>"+esc(t.pipeline)+"</td>"
        +"<td>"+badge(t.status)+"</td>"
        +"<td>"+fmtIso(t.created_at)+"</td>"
        +"<td>"+fmtIso(t.finished_at)+"</td>"
        +"<td>"+esc(t.name||"")+"</td>"
        +'<td class="row-tools">'
          +(pending
             ?'<button class="btn small primary" data-act="start" data-id="'+esc(t.id)+'">执行</button>'
             :'<button class="btn small primary" data-act="open" data-id="'+esc(t.id)+'">查看</button>'
               +'<button class="btn small" data-act="replay" data-id="'+esc(t.id)+'">重新执行</button>')
        +"</td></tr>";
    }).join("") || '<tr><td colspan="7" class="empty">暂无任务，先创建一个吧</td></tr>';
  }catch(e){ /* 列表轮询静默失败，避免频繁弹提示 */ }
}

$("#taskTable").addEventListener("click", ev=>{
  const btn=ev.target.closest("[data-act]");
  if(btn){ ev.stopPropagation(); const id=btn.dataset.id;
    if(btn.dataset.act==="open") selectTask(id);
    else if(btn.dataset.act==="start") doStart(id);
    else if(btn.dataset.act==="replay") doReplay(id);
    return; }
  const tr=ev.target.closest("tr.selectable");
  if(tr) selectTask(tr.dataset.id);
});
$("#taskTable").addEventListener("click", ev=>{ // 表头排序
  const th=ev.target.closest("th"); if(!th) return;
  const k=th.dataset.sort; if(!k) return;
  if(sortKey===k) sortAsc=!sortAsc; else { sortKey=k; sortAsc=false; }
  refreshList();
});

/*** ---------- 详情 ---------- ***/
function selectTask(id){
  currentId=id; refreshList();
  $("#detail").style.display="block";
  refreshDetail();
  clearInterval(detailTimer);
  detailTimer=setInterval(refreshDetail,2000);
  $("#detail").scrollIntoView({behavior:"smooth",block:"start"});
}
function renderSteps(task){
  const steps=task.steps||[]; const byKey={}; steps.forEach(s=>byKey[s.step_key]=s);
  $("#steps").innerHTML = steps.map(s=>{
    const parents=(s.parents||[]).map(p=>byKey[p]).filter(Boolean)
      .map(p=>esc(p.step_key)).join(", ") || "—";
    const err = s.status==="FAILED" && s.error_info
      ? '<div class="err">'+esc(JSON.stringify(s.error_info))+"</div>" : "";
    return '<div class="step-card">'
      +'<div class="step-head">'
        +'<span class="step-key">'+esc(s.step_key)+"</span>"
        +badge(s.status)
        +(s.status==="RETRYING"?'<span class="step-meta">attempt '+esc(s.attempt)+'/'+esc(s.max_attempts)+"</span>":"")
      +'</div>'
      +'<div class="step-meta">'
        +'<span>attempt '+esc(s.attempt)+'/'+esc(s.max_attempts)+"</span>"
        +'<span>耗时 '+fmtMs(s.duration_ms)+"</span>"
        +'<span>开始 '+fmtIso(s.started_at)+"</span>"
        +'<span>结束 '+fmtIso(s.finished_at)+"</span>"
        +(s.result_hash?'<span class="mono">hash '+esc(s.result_hash.slice(0,10))+"</span>":"")
      +'</div>'
      +'<div class="step-parents"><span class="arrow">依赖: </span>'+parents+"</div>"
      +pgBar(s)
      +err+"</div>";
  }).join("") || '<div class="empty">无步骤</div>';
}
function renderHistory(events){
  events=events||[];
  events.sort((a,b)=>String(a.created_at||"").localeCompare(String(b.created_at||"")));
  $("#history").innerHTML = events.slice().reverse().map(e=>{
    const d=JSON.stringify(e.detail);
    let detail="";
    try{ const o=JSON.parse(d); detail=Object.keys(o).map(k=>k+"="+JSON.stringify(o[k])).join("  ");}
    catch(_){ detail=d && d!=="null" ? d.slice(0,200) : ""; }
    return '<li><span class="h-time">'+fmtIso(e.created_at)+"</span>"
      +'<span class="h-step">'+esc(e.step_key)+"</span>"
      +badge(e.event,(STATUS_CN[e.event]||e.event)+" #"+esc(e.attempt))+"</li>"
      + (detail?'<li style="padding-top:0"><span class="h-time"></span><span class="h-step"></span><span class="h-detail">'+esc(detail)+"</span></li>":"");
  }).join("") || '<li class="empty">暂无历史事件</li>';
}
// 基于 step.progress 渲染进度条；无进度(未开始/未上报)则返回空
function pgBar(s){
  const pg=s.progress; if(!pg || !(pg.total>0)) return "";
  let pct=Math.round((pg.done||0)/pg.total*100);
  if(pct>100) pct=100;
  const label=pg.phase?(" · "+esc(pg.phase)):"";
  return '<div style="margin-top:4px;background:#ececf1;border-radius:3px;height:8px;position:relative">'
    +'<div style="width:'+pct+'%;background:#3b82f6;height:8px;border-radius:3px"></div>'
    +'<span style="position:absolute;left:6px;top:-3px;font-size:11px;color:#555">'+pct+"%"+label+"</span></div>";
}
async function refreshDetail(){
  if(!currentId) return;
  let task=null, events=null;
  try{ const r=await fetch("/api/tasks/"+currentId); if(r.ok) task=await r.json(); }
  catch(e){}
  try{ const r=await fetch("/api/tasks/"+currentId+"/history"); if(r.ok) events=(await r.json()).events; }
  catch(e){}
  if(!task) return;
  $("#dTitle").textContent="任务详情 · "+task.id.slice(0,8)+(task.name?" · "+task.name:"");
  $("#dStatus").outerHTML='<span id="dStatus" class="badge" style="background:'+(KIND_COLORS[task.status]||"var(--grey)")+'">'+esc(STATUS_CN[task.status]||task.status)+"</span>";
  $("#dMeta").innerHTML =
    '<div><b>管线</b>'+esc(task.pipeline)+"</div>"
    +'<div><b>创建</b>'+fmtIso(task.created_at)+"</div>"
    +'<div><b>开始</b>'+fmtIso(task.started_at)+"</div>"
    +'<div><b>完成</b>'+fmtIso(task.finished_at)+"</div>"
    +(task.params?'<div><b>参数</b><span class="mono">'+esc(JSON.stringify(task.params))+"</span></div>":"");
  const de=$("#dError");
  if(task.error_info){ de.style.display="block"; de.textContent="失败原因: "+JSON.stringify(task.error_info); }
  else de.style.display="none";
  renderSteps(task);
  renderHistory(events);
  const startBtn=$("#startBtn");
  startBtn.disabled = task.status!=="PENDING";
  startBtn.style.display = task.status==="PENDING" ? "" : "none";
}

/*** ---------- 操作 ---------- ***/
async function doAction(id,path,label){
  try{
    const r=await fetch("/api/tasks/"+id+path,{method:"POST"});
    if(!r.ok){ const m=await r.json().catch(()=>({})); throw new Error(m.detail||("HTTP "+r.status)); }
    toast(label+" 已触发"); refreshList(); if(currentId===id) refreshDetail();
  }catch(e){ toast(label+" 失败: "+e.message); }
}
/*** ---------- 创建（两步走弹窗） ---------- ***/
// 硬编码展示用的 pipeline 步骤+依赖概览（与 graph.py 的 parent_keys 一致）
const PIPELINE_OVERVIEW = {
  "animate_prep": {
    steps:["ingest","transcode","frame_extract","ocr_subtitle","render_preview","vectorize"],
    deps:{transcode:["ingest"], frame_extract:["transcode"],
          ocr_subtitle:["frame_extract"], render_preview:["frame_extract"], vectorize:["ocr_subtitle"]}
  },
  "simple_linear": {
    steps:["step_a","step_b","step_c"],
    deps:{step_b:["step_a"], step_c:["step_b"]}
  }
};
function openCreateModal(){
  $("#modalPipeline").value="animate_prep";
  $("#modalName").value="";
  renderModalOverview();
  $("#createModal").classList.add("show");
}
function closeCreateModal(){ $("#createModal").classList.remove("show"); }
function renderModalOverview(){
  const pname=$("#modalPipeline").value;
  const ov=PIPELINE_OVERVIEW[pname]||{steps:[],deps:{}};
  const children={};
  Object.keys(ov.deps).forEach(k=>(ov.deps[k]||[]).forEach(p=>(children[p]=(children[p]||[]).concat(k))));
  $("#modalOverview").innerHTML = ov.steps.map(s=>{
    const parents=(ov.deps[s]||[]).filter(p=>ov.steps.includes(p));
    const depsTxt = parents.length
      ? '<span class="ov-dep"><span class="arr">依赖: </span>'+parents.map(p=>'<span class="ov-chip">'+esc(p)+"</span>").join(" ")+"</span>"
      : '<span class="ov-dep"><span class="ov-chip" style="opacity:.5">根步骤（无前置依赖）</span></span>';
    const branch = parents.length===1 && (children[parents[0]]||[]).length>1
      ? '<span class="ov-chip" style="border-color:var(--orange);color:var(--orange)">并行分支</span>' : "";
    return '<div class="ov-step"><span class="ov-key">'+esc(s)+"</span>"+depsTxt+branch+"</div>";
  }).join("");
}
function doCreate(run){
  const pipeline=$("#modalPipeline").value;
  const name=$("#modalName").value.trim();
  const body={pipeline:pipeline, auto_start:!!run}; if(name) body.name=name;
  const scale=parseFloat($("#modalScale").value); if(scale>1) body.params={duration_scale:scale};
  const btn=$((run?"#createRunBtn":"#confirmCreateBtn"));
  btn.disabled=true;
  fetch("/api/tasks",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(body)})
    .then(r=>{ if(!r.ok){ return r.json().then(m=>({err:m})).catch(()=>({err:{detail:"HTTP "+r.status}})); } return r.json(); })
    .then(res=>{ if(res.err) throw new Error(res.err.detail||"创建失败");
      toast(run?"任务已创建并执行":"任务已创建（尚未执行）");
      closeCreateModal(); refreshList();
      if(run) selectTask(res.id);
      else if(currentId) refreshDetail(); })
    .catch(e=>toast("创建失败: "+e.message))
    .finally(()=>{ btn.disabled=false; });
}
function doStart(id){
  fetch("/api/tasks/"+id+"/start",{method:"POST"})
    .then(r=>{ if(!r.ok){ return r.json().then(m=>({err:m})).catch(()=>({err:{detail:"HTTP "+r.status}})); } return r.json(); })
    .then(res=>{ if(res.err) throw new Error(res.err.detail||"执行失败");
      toast("任务已开始执行"); refreshList(); selectTask(id); })
    .catch(e=>toast("执行失败: "+e.message));
}

function doReplay(id){
  fetch("/api/tasks/"+id+"/replay",{method:"POST"})
    .then(r=>{ if(!r.ok){ return r.json().then(m=>({err:m})).catch(()=>({err:{detail:"HTTP "+r.status}})); } return r.json(); })
    .then(res=>{ if(res.err) throw new Error(res.err.detail||"重新执行失败");
      toast("任务已重新执行"); refreshList(); selectTask(id); })
    .catch(e=>toast("重新执行失败: "+e.message));
}

$("#openCreateBtn").addEventListener("click", openCreateModal);
$("#closeModalBtn").addEventListener("click", closeCreateModal);
$("#createModal").addEventListener("click", ev=>{ if(ev.target.id==="createModal") closeCreateModal(); });
$("#modalPipeline").addEventListener("change", renderModalOverview);
$("#confirmCreateBtn").addEventListener("click", ()=>doCreate(false));
$("#createRunBtn").addEventListener("click", ()=>doCreate(true));
$("#modalName").addEventListener("keydown", e=>{ if(e.key==="Enter") doCreate(true); });
$("#replayBtn").addEventListener("click", ()=>currentId&&doAction(currentId,"/replay","重新执行"));
$("#startBtn").addEventListener("click", ()=>currentId&&doStart(currentId));
$("#closeBtn").addEventListener("click", ()=>{ $("#detail").style.display="none";
  currentId=null; clearInterval(detailTimer); refreshList(); });
$("#themeBtn").addEventListener("click", ()=>{
  const cur=document.documentElement.getAttribute("data-theme")==="dark"?"light":"dark";
  document.documentElement.setAttribute("data-theme",cur); localStorage.setItem("obs_theme",cur);
});

refreshList();
listTimer=setInterval(refreshList,3000);
</script>
</body>
</html>
"""
