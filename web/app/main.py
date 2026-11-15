"""FastAPI web service: trigger/inspect collections and read published days."""
from __future__ import annotations

import logging
import os
import socket
import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, Response
from pydantic import BaseModel, Field

from . import archive, db
from .collector import CollectionError, collect_day
from .station_client import StationClient, StationError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("web")

DB_PATH = os.environ.get("DB_PATH", "/data/quality.db")
STATION_URL = os.environ.get("STATION_URL", "http://station:8000")
WORKER = os.environ.get("WORKER_NAME") or f"{socket.gethostname()}-{os.getpid()}"
# Test-only hooks (one-shot failpoint injection + file inspection) are mounted
# only when explicitly enabled; they let the demo prove interruption/corruption
# behaviour over HTTP against a real shared volume.
HANDOVER_TEST_HOOKS = os.environ.get("HANDOVER_TEST_HOOKS") == "1"

_hook_lock = threading.Lock()
_oneshot_failpoint: str | None = None

# Background collection threads per process; the DB unique index is the real
# cross-process guard, this is just local bookkeeping.
_threads: dict[int, threading.Thread] = {}


@asynccontextmanager
async def lifespan(app: FastAPI) -> None:
    db.init_db(DB_PATH)
    recovered = db.recover_interrupted(DB_PATH)
    if recovered:
        log.warning("marked %s interrupted run(s) from a previous worker life", recovered)
    swept = archive.cleanup_orphans(DB_PATH)
    if swept:
        log.warning("removed %s orphaned handover file(s)", swept)
    log.info("worker %s ready, db=%s station=%s", WORKER, DB_PATH, STATION_URL)
    yield


app = FastAPI(title="Section water quality collector", version="1.0", lifespan=lifespan)


class CollectBody(BaseModel):
    section: str = Field(..., examples=["SECTION-A"])
    date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$", examples=["2026-11-14"])


def _failpoint(stage: str) -> None:
    global _oneshot_failpoint
    with _hook_lock:
        oneshot = _oneshot_failpoint == stage
        if oneshot:
            _oneshot_failpoint = None
    if oneshot:
        raise archive.HandoverError(f"injected one-shot handover failure at {stage}")


def _run_collection(run_id: int, section: str, date_str: str) -> None:
    client = StationClient(STATION_URL)
    try:
        collect_day(DB_PATH, run_id, section, date_str, client, WORKER)
    except CollectionError as exc:
        log.error("run %s failed: %s", run_id, exc)
        db.fail_run(DB_PATH, run_id, str(exc))
    except StationError as exc:
        log.error("run %s station error: %s", run_id, exc)
        db.fail_run(DB_PATH, run_id, f"station error: {exc}")
    except Exception as exc:  # defensive: never leave a run dangling
        log.exception("run %s unexpected error", run_id)
        db.fail_run(DB_PATH, run_id, f"unexpected error: {exc}")


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "worker": WORKER}


@app.get("/api/sections")
def api_sections() -> dict:
    try:
        return {"sections": StationClient(STATION_URL).list_sections()}
    except StationError as exc:
        raise HTTPException(status_code=502, detail=str(exc))


@app.post("/api/collect")
def api_collect(body: CollectBody) -> JSONResponse:
    try:
        run_id = db.start_run(DB_PATH, body.section, body.date, WORKER)
    except db.ActiveRunError as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    thread = threading.Thread(
        target=_run_collection,
        args=(run_id, body.section, body.date),
        name=f"collect-{run_id}",
        daemon=True,
    )
    _threads[run_id] = thread
    thread.start()
    return JSONResponse(
        status_code=202,
        content={
            "run_id": run_id,
            "section": body.section,
            "date": body.date,
            "worker": WORKER,
            "status": "running",
        },
    )


@app.get("/api/runs/{run_id}")
def api_run(run_id: int) -> dict:
    run = db.get_run(DB_PATH, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="run not found")
    return run


@app.get("/api/runs")
def api_runs(section: str, date: str) -> list[dict]:
    return db.list_runs(DB_PATH, section, date)


@app.get("/api/published")
def api_published(section: str, date: str) -> dict:
    published = db.get_published(DB_PATH, section, date)
    if published is None:
        raise HTTPException(
            status_code=404,
            detail="no complete published version yet for this section/day",
        )
    return published


class HandoverBody(BaseModel):
    section: str = Field(..., examples=["SECTION-A"])
    date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$", examples=["2026-11-14"])
    revision: int | None = Field(
        None, ge=1, description="snapshot revision; defaults to the current published one"
    )


@app.post("/api/handovers")
def api_create_handover(body: HandoverBody) -> JSONResponse:
    """Freeze the committed day into an immutable CSV archive.

    201 for a newly created archive; 200 with the *original* metadata when the
    same section/day/revision was already archived (idempotent, same hash).
    """
    try:
        meta, created = archive.create_handover(
            DB_PATH,
            body.section,
            body.date,
            body.revision,
            worker=WORKER,
            failpoint=_failpoint,
        )
    except archive.NoPublishedSnapshot as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except archive.HandoverError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return JSONResponse(
        status_code=201 if created else 200,
        content={**meta, "created": created},
    )


@app.get("/api/handovers")
def api_list_handovers(section: str, date: str) -> list[dict]:
    return archive.list_handovers(DB_PATH, section, date)


@app.get("/api/handovers/{handover_id}")
def api_get_handover(handover_id: str) -> dict:
    try:
        return archive.get_handover(DB_PATH, handover_id)
    except archive.HandoverNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))


@app.get("/api/handovers/{handover_id}/download")
def api_download_handover(handover_id: str) -> Response:
    """Serve the exact archived bytes; never rebuild from current rows."""
    try:
        meta, data = archive.read_handover(DB_PATH, handover_id)
    except archive.HandoverNotFound as exc:
        raise HTTPException(status_code=404, detail=str(exc))
    except archive.HandoverCorrupt as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    return Response(
        content=data,
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": f'attachment; filename="{meta["filename"]}"',
            "Content-Length": str(len(data)),
            "X-Handover-SHA256": meta["sha256"],
            "X-Handover-Revision": str(meta["revision"]),
            "X-Handover-Rows": str(meta["row_count"]),
        },
    )


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return PAGE_HTML


if HANDOVER_TEST_HOOKS:
    # Mounted only with HANDOVER_TEST_HOOKS=1; used by demo/demo.py to prove
    # interruption and tamper handling against a real shared volume.
    class FailpointBody(BaseModel):
        stage: str = Field("after_file_write", pattern=r"^after_file_write$")

    @app.post("/api/_test/failpoint")
    def _set_failpoint(body: FailpointBody) -> dict:
        global _oneshot_failpoint
        with _hook_lock:
            _oneshot_failpoint = body.stage
        return {"armed": body.stage}

    @app.get("/api/_test/handover-files")
    def _handover_files() -> dict:
        directory = archive.handover_dir(DB_PATH)
        names = sorted(n for n in os.listdir(directory)) if os.path.isdir(directory) else []
        return {"directory": directory, "files": names}

    @app.post("/api/_test/corrupt-handover/{handover_id}")
    def _corrupt_handover(handover_id: str) -> dict:
        meta = archive.get_handover(DB_PATH, handover_id)
        path = os.path.join(archive.handover_dir(DB_PATH), meta["filename"])
        with open(path, "ab") as fh:
            fh.write(b"#tampered\n")
        return {"corrupted": meta["filename"], "size_bytes": os.path.getsize(path)}

    @app.delete("/api/_test/handover-file/{handover_id}")
    def _delete_handover_file(handover_id: str) -> dict:
        meta = archive.get_handover(DB_PATH, handover_id)
        path = os.path.join(archive.handover_dir(DB_PATH), meta["filename"])
        os.unlink(path)
        return {"deleted": meta["filename"]}


PAGE_HTML = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<title>断面水质采集台 · 氨氮逐时观测</title>
<style>
  body { font-family: system-ui, "Segoe UI", sans-serif; margin: 2rem; background: #f6f8fa; color: #1f2933; }
  h1 { font-size: 1.3rem; }
  .panel { background: #fff; border: 1px solid #d9e2ec; border-radius: 8px; padding: 1rem 1.2rem; margin-bottom: 1.2rem; }
  label { margin-right: .4rem; font-weight: 600; }
  select, input { padding: .35rem .5rem; border: 1px solid #9fb3c8; border-radius: 6px; }
  button { padding: .4rem 1rem; border: 0; border-radius: 6px; background: #0b6e4f; color: #fff; cursor: pointer; }
  button:disabled { background: #94a3b8; cursor: default; }
  table { border-collapse: collapse; width: 100%; font-size: .9rem; }
  th, td { border: 1px solid #d9e2ec; padding: .35rem .6rem; text-align: left; }
  th { background: #eef2f6; }
  .muted { color: #627d98; font-size: .85rem; }
  .tag { display: inline-block; padding: .1rem .5rem; border-radius: 999px; font-size: .8rem; }
  .running { background: #fff3cd; } .success { background: #d1f0dd; }
  .failed { background: #f8d4d4; } .interrupted { background: #e1d5f7; }
  #msg { white-space: pre-wrap; font-family: ui-monospace, monospace; font-size: .85rem; }
  .mono { font-family: ui-monospace, monospace; font-size: .8rem; word-break: break-all; }
  a.dl { color: #0b6e4f; font-weight: 600; }
  #handoverMsg { white-space: pre-wrap; color: #b02a37; }
</style>
</head>
<body>
<h1>断面水质采集台 — 逐时氨氮（NH₃-N）观测</h1>

<div class="panel">
  <label>断面
    <select id="section"></select>
  </label>
  <label>UTC 日期
    <input id="date" type="date" required>
  </label>
  <button id="collectBtn">开始采集并整日发布</button>
  <button id="viewBtn">查看已发布版本</button>
  <p class="muted">采集为后台任务：逐页核对修订号，收齐总行数并确认源端修订未变后才整日发布；修订变化或读取失败时保留旧版。</p>
</div>

<div class="panel">
  <h3>采集任务</h3>
  <div id="runs"></div>
</div>

<div class="panel">
  <h3>已发布观测（时刻 / 数值 / 来源页 / 修订号）</h3>
  <div id="published"></div>
</div>

<div class="panel">
  <h3>复核交接档（不可变 CSV 归档）</h3>
  <p class="muted">把当前已发布修订冻结为交接 CSV（时刻、NH₃-N、来源页），登记修订号、行数与 SHA-256。同一修订重复归档只返回原档；后续更正/重新采集不会改变旧档。</p>
  <label>快照修订号
    <input id="handoverRevision" type="number" min="1" placeholder="留空=当前已发布修订" style="width:11rem">
  </label>
  <button id="handoverBtn">生成交接档</button>
  <button id="handoverRefreshBtn">刷新档案列表</button>
  <div id="handoverMsg"></div>
  <div id="handovers"></div>
</div>

<div class="panel">
  <h3>消息</h3>
  <div id="msg" class="muted"></div>
</div>

<script>
const $ = id => document.getElementById(id);
const msg = t => { $('msg').textContent = t; };
const esc = s => String(s).replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));

async function loadSections() {
  const r = await fetch('/api/sections');
  if (!r.ok) { msg('无法获取断面列表：' + r.status); return; }
  const data = await r.json();
  $('section').innerHTML = data.sections.map(s => `<option>${esc(s)}</option>`).join('');
}

function params() {
  return new URLSearchParams({ section: $('section').value, date: $('date').value });
}

async function startCollect() {
  const date = $('date').value;
  if (!date) { msg('请选择 UTC 日期'); return; }
  $('collectBtn').disabled = true;
  try {
    const r = await fetch('/api/collect', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ section: $('section').value, date })
    });
    const data = await r.json();
    if (!r.ok) { msg('启动失败 (' + r.status + '): ' + JSON.stringify(data.detail)); return; }
    msg('任务已启动 run_id=' + data.run_id + ' worker=' + data.worker);
    pollRun(data.run_id);
  } finally {
    $('collectBtn').disabled = false;
  }
}

async function pollRun(runId) {
  const timer = setInterval(async () => {
    const r = await fetch('/api/runs/' + runId);
    const run = await r.json();
    await renderRuns();
    if (run.status !== 'running') {
      clearInterval(timer);
      msg('任务结束：' + run.status + (run.error ? ' — ' + run.error : ''));
      await renderPublished();
    }
  }, 700);
}

async function renderRuns() {
  const r = await fetch('/api/runs?' + params());
  const runs = await r.json();
  if (!runs.length) { $('runs').innerHTML = '<p class="muted">暂无任务</p>'; return; }
  $('runs').innerHTML = `<table><tr><th>run</th><th>worker</th><th>状态</th><th>修订号</th>
    <th>行数</th><th>页尝试</th><th>开始(UTC)</th><th>结束(UTC)</th><th>错误</th></tr>` +
    runs.map(r => `<tr><td>${r.id}</td><td>${esc(r.worker)}</td>
      <td><span class="tag ${r.status}">${r.status}</span></td>
      <td>${r.revision ?? '—'}</td><td>${r.rows_collected}</td><td>${r.page_attempts}</td>
      <td>${esc(r.started_at)}</td><td>${esc(r.finished_at ?? '—')}</td>
      <td>${esc(r.error ?? '')}</td></tr>`).join('') + '</table>';
}

async function renderPublished() {
  const r = await fetch('/api/published?' + params());
  if (r.status === 404) {
    $('published').innerHTML = '<p class="muted">尚无完整发布版本（不完整采集不会公布）。</p>';
    $('handoverRevision').placeholder = '留空=当前已发布修订';
    return;
  }
  const d = await r.json();
  $('handoverRevision').placeholder = '当前修订 ' + d.revision;
  $('published').innerHTML =
    `<p>修订号 <b>${d.revision}</b> · 发布于 ${esc(d.published_at)} · 发布者 ${esc(d.published_by)} · 共 ${d.rows.length} 行</p>` +
    '<table><tr><th>时刻(UTC)</th><th>NH₃-N (mg/L)</th><th>来源页</th><th>修订号</th></tr>' +
    d.rows.map(x => `<tr><td>${esc(x.time)}</td><td>${x.nh3n}</td><td>${x.source_page}</td><td>${x.revision}</td></tr>`).join('') +
    '</table>';
}

async function renderHandovers() {
  const r = await fetch('/api/handovers?' + params());
  if (!r.ok) { $('handovers').innerHTML = '<p class="muted">无法读取档案列表</p>'; return; }
  const hs = await r.json();
  if (!hs.length) { $('handovers').innerHTML = '<p class="muted">该断面/日期尚无交接档。</p>'; return; }
  $('handovers').innerHTML =
    '<table><tr><th>ID</th><th>修订号</th><th>行数</th><th>SHA-256</th><th>大小</th>' +
    '<th>归档时间(UTC)</th><th>归档者</th><th>下载</th></tr>' +
    hs.map(h => `<tr><td class="mono">${esc(h.id)}</td><td>${h.revision}</td><td>${h.row_count}</td>
      <td class="mono">${esc(h.sha256)}</td><td>${h.size_bytes} B</td><td>${esc(h.created_at)}</td>
      <td>${esc(h.created_by)}</td>
      <td><a class="dl" href="${esc(h.download_url)}" download>下载 CSV</a></td></tr>`).join('') + '</table>';
}

async function createHandover() {
  const date = $('date').value;
  if (!date) { $('handoverMsg').textContent = '请选择 UTC 日期'; return; }
  $('handoverBtn').disabled = true;
  $('handoverMsg').textContent = '';
  try {
    const raw = $('handoverRevision').value.trim();
    const body = { section: $('section').value, date };
    if (raw) body.revision = Number(raw);
    const r = await fetch('/api/handovers', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(body)
    });
    const data = await r.json();
    if (!r.ok) {
      $('handoverMsg').textContent = '归档失败 (' + r.status + '): ' + (data.detail || JSON.stringify(data));
      return;
    }
    $('handoverMsg').textContent = (data.created ? '已创建新交接档' : '该修订已有原档，返回原档') +
      `：revision=${data.revision} rows=${data.row_count} sha256=${data.sha256}`;
    await renderHandovers();
  } finally {
    $('handoverBtn').disabled = false;
  }
}

$('collectBtn').onclick = startCollect;
$('viewBtn').onclick = async () => { await renderRuns(); await renderPublished(); await renderHandovers(); };
$('handoverBtn').onclick = createHandover;
$('handoverRefreshBtn').onclick = renderHandovers;
$('section').onchange = async () => { await renderRuns(); await renderPublished(); await renderHandovers(); };
$('date').onchange = async () => { await renderRuns(); await renderPublished(); await renderHandovers(); };
$('date').value = new Date().toISOString().slice(0, 10);
loadSections().then(renderRuns).then(renderPublished).then(renderHandovers);
</script>
</body>
</html>
"""
