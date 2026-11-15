"""End-to-end demonstration for the water-quality collection platform.

Runnable with an isolated interpreter:

    python -I demo/demo.py                 # against an already running stack
    python -I demo/demo.py --start         # also boots station + 2 web workers locally

Scenarios:
  1. Mid-flight correction  -> stale cursor 409, run fails, nothing partial is
     published; re-collection publishes the new revision.
  2. Transient read failure -> current page is retried, run still succeeds.
  3. Two web processes collecting the same day -> only one runs; plus an
     end-to-end stale-revision check proving a late lower revision can never
     overwrite a newer published revision, plus a direct DB-guard unit check.
  4. Handover archives -> exact frozen CSV per committed revision, idempotent
     repeats, cross-process racing registration, interrupted writes leaving no
     downloadable artifact, and download-time checksum/missing-file errors that
     never rebuild from current data.
  5. Archiving racing a publish -> every archive holds one committed revision's
     complete observation set, never a mixture.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "web"))  # db/archive are stdlib-only, safe under -I

RESULTS: list[tuple[bool, str]] = []


def check(ok: bool, label: str, detail: str = "") -> None:
    RESULTS.append((ok, label))
    mark = "PASS" if ok else "FAIL"
    print(f"[{mark}] {label}" + (f" -- {detail}" if detail else ""))
    if not ok:
        raise AssertionError(label)


def request(method: str, url: str, body: dict | None = None) -> tuple[int, dict]:
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, json.loads(exc.read().decode())
        except Exception:
            return exc.code, {}


def request_raw(url: str) -> tuple[int, dict[str, str], bytes]:
    req = urllib.request.Request(url)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, dict(exc.headers), exc.read()


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def expected_csv(published: dict) -> bytes:
    """Re-render the published rows exactly the way the archive does."""
    buf = io.StringIO(newline="")
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(["time", "nh3n", "source_page"])
    for r in published["rows"]:
        writer.writerow([r["time"], repr(float(r["nh3n"])), r["source_page"]])
    return buf.getvalue().encode("utf-8")


def wait_run(base: str, run_id: int, timeout: float = 30.0) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        status, run = request("GET", f"{base}/api/runs/{run_id}")
        if status == 200 and run["status"] != "running":
            return run
        time.sleep(0.4)
    raise TimeoutError(f"run {run_id} did not finish in {timeout}s")


def reset_day(station: str, section: str, date: str) -> None:
    request("POST", f"{station}/admin/reset", {"section": section, "date": date})


def published_or_none(web: str, section: str, date: str) -> dict | None:
    status, body = request("GET", f"{web}/api/published?section={section}&date={date}")
    return body if status == 200 else None


def scenario_mid_correction(station: str, web: str, section: str, date: str) -> None:
    print("\n=== 场景 1：采集中途县站更正记录（旧游标失效 / 整日发布）===")
    reset_day(station, section, date)

    # Capture a cursor minted at revision 1, before any correction happens.
    status, page = request("GET", f"{station}/observations?section={section}&date={date}")
    old_cursor = page["next_cursor"]
    check(status == 200 and page["revision"] == 1 and old_cursor, "取得修订号 1 的分页游标")

    status, started = request("POST", f"{web}/api/collect", {"section": section, "date": date})
    check(status == 202, "启动采集任务", json.dumps(started))
    run_id = started["run_id"]

    # Wait until the first page is being/has been processed, then correct.
    time.sleep(0.7)
    status, corr = request(
        "POST", f"{station}/admin/correct",
        {"section": section, "date": date, "time": "08:00", "nh3n": 9.999},
    )
    check(status == 200 and corr["revision"] == 2, "县站更正后修订号递增到 2", json.dumps(corr))

    run = wait_run(web, run_id)
    check(run["status"] == "failed", "混入旧修订页的采集任务失败", run.get("error", ""))
    check("revision" in run["error"].lower() or "cursor" in run["error"].lower(),
          "失败原因为修订变化/旧游标", run["error"])
    check(published_or_none(web, section, date) is None,
          "失败后没有任何部分数据被公布")

    # Direct proof that the old cursor is now dead at the source.
    status, _ = request("GET", f"{station}/observations?section={section}&date={date}&cursor={old_cursor}")
    check(status == 409, "复用更正前游标返回 409 stale_cursor")

    # Re-collect: full day at revision 2 gets published atomically.
    status, started2 = request("POST", f"{web}/api/collect", {"section": section, "date": date})
    check(status == 202, "重新启动采集")
    run2 = wait_run(web, started2["run_id"])
    check(run2["status"] == "success" and run2["revision"] == 2,
          "重新采集成功并按修订号 2 发布", json.dumps(run2))

    published = published_or_none(web, section, date)
    check(published is not None and published["revision"] == 2, "已发布版本修订号为 2")
    check(len(published["rows"]) == 24, "收齐总行数 24 行")
    times = [r["time"] for r in published["rows"]]
    check(len(set(times)) == 24 and all(t.endswith(":00") for t in times),
          "当日时刻唯一且为整点 UTC 时刻")
    row8 = next(r for r in published["rows"] if r["time"] == "08:00")
    check(row8["nh3n"] == 9.999, "更正后的数值被发布", json.dumps(row8))
    check(all(r["source_page"] >= 1 and r["revision"] == 2 for r in published["rows"]),
          "每行都记录来源页与修订号")

    # Same revision again: idempotent success, still revision 2.
    status, started3 = request("POST", f"{web}/api/collect", {"section": section, "date": date})
    run3 = wait_run(web, started3["run_id"])
    check(run3["status"] == "success" and run3["revision"] == 2,
          "同修订号重复采集幂等成功")


def scenario_read_failure(station: str, web: str, section: str, date: str) -> None:
    print("\n=== 场景 2：页读取失败，仅重试当前页后成功 ===")
    reset_day(station, section, date)
    request("POST", f"{station}/admin/fail-next",
            {"section": section, "date": date})

    # The injected failure surfaces directly at the station endpoint.
    status, _ = request("GET", f"{station}/observations?section={section}&date={date}")
    check(status == 500, "县站确实对下一次读取返回 500")

    # Re-arm: the probe above consumed the one-shot failure.
    request("POST", f"{station}/admin/fail-next",
            {"section": section, "date": date})

    status, started = request("POST", f"{web}/api/collect", {"section": section, "date": date})
    run = wait_run(web, started["run_id"])
    check(run["status"] == "success", "重试后整日采集成功", run.get("error", ""))
    check(run["page_attempts"] >= 4,
          "3 个页面中至少 1 页发生重试（尝试次数 > 页数）",
          f"page_attempts={run['page_attempts']}")
    check(len(published_or_none(web, section, date)["rows"]) == 24, "发布 24 行完整数据")


def scenario_concurrency(station: str, web1: str, web2: str, section: str, date: str) -> None:
    print("\n=== 场景 3：两个 Web 进程并发采集同日 + 陈旧修订防护 ===")
    reset_day(station, section, date)
    request("POST", f"{station}/admin/latency", {"delay_ms": 1200})
    try:
        s1, r1 = request("POST", f"{web1}/api/collect", {"section": section, "date": date})
        s2, r2 = request("POST", f"{web2}/api/collect", {"section": section, "date": date})
        statuses = sorted([s1, s2])
        check(statuses == [202, 409],
              "并发同日采集：一个受理、一个被唯一活动任务约束拒绝",
              f"web1={s1} web2={s2} detail={r2.get('detail')}")

        # While the winner runs, no partial day is visible anywhere.
        time.sleep(0.8)
        check(published_or_none(web1, section, date) is None
              and published_or_none(web2, section, date) is None,
              "采集进行中两进程均看不到未完成版本")

        winner_base = web1 if s1 == 202 else web2
        run = wait_run(winner_base, r1["run_id"] if s1 == 202 else r2["run_id"])
        check(run["status"] == "success", "胜出任务完整发布", json.dumps(run))
        check(published_or_none(web1, section, date)["revision"] == 1
              and published_or_none(web2, section, date)["revision"] == 1,
              "两个进程读取同一 SQLite，均看到修订号 1 的完整版本")
    finally:
        request("POST", f"{station}/admin/latency", {"delay_ms": 350})

    # End-to-end stale guard: publish rev 2, then make the station serve rev 1
    # again and confirm the late lower revision cannot overwrite the newer one.
    request("POST", f"{station}/admin/correct",
            {"section": section, "date": date, "time": "10:00", "nh3n": 7.777})
    _, started = request("POST", f"{web1}/api/collect", {"section": section, "date": date})
    run = wait_run(web1, started["run_id"])
    check(run["status"] == "success" and run["revision"] == 2, "采集并发布修订号 2")

    reset_day(station, section, date)  # station goes back to revision 1
    _, started = request("POST", f"{web2}/api/collect", {"section": section, "date": date})
    run = wait_run(web2, started["run_id"])
    check(run["status"] == "failed", "迟到的旧修订采集被拒绝")
    check("refusing to publish revision 1" in run["error"], "拒绝原因是陈旧修订防护", run["error"])
    published = published_or_none(web1, section, date)
    check(published["revision"] == 2, "已发布的较新修订号 2 未被覆盖")

    # Direct DB-layer check (the exact cross-process transaction guard).
    from app import db  # noqa: E402  (path inserted at top of file)

    tmp = ROOT / "data" / f"guard-{os.getpid()}.db"
    tmp.parent.mkdir(exist_ok=True)
    db.init_db(str(tmp))
    rid = db.start_run(str(tmp), section, date, "guard-test")
    db.publish_version(str(tmp), section, date, 2,
                       [{"time": "00:00", "nh3n": 1.0, "source_page": 1}], "guard-test", rid)
    rid2 = db.start_run(str(tmp), section, date, "guard-test")
    blocked = False
    try:
        db.publish_version(str(tmp), section, date, 1,
                           [{"time": "00:00", "nh3n": 0.5, "source_page": 1}],
                           "guard-test", rid2)
    except db.StaleRevisionError:
        blocked = True
    check(blocked, "DB 事务直接验证：旧修订无法覆盖新修订")
    kept = db.get_published(str(tmp), section, date)
    check(kept["revision"] == 2 and kept["rows"][0]["nh3n"] == 1.0,
          "防护触发后原有数据原样保留")
    tmp.unlink(missing_ok=True)
    for suffix in ("-wal", "-shm"):
        Path(str(tmp) + suffix).unlink(missing_ok=True)


def scenario_handover(station: str, web1: str, web2: str, section: str, date: str,
                      hooks: bool, fail_section: str, fail_day: str) -> None:
    print("\n=== 场景 4：复核交接档（不可变 CSV / 幂等 / 并发 / 中断 / 下载校验）===")
    request("POST", f"{station}/admin/latency", {"delay_ms": 350})

    def archive(base: str, s: str = section, d: str = date,
                revision: int | None = None) -> tuple[int, dict]:
        body = {"section": s, "date": d}
        if revision is not None:
            body["revision"] = revision
        return request("POST", f"{base}/api/handovers", body)

    def download(web: str, handover: dict) -> tuple[int, dict[str, str], bytes]:
        return request_raw(f"{web}{handover['download_url']}")

    reset_day(station, section, date)
    run = collect_for(web1, section, date)
    check(run["status"] == "success" and run["revision"] == 1, "采集并发布修订号 1")
    published = published_or_none(web1, section, date)

    # Two processes racing to archive the same *unarchived* revision: exactly
    # one registration wins, both callers receive the same id/hash.
    with ThreadPoolExecutor(max_workers=2) as pool:
        raced = list(pool.map(lambda base: archive(base), [web1, web2]))
    statuses = [r[0] for r in raced]
    ids = {r[1]["id"] for r in raced}
    check(sorted(statuses) == [200, 201] and len(ids) == 1,
          "两实例并发归档同一修订：恰一个新建、均返回同一原档",
          f"status={statuses} ids={ids}")
    check(sum(1 for r in raced if r[1].get("created")) == 1,
          "并发归档只有一个请求报告 created=true")
    h1 = next(r[1] for r in raced if r[1]["created"])

    check(h1["revision"] == 1 and h1["row_count"] == 24 and h1["id"] and h1["sha256"],
          "登记修订号、行数与 SHA-256", json.dumps(h1))
    check(h1["sha256"] == sha256(expected_csv(published)),
          "SHA-256 与已发布整日 CSV 内容一致", h1["sha256"])

    status, headers, data = download(web2, h1)
    check(status == 200 and data == expected_csv(published), "下载内容即冻结的 CSV 字节")
    check(headers.get("x-handover-sha256") == h1["sha256"],
          "下载响应头携带登记的 SHA-256")
    check(any(v.startswith("attachment")
              for k, v in headers.items() if k.lower() == "content-disposition"),
          "下载带有附件文件名")
    parsed = list(csv.DictReader(io.StringIO(data.decode())))
    check(list(parsed[0].keys()) == ["time", "nh3n", "source_page"], "CSV 列为时刻/数值/来源页")
    check(len(parsed) == h1["row_count"] and all(r["source_page"] for r in parsed),
          "CSV 行数等于登记行数且每行带来源页")

    # Idempotent repeat on the other process and with an explicit revision.
    status, h1b = archive(web1)
    check(status == 200 and h1b["created"] is False and h1b["id"] == h1["id"]
          and h1b["sha256"] == h1["sha256"],
          "同修订重复归档返回原档 ID 与 SHA-256")
    status, h1c = archive(web2, revision=1)
    check(status == 200 and h1c["id"] == h1["id"], "显式指定同一修订号亦返回原档")

    # A late correction replaces the local day; the archive must not change.
    status, corr = request("POST", f"{station}/admin/correct",
                            {"section": section, "date": date, "time": "00:00", "nh3n": 8.888})
    check(status == 200 and corr["revision"] == 2, "县站更正，修订号升为 2")
    run2 = collect_for(web2, section, date)
    check(run2["status"] == "success" and run2["revision"] == 2,
          "本地日档被修订号 2 替换", json.dumps(run2))
    published2 = published_or_none(web1, section, date)
    check(published2["revision"] == 2 and published2["rows"][0]["nh3n"] == 8.888,
          "当前日档已是修订号 2 的新数值")

    status, headers2, data2 = download(web2, h1)
    check(status == 200 and data2 == data
          and headers2.get("x-handover-sha256") == h1["sha256"],
          "旧交接档不随日档更新而改变（字节与 SHA-256 均不变）")

    status, h2 = archive(web2)
    check(status == 201 and h2["revision"] == 2 and h2["sha256"] != h1["sha256"],
          "新修订另行生成一份交接档")
    check(download(web1, h2)[2] == expected_csv(published2), "新档内容对应修订号 2")
    status, body = archive(web1, revision=3)
    check(status == 404 and "not 3" in body.get("detail", ""),
          "请求未提交的修订号拒绝从当前日档重建", body.get("detail", ""))

    if not hooks:
        print("[SKIP] Web 未启用 HANDOVER_TEST_HOOKS=1，跳过中断/损坏注入检查")
        return

    # Interrupted write: file written but registration never commits -> no
    # downloadable partial archive; a retry afterwards completes normally.
    request("POST", f"{station}/admin/reset", {"section": fail_section, "date": fail_day})
    runf = collect_for(web1, fail_section, fail_day)
    check(runf["status"] == "success", "中断测试日完整发布")
    check(request("GET", f"{web1}/api/handovers?section={fail_section}&date={fail_day}")[1] == [],
          "中断前无档案登记")

    status, _ = request("POST", f"{web1}/api/_test/failpoint", {"stage": "after_file_write"})
    check(status == 200, "挂载一次性中断注入")
    status, fh = archive(web1, s=fail_section, d=fail_day)
    check(status == 500 and "injected" in fh.get("detail", ""),
          "写完文件后中断返回错误", json.dumps(fh))
    check(request("GET", f"{web1}/api/handovers?section={fail_section}&date={fail_day}")[1] == [],
          "中断后无档案登记")
    files = request("GET", f"{web1}/api/_test/handover-files")[1]["files"]
    check(not any(fail_section in n and fail_day in n for n in files),
          "中断后临时文件与最终文件均被清理", json.dumps(files))

    status, retried = archive(web2, s=fail_section, d=fail_day)
    check(status == 201 and retried["row_count"] == 24, "恢复后重试完整归档", json.dumps(retried))
    status, headers3, _ = download(web1, retried)
    check(status == 200 and headers3.get("x-handover-sha256") == retried["sha256"],
          "重试后的档案可下载且摘要一致")

    # Download-time integrity failures must error rather than rebuild.
    status, _ = request("POST", f"{web2}/api/_test/corrupt-handover/{retried['id']}")
    torn = request_raw(f"{web1}/api/handovers/{retried['id']}/download")
    check(torn[0] == 500 and b"mismatch" in torn[2],
          "摘要不符时下载报错", torn[2][:220].decode(errors="replace"))
    status, _ = request("DELETE", f"{web1}/api/_test/handover-file/{retried['id']}")
    torn = request_raw(f"{web2}/api/handovers/{retried['id']}/download")
    check(torn[0] == 500 and b"missing" in torn[2],
          "文件缺失时下载报错而非重建", torn[2][:220].decode(errors="replace"))
    listing = request("GET", f"{web1}/api/handovers?section={fail_section}&date={fail_day}")
    check(listing[1][0]["sha256"] == retried["sha256"], "报错后登记元数据保持不变")


def scenario_handover_vs_publish(station: str, web1: str, web2: str,
                                 section: str, date: str) -> None:
    """Archive while the other process publishes new revisions.

    Every archive must contain the complete observation set of exactly one
    committed revision -- never a mixture of rows from two revisions.
    """
    print("\n=== 场景 5：归档与整日发布并发（快照只含同一已提交修订）===")
    reset_day(station, section, date)
    run = collect_for(web1, section, date)
    check(run["status"] == "success" and run["revision"] == 1, "发布修订号 1")
    expected = {1: expected_csv(published_or_none(web1, section, date))}

    stop = False
    archive_errors: list[str] = []

    def archiver(tag: str, base: str) -> None:
        while not stop:
            status, body = request("POST", f"{base}/api/handovers",
                                   {"section": section, "date": date})
            # 201/200 are fine; 404 only if the day disappeared (it never does).
            if status not in (200, 201):
                archive_errors.append(f"{tag}: {status} {body}")
            time.sleep(0.01)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(archiver, "web1", web1), pool.submit(archiver, "web2", web2)]
        for revision, corrected_time, value in ((2, "00:00", 3.333), (3, "01:00", 4.444)):
            request("POST", f"{station}/admin/correct",
                    {"section": section, "date": date,
                     "time": corrected_time, "nh3n": value})
            run = collect_for(web2 if revision == 2 else web1, section, date)
            check(run["status"] == "success" and run["revision"] == revision,
                  f"并发归档期间发布修订号 {revision}")
            expected[revision] = expected_csv(published_or_none(web1, section, date))
        stop = True
        for fut in futures:
            fut.result(timeout=10)

    check(not archive_errors, "并发归档请求均成功", "; ".join(archive_errors[:3]))
    archives = request("GET", f"{web1}/api/handovers?section={section}&date={date}")[1]
    check({a["revision"] for a in archives} >= {1, 2, 3},
          "并发期间至少归档了三个修订版本", str(sorted(a["revision"] for a in archives)))
    mismatched = []
    for a in archives:
        status, headers, data = request_raw(f"{web2}{a['download_url']}")
        if status != 200 or headers.get("x-handover-sha256") != a["sha256"]:
            mismatched.append((a["revision"], "download/hash", status))
            continue
        if data != expected[a["revision"]]:
            mismatched.append((a["revision"], "mixed or wrong snapshot", sha256(data)))
        rows = list(csv.DictReader(io.StringIO(data.decode())))
        if len(rows) != 24:
            mismatched.append((a["revision"], f"row count {len(rows)}", ""))
    check(not mismatched,
          "每个档案都是某一已提交修订的完整 24 行观测，无跨修订混合",
          json.dumps(mismatched[:5]))
    check(all(len(a["sha256"]) == 64 and a["row_count"] == 24 for a in archives),
          "全部档案登记完整摘要与行数")


def collect_for(base: str, section: str, date: str) -> dict:
    _, started = request("POST", f"{base}/api/collect", {"section": section, "date": date})
    return wait_run(base, started["run_id"])


def hooks_enabled(web: str) -> bool:
    status, _ = request("GET", f"{web}/api/_test/handover-files")
    return status == 200


def wait_url(url: str, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2):
                return
        except Exception:
            time.sleep(0.4)
    raise TimeoutError(f"{url} did not become healthy")


def start_local_stack(workdir: Path) -> tuple[subprocess.Popen, list[subprocess.Popen]]:
    """Boot station + two web workers on this host for a dependency-free demo."""
    data = workdir / "data"
    data.mkdir(parents=True, exist_ok=True)
    env_common = dict(os.environ)
    env_common["PYTHONPATH"] = os.pathsep.join(
        [str(ROOT)] + ([p for p in [os.environ.get("PYTHONPATH")] if p])
    )
    station_env = dict(env_common, PAGE_SIZE="8", PAGE_DELAY_MS="350")
    station = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "station.app.main:app", "--port", "8001"],
        cwd=ROOT, env=station_env,
    )
    wait_url("http://127.0.0.1:8001/health")
    webs = []
    for name, port in (("web1", 8011), ("web2", 8012)):
        env = dict(
            env_common,
            DB_PATH=str(data / "quality.db"),
            STATION_URL="http://127.0.0.1:8001",
            WORKER_NAME=name,
            HANDOVER_TEST_HOOKS="1",
        )
        webs.append(subprocess.Popen(
            [sys.executable, "-m", "uvicorn", "web.app.main:app", "--port", str(port)],
            cwd=ROOT, env=env,
        ))
        wait_url(f"http://127.0.0.1:{port}/health")
    return station, webs


def main() -> int:
    parser = argparse.ArgumentParser(description="Water-quality platform demo")
    parser.add_argument("--station", default="http://localhost:8001")
    parser.add_argument("--web1", default="http://localhost:8011")
    parser.add_argument("--web2", default="http://localhost:8012")
    parser.add_argument("--section", default="SECTION-A")
    parser.add_argument("--date1", default="2026-11-13")
    parser.add_argument("--date2", default="2026-11-12")
    parser.add_argument("--date3", default="2026-11-11")
    parser.add_argument("--date4", default="2026-11-10")
    parser.add_argument("--date5", default="2026-11-09")
    parser.add_argument("--date6", default="2026-11-08")
    parser.add_argument("--start", action="store_true",
                        help="boot station + two web workers locally first")
    args = parser.parse_args()

    print(f"station={args.station} web1={args.web1} web2={args.web2}")
    procs: list[subprocess.Popen] = []
    workdir = ROOT / "data" / f"demo-{os.getpid()}"
    try:
        if args.start:
            station_p, web_procs = start_local_stack(workdir)
            procs = [station_p, *web_procs]

        scenario_mid_correction(args.station, args.web1, args.section, args.date1)
        scenario_read_failure(args.station, args.web1, args.section, args.date2)
        scenario_concurrency(args.station, args.web1, args.web2, args.section, args.date3)
        scenario_handover(
            args.station, args.web1, args.web2, args.section,
            args.date4, hooks_enabled(args.web1), "SECTION-B", args.date5,
        )
        scenario_handover_vs_publish(
            args.station, args.web1, args.web2, args.section, args.date6,
        )
    except Exception as exc:
        print(f"\n演示中断: {exc!r}")
        return 1
    finally:
        for proc in procs:
            proc.terminate()
        for proc in procs:
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        if workdir.exists():
            shutil.rmtree(workdir, ignore_errors=True)

    passed = sum(1 for ok, _ in RESULTS if ok)
    print(f"\n==== 结果: {passed}/{len(RESULTS)} 项检查通过 ====")
    return 0 if passed == len(RESULTS) else 1


if __name__ == "__main__":
    raise SystemExit(main())
