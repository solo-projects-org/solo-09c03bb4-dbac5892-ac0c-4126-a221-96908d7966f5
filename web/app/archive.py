"""Immutable CSV handover archives for published days.

A handover archive is a frozen copy of *one committed revision* of a published
section/day, handed to the external pollution-review lab.  Once created it must
never be rebuilt from the live ``published`` table: the county station may
correct a day and the collector replaces the local rows, but the archive keeps
the exact bytes (and SHA-256) that were registered.

Guarantees:

* **Snapshot consistency** -- rows are read inside a single SQLite read
  transaction, so two web processes archiving while a new revision is being
  published can only observe one committed revision, never a mixture.  Rows
  whose revision differs from the requested one abort the archive.
* **One archive per (section, date, revision)** -- a ``UNIQUE`` index means a
  repeated request for the same version returns the original archive, even
  when the request lands on the other web process.
* **No half-written artifacts** -- the CSV is written to a temporary file,
  fsynced and atomically renamed, then registered in the same database.  A
  failure before registration removes the file, so a crash or error can never
  leave a downloadable partial archive.  Retrying completes the operation.
* **Download is verified** -- a missing file or a size/SHA-256 mismatch raises
  ``HandoverCorrupt`` instead of silently rebuilding from current data.

Only the standard library is used, mirroring ``db.py``, so the demo harness can
import this module under ``python -I``.
"""
from __future__ import annotations

import csv
import hashlib
import io
import os
import re
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from typing import Callable, Optional

from . import db

CSV_HEADER = ("time", "nh3n", "source_page")
_UNSAFE = re.compile(r"[^A-Za-z0-9._-]+")
_TMP_PREFIX = "."


class HandoverError(Exception):
    """Base class for handover archive failures."""


class NoPublishedSnapshot(HandoverError):
    """No committed day at the requested revision exists locally."""


class HandoverNotFound(HandoverError):
    """No archive is registered under this id/version."""


class HandoverCorrupt(HandoverError):
    """The registered archive's file is missing or does not match its hash."""


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def handover_dir(db_path: str) -> str:
    """Directory for archive files; lives on the same shared volume as the DB."""
    return os.path.join(os.path.dirname(os.path.abspath(db_path)), "handovers")


def _safe_component(value: str) -> str:
    cleaned = _UNSAFE.sub("_", value).strip("._-")
    return cleaned or "x"


def _row_to_meta(row) -> dict:
    return {
        "id": row["id"],
        "section": row["section"],
        "date": row["date"],
        "revision": int(row["revision"]),
        "row_count": int(row["row_count"]),
        "sha256": row["sha256"],
        "size_bytes": int(row["size_bytes"]),
        "filename": row["filename"],
        "created_at": row["created_at"],
        "created_by": row["created_by"],
        "published_at": row["published_at"],
        "download_url": f"/api/handovers/{row['id']}/download",
    }


def _find(db_path: str, section: str, date: str, revision: int) -> Optional[dict]:
    conn = db.get_conn(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM handovers WHERE section=? AND date=? AND revision=?",
            (section, date, revision),
        ).fetchone()
        return _row_to_meta(row) if row else None
    finally:
        conn.close()


def find_handover(db_path: str, section: str, date: str, revision: int) -> Optional[dict]:
    """Return the registered archive metadata for an exact version, if any."""
    return _find(db_path, section, date, revision)


def get_handover(db_path: str, handover_id: str) -> dict:
    conn = db.get_conn(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM handovers WHERE id=?", (handover_id,)
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        raise HandoverNotFound(f"handover {handover_id} not found")
    return _row_to_meta(row)


def list_handovers(db_path: str, section: str, date: str) -> list[dict]:
    conn = db.get_conn(db_path)
    try:
        rows = conn.execute(
            "SELECT * FROM handovers WHERE section=? AND date=? ORDER BY revision DESC, id",
            (section, date),
        ).fetchall()
        return [_row_to_meta(r) for r in rows]
    finally:
        conn.close()


def _read_snapshot(
    db_path: str, section: str, date: str, revision: Optional[int]
) -> dict:
    """Read one committed revision in a single read transaction.

    SQLite's deferred ``BEGIN`` pins the snapshot at the first read, so the
    header and the rows always come from the same committed revision even if a
    publish commits in between (WAL mode, two web processes).
    """
    conn = db.get_conn(db_path)
    try:
        conn.execute("BEGIN")
        try:
            if revision is None:
                head = conn.execute(
                    "SELECT revision FROM published WHERE section=? AND date=? LIMIT 1",
                    (section, date),
                ).fetchone()
                if head is None:
                    raise NoPublishedSnapshot(
                        f"no complete published version for {section} {date}"
                    )
                revision = int(head["revision"])
            rows = conn.execute(
                "SELECT time, nh3n, source_page, revision, published_at, published_by "
                "FROM published WHERE section=? AND date=? ORDER BY time",
                (section, date),
            ).fetchall()
        finally:
            conn.execute("COMMIT")
    finally:
        conn.close()

    if not rows:
        raise NoPublishedSnapshot(
            f"no complete published version for {section} {date}"
        )
    revisions = {int(r["revision"]) for r in rows}
    if revisions != {revision}:
        current = max(revisions)
        raise NoPublishedSnapshot(
            f"{section} {date} is published at revision {current}, not {revision}; "
            "refusing to rebuild a mismatched handover from current data"
        )
    published_at = rows[0]["published_at"]
    published_by = rows[0]["published_by"]
    if any(r["published_at"] != published_at or r["published_by"] != published_by
           for r in rows):
        raise NoPublishedSnapshot(
            f"{section} {date} snapshot mixes publication metadata; retry"
        )
    return {
        "section": section,
        "date": date,
        "revision": revision,
        "published_at": published_at,
        "published_by": published_by,
        "rows": [
            {
                "time": r["time"],
                "nh3n": float(r["nh3n"]),
                "source_page": int(r["source_page"]),
            }
            for r in rows
        ],
    }


def render_csv(snapshot: dict) -> bytes:
    """Serialise the snapshot deterministically (header + one row per hour)."""
    buf = io.StringIO(newline="")
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(CSV_HEADER)
    for r in snapshot["rows"]:
        writer.writerow([r["time"], repr(float(r["nh3n"])), r["source_page"]])
    return buf.getvalue().encode("utf-8")


def _write_artifact(directory: str, filename: str, payload: bytes) -> str:
    """Write payload to a temp file, fsync, atomically rename to ``filename``."""
    os.makedirs(directory, exist_ok=True)
    final_path = os.path.join(directory, filename)
    tmp_path = os.path.join(
        directory,
        f"{_TMP_PREFIX}{filename}.{os.getpid()}.{threading.get_ident()}.tmp",
    )
    fd = os.open(tmp_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    try:
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(payload)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, final_path)
        except BaseException:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
    except OSError as exc:
        raise HandoverError(f"cannot write handover file: {exc}") from exc
    try:  # make the rename durable across crashes
        dir_fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except OSError:
        pass
    return final_path


def _register(
    db_path: str,
    handover_id: str,
    section: str,
    date: str,
    revision: int,
    run_id: Optional[int],
    row_count: int,
    sha256: str,
    size_bytes: int,
    filename: str,
    created_by: str,
    published_at: str,
) -> tuple[dict, bool]:
    """Insert the archive row; return (metadata, created).

    If a concurrent request registered the same version first, the unique
    index wins and the existing row is returned with ``created=False``.
    """
    conn = db.get_conn(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute(
                "INSERT INTO handovers (id, section, date, revision, run_id, "
                "row_count, sha256, size_bytes, filename, created_at, "
                "created_by, published_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    handover_id, section, date, revision, run_id, row_count,
                    sha256, size_bytes, filename, utc_now(), created_by,
                    published_at,
                ),
            )
            conn.execute("COMMIT")
            created = True
        except sqlite3.IntegrityError:
            conn.execute("ROLLBACK")
            created = False
    finally:
        conn.close()

    if created:
        return get_handover(db_path, handover_id), True
    existing = _find(db_path, section, date, revision)
    if existing is None:
        raise HandoverError(
            "handover registration conflicted but no archive was found; retry"
        )
    return existing, False


def create_handover(
    db_path: str,
    section: str,
    date: str,
    revision: Optional[int] = None,
    run_id: Optional[int] = None,
    worker: str = "unknown",
    failpoint: Optional[Callable[[str], None]] = None,
) -> tuple[dict, bool]:
    """Create (or return the existing) archive for one committed revision.

    Returns ``(metadata, created)``.  ``created`` is False when a previously
    registered archive for the same section/day/revision is returned, so a
    repeated operation on the same version always yields the original file and
    hash.  ``failpoint`` is a test hook invoked with ``"after_file_write"``
    after the artifact is written but before it is registered.
    """
    if revision is not None:
        existing = _find(db_path, section, date, int(revision))
        if existing is not None:
            return existing, False
        revision = int(revision)

    snapshot = _read_snapshot(db_path, section, date, revision)
    # A concurrent (or repeated) request may already have frozen this exact
    # revision; return the original registration without writing a second file.
    existing = _find(db_path, section, date, snapshot["revision"])
    if existing is not None:
        return existing, False

    payload = render_csv(snapshot)
    sha256 = hashlib.sha256(payload).hexdigest()
    handover_id = uuid.uuid4().hex
    filename = (
        f"{_safe_component(section)}_{_safe_component(date)}"
        f"_r{snapshot['revision']}_{handover_id[:12]}.csv"
    )

    directory = handover_dir(db_path)
    final_path = _write_artifact(directory, filename, payload)
    try:
        if failpoint is not None:
            failpoint("after_file_write")
        meta, created = _register(
            db_path=db_path,
            handover_id=handover_id,
            section=section,
            date=date,
            revision=snapshot["revision"],
            run_id=run_id,
            row_count=len(snapshot["rows"]),
            sha256=sha256,
            size_bytes=len(payload),
            filename=filename,
            created_by=worker,
            published_at=snapshot["published_at"],
        )
        if created:
            return meta, True
        # Another process registered this exact version first; our artifact is
        # unreferenced, remove it so only the winner's file can be downloaded.
        _safe_unlink(final_path)
        return meta, False
    except BaseException:
        # Never leave an unregistered artifact that could be mistaken for a
        # handover; retrying the operation starts from a clean state.
        _safe_unlink(final_path)
        raise


def _safe_unlink(path: str) -> None:
    try:
        os.unlink(path)
    except OSError:
        pass


def read_handover(db_path: str, handover_id: str) -> tuple[dict, bytes]:
    """Return (metadata, exact bytes) after verifying size and SHA-256.

    Never rebuilds content from the current ``published`` rows.
    """
    meta = get_handover(db_path, handover_id)
    filename = meta["filename"]
    if os.path.basename(filename) != filename:
        raise HandoverCorrupt(f"unsafe archive filename for handover {handover_id}")
    path = os.path.join(handover_dir(db_path), filename)
    try:
        with open(path, "rb") as fh:
            data = fh.read()
    except FileNotFoundError as exc:
        raise HandoverCorrupt(
            f"handover {handover_id} file is missing: {filename}"
        ) from exc
    except OSError as exc:
        raise HandoverCorrupt(
            f"handover {handover_id} file cannot be read: {exc}"
        ) from exc

    actual = hashlib.sha256(data).hexdigest()
    if len(data) != int(meta["size_bytes"]) or actual != meta["sha256"]:
        raise HandoverCorrupt(
            f"handover {handover_id} checksum mismatch: registered "
            f"{meta['sha256']} ({meta['size_bytes']} bytes), found {actual} "
            f"({len(data)} bytes)"
        )
    return meta, data


def cleanup_orphans(db_path: str, older_than_seconds: float = 3600) -> int:
    """Remove unreferenced archive/temp files left by crashed operations.

    Only files older than ``older_than_seconds`` are touched so an in-flight
    archive on the other web process (file written, registration pending) is
    never deleted.
    """
    directory = handover_dir(db_path)
    if not os.path.isdir(directory):
        return 0
    conn = db.get_conn(db_path)
    try:
        known = {r["filename"] for r in conn.execute("SELECT filename FROM handovers")}
    finally:
        conn.close()

    removed = 0
    now = datetime.now(timezone.utc).timestamp()
    for name in os.listdir(directory):
        if name in known:
            continue
        if not (name.endswith(".csv") or name.endswith(".tmp")):
            continue
        path = os.path.join(directory, name)
        try:
            if now - os.path.getmtime(path) < older_than_seconds:
                continue
            os.unlink(path)
            removed += 1
        except OSError:
            continue
    return removed
