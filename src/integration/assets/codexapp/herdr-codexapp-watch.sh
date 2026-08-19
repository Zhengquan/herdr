#!/bin/sh
# installed by herdr
# managed by herdr; reinstalling or updating the integration overwrites this file.
# add customizations beside this file instead of editing it.
# HERDR_INTEGRATION_ID=codexapp
# HERDR_INTEGRATION_VERSION=2
#
# The Codex desktop app never runs inside a Herdr pane, so process detection
# and screen manifests cannot observe it. This watcher bridges that gap the
# same way the WorkBuddy bridge does: it runs inside a Herdr pane, polls the
# Codex app's local state read-only, renders a live task dashboard into the
# pane, and reports the aggregated state over the Herdr socket API.
#
# Signal model (validated against live Codex app data):
#   * ~/.codex/sqlite/codex-dev.db -> local_thread_catalog: the thread
#     directory (id, title, source_kind). source_updated_at is only a
#     coarse-grained recency hint — it does NOT track execution.
#   * ~/.codex/thread-writer-locks/<thread_id>.lock: per-thread lock file,
#     but its mtime is pinned to rollout creation and does NOT refresh during
#     execution. Not used for state detection.
#   * ~/.codex/sessions/YYYY/MM/DD/rollout-<ts>-<thread_id>.jsonl: the full
#     event stream. The rollout file is appended to every few seconds while a
#     turn runs (measured 0-4s cadence), so its mtime IS the execution
#     signal. payload.type task_started / task_complete / turn_aborted give
#     an explicit turn lifecycle. The LAST lifecycle event wins: a
#     task_started after a task_complete cancels the earlier completion (the
#     user started a new turn in the same thread).
#
# Sound policy: herdr plays a Done sound on any working->idle transition, so
# this watcher only produces that transition when it can confirm the tracked
# turn actually ended (task_complete or turn_aborted as the last lifecycle
# event in the rollout tail, or the grace window expiring). While waiting for
# confirmation it holds the reported state at working (no sound).

set -u

[ "${HERDR_ENV:-}" = "1" ] || exit 0
[ -n "${HERDR_PANE_ID:-}" ] || exit 0
[ -n "${HERDR_SOCKET_PATH:-}" ] || exit 0
command -v python3 >/dev/null 2>&1 || exit 0

CODEXAPP_HOME_DIR="${CODEX_HOME:-$HOME/.codex}"
POLL_INTERVAL="${HERDR_CODEXAPP_POLL_INTERVAL:-3}"
STATE_FILE="${TMPDIR:-/tmp}/herdr-codexapp-watch.${HERDR_PANE_ID}.state"

while true; do
  # Uninstall removes this script; stop the bridge when that happens.
  [ -f "$0" ] || exit 0

  HERDR_CODEXAPP_HOME="$CODEXAPP_HOME_DIR" HERDR_CODEXAPP_STATE_FILE="$STATE_FILE" python3 - <<'PY'
import glob
import json
import os
import random
import socket
import sqlite3
import sys
import time
import unicodedata

source = "herdr:codexapp"
agent = "codexapp"
pane_id = os.environ["HERDR_PANE_ID"]
socket_path = os.environ["HERDR_SOCKET_PATH"]
codex_home = os.environ["HERDR_CODEXAPP_HOME"]
state_file = os.environ["HERDR_CODEXAPP_STATE_FILE"]

NOW = time.time()

# ---------------------------------------------------------------------------
# Tunables. The active window matches the WorkBuddy bridge's window (45s) —
# generous against the observed multi-second write cadence, tight enough
# that a finished task goes idle promptly.
# ---------------------------------------------------------------------------
ROLLOUT_FRESH_S = 45      # a rollout mtime this fresh = the turn is executing
HOLD_WORKING_S = 20       # hold working this long waiting for completion confirmation
ENTER_WORKING_POLLS = 1
LEAVE_WORKING_POLLS = 3
GENERIC_POLLS = 2
TERMINAL_EVENTS = {"task_complete", "turn_aborted"}
LIFECYCLE_EVENTS = {"task_started", "task_complete", "turn_aborted"}
# How many trailing bytes of a rollout to scan for the last lifecycle event.
ROLLOUT_TAIL_BYTES = 64 * 1024


def send(method, params):
    request_id = f"{source}:{int(time.time() * 1000)}:{random.randrange(1_000_000):06d}"
    request = {"id": request_id, "method": method, "params": params}
    try:
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.settimeout(0.5)
        client.connect(socket_path)
        client.sendall((json.dumps(request) + "\n").encode())
        try:
            client.recv(4096)
        except Exception:
            pass
        client.close()
    except Exception:
        pass


def find_db():
    """Return the path to the codex thread catalog DB, or None."""
    sqlite_dir = os.path.join(codex_home, "sqlite")
    try:
        names = os.listdir(sqlite_dir)
    except Exception:
        return None
    # The DB is named codex-dev.db today; glob to be robust to suffix changes.
    candidates = [n for n in names if n.startswith("codex") and n.endswith(".db")]
    if not candidates:
        return None
    # Prefer the non-snapshot DB (codex-dev.db) over history/summaries.
    preferred = [n for n in candidates if "history" not in n and "summaries" not in n]
    chosen = preferred[0] if preferred else candidates[0]
    return os.path.join(sqlite_dir, chosen)


def load_threads():
    """Return (error_or_None, rows) where each row is
    (thread_id, title, source_kind, updated_at_seconds)."""
    db_path = find_db()
    if not db_path or not os.path.isfile(db_path):
        return ("Codex database not found", [])
    try:
        db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        rows = db.execute(
            "SELECT thread_id, display_title, source_kind, source_updated_at "
            "FROM local_thread_catalog "
            "WHERE source_updated_at IS NOT NULL "
            "ORDER BY source_updated_at DESC LIMIT 20"
        ).fetchall()
        db.close()
        return (None, rows)
    except Exception:
        return ("Codex database unreadable", [])


def rollout_path_for(thread_id):
    """Find the most recent rollout JSONL for a thread id."""
    pattern = os.path.join(codex_home, "sessions", "*", "*", "*", f"*-{thread_id}.jsonl")
    matches = glob.glob(pattern)
    if not matches:
        return None
    return max(matches, key=os.path.getmtime)


def rollout_age(thread_id):
    """Return the age in seconds of the thread's most recent rollout write.

    This is the RELIABLE execution signal: the rollout file is appended to
    every few seconds while a turn runs (measured 0-4s cadence) and stops
    when the turn ends. Thread-writer locks do NOT track execution — their
    mtime is pinned to rollout creation — so they are not used.
    """
    path = rollout_path_for(thread_id)
    if not path:
        return None
    try:
        return NOW - os.path.getmtime(path)
    except OSError:
        return None


def rollout_last_lifecycle(thread_id):
    """Return the LAST lifecycle event in the rollout tail, or None.

    Scans backwards through the tail for task_started / task_complete /
    turn_aborted and returns the most recent one. Crucially this is not just
    "the last terminal event": a thread can complete one turn and start
    another in the same rollout, so a task_started after a task_complete must
    cancel the completion — the last lifecycle event wins.
    """
    path = rollout_path_for(thread_id)
    if not path:
        return None
    # Read only the tail bytes; a busy rollout can be megabytes.
    try:
        with open(path, "rb") as handle:
            handle.seek(0, os.SEEK_END)
            size = handle.tell()
            handle.seek(max(0, size - ROLLOUT_TAIL_BYTES))
            tail = handle.read().decode("utf-8", errors="replace")
    except Exception:
        return None
    latest = None
    for line in tail.splitlines():
        try:
            event = json.loads(line)
        except Exception:
            continue
        payload = event.get("payload") or {}
        ptype = payload.get("type")
        if ptype in LIFECYCLE_EVENTS:
            latest = ptype
    return latest


def is_active(row):
    """A thread is executing when its rollout was written recently AND its
    last lifecycle event is not a terminal one."""
    thread_id = row[0]
    age = rollout_age(thread_id)
    if age is None or age >= ROLLOUT_FRESH_S:
        return False
    return rollout_last_lifecycle(thread_id) not in TERMINAL_EVENTS


def short_title(title):
    return str(title or "task").replace("\n", " ")[:120]


# ===========================================================================
# Dashboard rendering
# ===========================================================================
RESET = "\x1b[0m"
BOLD = "\x1b[1m"


def fg(code):
    return f"\x1b[38;5;{code}m"


def bg(code):
    return f"\x1b[48;5;{code}m"


C_ACCENT = 39
C_ACCENT2 = 45
C_GREEN = 42
C_BLUE = 75
C_RED = 203
C_AMBER = 214
C_MUTE = 244
C_FAINT = 240
C_TEXT = 252

STATE_THEME = {
    "working": (C_GREEN, "RUNNING", "▶"),
    "blocked": (C_RED, "BLOCKED", "■"),
    "idle": (C_MUTE, "IDLE", "◇"),
}

ROW_ICONS = {
    "active": ("▶", C_GREEN, "running"),
    "recent": ("✓", C_GREEN, "done"),
    "aborted": ("⊘", C_AMBER, "aborted"),
    "stale": ("◌", C_FAINT, "idle"),
    "old": ("◇", C_MUTE, "old"),
}


def fmt_age(seconds):
    try:
        secs = max(0, int(NOW - float(seconds)))
    except Exception:
        return ""
    return fmt_age_from_age(secs)


def fmt_age_from_age(secs):
    """Format an age in seconds as a compact relative string."""
    secs = max(0, int(secs))
    if secs < 60:
        return f"{secs}s"
    mins = secs // 60
    if mins < 60:
        return f"{mins}m"
    hours = mins // 60
    if hours < 24:
        return f"{hours}h"
    return f"{hours // 24}d"


def dwidth(text):
    total = 0
    for ch in text:
        if unicodedata.combining(ch):
            continue
        total += 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1
    return total


def dtrunc(text, limit):
    if dwidth(text) <= limit:
        return text
    out = []
    used = 0
    budget = max(0, limit - 1)
    for ch in text:
        w = 0 if unicodedata.combining(ch) else (
            2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1
        )
        if used + w > budget:
            break
        out.append(ch)
        used += w
    return "".join(out) + "…"


def dpad(text, width):
    pad = width - dwidth(text)
    return text + (" " * pad if pad > 0 else "")


WIDTH = 94
COL_STATUS = 11
COL_WHEN = 7


def render_dashboard(state, rows, error, confirming):
    color, label, glyph = STATE_THEME.get(state, STATE_THEME["idle"])
    accent = fg(C_ACCENT)
    a2 = fg(C_ACCENT2)
    text = fg(C_TEXT)
    mute = fg(C_MUTE)
    faint = fg(C_FAINT)
    inner = WIDTH - 2

    top = f"{accent}╭{'─' * inner}╮{RESET}"
    bot = f"{accent}╰{'─' * inner}╯{RESET}"
    sep = f"{accent}├{'─' * inner}┤{RESET}"

    def row(content_visible, content_ansi):
        pad = inner - content_visible
        if pad < 0:
            pad = 0
        return f"{accent}│{RESET}{content_ansi}{' ' * pad}{accent}│{RESET}"

    dot = f"{fg(C_GREEN)}●{RESET}" if state == "working" else f"{faint}○{RESET}"
    host_txt = "executing" if state == "working" else "idle"
    clock = time.strftime("%H:%M:%S")
    left_v = "  Codex  bridge"
    left_a = f"  {BOLD}{a2}Codex{RESET}{mute}  bridge{RESET}"
    hold_hint = "  confirming" if confirming else ""
    right_v = f"{host_txt}  {clock}  "
    right_a = f"{dot} {mute}{host_txt}{RESET}{faint}{hold_hint}{RESET}  {faint}{clock}{RESET}  "
    mid_pad = inner - dwidth(left_v) - dwidth(right_v)
    if mid_pad < 1:
        mid_pad = 1
    header_a = left_a + (" " * mid_pad) + right_a
    header_v = left_v + (" " * mid_pad) + right_v

    badge_v = f"  {glyph} {label} "
    badge_a = f"  {bg(color)}{fg(16)}{BOLD} {glyph} {label} {RESET}"
    lines = [
        top,
        row(dwidth(header_v), header_a),
        sep,
        row(dwidth(badge_v), badge_a),
        row(0, ""),
    ]

    if error:
        emsg = dtrunc(str(error), inner - 4)
        lines.append(row(dwidth(f"  {emsg}"), f"  {fg(C_AMBER)}{emsg}{RESET}"))
    elif not rows:
        m = "  no threads yet"
        lines.append(row(dwidth(m), f"  {faint}no threads yet{RESET}"))
    else:
        h_status = dpad("STATUS", COL_STATUS)
        h_when = dpad("WHEN", COL_WHEN)
        head_v = f"  {h_status}  {h_when}  TASK"
        head_a = f"  {faint}{h_status}  {h_when}  TASK{RESET}"
        lines.append(row(dwidth(head_v), head_a))

        prefix_cols = 2 + COL_STATUS + 2 + COL_WHEN + 2
        task_budget = inner - prefix_cols
        for row_data in rows[:12]:
            thread_id, title, source_kind, updated_at = row_data
            active = is_active(row_data)
            if active:
                icon, icolor, badge = ROW_ICONS["active"]
            else:
                last_event = rollout_last_lifecycle(thread_id)
                r_age = rollout_age(thread_id)
                if last_event == "task_complete" and r_age is not None and r_age < 300:
                    icon, icolor, badge = ROW_ICONS["recent"]
                elif last_event == "turn_aborted" and r_age is not None and r_age < 300:
                    icon, icolor, badge = ROW_ICONS["aborted"]
                elif r_age is not None and r_age < ROLLOUT_FRESH_S:
                    icon, icolor, badge = ROW_ICONS["stale"]
                else:
                    icon, icolor, badge = ROW_ICONS["old"]
            ic = fg(icolor)

            status_field = f"{icon} {badge}"
            status_v = dpad(status_field, COL_STATUS)
            status_pad = COL_STATUS - dwidth(status_field)
            status_a = f"{ic}{icon}{RESET} {ic}{badge}{RESET}" + (
                " " * (status_pad if status_pad > 0 else 0)
            )

            # Prefer the rollout age (accurate to seconds) over the catalog
            # timestamp (coarse) for the WHEN column.
            r_age_display = rollout_age(thread_id)
            when = fmt_age_from_age(r_age_display) if r_age_display is not None else fmt_age(updated_at)
            when_v = dpad(when, COL_WHEN)

            kind_tag = f" [{source_kind}]" if source_kind else ""
            task = dtrunc(str(title or "(untitled)").replace("\n", " ") + kind_tag, task_budget)
            line_v = f"  {status_v}  {when_v}  {task}"
            line_a = f"  {status_a}  {faint}{when_v}{RESET}  {text}{task}{RESET}"
            lines.append(row(dwidth(line_v), line_a))

    lines.append(row(0, ""))
    total = len(rows)
    running = sum(1 for r in rows if is_active(r)) if rows else 0
    foot_v = f"  {total} threads · {running} active · poll {POLL_HINT}s"
    foot_a = f"  {faint}{total} threads · {running} active · poll {POLL_HINT}s{RESET}"
    lines.append(sep)
    lines.append(row(dwidth(foot_v), foot_a))
    lines.append(bot)

    sys.stdout.write("\x1b[?25l\x1b[2J\x1b[H" + "\n".join(lines) + "\n")
    sys.stdout.flush()


POLL_HINT = os.environ.get("HERDR_CODEXAPP_POLL_INTERVAL", "3")


# ===========================================================================
# Aggregate with completion confirmation
# ===========================================================================
error, rows = load_threads()

try:
    with open(state_file, encoding="utf-8") as handle:
        persisted = json.load(handle)
except Exception:
    persisted = {}

tracked_id = persisted.get("tracked_id")
last_active_seen_ms = persisted.get("last_active_seen_ms")
confirming = False
confirmed_completion = False

active_row = next((r for r in rows if is_active(r)), None)
tracked_row = next((r for r in rows if r[0] == tracked_id), None) if tracked_id else None

if error:
    state, message = ("idle", error)
elif active_row is not None:
    # A thread is genuinely executing: track it and report working.
    tracked_id = active_row[0]
    last_active_seen_ms = int(NOW * 1000)
    state, message = ("working", short_title(active_row[1]))
elif tracked_row is not None:
    # The tracked thread's rollout is no longer fresh; check whether its
    # last lifecycle event is terminal. A task_started after an earlier
    # task_complete means the user started a new turn, so last-event-wins
    # prevents a stale completion from masking the new turn.
    last_event = rollout_last_lifecycle(tracked_id)
    if last_event in TERMINAL_EVENTS:
        # Confirmed completion (or user abort). Produce the working->idle
        # transition that fires the Done sound exactly once.
        confirmed_completion = True
        state, message = ("idle", short_title(tracked_row[1]))
        tracked_id = None
    elif last_active_seen_ms and (NOW * 1000 - last_active_seen_ms) < HOLD_WORKING_S * 1000:
        # No terminal event yet; hold working briefly while we wait for the
        # completion write to land in the rollout.
        confirming = True
        state, message = ("working", short_title(tracked_row[1]))
    else:
        # Grace window expired; fall back to idle once.
        state, message = ("idle", short_title(tracked_row[1]))
        tracked_id = None
else:
    # No active thread and nothing tracked.
    fallback_title = rows[0][1] if rows else None
    state, message = ("idle", short_title(fallback_title) if fallback_title else None)
    tracked_id = None

render_dashboard(state, rows, error, confirming)

candidate = f"{state}:{message or ''}"
reported = persisted.get("reported")
reported_state = str(reported).split(":", 1)[0] if reported else None


def emit():
    params = {
        "pane_id": pane_id,
        "source": source,
        "agent": agent,
        "state": state,
        "seq": time.time_ns(),
    }
    if message:
        params["message"] = message
    send("pane.report_agent", params)
    persisted["reported"] = candidate
    persisted.pop("candidate", None)
    persisted.pop("candidate_count", None)


if reported is None:
    emit()
elif candidate == reported:
    persisted.pop("candidate", None)
    persisted.pop("candidate_count", None)
else:
    if state == "working":
        needed = ENTER_WORKING_POLLS
    elif confirmed_completion:
        needed = 1
    elif reported_state == "working":
        needed = LEAVE_WORKING_POLLS
    else:
        needed = GENERIC_POLLS

    if candidate == persisted.get("candidate"):
        count = persisted.get("candidate_count", 1) + 1
    else:
        count = 1
    persisted["candidate"] = candidate
    persisted["candidate_count"] = count
    if count >= needed:
        emit()

persisted["tracked_id"] = tracked_id
persisted["last_active_seen_ms"] = last_active_seen_ms

try:
    with open(state_file, "w", encoding="utf-8") as handle:
        json.dump(persisted, handle)
except Exception:
    pass
PY

  [ "${HERDR_CODEXAPP_WATCH_ONCE:-}" = "1" ] && exit 0
  sleep "$POLL_INTERVAL"
done
