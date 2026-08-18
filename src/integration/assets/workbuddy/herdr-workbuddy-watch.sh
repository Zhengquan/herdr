#!/bin/sh
# installed by herdr
# managed by herdr; reinstalling or updating the integration overwrites this file.
# add customizations beside this file instead of editing it.
# HERDR_INTEGRATION_ID=workbuddy
# HERDR_INTEGRATION_VERSION=6
#
# WorkBuddy is a standalone macOS desktop app: it never runs inside a Herdr
# pane, so process detection and screen manifests cannot observe it. This
# watcher bridges that gap. It runs inside a Herdr pane, polls WorkBuddy's
# local session database read-only, renders a live task dashboard into the
# pane, and reports the aggregated state over the Herdr socket API.
#
# Sound policy: herdr plays a Done sound on any working→idle transition. For
# a CLI agent that return is always a real completion, but for WorkBuddy
# (long-lived app, many sessions) an idle reading is not necessarily a
# completion. So this watcher only produces a working→idle transition when it
# can confirm the tracked task actually reached a terminal status
# (completed/failed/...) with a fresh updated_at. While it waits for that
# confirmation it holds the reported state at working (no sound); if
# confirmation never arrives within a grace window it falls back to idle once.

set -u

[ "${HERDR_ENV:-}" = "1" ] || exit 0
[ -n "${HERDR_PANE_ID:-}" ] || exit 0
[ -n "${HERDR_SOCKET_PATH:-}" ] || exit 0
command -v python3 >/dev/null 2>&1 || exit 0

WORKBUDDY_HOME_DIR="${WORKBUDDY_HOME:-$HOME/.workbuddy}"
DB_PATH="$WORKBUDDY_HOME_DIR/workbuddy.db"
POLL_INTERVAL="${HERDR_WORKBUDDY_POLL_INTERVAL:-3}"
STATE_FILE="${TMPDIR:-/tmp}/herdr-workbuddy-watch.${HERDR_PANE_ID}.state"

while true; do
  # Uninstall removes this script; stop the bridge when that happens.
  [ -f "$0" ] || exit 0

  HERDR_WORKBUDDY_DB="$DB_PATH" HERDR_WORKBUDDY_HOME="$WORKBUDDY_HOME_DIR" HERDR_WORKBUDDY_STATE_FILE="$STATE_FILE" python3 - <<'PY'
import json
import os
import random
import socket
import sqlite3
import sys
import time
import unicodedata

source = "herdr:workbuddy"
agent = "workbuddy"
pane_id = os.environ["HERDR_PANE_ID"]
socket_path = os.environ["HERDR_SOCKET_PATH"]
db_path = os.environ["HERDR_WORKBUDDY_DB"]
state_file = os.environ["HERDR_WORKBUDDY_STATE_FILE"]
workbuddy_home = os.environ["HERDR_WORKBUDDY_HOME"]

NOW = time.time()
NOW_MS = NOW * 1000

# ---------------------------------------------------------------------------
# Signal model (validated against live WorkBuddy data)
# ---------------------------------------------------------------------------
# * A session that is genuinely executing cycles its DB `status` between
#   'working' and 'planning' (and occasionally other active verbs) while its
#   `updated_at` keeps advancing every few seconds. This is the RELIABLE
#   "task is running" signal.
# * When a task finishes, WorkBuddy writes a terminal status ('completed' /
#   'Failed' / 'Terminated' / ...) and refreshes that row's `updated_at`. A
#   terminal row with a fresh updated_at is the RELIABLE "task just finished"
#   signal — and it is what authorizes a working→idle transition (and thus the
#   Done sound).
# * The interactive engine heartbeat file (<pid>.json mtime) is NOT reliable
#   for execution detection: it drifts up to ~30s stale during genuine
#   execution. It is kept only as a loose phantom guard that rejects a
#   prewarm-only pool worker.
# ---------------------------------------------------------------------------
ACTIVE_STATUSES = ("working", "planning", "running", "executing", "in_progress")
TERMINAL_STATUSES = {
    "completed", "failed", "terminated", "error", "archived", "cancelled",
}
ACTIVE_UPDATED_S = 45       # an active row whose updated_at is this fresh = executing
COMPLETION_FRESH_S = 45     # a terminal row whose updated_at is this fresh = just finished
HOLD_WORKING_S = 20         # hold working this long waiting for completion confirmation
ENGINE_LIVE_S = 60          # loose host-alive window (heartbeat drifts a lot)
FAILED_RECENT_MS = 30 * 60_000
ENTER_WORKING_POLLS = 1     # confirmations required to ENTER working (report quickly)
LEAVE_WORKING_POLLS = 3     # confirmations to LEAVE working on a *fallback* (absorb blips)
GENERIC_POLLS = 2           # confirmations for any other transition


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


def load_sessions():
    """Return (error_or_None, rows) where each row is
    (id, status, title, last_activity_at, updated_at)."""
    if not os.path.isfile(db_path):
        return ("WorkBuddy database not found", [])
    try:
        db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        rows = db.execute(
            "SELECT id, status, title, last_activity_at, updated_at FROM sessions "
            "WHERE deleted_at IS NULL AND status != 'archived' "
            "ORDER BY updated_at DESC LIMIT 20"
        ).fetchall()
        db.close()
        return (None, rows)
    except Exception:
        return ("WorkBuddy database unreadable", [])


def engine_live():
    """True while at least one real (non-prewarm, non-idle) WorkBuddy host is
    present. Used only as a weak phantom guard, with a loose window because the
    interactive heartbeat mtime drifts substantially during execution.
    """
    sessions_dir = os.path.join(workbuddy_home, "sessions")
    try:
        names = os.listdir(sessions_dir)
    except Exception:
        return False
    for name in names:
        if not name.endswith(".json"):
            continue
        path = os.path.join(sessions_dir, name)
        try:
            if NOW - os.path.getmtime(path) >= ENGINE_LIVE_S:
                continue
        except OSError:
            continue
        try:
            with open(path, encoding="utf-8") as handle:
                data = json.load(handle)
        except Exception:
            continue
        if str(data.get("kind", "")).lower() == "prewarm":
            continue
        meta = data.get("meta") or {}
        if str(meta.get("status", "")).lower() == "idle":
            continue
        return True
    return False


def age_ms(value):
    try:
        return NOW_MS - int(value)
    except Exception:
        return float("inf")


def is_active_status(status):
    return str(status).lower() in ACTIVE_STATUSES


def is_terminal_status(status):
    return str(status).lower() in TERMINAL_STATUSES


def find_active_row(rows):
    for r in rows:
        if is_active_status(r[1]) and age_ms(r[4]) < ACTIVE_UPDATED_S * 1000:
            return r
    return None


def find_row_by_id(rows, row_id):
    if not row_id:
        return None
    for r in rows:
        if r[0] == row_id:
            return r
    return None


def short_title(title):
    return str(title or "task").replace("\n", " ")[:120]


# ===========================================================================
# Dashboard rendering
# ===========================================================================
RESET = "\x1b[0m"
BOLD = "\x1b[1m"
DIM = "\x1b[2m"


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
    "working": ("▶", C_GREEN, "running"),
    "running": ("▶", C_GREEN, "running"),
    "executing": ("▶", C_GREEN, "running"),
    "in_progress": ("▶", C_GREEN, "running"),
    "planning": ("◐", C_BLUE, "planning"),
    "error": ("✕", C_RED, "failed"),
    "failed": ("✕", C_RED, "failed"),
    "terminated": ("⊘", C_MUTE, "stopped"),
    "completed": ("✓", C_MUTE, "done"),
}


def fmt_age(ms):
    try:
        secs = max(0, int((NOW_MS - int(ms)) / 1000))
    except Exception:
        return ""
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


def render_dashboard(state, rows, error, live, confirming):
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

    dot = f"{fg(C_GREEN)}●{RESET}" if live else f"{faint}○{RESET}"
    engine_txt = "host live" if live else "host idle"
    clock = time.strftime("%H:%M:%S")
    left_v = "  WorkBuddy  bridge"
    left_a = f"  {BOLD}{a2}WorkBuddy{RESET}{mute}  bridge{RESET}"
    hold_hint = "  confirming" if confirming else ""
    right_v = f"{engine_txt}  {clock}  "
    right_a = f"{dot} {mute}{engine_txt}{RESET}{faint}{hold_hint}{RESET}  {faint}{clock}{RESET}  "
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
        m = "  no sessions yet"
        lines.append(row(dwidth(m), f"  {faint}no sessions yet{RESET}"))
    else:
        h_status = dpad("STATUS", COL_STATUS)
        h_when = dpad("WHEN", COL_WHEN)
        head_v = f"  {h_status}  {h_when}  TASK"
        head_a = f"  {faint}{h_status}  {h_when}  TASK{RESET}"
        lines.append(row(dwidth(head_v), head_a))

        prefix_cols = 2 + COL_STATUS + 2 + COL_WHEN + 2
        task_budget = inner - prefix_cols
        for r in rows[:12]:
            _id, status, title, last_act, updated = r
            name = str(status).lower()
            fresh_active = is_active_status(name) and age_ms(updated) < ACTIVE_UPDATED_S * 1000
            just_done = is_terminal_status(name) and age_ms(updated) < COMPLETION_FRESH_S * 1000
            if fresh_active:
                icon, icolor, badge = ROW_ICONS.get(name, ("▶", C_GREEN, "running"))
            elif just_done and name in ("failed", "error"):
                icon, icolor, badge = "✕", C_RED, "failed"
            elif just_done:
                icon, icolor, badge = "✓", C_GREEN, "done"
            else:
                icon, icolor, badge = ROW_ICONS.get(name, ("◇", C_FAINT, name[:COL_STATUS - 2]))
            ic = fg(icolor)

            status_field = f"{icon} {badge}"
            status_v = dpad(status_field, COL_STATUS)
            status_pad = COL_STATUS - dwidth(status_field)
            status_a = f"{ic}{icon}{RESET} {ic}{badge}{RESET}" + (
                " " * (status_pad if status_pad > 0 else 0)
            )

            when = fmt_age(last_act)
            when_v = dpad(when, COL_WHEN)

            task = dtrunc(str(title or "(untitled)").replace("\n", " "), task_budget)
            line_v = f"  {status_v}  {when_v}  {task}"
            line_a = f"  {status_a}  {faint}{when_v}{RESET}  {text}{task}{RESET}"
            lines.append(row(dwidth(line_v), line_a))

    lines.append(row(0, ""))
    total = len(rows)
    running = sum(1 for r in rows if is_active_status(r[1])) if rows else 0
    foot_v = f"  {total} sessions · {running} active · poll {POLL_HINT}s"
    foot_a = f"  {faint}{total} sessions · {running} active · poll {POLL_HINT}s{RESET}"
    lines.append(sep)
    lines.append(row(dwidth(foot_v), foot_a))
    lines.append(bot)

    sys.stdout.write("\x1b[?25l\x1b[2J\x1b[H" + "\n".join(lines) + "\n")
    sys.stdout.flush()


POLL_HINT = os.environ.get("HERDR_WORKBUDDY_POLL_INTERVAL", "3")


# ===========================================================================
# Aggregate with completion confirmation
# ===========================================================================
error, rows = load_sessions()
live = engine_live()

try:
    with open(state_file, encoding="utf-8") as handle:
        persisted = json.load(handle)
except Exception:
    persisted = {}

tracked_id = persisted.get("tracked_id")
last_active_seen_ms = persisted.get("last_active_seen_ms")

active = find_active_row(rows)
tracked = find_row_by_id(rows, tracked_id)
confirming = False
confirmed_completion = False

if error:
    state, message = ("idle", error)
elif active is not None:
    # A task is genuinely running: track it and report working.
    tracked_id = active[0]
    last_active_seen_ms = NOW_MS
    state, message = ("working", short_title(active[2]))
elif tracked is not None and is_terminal_status(tracked[1]) and age_ms(tracked[4]) < COMPLETION_FRESH_S * 1000:
    # The task we were tracking just reached a terminal status with a fresh
    # updated_at: this is a confirmed completion. Produce the working→idle
    # transition that fires the Done sound exactly once.
    confirmed_completion = True
    state, message = ("idle", short_title(tracked[2]))
    tracked_id = None
elif tracked_id is not None and last_active_seen_ms and (NOW_MS - last_active_seen_ms) < HOLD_WORKING_S * 1000:
    # The active row disappeared but we have not yet seen a terminal status.
    # Hold the reported state at working (no idle → no sound) while we wait a
    # brief grace window for the completion write to land.
    confirming = True
    state, message = ("working", short_title(tracked[2] if tracked else None))
else:
    # No active task and either no tracked session or the grace window has
    # expired. Fall back to idle. If herdr's previous state was working this
    # will fire Done once — acceptable, because a task likely ended without a
    # clean terminal write (or the watcher was just started).
    fallback_title = None
    if tracked is not None:
        fallback_title = tracked[2]
    elif rows:
        fallback_title = rows[0][2]
    state, message = ("idle", short_title(fallback_title) if fallback_title else None)
    tracked_id = None

render_dashboard(state, rows, error, live, confirming)

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
        # A genuine completion: report promptly so the Done sound is not
        # delayed by the leave-working dwell that exists to absorb blips.
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

# Persist tracking state so the next poll can confirm completion.
persisted["tracked_id"] = tracked_id
persisted["last_active_seen_ms"] = last_active_seen_ms

try:
    with open(state_file, "w", encoding="utf-8") as handle:
        json.dump(persisted, handle)
except Exception:
    pass
PY

  [ "${HERDR_WORKBUDDY_WATCH_ONCE:-}" = "1" ] && exit 0
  sleep "$POLL_INTERVAL"
done
