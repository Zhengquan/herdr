#!/bin/sh
# installed by herdr
# managed by herdr; reinstalling or updating the integration overwrites this file.
# add customizations beside this file instead of editing it.
# HERDR_INTEGRATION_ID=workbuddy
# HERDR_INTEGRATION_VERSION=4
#
# WorkBuddy is a standalone macOS desktop app: it never runs inside a Herdr
# pane, so process detection and screen manifests cannot observe it. This
# watcher bridges that gap. It runs inside a Herdr pane, polls WorkBuddy's
# local session database read-only, renders a live task dashboard into the
# pane, and reports the aggregated state over the Herdr socket API.

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
# Tunables. All time windows are chosen to be longer than WorkBuddy's ~1s
# engine heartbeat cadence and its ~3-10s background-writer cadence, so that
# routine bookkeeping never registers as task execution.
# ---------------------------------------------------------------------------
ENGINE_LIVE_S = 12          # a non-prewarm engine heartbeat this fresh = a host is alive
WORKING_UPDATED_S = 15      # a working row whose updated_at is this fresh = actively executing
FAILED_RECENT_MS = 30 * 60_000
ENTER_WORKING_POLLS = 1     # confirmations required to ENTER working (report quickly)
LEAVE_WORKING_POLLS = 3     # confirmations required to LEAVE working (absorb writer blips)
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
    (status, title, last_activity_at, updated_at)."""
    if not os.path.isfile(db_path):
        return ("WorkBuddy database not found", [])
    try:
        db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        rows = db.execute(
            "SELECT status, title, last_activity_at, updated_at FROM sessions "
            "WHERE deleted_at IS NULL AND status != 'archived' "
            "ORDER BY updated_at DESC LIMIT 20"
        ).fetchall()
        db.close()
        return (None, rows)
    except Exception:
        return ("WorkBuddy database unreadable", [])


def engine_live():
    """True while a *task-executing* WorkBuddy engine is heartbeating.

    Each session spawns a host-cli engine that rewrites its <pid>.json
    heartbeat about once a second. Two kinds of engine must NOT count as a
    running task, or the bridge fires phantom completion sounds:

      * prewarm pool workers (kind == "prewarm"): idle engines kept warm for
        fast session startup. They refresh on a timer while executing nothing.
      * any descriptor whose meta.status is "idle".

    Only a fresh heartbeat from a non-prewarm, non-idle engine is treated as a
    live host. Host liveness alone is still not "a task is running" — that is
    decided in aggregate() together with the database — but it gates it.
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
            # Fresh but unreadable: be conservative and ignore it rather than
            # letting an unknown descriptor pin the state to working.
            continue
        if str(data.get("kind", "")).lower() == "prewarm":
            continue
        meta = data.get("meta") or {}
        if str(meta.get("status", "")).lower() == "idle":
            continue
        return True
    return False


def aggregate(rows, error, live):
    """Return (state, message).

    Working requires BOTH signals to agree:
      * the database has a session whose status == 'working' with a fresh
        updated_at (an engine is actively writing to it), AND
      * a non-prewarm engine is heartbeating (engine_live()).

    Requiring both eliminates the two historical failure modes: a stuck
    'working' status with no live engine (no phantom working), and a live but
    idle prewarm engine with no working row (no phantom sound).
    """
    if error:
        return ("idle", error)

    def age_ms(value):
        try:
            return NOW_MS - int(value)
        except Exception:
            return float("inf")

    working_row = next(
        (
            r
            for r in rows
            if str(r[0]).lower() == "working"
            and age_ms(r[3]) < WORKING_UPDATED_S * 1000
        ),
        None,
    )
    if live and working_row is not None:
        title = str(working_row[1] or "task")
        return ("working", title[:120])

    failed = [
        r
        for r in rows
        if str(r[0]).lower() in ("error", "failed")
        and age_ms(r[2]) < FAILED_RECENT_MS
    ]
    if failed:
        return ("blocked", f"task failed: {str(failed[0][1] or 'task')[:110]}")

    if rows and rows[0][1]:
        return ("idle", str(rows[0][1])[:120])
    return ("idle", None)


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


# Palette (256-color, renders on any modern terminal).
C_ACCENT = 39     # cyan-blue brand accent
C_ACCENT2 = 45    # lighter cyan
C_GREEN = 42
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


def fmt_ts(ms):
    try:
        return time.strftime("%H:%M", time.localtime(int(ms) / 1000))
    except Exception:
        return "--:--"


def fmt_age(ms):
    try:
        secs = max(0, int((NOW_MS - int(ms)) / 1000))
    except Exception:
        return ""
    if secs < 60:
        return f"{secs}s ago"
    mins = secs // 60
    if mins < 60:
        return f"{mins}m ago"
    hours = mins // 60
    if hours < 24:
        return f"{hours}h ago"
    return f"{hours // 24}d ago"


import unicodedata


def dwidth(text):
    """Display width in terminal columns. CJK / wide glyphs count as 2."""
    total = 0
    for ch in text:
        if unicodedata.combining(ch):
            continue
        total += 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1
    return total


def dtrunc(text, limit):
    """Truncate text to at most `limit` display columns, appending '…'."""
    if dwidth(text) <= limit:
        return text
    out = []
    used = 0
    budget = max(0, limit - 1)  # reserve a column for the ellipsis
    for ch in text:
        w = 0 if unicodedata.combining(ch) else (
            2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1
        )
        if used + w > budget:
            break
        out.append(ch)
        used += w
    return "".join(out) + "…"


WIDTH = 72


def rule(left, right=""):
    inner = WIDTH - 2
    pad = inner - len(left) - len(right)
    if pad < 1:
        pad = 1
    return f"{left}{' ' * pad}{right}"


def render_dashboard(state, rows, error, live):
    color, label, glyph = STATE_THEME.get(state, STATE_THEME["idle"])
    accent = fg(C_ACCENT)
    a2 = fg(C_ACCENT2)
    text = fg(C_TEXT)
    mute = fg(C_MUTE)
    faint = fg(C_FAINT)

    top = f"{accent}╭{'─' * (WIDTH - 2)}╮{RESET}"
    bot = f"{accent}╰{'─' * (WIDTH - 2)}╯{RESET}"

    def row(content_visible, content_ansi):
        pad = (WIDTH - 2) - content_visible
        if pad < 0:
            pad = 0
        return f"{accent}│{RESET}{content_ansi}{' ' * pad}{accent}│{RESET}"

    # Header line: brand + engine dot + clock.
    dot = f"{fg(C_GREEN)}●{RESET}" if live else f"{faint}○{RESET}"
    engine_txt = "engine live" if live else "engine idle"
    clock = time.strftime("%H:%M:%S")
    left_v = f"  WorkBuddy  bridge"
    left_a = f"  {BOLD}{a2}WorkBuddy{RESET}{mute}  bridge{RESET}"
    right_v = f"{engine_txt}  {clock}  "
    right_a = f"{dot} {mute}{engine_txt}{RESET}  {faint}{clock}{RESET}  "
    mid_pad = (WIDTH - 2) - dwidth(left_v) - dwidth(right_v)
    if mid_pad < 1:
        mid_pad = 1
    header_a = left_a + (" " * mid_pad) + right_a
    header_v = left_v + (" " * mid_pad) + right_v

    # Status badge line.
    badge_v = f"  {glyph} {label} "
    badge_a = f"  {bg(color)}{fg(16)}{BOLD} {glyph} {label} {RESET}"
    lines = [
        top,
        row(dwidth(header_v), header_a),
        f"{accent}├{'─' * (WIDTH - 2)}┤{RESET}",
        row(dwidth(badge_v), badge_a),
        row(0, ""),
    ]

    if error:
        emsg = dtrunc(str(error), WIDTH - 6)
        lines.append(row(dwidth(f"  {emsg}"), f"  {fg(C_AMBER)}{emsg}{RESET}"))
    elif not rows:
        m = "  no sessions yet"
        lines.append(row(dwidth(m), f"  {faint}no sessions yet{RESET}"))
    else:
        head_v = "  STATUS      WHEN       TASK"
        head_a = f"  {faint}STATUS      WHEN       TASK{RESET}"
        lines.append(row(dwidth(head_v), head_a))
        for status, title, last_act, updated in rows[:12]:
            name = str(status).lower()
            if name == "working" and live:
                icon, ic, badge = "▶", fg(C_GREEN), "running"
            elif name == "working":
                icon, ic, badge = "◌", faint, "stale"
            elif name in ("error", "failed"):
                icon, ic, badge = "✕", fg(C_RED), "failed"
            elif name == "completed":
                icon, ic, badge = "✓", mute, "done"
            else:
                icon, ic, badge = "◇", faint, name[:7]
            when = fmt_age(last_act)
            # Prefix is all single-width; the title may contain CJK so it is
            # truncated by display width and padding is measured the same way.
            prefix_v = f"  {icon} {badge:<8}  {when:<9}  "
            task = dtrunc(str(title or "(untitled)").replace("\n", " "),
                          (WIDTH - 2) - dwidth(prefix_v))
            line_v = prefix_v + task
            line_a = (
                f"  {ic}{icon}{RESET} {ic}{badge:<8}{RESET}  "
                f"{faint}{when:<9}{RESET}  {text}{task}{RESET}"
            )
            lines.append(row(dwidth(line_v), line_a))

    lines.append(row(0, ""))
    total = len(rows)
    running = sum(1 for r in rows if str(r[0]).lower() == "working") if rows else 0
    foot_v = f"  {total} sessions · {running} working · poll {POLL_HINT}s"
    foot_a = (
        f"  {faint}{total} sessions · {running} working · "
        f"poll {POLL_HINT}s{RESET}"
    )
    lines.append(f"{accent}├{'─' * (WIDTH - 2)}┤{RESET}")
    lines.append(row(dwidth(foot_v), foot_a))
    lines.append(bot)

    sys.stdout.write("\x1b[?25l\x1b[2J\x1b[H" + "\n".join(lines) + "\n")
    sys.stdout.flush()


POLL_HINT = os.environ.get("HERDR_WORKBUDDY_POLL_INTERVAL", "3")


# ===========================================================================
# Poll + state machine
# ===========================================================================
error, rows = load_sessions()
live = engine_live()
state, message = aggregate(rows, error, live)
render_dashboard(state, rows, error, live)

candidate = f"{state}:{message or ''}"
try:
    with open(state_file, encoding="utf-8") as handle:
        persisted = json.load(handle)
except Exception:
    persisted = {}

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
    # First ever observation: report immediately so the pane gets state
    # without waiting out the dwell window.
    emit()
elif candidate == reported:
    # Nothing changed; clear any stale in-flight candidate.
    persisted.pop("candidate", None)
    persisted.pop("candidate_count", None)
else:
    # Hysteresis: how many consecutive confirmations does THIS transition
    # need? Entering working is quick so long builds show up fast. Leaving
    # working is slow so a background writer that momentarily flips the row to
    # completed and back cannot fire a false completion sound.
    if state == "working":
        needed = ENTER_WORKING_POLLS
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

try:
    with open(state_file, "w", encoding="utf-8") as handle:
        json.dump(persisted, handle)
except Exception:
    pass
PY

  [ "${HERDR_WORKBUDDY_WATCH_ONCE:-}" = "1" ] && exit 0
  sleep "$POLL_INTERVAL"
done
