#!/bin/sh
# installed by herdr
# managed by herdr; reinstalling or updating the integration overwrites this file.
# add customizations beside this file instead of editing it.
# HERDR_INTEGRATION_ID=workbuddy
# HERDR_INTEGRATION_VERSION=8
#
# WorkBuddy is a standalone macOS desktop app: it never runs inside a Herdr
# pane, so process detection and screen manifests cannot observe it. This
# watcher bridges that gap. It runs inside a Herdr pane, polls WorkBuddy's
# local state read-only, renders a live task dashboard into the pane, and
# reports the aggregated state over the Herdr socket API.
#
# Sound policy: herdr plays a Done sound on any working→idle transition. For
# a CLI agent that return is always a real completion, but for WorkBuddy
# (long-lived app, many sessions) an idle reading is not necessarily a
# completion. So this watcher only produces a working→idle transition when the
# tracked session's transcript shows a settled turn end. While it waits for
# that confirmation it holds the reported state at working (no sound); if
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
import glob
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
# Signal model (validated against live WorkBuddy 2.132.x data)
# ---------------------------------------------------------------------------
# * `sessions.status` in workbuddy.db is NOT an execution signal. A genuinely
#   executing session keeps the status it was left with — in practice
#   'completed' — so status alone reports every running task as finished.
#   The table is still the session directory: ids, titles, cwd, ordering.
# * ~/.workbuddy/projects/<cwd-slug>/<session_id>.jsonl is the conversation
#   transcript and it IS authoritative for turn lifecycle. Records are
#   appended in turn order: a user `message` starts a turn, then `reasoning`,
#   `function_call`, `function_call_result` and `file-history-snapshot`
#   records stream while work happens, and the turn ends with an assistant
#   `message` (status 'completed', or 'incomplete' when interrupted). Every
#   assistant text block is written as a completed `message`, so only the
#   LAST record in the file distinguishes a finished turn from a mid-turn
#   narration that is about to be followed by another tool call.
# * ~/.workbuddy/sessions/<pid>.json is a per-host heartbeat carrying the
#   conversation id it serves. It is not an execution signal — a host stays
#   alive while its conversation sits idle — but a missing host proves the
#   conversation cannot be executing, which retires transcripts abandoned
#   mid-turn by a crash.
# ---------------------------------------------------------------------------
IN_FLIGHT = "in_flight"
TURN_END = "turn_end"
DB_FAILED_STATUSES = {"failed", "error"}
DB_STOPPED_STATUSES = {"terminated", "cancelled"}
TRANSCRIPT_FRESH_S = 45     # no readable turn record: treat fresh writes as work
SETTLE_S = 4                # a turn end must be this old to confirm completion
ABANDONED_TURN_S = 120      # in-flight transcript with no live host: give up
HOST_LIVE_S = 60            # heartbeat window for "this conversation has a host"
HOLD_WORKING_S = 20         # hold working this long waiting for confirmation
ENTER_WORKING_POLLS = 1     # confirmations required to ENTER working (report quickly)
LEAVE_WORKING_POLLS = 3     # confirmations to LEAVE working on a *fallback*
GENERIC_POLLS = 2           # confirmations for any other transition
TAIL_BLOCK_BYTES = 64 * 1024
MAX_TAIL_BYTES = 4 * 1024 * 1024

transcript_paths = {}
transcript_stats = {}
turn_states = {}


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
    (id, status, title, last_activity_at, updated_at, cwd)."""
    if not os.path.isfile(db_path):
        return ("WorkBuddy database not found", [])
    try:
        db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        rows = db.execute(
            "SELECT id, status, title, last_activity_at, updated_at, cwd FROM sessions "
            "WHERE deleted_at IS NULL AND status != 'archived' "
            "ORDER BY updated_at DESC LIMIT 20"
        ).fetchall()
        db.close()
        return (None, rows)
    except Exception:
        return ("WorkBuddy database unreadable", [])


def load_hosts():
    """Return the set of conversation ids with a fresh host heartbeat."""
    live = set()
    sessions_dir = os.path.join(workbuddy_home, "sessions")
    try:
        names = os.listdir(sessions_dir)
    except Exception:
        return live
    for name in names:
        if not name.endswith(".json"):
            continue
        path = os.path.join(sessions_dir, name)
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            continue
        try:
            with open(path, encoding="utf-8") as handle:
                data = json.load(handle)
        except Exception:
            continue
        if not isinstance(data, dict):
            continue
        beat = data.get("lastHeartbeat")
        try:
            beat_s = float(beat) / 1000
        except Exception:
            beat_s = mtime
        if NOW - max(beat_s, mtime) >= HOST_LIVE_S:
            continue
        session_id = data.get("sessionId")
        if session_id:
            live.add(str(session_id))
    return live


def transcript_path(session_id, cwd):
    """Locate a session's transcript. WorkBuddy slugs the working directory by
    replacing path separators with dashes, so the path is derived directly and
    only falls back to a scan when that derivation misses."""
    if session_id in transcript_paths:
        return transcript_paths[session_id]
    path = None
    projects_dir = os.path.join(workbuddy_home, "projects")
    if cwd:
        slug = str(cwd).replace("\\", "-").replace("/", "-").lstrip("-")
        candidate = os.path.join(projects_dir, slug, f"{session_id}.jsonl")
        if os.path.isfile(candidate):
            path = candidate
    if path is None:
        matches = glob.glob(os.path.join(projects_dir, "*", f"{session_id}.jsonl"))
        if matches:
            path = max(matches, key=os.path.getmtime)
    transcript_paths[session_id] = path
    return path


def transcript_stat(session_id, cwd):
    """Return (age_seconds, size_bytes) for a session's transcript."""
    if session_id in transcript_stats:
        return transcript_stats[session_id]
    result = (None, 0)
    path = transcript_path(session_id, cwd)
    if path:
        try:
            stat = os.stat(path)
            result = (NOW - stat.st_mtime, stat.st_size)
        except OSError:
            result = (None, 0)
    transcript_stats[session_id] = result
    return result


def last_complete_record(path, size):
    """Return the last fully written JSONL record of a transcript.

    Records are read backwards a block at a time so an active transcript costs
    one small read regardless of its length. Bytes after the final newline are
    a record still being appended, so they are skipped: the previous record is
    the newest one that is safe to classify.
    """
    if size <= 0:
        return None
    try:
        with open(path, "rb") as handle:
            position = size
            tail = b""
            while position > 0 and len(tail) < MAX_TAIL_BYTES:
                amount = min(TAIL_BLOCK_BYTES, position)
                position -= amount
                handle.seek(position)
                tail = handle.read(amount) + tail
                lines = tail.split(b"\n")
                # lines[-1] follows the last newline and may be a partial
                # append; lines[0] is only whole when the read reached the
                # start of the file.
                bounded = lines[:-1] if position == 0 else lines[1:-1]
                for line in reversed(bounded):
                    if not line.strip():
                        continue
                    try:
                        record = json.loads(line.decode("utf-8", errors="replace"))
                    except Exception:
                        continue
                    if isinstance(record, dict):
                        return record
    except OSError:
        return None
    return None


def turn_state(session_id, cwd):
    """Return (kind, message_status, age_seconds) for a session's last turn.

    `kind` is TURN_END once the transcript's last record is an assistant
    message, IN_FLIGHT while any other record is last, and None when no
    transcript record can be read.
    """
    if session_id in turn_states:
        return turn_states[session_id]
    age, size = transcript_stat(session_id, cwd)
    path = transcript_path(session_id, cwd)
    kind = None
    status = None
    if path:
        record = last_complete_record(path, size)
        if record is not None:
            if record.get("type") == "message" and record.get("role") == "assistant":
                kind = TURN_END
                status = str(record.get("status") or "").lower()
            else:
                kind = IN_FLIGHT
    result = (kind, status, age)
    turn_states[session_id] = result
    return result


def session_active(row, live_hosts):
    """True while a session's transcript shows a turn still in flight."""
    session_id = row[0]
    kind, _status, age = turn_state(session_id, row[5])
    if kind == TURN_END:
        return False
    if kind == IN_FLIGHT:
        if session_id in live_hosts:
            # A tool call can run for minutes without a transcript write, so an
            # in-flight turn stays working as long as its host is alive.
            return True
        return age is not None and age < ABANDONED_TURN_S
    return age is not None and age < TRANSCRIPT_FRESH_S


def age_ms(value):
    try:
        return NOW_MS - int(value)
    except Exception:
        return float("inf")


def effective_age_s(row):
    """Seconds since a session last showed any activity."""
    age, _size = transcript_stat(row[0], row[5])
    db_age = age_ms(row[4]) / 1000
    if age is None:
        return db_age
    return min(age, db_age)


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
    "running": ("▶", C_GREEN, "running"),
    "done": ("✓", C_MUTE, "done"),
    "failed": ("✕", C_RED, "failed"),
    "stopped": ("⊘", C_AMBER, "stopped"),
    "unknown": ("◇", C_FAINT, "idle"),
}


def row_badge(row, live_hosts):
    """Pick the STATUS column glyph for a session row."""
    if session_active(row, live_hosts):
        return ROW_ICONS["running"]
    db_status = str(row[1] or "").lower()
    if db_status in DB_FAILED_STATUSES:
        return ROW_ICONS["failed"]
    if db_status in DB_STOPPED_STATUSES:
        return ROW_ICONS["stopped"]
    kind, status, _age = turn_state(row[0], row[5])
    if kind == TURN_END:
        return ROW_ICONS["stopped"] if status == "incomplete" else ROW_ICONS["done"]
    return ROW_ICONS["unknown"]


def fmt_age_from_age(secs):
    try:
        secs = max(0, int(secs))
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


def render_dashboard(state, rows, error, live, confirming, live_hosts):
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

    # right_v must include every visible glyph in right_a (dot + hold_hint),
    # or row() padding pushes the right border out by that many columns.
    dot_v = "●" if live else "○"
    dot = f"{fg(C_GREEN)}●{RESET}" if live else f"{faint}○{RESET}"
    engine_txt = "host live" if live else "host idle"
    clock = time.strftime("%H:%M:%S")
    left_v = "  WorkBuddy  bridge"
    left_a = f"  {BOLD}{a2}WorkBuddy{RESET}{mute}  bridge{RESET}"
    hold_hint = "  confirming" if confirming else ""
    right_v = f"{dot_v} {engine_txt}{hold_hint}  {clock}  "
    right_a = f"{dot} {mute}{engine_txt}{RESET}{faint}{hold_hint}{RESET}  {faint}{clock}{RESET}  "
    mid_pad = inner - dwidth(left_v) - dwidth(right_v)
    if mid_pad < 1:
        mid_pad = 1
    header_a = left_a + (" " * mid_pad) + right_a
    header_v = left_v + (" " * mid_pad) + right_v

    # Leading space inside the color block is intentional padding; keep it in
    # badge_v so row() width matches the painted line.
    badge_v = f"   {glyph} {label} "
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
            icon, icolor, badge = row_badge(r, live_hosts)
            ic = fg(icolor)

            status_field = f"{icon} {badge}"
            status_v = dpad(status_field, COL_STATUS)
            status_pad = COL_STATUS - dwidth(status_field)
            status_a = f"{ic}{icon}{RESET} {ic}{badge}{RESET}" + (
                " " * (status_pad if status_pad > 0 else 0)
            )

            when = fmt_age_from_age(effective_age_s(r))
            when_v = dpad(when, COL_WHEN)

            task = dtrunc(str(r[2] or "(untitled)").replace("\n", " "), task_budget)
            line_v = f"  {status_v}  {when_v}  {task}"
            line_a = f"  {status_a}  {faint}{when_v}{RESET}  {text}{task}{RESET}"
            lines.append(row(dwidth(line_v), line_a))

    lines.append(row(0, ""))
    total = len(rows)
    running = sum(1 for r in rows if session_active(r, live_hosts)) if rows else 0
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
live_hosts = load_hosts()
live = bool(live_hosts)
if rows:
    rows.sort(key=effective_age_s)

try:
    with open(state_file, encoding="utf-8") as handle:
        persisted = json.load(handle)
except Exception:
    persisted = {}

tracked_id = persisted.get("tracked_id")
last_active_seen_ms = persisted.get("last_active_seen_ms")

active = next((r for r in rows if session_active(r, live_hosts)), None)
tracked = next((r for r in rows if r[0] == tracked_id), None) if tracked_id else None
confirming = False
confirmed_completion = False

if error:
    state, message = ("idle", error)
elif active is not None:
    # A turn is genuinely in flight: track it and report working.
    tracked_id = active[0]
    last_active_seen_ms = NOW_MS
    state, message = ("working", short_title(active[2]))
elif tracked is not None:
    kind, _status, age = turn_state(tracked_id, tracked[5])
    if kind == TURN_END and age is not None and age >= SETTLE_S:
        # The tracked session's transcript ends with an assistant message that
        # has stopped being followed by tool calls, so the turn really is over.
        # Produce the working→idle transition that fires the Done sound once.
        confirmed_completion = True
        state, message = ("idle", short_title(tracked[2]))
        tracked_id = None
    elif last_active_seen_ms and (NOW_MS - last_active_seen_ms) < HOLD_WORKING_S * 1000:
        # A mid-turn assistant message is followed by its tool call within
        # milliseconds, so an unsettled turn end is indistinguishable from
        # ongoing work. Hold working (no idle → no sound) until it settles.
        confirming = True
        state, message = ("working", short_title(tracked[2]))
    else:
        # Grace window expired without a settled turn end. Fall back to idle.
        state, message = ("idle", short_title(tracked[2]))
        tracked_id = None
else:
    # No in-flight turn and nothing tracked.
    fallback_title = rows[0][2] if rows else None
    state, message = ("idle", short_title(fallback_title) if fallback_title else None)
    tracked_id = None

render_dashboard(state, rows, error, live, confirming, live_hosts)

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
