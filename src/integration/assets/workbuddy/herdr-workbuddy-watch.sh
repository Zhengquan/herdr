#!/bin/sh
# installed by herdr
# managed by herdr; reinstalling or updating the integration overwrites this file.
# add customizations beside this file instead of editing it.
# HERDR_INTEGRATION_ID=workbuddy
# HERDR_INTEGRATION_VERSION=3
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
    """Return (error_message_or_None, rows) from WorkBuddy's session database."""
    if not os.path.isfile(db_path):
        return ("WorkBuddy database not found", [])
    try:
        db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        rows = db.execute(
            "SELECT status, title, last_activity_at FROM sessions "
            "WHERE deleted_at IS NULL AND status != 'archived' "
            "ORDER BY last_activity_at DESC LIMIT 20"
        ).fetchall()
        db.close()
        return (None, rows)
    except Exception:
        return ("WorkBuddy database unreadable", [])

# Liveness comes from the engine process descriptors in ~/.workbuddy/sessions/:
# each actively executing session spawns a host-cli engine that rewrites its
# <pid>.json heartbeat about every second. The heartbeat stops when execution
# finishes, even though sessions.status stays 'working' in the database — so
# heartbeat freshness, not the status field, is the real "is it running"
# signal. A long tool call keeps heartbeating, so this covers long builds.
ENGINE_LIVE_S = 20
# A failed session only holds the bridge in blocked while it is recent;
# otherwise an old failure would pin the state forever.
FAILED_RECENT_MS = 30 * 60_000

def engine_live():
    """True while any WorkBuddy engine process is heartbeating."""
    sessions_dir = os.path.join(workbuddy_home, "sessions")
    now = time.time()
    try:
        names = os.listdir(sessions_dir)
    except Exception:
        return False
    for name in names:
        if not name.endswith(".json"):
            continue
        try:
            if now - os.path.getmtime(os.path.join(sessions_dir, name)) < ENGINE_LIVE_S:
                return True
        except OSError:
            continue
    return False

def aggregate(rows, error, live):
    """Return (state, message) for herdr reporting."""
    if error:
        return ("idle", error)
    now = time.time() * 1000

    def age_ms(row):
        try:
            return now - int(row[2])
        except Exception:
            return float("inf")

    if live:
        # An engine is heartbeating: something is executing right now. The
        # database status lags behind reality in both directions, so the
        # heartbeat wins; take the title from the most recent session.
        title = next(
            (str(r[1]) for r in rows if str(r[0]).lower() == "working" and r[1]),
            str(rows[0][1]) if rows and rows[0][1] else "task",
        )
        return ("working", title[:120])

    failed = [
        r for r in rows
        if str(r[0]).lower() in ("error", "failed") and age_ms(r) < FAILED_RECENT_MS
    ]
    if failed:
        return ("blocked", f"task failed: {str(failed[0][1] or 'task')[:110]}")

    if rows and rows[0][1]:
        return ("idle", str(rows[0][1])[:120])
    return ("idle", None)

def fmt_ts(ms):
    try:
        return time.strftime("%H:%M", time.localtime(int(ms) / 1000))
    except Exception:
        return "--:--"

def render_dashboard(state, rows, error, live):
    """Redraw the pane as a live task list."""
    green, red, dim, bold, reset = (
        "\x1b[32m", "\x1b[31m", "\x1b[2m", "\x1b[1m", "\x1b[0m",
    )
    state_color = {"working": green, "blocked": red}.get(state, dim)
    engine = f"{green}engine live{reset}" if live else f"{dim}engine idle{reset}"
    lines = [
        f"{bold}WorkBuddy{reset}  {state_color}{state.upper()}{reset}  "
        f"{engine}  {dim}updated {time.strftime('%H:%M:%S')}{reset}",
        "",
    ]
    if error:
        lines.append(f"{dim}{error}{reset}")
    elif not rows:
        lines.append(f"{dim}no sessions{reset}")
    else:
        for status, title, ts in rows[:14]:
            name = str(status).lower()
            if name == "working" and live:
                icon, color, label = "●", green, "working"
            elif name == "working":
                # The database says working but no engine is heartbeating:
                # the turn already finished, the status is just stale.
                icon, color, label = "◌", dim, "stale"
            elif name in ("error", "failed"):
                icon, color, label = "✕", red, name
            else:
                icon, color, label = "○", dim, name
            text = str(title or "(untitled)").replace("\n", " ")[:56]
            lines.append(f"{color}{icon}{reset} {label:<10} {fmt_ts(ts)}  {text}")
    sys.stdout.write("\x1b[2J\x1b[H" + "\n".join(lines) + "\n")
    sys.stdout.flush()

error, rows = load_sessions()
live = engine_live()
state, message = aggregate(rows, error, live)
render_dashboard(state, rows, error, live)

# Transition dwell: WorkBuddy's status field is authoritative while a task
# runs, but around turn completion a background writer (title/summary/usage
# sync) can flip it working->completed->working within seconds. Reporting
# every flip makes herdr play spurious done/request sounds. Require the
# candidate state to survive two consecutive polls before reporting it.
candidate = f"{state}:{message or ''}"
try:
    with open(state_file, encoding="utf-8") as handle:
        persisted = json.load(handle)
except Exception:
    persisted = {}

reported = persisted.get("reported")
if candidate == reported:
    pass
elif candidate == persisted.get("candidate"):
    if persisted.get("candidate_count", 1) >= 1:
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
    else:
        persisted["candidate_count"] = persisted.get("candidate_count", 0) + 1
else:
    persisted["candidate"] = candidate
    persisted["candidate_count"] = 1

if persisted.get("reported") is None:
    # First ever observation: report immediately so the pane gets state
    # without waiting out the dwell window.
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

try:
    with open(state_file, "w", encoding="utf-8") as handle:
        json.dump(persisted, handle)
except Exception:
    pass
PY

  [ "${HERDR_WORKBUDDY_WATCH_ONCE:-}" = "1" ] && exit 0
  sleep "$POLL_INTERVAL"
done
