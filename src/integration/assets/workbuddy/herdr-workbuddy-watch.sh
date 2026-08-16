#!/bin/sh
# installed by herdr
# managed by herdr; reinstalling or updating the integration overwrites this file.
# add customizations beside this file instead of editing it.
# HERDR_INTEGRATION_ID=workbuddy
# HERDR_INTEGRATION_VERSION=2
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

  HERDR_WORKBUDDY_DB="$DB_PATH" HERDR_WORKBUDDY_STATE_FILE="$STATE_FILE" python3 - <<'PY'
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

def aggregate(rows, error):
    """Return (state, message) for herdr reporting."""
    if error:
        return ("idle", error)
    working = [r for r in rows if str(r[0]).lower() == "working"]
    if working:
        return ("working", str(working[0][1] or "task")[:120])
    failed = [r for r in rows if str(r[0]).lower() in ("error", "failed")]
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

def render_dashboard(state, rows, error):
    """Redraw the pane as a live task list."""
    green, red, dim, bold, reset = (
        "\x1b[32m", "\x1b[31m", "\x1b[2m", "\x1b[1m", "\x1b[0m",
    )
    state_color = {"working": green, "blocked": red}.get(state, dim)
    lines = [
        f"{bold}WorkBuddy{reset}  {state_color}{state.upper()}{reset}  "
        f"{dim}updated {time.strftime('%H:%M:%S')}{reset}",
        "",
    ]
    if error:
        lines.append(f"{dim}{error}{reset}")
    elif not rows:
        lines.append(f"{dim}no sessions{reset}")
    else:
        for status, title, ts in rows[:14]:
            name = str(status).lower()
            if name == "working":
                icon, color = "●", green
            elif name in ("error", "failed"):
                icon, color = "✕", red
            else:
                icon, color = "○", dim
            text = str(title or "(untitled)").replace("\n", " ")[:56]
            lines.append(f"{color}{icon}{reset} {name:<10} {fmt_ts(ts)}  {text}")
    sys.stdout.write("\x1b[2J\x1b[H" + "\n".join(lines) + "\n")
    sys.stdout.flush()

error, rows = load_sessions()
state, message = aggregate(rows, error)
render_dashboard(state, rows, error)

signature = f"{state}:{message or ''}"
try:
    with open(state_file, encoding="utf-8") as handle:
        last = handle.read()
except Exception:
    last = None

if signature != last:
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
    try:
        with open(state_file, "w", encoding="utf-8") as handle:
            handle.write(signature)
    except Exception:
        pass
PY

  [ "${HERDR_WORKBUDDY_WATCH_ONCE:-}" = "1" ] && exit 0
  sleep "$POLL_INTERVAL"
done
