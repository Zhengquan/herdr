from contextlib import closing
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import tempfile
import time
import unicodedata
import unittest


REPO_ROOT = Path(__file__).resolve().parent.parent
WATCHER = REPO_ROOT / "src/integration/assets/workbuddy/herdr-workbuddy-watch.sh"
SESSION_ID = "11111111-2222-3333-4444-555555555555"
SESSION_CWD = "/tmp/workbuddy-project"
DASHBOARD_WIDTH = 94
CSI_RE = re.compile(r"\x1b\[[0-9;?]*[ -/]*[@-~]")


def strip_csi(text: str) -> str:
    return CSI_RE.sub("", text)


def display_width(text: str) -> int:
    total = 0
    for ch in text:
        if unicodedata.combining(ch):
            continue
        total += 2 if unicodedata.east_asian_width(ch) in ("W", "F") else 1
    return total


class WorkbuddyIntegrationAssetTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory(prefix="herdr-workbuddy-test-")
        self.base = Path(self.temp_dir.name)
        self.workbuddy_home = self.base / "workbuddy-home"
        self.workbuddy_home.mkdir(parents=True)

        with closing(sqlite3.connect(self.workbuddy_home / "workbuddy.db")) as database:
            database.execute(
                "CREATE TABLE sessions ("
                "id TEXT, status TEXT, title TEXT, last_activity_at INTEGER, "
                "updated_at INTEGER, cwd TEXT, deleted_at INTEGER)"
            )
            # WorkBuddy leaves an executing session at its previous terminal
            # status, so the row a running task writes still reads 'completed'.
            database.execute(
                "INSERT INTO sessions VALUES (?, ?, ?, ?, ?, ?, NULL)",
                (
                    SESSION_ID,
                    "completed",
                    "long tool call",
                    int(time.time() * 1000),
                    int(time.time() * 1000),
                    SESSION_CWD,
                ),
            )
            database.commit()

        project_dir = self.workbuddy_home / "projects" / SESSION_CWD.replace("/", "-").lstrip("-")
        project_dir.mkdir(parents=True)
        self.transcript = project_dir / f"{SESSION_ID}.jsonl"
        self._append(
            {"type": "message", "role": "user", "timestamp": 1},
            {"type": "reasoning", "timestamp": 2},
            # Every assistant text block is written as a completed message, so
            # a mid-turn narration is only distinguishable from a finished turn
            # by the tool call that follows it.
            {"type": "message", "role": "assistant", "status": "completed", "timestamp": 3},
            {"type": "function_call", "name": "Bash", "timestamp": 4},
        )

        self.hosts_dir = self.workbuddy_home / "sessions"
        self.hosts_dir.mkdir()
        self.host_file = self.hosts_dir / "4242.json"

    def tearDown(self):
        self.temp_dir.cleanup()

    def _append(self, *records, newline=True):
        with self.transcript.open("a", encoding="utf-8") as handle:
            for record in records:
                handle.write(json.dumps(record) + ("\n" if newline else ""))

    def _age_transcript(self, seconds):
        timestamp = time.time() - seconds
        os.utime(self.transcript, (timestamp, timestamp))

    def _write_host(self):
        self.host_file.write_text(
            json.dumps(
                {
                    "pid": 4242,
                    "sessionId": SESSION_ID,
                    "lastHeartbeat": int(time.time() * 1000),
                    "kind": "interactive",
                }
            ),
            encoding="utf-8",
        )

    def _run_watcher_once(self):
        state_file = self.base / "herdr-workbuddy-watch.p_test.state"
        env = os.environ.copy()
        env.update(
            {
                "WORKBUDDY_HOME": str(self.workbuddy_home),
                "HERDR_WORKBUDDY_WATCH_ONCE": "1",
                "HERDR_ENV": "1",
                "HERDR_PANE_ID": "p_test",
                # The watcher deliberately tolerates a missing server. Its
                # persisted `reported` value records the exact state it tried
                # to send without requiring a test-only socket seam.
                "HERDR_SOCKET_PATH": str(self.base / "missing.sock"),
                "TMPDIR": str(self.base),
            }
        )
        process = subprocess.run(
            ["bash", str(WATCHER)],
            env=env,
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )
        self.assertEqual(
            process.returncode,
            0,
            f"stdout={process.stdout!r} stderr={process.stderr!r}",
        )
        return json.loads(state_file.read_text(encoding="utf-8")), process.stdout

    def _reported_state(self):
        return self._run_watcher_once()[0]["reported"].split(":", 1)[0]

    def _append_pending_question(self):
        self._append(
            {
                "type": "function_call_result",
                "name": "Bash",
                "status": "completed",
            },
            {
                "type": "message",
                "role": "assistant",
                "status": "completed",
            },
            {"type": "function_call", "name": "AskUserQuestion"},
        )

    def test_in_flight_turn_reports_working_while_its_host_lives(self):
        self._write_host()
        # A tool call can run for minutes without a transcript write.
        self._age_transcript(120)
        self.assertEqual(self._reported_state(), "working")

    def test_pending_question_reports_blocked_immediately_from_working(self):
        self._write_host()
        self.assertEqual(self._reported_state(), "working")

        self._append_pending_question()
        state, stdout = self._run_watcher_once()

        self.assertEqual(state["reported"].split(":", 1)[0], "blocked")
        self.assertIn("BLOCKED", strip_csi(stdout))
        self.assertIn("blocked", strip_csi(stdout))

    def test_answered_question_returns_to_working(self):
        self._write_host()
        self._append_pending_question()
        self.assertEqual(self._reported_state(), "blocked")

        self._append(
            {
                "type": "function_call_result",
                "name": "AskUserQuestion",
                "status": "completed",
            }
        )
        self.assertEqual(self._reported_state(), "working")

    def test_abandoned_question_does_not_stay_blocked(self):
        self._append_pending_question()
        self._age_transcript(600)
        self.assertEqual(self._reported_state(), "idle")

    def test_turn_end_needs_to_settle_before_it_confirms_completion(self):
        self._write_host()
        self._age_transcript(120)
        self.assertEqual(self._reported_state(), "working")

        self._append(
            {"type": "function_call_result", "name": "Bash", "status": "completed"},
            {"type": "message", "role": "assistant", "status": "completed"},
        )
        self.assertEqual(self._reported_state(), "working")

        self._age_transcript(10)
        self.assertEqual(self._reported_state(), "idle")

    def test_partial_trailing_record_does_not_end_the_turn(self):
        self._write_host()
        self._append(
            {"type": "message", "role": "assistant", "status": "completed"},
            newline=False,
        )
        self.assertEqual(self._reported_state(), "working")

    def test_turn_abandoned_without_a_host_stops_reporting_working(self):
        self._age_transcript(600)
        self.assertEqual(self._reported_state(), "idle")

    def test_missing_database_reports_idle(self):
        (self.workbuddy_home / "workbuddy.db").unlink()
        state, _stdout = self._run_watcher_once()
        self.assertEqual(state["reported"], "idle:WorkBuddy database not found")

    def test_dashboard_rows_share_one_border_width(self):
        # Regression: header right_a used to include ● / hold_hint while
        # right_v omitted them, so row() padding shoved the right border out.
        self._write_host()
        _state, stdout = self._run_watcher_once()
        lines = [line for line in strip_csi(stdout).splitlines() if line]
        self.assertGreaterEqual(len(lines), 3)
        widths = [display_width(line) for line in lines]
        self.assertTrue(
            all(width == DASHBOARD_WIDTH for width in widths),
            f"border widths={widths} lines={lines[:4]!r}",
        )


if __name__ == "__main__":
    unittest.main()
