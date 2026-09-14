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
WATCHER = REPO_ROOT / "src/integration/assets/codexapp/herdr-codexapp-watch.sh"
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


class CodexAppIntegrationAssetTests(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory(prefix="herdr-codexapp-test-")
        self.base = Path(self.temp_dir.name)
        self.codex_home = self.base / "codex-home"
        sqlite_dir = self.codex_home / "sqlite"
        sqlite_dir.mkdir(parents=True)
        with closing(sqlite3.connect(sqlite_dir / "codex-dev.db")) as database:
            database.execute(
                "CREATE TABLE local_thread_catalog ("
                "thread_id TEXT, display_title TEXT, source_kind TEXT, "
                "source_updated_at REAL)"
            )
            database.execute(
                "INSERT INTO local_thread_catalog VALUES (?, ?, ?, ?)",
                ("active-thread", "long tool call", "vscode", time.time()),
            )
            database.execute(
                "INSERT INTO local_thread_catalog VALUES (?, ?, ?, ?)",
                ("chatgpt-thread", "web conversation", "chatgpt", time.time() + 60),
            )
            database.commit()

        rollout_dir = self.codex_home / "sessions/2026/08/23"
        rollout_dir.mkdir(parents=True)
        self.rollout = rollout_dir / "rollout-active-thread.jsonl"
        with self.rollout.open("w", encoding="utf-8") as handle:
            handle.write(json.dumps({"payload": {"type": "task_started"}}) + "\n")
            # Push task_started beyond the watcher's old 64 KiB tail window.
            padding = json.dumps({"payload": {"type": "token_count", "value": "x" * 80}})
            for _ in range(900):
                handle.write(padding + "\n")
        self._make_rollout_stale()

    def tearDown(self):
        self.temp_dir.cleanup()

    def _make_rollout_stale(self):
        timestamp = time.time() - 120
        os.utime(self.rollout, (timestamp, timestamp))

    def _run_watcher_once(self):
        state_file = self.base / "herdr-codexapp-watch.p_test.state"
        env = os.environ.copy()
        env.update(
            {
                "CODEX_HOME": str(self.codex_home),
                "HERDR_CODEXAPP_WATCH_ONCE": "1",
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
            timeout=5,
            check=False,
        )
        self.assertEqual(
            process.returncode,
            0,
            f"stdout={process.stdout!r} stderr={process.stderr!r}",
        )
        return json.loads(state_file.read_text(encoding="utf-8")), process.stdout

    def test_stale_rollout_stays_working_until_terminal_lifecycle_event(self):
        state, _stdout = self._run_watcher_once()
        self.assertEqual(state["reported"].split(":", 1)[0], "working")

        terminal_event = json.dumps({"payload": {"type": "task_complete"}})
        midpoint = len(terminal_event) // 2
        with self.rollout.open("a", encoding="utf-8") as handle:
            handle.write(terminal_event[:midpoint])
        self._make_rollout_stale()

        # A poll racing a partial JSONL append must not consume and lose it.
        state, _stdout = self._run_watcher_once()
        self.assertEqual(state["reported"].split(":", 1)[0], "working")

        with self.rollout.open("a", encoding="utf-8") as handle:
            handle.write(terminal_event[midpoint:] + "\n")
        self._make_rollout_stale()

        state, _stdout = self._run_watcher_once()
        self.assertEqual(state["reported"].split(":", 1)[0], "idle")

    def test_dashboard_excludes_chatgpt_conversations(self):
        state, stdout = self._run_watcher_once()
        visible = strip_csi(stdout)
        self.assertEqual(state["reported"], "working:long tool call")
        self.assertIn("long tool call [vscode]", visible)
        self.assertIn("1 threads · 1 active", visible)
        self.assertNotIn("web conversation", visible)
        self.assertNotIn("[chatgpt]", visible)

    def test_dashboard_rows_share_one_border_width(self):
        # Same header right_v/right_a mismatch as the WorkBuddy bridge.
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
