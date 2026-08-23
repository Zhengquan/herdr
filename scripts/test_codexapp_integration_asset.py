from contextlib import closing
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import time
import unittest


REPO_ROOT = Path(__file__).resolve().parent.parent
WATCHER = REPO_ROOT / "src/integration/assets/codexapp/herdr-codexapp-watch.sh"


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
        return json.loads(state_file.read_text(encoding="utf-8"))

    def test_stale_rollout_stays_working_until_terminal_lifecycle_event(self):
        state = self._run_watcher_once()
        self.assertEqual(state["reported"].split(":", 1)[0], "working")

        terminal_event = json.dumps({"payload": {"type": "task_complete"}})
        midpoint = len(terminal_event) // 2
        with self.rollout.open("a", encoding="utf-8") as handle:
            handle.write(terminal_event[:midpoint])
        self._make_rollout_stale()

        # A poll racing a partial JSONL append must not consume and lose it.
        state = self._run_watcher_once()
        self.assertEqual(state["reported"].split(":", 1)[0], "working")

        with self.rollout.open("a", encoding="utf-8") as handle:
            handle.write(terminal_event[midpoint:] + "\n")
        self._make_rollout_stale()

        state = self._run_watcher_once()
        self.assertEqual(state["reported"].split(":", 1)[0], "idle")


if __name__ == "__main__":
    unittest.main()
