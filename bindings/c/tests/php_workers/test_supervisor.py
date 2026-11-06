import json
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest

sys.path.insert(0, str(Path(__file__).parent))
from run import Suite, Worker, snapshot_tree, verify_recovery


class SupervisorTests(unittest.TestCase):
    def test_readonly_snapshot_covers_identity_durable_files(self):
        with tempfile.TemporaryDirectory() as directory:
            suite = Suite.__new__(Suite)
            suite.output = Path(directory)
            suite.database = suite.output / "marked.db"
            suite.database.write_bytes(b"marker")
            suite.mode = "shared"
            identity = suite.database.stat()
            sidecars = suite.output / f".turso-mvcc-{identity.st_dev:016x}-{identity.st_ino:016x}"
            sidecars.mkdir()
            for name in ["manifest", "logical", "physical-wal", "authority", "physical-tshm"]:
                (sidecars / name).write_bytes(b"initial")
            before = suite.durable_bytes()
            for name in ["manifest", "logical", "physical-wal"]:
                (sidecars / name).write_bytes(b"changed")
                self.assertNotEqual(suite.durable_bytes(), before)
                (sidecars / name).write_bytes(b"initial")
            for name in ["authority", "physical-tshm"]:
                (sidecars / name).write_bytes(b"participant changed")
            self.assertEqual(suite.durable_bytes(), before)
            (sidecars / "logical").unlink()
            with self.assertRaisesRegex(AssertionError, "durable files are missing"):
                suite.durable_bytes()

    def test_rejected_upgrade_snapshot_detects_identity_directory_creation(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = Path(directory)
            (fixture / "old.db").write_bytes(b"old database")
            before = snapshot_tree(fixture)
            identity = fixture / ".turso-mvcc-new-identity"
            identity.mkdir()
            self.assertNotEqual(snapshot_tree(fixture), before)
            (identity / "manifest").write_bytes(b"unexpected upgrade")
            self.assertNotEqual(snapshot_tree(fixture), before)

    def worker(self, script):
        worker = Worker.__new__(Worker)
        worker.name = "protocol-negative-control"
        worker.buffer = bytearray()
        worker.pending = 1
        worker.suite = SimpleNamespace(args=SimpleNamespace(deadline=.2), record=lambda *a, **k: None)
        worker.process = subprocess.Popen([sys.executable, "-c", script], stdout=subprocess.PIPE)
        def cleanup():
            if worker.process.poll() is None:
                worker.process.kill()
            worker.process.wait(timeout=2)
            worker.process.stdout.close()
        self.addCleanup(cleanup)
        return worker

    def test_deadline_rejects_silent_worker(self):
        worker = self.worker("import time; time.sleep(5)")
        with self.assertRaises(TimeoutError):
            worker.receive()

    def test_stale_response_cannot_satisfy_new_command(self):
        worker = self.worker("print(" + repr(json.dumps({"id": 99, "ok": True})) + ")")
        with self.assertRaises(AssertionError):
            worker.result()

    def test_partial_response_and_exit_is_failure(self):
        worker = self.worker("import sys; sys.stdout.write('{'); sys.stdout.flush()")
        with self.assertRaisesRegex(RuntimeError, "exited before response"):
            worker.receive()

    def test_acknowledged_and_unreported_transactions_are_atomic(self):
        first = [["ack", 0, "a"], ["ack", 1, "b"]]
        second = [["unknown", 0, "c"], ["unknown", 1, "d"]]
        ledger = [{"kind": "intent", "transaction": "ack", "rows": first},
                  {"kind": "ack", "transaction": "ack", "rows": first},
                  {"kind": "intent", "transaction": "unknown", "rows": second}]
        outcomes = {"ack": "complete", "unknown": "complete-or-absent"}
        self.assertEqual(verify_recovery(ledger, outcomes, first), {"ack"})
        self.assertEqual(verify_recovery(ledger, outcomes, first + second), {"ack"})
        for corrupt in [[], first[:1], first + second[:1], first + [["unrecorded", 0, "x"]]]:
            with self.assertRaises(AssertionError):
                verify_recovery(ledger, outcomes, corrupt)
        malformed = [dict(row) for row in ledger]
        malformed[1]["rows"] = [["ack", 0, "wrong"]]
        with self.assertRaisesRegex(AssertionError, "payload differs"):
            verify_recovery(malformed, outcomes, first)


if __name__ == "__main__":
    unittest.main()
