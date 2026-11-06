import json
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import unittest

sys.path.insert(0, str(Path(__file__).parent))
from run import Worker, verify_recovery


class SupervisorTests(unittest.TestCase):
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
