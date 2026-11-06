import importlib.util
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


spec = importlib.util.spec_from_file_location(
    "recovery", Path(__file__).parents[1] / "check_cache_mixed_recovery.py")
recovery = importlib.util.module_from_spec(spec)
spec.loader.exec_module(recovery)


class RecoveryControllerTests(unittest.TestCase):
    def worker(self, script):
        process = subprocess.Popen([sys.executable, "-c", script], stdout=subprocess.PIPE)
        def cleanup():
            if process.poll() is None:
                process.kill()
            process.wait(timeout=2)
            process.stdout.close()
        self.addCleanup(cleanup)
        return process

    def test_silent_worker_is_bounded(self):
        events = recovery.Events(self.worker("import time; time.sleep(10)"), .1)
        with self.assertRaises(TimeoutError):
            events.next()

    def test_partial_event_then_exit_is_failure(self):
        events = recovery.Events(self.worker("import sys; sys.stdout.write('{')"), 1)
        with self.assertRaises(RuntimeError):
            events.next()

    def fake_binary(self, directory, wrong_ack=False, wrong_recovery=False):
        binary = directory / "fake-engine"
        binary.write_text(f'''#!{sys.executable}
import json,sys
if sys.argv[1] == 'init':
    sys.exit(0)
if sys.argv[1] == 'check-recovery':
    result={{"ok":True,"generation":int(sys.argv[4])+{int(wrong_recovery)}}}
    if len(sys.argv)>5: result.update(cache_restart=True,namespace='a'*32)
    print(json.dumps(result))
    sys.exit(0)
for phase in ('ready','before_commit','committed','published','ack'):
    event={{"event":phase}}
    if phase=='ready' and len(sys.argv)>5: event['namespace']='a'*32
    if phase=='ack': event['generation']={3 if wrong_ack else 2}
    print(json.dumps(event),flush=True)
    assert sys.stdin.readline()=='CONTINUE\\n'
''')
        binary.chmod(0o700)
        return binary

    def test_wrong_acknowledgment_cannot_enter_ledger(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = self.fake_binary(root, wrong_ack=True)
            with self.assertRaisesRegex(AssertionError, "received"):
                recovery.run_case(binary, recovery.digest(binary), root, "ack", 2)
            ledger = (root / "ack/ledger.jsonl").read_text().splitlines()
            self.assertEqual(len(ledger), 1)

    def test_success_with_wrong_recovered_generation_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = self.fake_binary(root, wrong_recovery=True)
            with self.assertRaisesRegex(AssertionError, "fresh recovery"):
                recovery.run_case(binary, recovery.digest(binary), root, "committed", 2)

    def test_cache_restart_cannot_reuse_old_namespace(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = self.fake_binary(root)
            with self.assertRaisesRegex(AssertionError, "new cache namespace"):
                recovery.run_case(binary, recovery.digest(binary), root, "committed", 2,
                                  "127.0.0.1:1")


if __name__ == "__main__":
    unittest.main()
