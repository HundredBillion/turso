import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).parents[1]))
spec = importlib.util.spec_from_file_location("mixed", Path(__file__).parents[1] / "run_cache_mixed_probe.py")
mixed = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mixed)


class MixedTraceTests(unittest.TestCase):
    def fixture(self, directory):
        root = Path(directory)
        trace, summary, log = (root / name for name in ["trace.csv", "summary.csv", "run.log"])
        trace.write_text("sequence,kind,arrival_latency_ns,late_start_ns,gate_wait_ns,service_ns,model_update_ns,cache_hit,finished_ns\n"
                         "0,3,10,1,2,9,3,false,10\n1,1,20,1,2,19,0,true,250000020\n2,3,30,1,2,29,3,false,500000030\n3,2,40,1,2,39,0,false,750000040\n")
        summary.write_text("requests,writes,hits,misses,write_every,rate,checkpoints,elapsed_s,drained_s,throughput,p50_us,p95_us,p99_us,late_p99_us,gate_p99_us,request_end_ns\n"
                           "4,2,1,1,2,4,2,1.0,1.1,4.0,0.020,0.040,0.040,0.001,0.002,750000040\n")
        log.write_text("checkpoint completed_requests=2 elapsed_ns=100 busy:log:checkpointed=0:3:3 start_ns=20 end_ns=120 observed_completed=2\n"
                       "checkpoint completed_requests=4 elapsed_ns=200 busy:log:checkpointed=0:6:6 start_ns=130 end_ns=330 observed_completed=4\n")
        return trace, summary, log

    def test_accounting_accepts_complete_trace(self):
        with tempfile.TemporaryDirectory() as directory:
            mixed.validate_trace(*self.fixture(directory), 4, 4, 2, 2)

    def test_changed_timing_or_write_oracle_is_rejected(self):
        for old, new in [("0,3,10,1,2,9,3,false", "0,3,10,1,2,9,3,true"),
                         ("1,1,20,1,2,19,0,true", "1,1,20,1,2,18,0,true"),
                         ("2,3,30,1,2,29,3,false", "3,3,30,1,2,29,3,false")]:
            with self.subTest(change=new), tempfile.TemporaryDirectory() as directory:
                paths = self.fixture(directory)
                paths[0].write_text(paths[0].read_text().replace(old, new))
                with self.assertRaises(RuntimeError):
                    mixed.validate_trace(*paths, 4, 4, 2, 2)

    def test_invalid_checkpoint_timing_or_backlog_is_rejected(self):
        for old, new in [("end_ns=120", "end_ns=119"), ("observed_completed=2", "observed_completed=5")]:
            with self.subTest(change=new), tempfile.TemporaryDirectory() as directory:
                paths = self.fixture(directory)
                paths[2].write_text(paths[2].read_text().replace(old, new))
                with self.assertRaisesRegex(RuntimeError, "checkpoint"):
                    mixed.validate_trace(*paths, 4, 4, 2, 2)

    def test_missing_checkpoint_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            paths = self.fixture(directory)
            paths[2].write_text(paths[2].read_text().splitlines()[0] + "\n")
            with self.assertRaisesRegex(RuntimeError, "checkpoint"):
                mixed.validate_trace(*paths, 4, 4, 2, 2)


if __name__ == "__main__":
    unittest.main()
