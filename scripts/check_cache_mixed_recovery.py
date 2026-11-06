#!/usr/bin/env python3
"""Check mixed-write process recovery using an external durable intent/ACK ledger."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import select
import signal
import subprocess
import time


PHASES = ("ready", "before_commit", "committed", "published", "ack")


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def persist(stream, record):
    stream.write(json.dumps(record, sort_keys=True) + "\n")
    stream.flush()
    os.fsync(stream.fileno())


class Events:
    def __init__(self, process, timeout):
        self.process = process
        self.timeout = timeout
        self.buffer = bytearray()

    def next(self):
        deadline = time.monotonic() + self.timeout
        while b"\n" not in self.buffer:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not select.select([self.process.stdout], [], [], remaining)[0]:
                raise TimeoutError("worker event deadline exceeded")
            chunk = os.read(self.process.stdout.fileno(), 65536)
            if not chunk:
                raise RuntimeError("worker exited before the required event")
            self.buffer.extend(chunk)
            if len(self.buffer) > 1024 * 1024:
                raise RuntimeError("worker event exceeded size limit")
        line, _, rest = self.buffer.partition(b"\n")
        self.buffer = bytearray(rest)
        return json.loads(line)


def run_case(binary, binary_hash, output, phase, timeout, redis_address=None):
    fixture = output / phase
    fixture.mkdir()
    database_dir = fixture / "database"
    database_dir.mkdir()
    database = database_dir / "application.db"
    if digest(binary) != binary_hash:
        raise RuntimeError("executable changed during verification")
    with (fixture / "initialize.log").open("wb") as log:
        subprocess.run([str(binary), "init", str(database), "100"], check=True,
                       stdout=log, stderr=subprocess.STDOUT, timeout=timeout)
    with (fixture / "ledger.jsonl").open("x") as ledger:
        persist(ledger, {"kind": "intent", "transaction": 2, "document_id": 3,
                         "quantity": 1, "version": 2, "phase": phase})
        with (fixture / "worker.stderr.log").open("wb") as errors:
            command = [str(binary), "crash-worker", str(database), "3", "2"]
            if redis_address:
                command.append(redis_address)
            process = subprocess.Popen(command,
                                       stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=errors)
            events = Events(process, timeout)
            seen = []
            old_namespace = None
            try:
                for expected in PHASES:
                    event = events.next()
                    wanted = {"event": expected}
                    if expected == "ready" and redis_address:
                        old_namespace = event.get("namespace")
                        if not isinstance(old_namespace, str) or not re.fullmatch("[0-9a-f]{32}", old_namespace):
                            raise AssertionError("invalid old cache namespace")
                        wanted["namespace"] = old_namespace
                    if expected == "ack":
                        wanted["generation"] = 2
                    if event != wanted:
                        raise AssertionError(f"expected {wanted}, received {event}")
                    seen.append(event)
                    if expected == "ack":
                        persist(ledger, {"kind": "ack", "transaction": 2,
                                         "document_id": 3, "quantity": 1, "version": 2})
                    if expected == phase:
                        process.kill()
                        if process.wait(timeout=timeout) != -signal.SIGKILL:
                            raise AssertionError("worker was not terminated by SIGKILL")
                        break
                    process.stdin.write(b"CONTINUE\n")
                    process.stdin.flush()
            finally:
                if process.poll() is None:
                    process.kill()
                    process.wait(timeout=timeout)
                process.stdin.close()
                process.stdout.close()
                (fixture / "events.json").write_text(json.dumps(seen, indent=2) + "\n")

    records = [json.loads(line) for line in (fixture / "ledger.jsonl").read_text().splitlines()]
    if len(records) != (2 if phase == "ack" else 1):
        raise AssertionError("unexpected durable ledger records")
    expected_generation = 1 if phase == "before_commit" else 2
    if digest(binary) != binary_hash:
        raise RuntimeError("executable changed before fresh recovery")
    command = [str(binary), "check-recovery", str(database), "100", str(expected_generation)]
    if redis_address:
        command.extend([redis_address, old_namespace])
    recovered = subprocess.run(command, capture_output=True, text=True, timeout=timeout)
    (fixture / "recovery.stdout.log").write_text(recovered.stdout)
    (fixture / "recovery.stderr.log").write_text(recovered.stderr)
    recovered.check_returncode()
    result = json.loads(recovered.stdout)
    wanted = {"ok": True, "generation": expected_generation}
    if redis_address:
        new_namespace = result.get("namespace")
        if (not isinstance(new_namespace, str) or not re.fullmatch("[0-9a-f]{32}", new_namespace)
                or new_namespace == old_namespace):
            raise AssertionError("fresh recovery did not establish a new cache namespace")
        wanted.update(cache_restart=True, namespace=new_namespace)
    if result != wanted or type(result.get("generation")) is not int:
        raise AssertionError(f"fresh recovery did not attest success: {result}")
    return {"phase": phase, "expected_generation": expected_generation,
            "acknowledged": phase == "ack", "recovery": result,
            "outcome": "absent" if phase == "before_commit" else
                       "acknowledged-commit" if phase == "ack" else
                       "completed-commit-without-ack"}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--redis-address", help="dedicated test Redis endpoint; old keys are retained")
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    if not binary.is_file() or not os.access(binary, os.X_OK):
        parser.error("binary must be an executable regular file")
    if not 1 <= args.timeout <= 120:
        parser.error("timeout must be between 1 and 120 seconds")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    metadata = {"binary": str(binary), "sha256": digest(binary),
                "failure_model": "SIGKILL of application process; not power loss",
                "redis_address": args.redis_address,
                "cache_restart_verified": False, "timing_comparison": False}
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    results = [run_case(binary, metadata["sha256"], output, phase, args.timeout, args.redis_address)
               for phase in PHASES[1:]]
    metadata["cache_restart_verified"] = bool(args.redis_address)
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    (output / "summary.json").write_text(json.dumps(results, indent=2) + "\n")
    print(json.dumps({"status": "passed", "cases": len(results), **metadata}))


if __name__ == "__main__":
    main()
