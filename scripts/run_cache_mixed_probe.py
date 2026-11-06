#!/usr/bin/env python3
"""Screen prebuilt mixed-write probes with fresh databases and a private Redis."""
import argparse
import csv
import hashlib
import itertools
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import time
import uuid

from run_cache_application_probe import command, process_info, redis_info, service_address


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_trace(trace, summary, log, requests, rate, write_every, checkpoint_every):
    with trace.open() as source:
        rows = list(csv.DictReader(source))
    with summary.open() as source:
        results = list(csv.DictReader(source))
    if len(rows) != requests or len(results) != 1:
        raise RuntimeError("mixed trace/summary count mismatch")
    result = results[0]
    writes = 0
    hits = 0
    for sequence, row in enumerate(rows):
        write = sequence % write_every == 0
        if int(row["sequence"]) != sequence or row["kind"] not in ["0", "1", "2", "3"]:
            raise RuntimeError("invalid mixed sequence or kind")
        if (row["kind"] == "3") != write or row["cache_hit"] not in ["true", "false"]:
            raise RuntimeError("write trace schedule mismatch")
        if write and row["cache_hit"] != "false":
            raise RuntimeError("write cannot be a cache hit")
        latency, late, gate, service, model = [int(row[key]) for key in
            ["arrival_latency_ns", "late_start_ns", "gate_wait_ns", "service_ns", "model_update_ns"]]
        if min(latency, late, gate, service, model) < 0 or latency != late + service:
            raise RuntimeError("mixed arrival/service arithmetic mismatch")
        if gate > service or model > service or (not write and model != 0):
            raise RuntimeError("invalid gate/model accounting")
        finished = int(row["finished_ns"])
        if finished < latency or (rate and finished != sequence * 1_000_000_000 // rate + latency):
            raise RuntimeError("request deadline/completion arithmetic mismatch")
        writes += write
        hits += row["cache_hit"] == "true"
    expected = {"requests": requests, "writes": writes, "hits": hits,
                "misses": requests - writes - hits, "write_every": write_every,
                "rate": rate, "checkpoints": requests // checkpoint_every}
    if any(int(result[key]) != value for key, value in expected.items()):
        raise RuntimeError("mixed summary accounting mismatch")
    elapsed = float(result["elapsed_s"])
    if elapsed <= 0 or float(result["drained_s"]) < elapsed:
        raise RuntimeError("invalid mixed elapsed time")
    if rate and elapsed + .00001 < (requests - 1) / rate:
        raise RuntimeError("offered-rate horizon not completed")
    if abs(float(result["throughput"]) - requests / elapsed) > max(.1, requests / elapsed * .0001):
        raise RuntimeError("mixed throughput arithmetic mismatch")
    for key, prefix, percentiles in [("arrival_latency_ns", "p", [50, 95, 99]),
                                    ("late_start_ns", "late_p", [99]),
                                    ("gate_wait_ns", "gate_p", [99])]:
        values = sorted(int(row[key]) for row in rows)
        for percentile in percentiles:
            value = values[(requests * percentile + 99) // 100 - 1] / 1000
            if abs(float(result[f"{prefix}{percentile}_us"]) - value) > .00051:
                raise RuntimeError("mixed trace percentiles mismatch")
    checkpoints = re.findall(r"checkpoint completed_requests=(\d+) elapsed_ns=(\d+) busy:log:checkpointed=([0-9:-]+) start_ns=(\d+) end_ns=(\d+) observed_completed=(\d+)", log.read_text())
    if [int(row[0]) for row in checkpoints] != list(range(checkpoint_every, requests + 1, checkpoint_every)):
        raise RuntimeError("checkpoint cadence/count mismatch")
    if int(result["request_end_ns"]) != max(int(row["finished_ns"]) for row in rows):
        raise RuntimeError("request completion horizon mismatch")
    for threshold, elapsed_ns, triple, start_ns, end_ns, observed in checkpoints:
        if int(end_ns) - int(start_ns) != int(elapsed_ns) or not int(threshold) <= int(observed) <= requests:
            raise RuntimeError("invalid checkpoint timing/backlog evidence")
        if int(end_ns) > float(result["drained_s"]) * 1e9 + 501:
            raise RuntimeError("checkpoint exceeds drained horizon")
        if len(triple.split(":")) != 3:
            raise RuntimeError("invalid checkpoint result")
    return result


def wait_redis(container, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        result = subprocess.run(["docker", "exec", container, "redis-cli", "PING"], capture_output=True, text=True, timeout=2)
        if result.returncode == 0 and result.stdout.strip() == "PONG":
            return
        time.sleep(.05)
    raise RuntimeError("Redis readiness deadline exceeded")


def stop_service(service):
    if service and service.poll() is None:
        service.terminate()
        try:
            service.wait(timeout=10)
        except subprocess.TimeoutExpired:
            service.kill()
            service.wait(timeout=10)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", action="append", required=True, help="label=/absolute/path")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--requests", type=int, default=4000)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--rate", type=int, default=2000)
    parser.add_argument("--write-every", default="100,20")
    parser.add_argument("--checkpoint-every", type=int, default=200)
    parser.add_argument("--deployments", default="embedded,shared")
    parser.add_argument("--cache", default="false,true")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--redis-image", default="redis:7-alpine")
    args = parser.parse_args()
    binaries = [(label, Path(path).resolve(strict=True)) for label, path in
                (item.split("=", 1) for item in args.binary)]
    if len({label for label, _ in binaries}) != len(binaries) or any(not re.fullmatch(r"[A-Za-z0-9_-]+", label) for label, _ in binaries):
        parser.error("binary labels must be unique and contain only letters/numbers/hyphens/underscores")
    writes = [int(value) for value in args.write_every.split(",")]
    deployments = args.deployments.split(",")
    cache_values = args.cache.split(",")
    if not 100 <= args.rows <= 10000 or not 1 <= args.requests <= 100000 or not 1 <= args.workers <= 32:
        parser.error("invalid workload size")
    if args.rate < 0 or args.rate > 1000000 or min(writes) < 2 or args.checkpoint_every < 1 or not 1 <= args.repeats <= 10:
        parser.error("invalid scheduling configuration")
    if not set(deployments) <= {"embedded", "shared"} or not set(cache_values) <= {"false", "true"}:
        parser.error("invalid deployment/cache axes")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    hashes = {label: digest(binary) for label, binary in binaries}
    metadata = {"platform": platform.platform(), "rows": args.rows, "requests": args.requests,
                "workers": args.workers, "rate": args.rate, "write_every": writes,
                "checkpoint_every": args.checkpoint_every, "repeats": args.repeats,
                "warm": True, "hot": True, "deployments": deployments, "cache": cache_values,
                "binaries": {label: {"path": str(binary), "sha256": hashes[label]} for label, binary in binaries}}
    container = "turso-mixed-probe-" + uuid.uuid4().hex[:12]
    service = None
    try:
        command(["docker", "run", "-d", "--rm", "--name", container, "--memory", "256m",
                 "-p", "127.0.0.1::6379", args.redis_image, "redis-server", "--save", "",
                 "--appendonly", "no", "--maxmemory", "64mb", "--maxmemory-policy", "allkeys-lru"], capture_output=True)
        address = command(["docker", "port", container, "6379/tcp"], capture_output=True).stdout.strip()
        metadata["redis_image"] = command(["docker", "inspect", "--format", "{{.Image}}", container], capture_output=True).stdout.strip()
        wait_redis(container)
        metadata["redis_initial_info"] = redis_info(container)
        (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
        for label, binary in binaries:
            with (output / f"{label}-recovery.log").open("w") as recovery_log:
                command(["python3", str(Path(__file__).with_name("check_cache_mixed_recovery.py")),
                         "--binary", str(binary), "--output", str(output / f"{label}-recovery"), "--redis-address", address],
                        stdout=recovery_log, stderr=subprocess.STDOUT)
        variants = list(itertools.product(deployments, cache_values, writes))
        for repeat in range(args.repeats):
            order = binaries[repeat % len(binaries):] + binaries[:repeat % len(binaries)]
            cases = variants if repeat % 2 == 0 else list(reversed(variants))
            for case, (deployment, cache, write_every) in enumerate(cases):
                for label, binary in order:
                    if digest(binary) != hashes[label]:
                        raise RuntimeError("binary changed after metadata capture")
                    stem = f"{repeat}-{label}-{case:03d}-{deployment}-cache{int(cache=='true')}-write{write_every}"
                    database = output / f"{stem}.db"
                    command([str(binary), "init", str(database), str(args.rows)], timeout=60)
                    command(["docker", "exec", container, "redis-cli", "FLUSHALL"], capture_output=True)
                    before = redis_info(container)
                    before_bytes = {suffix: Path(str(database) + suffix).stat().st_size for suffix in ["", "-wal"] if Path(str(database) + suffix).exists()}
                    with (output / f"{stem}.service.log").open("w") as service_log:
                        try:
                            endpoint = "embedded"
                            if deployment == "shared":
                                service = subprocess.Popen([str(binary), "serve", str(database)], stdout=subprocess.PIPE, stderr=service_log)
                                endpoint = service_address(service)
                            service_before = process_info(service.pid) if service else None
                            env = os.environ | {"APP_DATABASE": endpoint, "APP_REDIS": address if cache == "true" else "",
                                "APP_WORKERS": str(args.workers), "APP_REQUESTS": str(args.requests), "APP_RATE": str(args.rate),
                                "APP_WRITE_EVERY": str(write_every), "APP_CHECKPOINT_EVERY": str(args.checkpoint_every),
                                "APP_WARM": "true", "APP_HOT": "true", "APP_TRACE": str(output / f"{stem}.trace.csv")}
                            with (output / f"{stem}.csv").open("w") as summary, (output / f"{stem}.log").open("w") as log:
                                command(["/usr/bin/time", "-l" if platform.system() == "Darwin" else "-v", str(binary), "run", str(database), str(args.rows)],
                                        stdout=summary, stderr=log, env=env, timeout=max(120, args.requests / max(args.rate, 1) * 5 + 60))
                            validate_trace(output / f"{stem}.trace.csv", output / f"{stem}.csv", output / f"{stem}.log",
                                           args.requests, args.rate, write_every, args.checkpoint_every)
                            resources = {"redis_before": before, "redis_after": redis_info(container),
                                         "service_before": service_before, "service_after": process_info(service.pid) if service else None,
                                         "database_bytes_before": before_bytes,
                                         "database_bytes_after": {suffix: Path(str(database) + suffix).stat().st_size for suffix in ["", "-wal"] if Path(str(database) + suffix).exists()}}
                            (output / f"{stem}.resources.json").write_text(json.dumps(resources, indent=2) + "\n")
                        finally:
                            stop_service(service)
                            if service and service.stdout:
                                service.stdout.close()
                            service = None
                    for suffix in ["", "-wal", "-shm"]:
                        Path(str(database) + suffix).unlink(missing_ok=True)
                    print(stem, flush=True)
    finally:
        try:
            stop_service(service)
        finally:
            subprocess.run(["docker", "rm", "-f", container], capture_output=True)


if __name__ == "__main__":
    main()
