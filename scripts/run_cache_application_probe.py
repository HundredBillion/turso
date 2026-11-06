#!/usr/bin/env python3
"""Run prebuilt immutable application probes with a disposable Redis instance."""
import argparse
import csv
import hashlib
import itertools
import json
import os
from pathlib import Path
import platform
import select
import subprocess
import time
import uuid


def command(args, **kwargs):
    return subprocess.run(args, check=True, text=True, **kwargs)


def redis_info(container):
    result = command(["docker", "exec", container, "redis-cli", "INFO"], capture_output=True)
    return dict(line.split(":", 1) for line in result.stdout.splitlines()
                if ":" in line and not line.startswith("#"))


def process_info(pid):
    root = Path("/proc") / str(pid)
    if not root.exists():
        return {"available": False}
    stat = (root / "stat").read_text().split(")", 1)[1].split()
    return {"user_ticks": int(stat[11]), "system_ticks": int(stat[12]),
            "rss_pages": int(stat[21]), "clock_ticks": os.sysconf("SC_CLK_TCK"),
            "page_bytes": os.sysconf("SC_PAGE_SIZE"),
            "io": (root / "io").read_text()}


def service_address(process, timeout=10):
    deadline = time.monotonic() + timeout
    response = bytearray()
    while b"\n" not in response:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not select.select([process.stdout], [], [], remaining)[0]:
            raise RuntimeError("database service startup timed out")
        chunk = os.read(process.stdout.fileno(), 256)
        if not chunk or len(response) + len(chunk) > 256:
            raise RuntimeError("invalid database service startup response")
        response.extend(chunk)
    return response.decode().strip()


def validate_trace(path, summary, requests, rate):
    with path.open() as source:
        rows = list(csv.DictReader(source))
    with summary.open() as source:
        aggregate = list(csv.DictReader(source))
    if len(rows) != requests or len(aggregate) != 1:
        raise RuntimeError("trace or summary request count mismatch")
    for row in rows:
        if row["kind"] not in ["0", "1", "2"] or row["cache_hit"] not in ["true", "false"]:
            raise RuntimeError("invalid trace category")
        if not int(row["arrival_latency_ns"]) >= int(row["late_start_ns"]) >= 0:
            raise RuntimeError("invalid trace timing")
    result = aggregate[0]
    hits = sum(row["cache_hit"] == "true" for row in rows)
    if int(result["requests"]) != requests or int(result["hits"]) != hits or int(result["misses"]) != requests - hits:
        raise RuntimeError("trace and aggregate counts differ")
    if int(result["rate"]) != rate or (rate and float(result["elapsed_s"]) + .00001 < (requests - 1) / rate):
        raise RuntimeError("offered-rate horizon was not completed")
    times = sorted(int(row["arrival_latency_ns"]) for row in rows)
    for percentile in [50, 95, 99]:
        expected = times[(requests * percentile + 99) // 100 - 1] / 1000
        if abs(float(result[f"p{percentile}_us"]) - expected) > .00051:
            raise RuntimeError("trace and aggregate latency differ")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", action="append", required=True, help="label=/absolute/path")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--requests", type=int, default=2000)
    parser.add_argument("--workers", default="1,8,32")
    parser.add_argument("--rates", default="0,2000")
    parser.add_argument("--deployments", default="embedded,shared")
    parser.add_argument("--cache", default="false,true")
    parser.add_argument("--warm", default="false,true")
    parser.add_argument("--hot", default="false,true")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--redis-image", default="redis:7-alpine")
    args = parser.parse_args()
    binaries = [(label, Path(path).resolve()) for label, path in
                (item.split("=", 1) for item in args.binary)]
    if len({label for label, _ in binaries}) != len(binaries):
        parser.error("binary labels must be unique")
    if any(not label.replace("-", "").replace("_", "").isalnum() for label, _ in binaries):
        parser.error("labels must contain letters, numbers, hyphens or underscores")
    workers = [int(value) for value in args.workers.split(",")]
    rates = [int(value) for value in args.rates.split(",")]
    deployments = args.deployments.split(",")
    if any(value not in ["embedded", "shared"] for value in deployments):
        parser.error("invalid deployment")
    axes = []
    for argument in [args.cache, args.warm, args.hot]:
        values = argument.split(",")
        if any(value not in ["false", "true"] for value in values):
            parser.error("cache, warm and hot accept false,true subsets")
        axes.append([value == "true" for value in values])
    if not 100 <= args.rows <= 10000 or not 1 <= args.requests <= 100000 or not 1 <= args.repeats <= 10:
        parser.error("workload exceeds probe limits")
    if any(not 1 <= value <= 32 for value in workers) or any(not 0 <= value <= 1000000 for value in rates):
        parser.error("invalid worker count or rate")
    args.output.mkdir(parents=True, exist_ok=False)
    output = args.output.resolve()
    metadata = {"platform": platform.platform(), "rows": args.rows,
                "requests": args.requests, "workers": workers, "rates": rates,
                "deployments": deployments, "cache_warm_hot_axes": axes,
                "repeats": args.repeats, "binaries": {
                    label: {"path": str(path), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
                    for label, path in binaries}}
    name = "turso-json-cache-probe-" + uuid.uuid4().hex[:12]
    service = None
    started = False
    try:
        command(["docker", "run", "--rm", "-d", "--name", name,
                 "-p", "127.0.0.1::6379", "--memory", "256m", args.redis_image,
                 "redis-server", "--save", "", "--appendonly", "no",
                 "--maxmemory", "128mb", "--maxmemory-policy", "allkeys-lru"], capture_output=True)
        started = True
        address = command(["docker", "port", name, "6379"], capture_output=True).stdout.strip()
        metadata["redis_image"] = json.loads(command(["docker", "image", "inspect", args.redis_image], capture_output=True).stdout)[0]["Id"]
        for _ in range(100):
            ping = subprocess.run(["docker", "exec", name, "redis-cli", "PING"], capture_output=True, text=True)
            if ping.returncode == 0 and ping.stdout.strip() == "PONG":
                break
            time.sleep(.1)
        else:
            raise RuntimeError("Redis did not become ready")
        metadata["redis_initial_info"] = redis_info(name)
        (output / "metadata.json").write_text(json.dumps(metadata, indent=2))
        for label, binary in binaries:
            database = output / f"{label}.db"
            command([str(binary), "init", str(database), str(args.rows)])
            prefix = f"negative-{label}"
            command(["docker", "exec", name, "redis-cli", "SET", f"{prefix}:0 0 0 0", "wrong-response"], capture_output=True)
            environment = os.environ | {"APP_DATABASE": "embedded", "APP_REDIS": address,
                "APP_PREFIX": prefix, "APP_REQUESTS": "1", "APP_WORKERS": "1",
                "APP_RATE": "0", "APP_WARM": "false", "APP_HOT": "false"}
            mismatch = subprocess.run([str(binary), "run", str(database), str(args.rows)],
                env=environment, capture_output=True, text=True, timeout=10)
            if mismatch.returncode == 0 or "oracle mismatch" not in mismatch.stderr:
                raise RuntimeError(f"{label}: corrupted cache value was not rejected")
            environment |= {"APP_REDIS": "", "APP_WORKERS": "8"}
            failure = subprocess.run([str(binary), "run", str(output / "missing-parent" / "db"), str(args.rows)],
                env=environment, capture_output=True, text=True, timeout=10)
            if failure.returncode == 0 or "worker preparation failed" not in failure.stderr:
                raise RuntimeError(f"{label}: worker initialization failure did not propagate")
            (output / f"{label}-negative-controls.log").write_text(mismatch.stderr + failure.stderr)
        variants = list(itertools.product(deployments, *axes, workers, rates))
        for repeat in range(args.repeats):
            ordered = binaries[repeat % len(binaries):] + binaries[:repeat % len(binaries)]
            for label, binary in ordered:
                database = output / f"{label}.db"
                with (output / f"service-{repeat}-{label}.log").open("w") as service_log:
                    service = subprocess.Popen([str(binary), "serve", str(database)], stdout=subprocess.PIPE,
                                               stderr=service_log, text=True)
                    endpoint = service_address(service)
                    if not endpoint:
                        raise RuntimeError("database service failed to bind")
                    trial_variants = variants if repeat % 2 == 0 else list(reversed(variants))
                    for number, (deployment, cache, warm, hot, count, rate) in enumerate(trial_variants):
                        stem = f"{repeat}-{label}-{number:03d}-{deployment}-cache{int(cache)}-warm{int(warm)}-hot{int(hot)}-w{count}-r{rate}"
                        # This Redis container is created exclusively for this run.
                        command(["docker", "exec", name, "redis-cli", "FLUSHDB"], capture_output=True)
                        before = {"redis": redis_info(name), "service": process_info(service.pid)}
                        environment = os.environ | {"APP_DATABASE": "embedded" if deployment == "embedded" else endpoint,
                            "APP_REDIS": address if cache else "", "APP_PREFIX": stem,
                            "APP_WORKERS": str(count), "APP_REQUESTS": str(args.requests), "APP_RATE": str(rate),
                            "APP_WARM": str(warm).lower(), "APP_HOT": str(hot).lower(),
                            "APP_TRACE": str(output / f"{stem}.trace.csv")}
                        timing_flag = "-v" if platform.system() == "Linux" else "-l"
                        with (output / f"{stem}.csv").open("w") as stdout, (output / f"{stem}.log").open("w") as stderr:
                            command(["/usr/bin/time", timing_flag, str(binary), "run", str(database), str(args.rows)],
                                    env=environment, stdout=stdout, stderr=stderr, timeout=300)
                        validate_trace(output / f"{stem}.trace.csv", output / f"{stem}.csv", args.requests, rate)
                        after = {"redis": redis_info(name), "service": process_info(service.pid)}
                        (output / f"{stem}.resources.json").write_text(json.dumps({"before": before, "after": after}, indent=2))
                        print(stem, flush=True)
                    service.terminate()
                    service.wait(timeout=10)
                    service = None
    finally:
        try:
            if service is not None:
                service.terminate()
                try:
                    service.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    service.kill()
                    service.wait(timeout=10)
        finally:
            if started:
                command(["docker", "stop", name], capture_output=True, timeout=30)


if __name__ == "__main__":
    main()
