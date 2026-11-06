import argparse
import csv
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import statistics
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser(description="Compare identical JSON benchmarks across three engine revisions")
    parser.add_argument("--upstream", default="2709799dad2ebb0f1749cfa62fa28514f568da28")
    parser.add_argument("--fork", default="78a77aac11310d4d928bff623195d34b33242dd5")
    parser.add_argument("--candidate", default="HEAD")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--toolchain", default="1.88.0")
    parser.add_argument("--jobs", type=int, default=2)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--warmup", type=float, default=1.0)
    parser.add_argument("--measurement", type=float, default=2.0)
    parser.add_argument("--samples", type=int, default=50)
    parser.add_argument("--summarize-only", action="store_true")
    args = parser.parse_args()
    if args.repeats < 1 or args.jobs < 1 or args.samples < 10 or args.warmup <= 0 or args.measurement <= 0:
        parser.error("positive repetitions, jobs and durations, and at least ten samples are required")
    output = args.output.resolve()
    if args.summarize_only:
        summarize(output, args.repeats)
        return
    if output.exists() and any(output.iterdir()):
        parser.error("output directory must be empty; choose a new directory for each comparison")
    output.mkdir(parents=True, exist_ok=True)
    repo = Path(command(["git", "rev-parse", "--show-toplevel"])).resolve()
    revisions = {name: command(["git", "rev-parse", "--verify", "--end-of-options", f"{ref}^{{commit}}"], repo)
                 for name, ref in [("upstream", args.upstream), ("fork", args.fork), ("candidate", args.candidate)]}
    environment = dict(os.environ, RUSTUP_TOOLCHAIN=args.toolchain, CARGO_INCREMENTAL="0",
                       CARGO_PROFILE_LIB_RELEASE_OPT_LEVEL="3", CARGO_PROFILE_LIB_RELEASE_DEBUG="0",
                       CARGO_PROFILE_LIB_RELEASE_CODEGEN_UNITS="16", CARGO_PROFILE_LIB_RELEASE_LTO="false",
                       CARGO_PROFILE_LIB_RELEASE_PANIC="unwind")
    environment["RUSTC"] = command(["rustup", "which", "--toolchain", args.toolchain, "rustc"], repo)
    metadata = {
        "revisions": revisions,
        "host": platform.platform(),
        "cpu_count": os.cpu_count(),
        "rustc": command([environment["RUSTC"], "-Vv"], repo, environment),
        "profile": "lib-release; benchmark panic=unwind; codegen-units=16; lto=false",
        "parameters": {key: value for key, value in vars(args).items() if key != "output"},
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "benchmark_binaries": {},
    }
    if Path("/proc/cpuinfo").exists():
        (output / "cpuinfo.txt").write_text(Path("/proc/cpuinfo").read_text())
    metadata_path = output / "metadata.json"
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")
    with tempfile.TemporaryDirectory(prefix="turso-json-comparison-") as directory:
        temporary = Path(directory)
        environment["CARGO_TARGET_DIR"] = str(temporary / "target")
        worktrees = {}
        try:
            for variant, revision in revisions.items():
                path = temporary / variant
                subprocess.run(["git", "worktree", "add", "--detach", str(path), revision], cwd=repo, check=True)
                worktrees[variant] = path
            fixtures = ["benches/Cargo.toml", "benches/json_benchmark.rs", "benches/json_extract_allocations.rs"]
            metadata["fixture_sha256"] = {name: digest(worktrees["candidate"] / name) for name in fixtures}
            lock = (worktrees["candidate"] / "Cargo.lock").read_bytes()
            for variant, path in worktrees.items():
                if (path / "Cargo.lock").read_bytes() != lock:
                    raise RuntimeError(f"{variant} has a different Cargo.lock; dependency differences need a separate comparison")
                if variant != "candidate":
                    for name in fixtures:
                        shutil.copyfile(worktrees["candidate"] / name, path / name)
                metadata["benchmark_binaries"][variant] = build(path, output, variant, environment, args.jobs, args.toolchain)
                listing = command([str(output / "binaries" / variant / "json_benchmark"), "--list"],
                                  path / "benches", dict(environment, DISABLE_RUSQLITE_BENCHMARK="1"))
                names = sorted(line.removesuffix(": benchmark") for line in listing.splitlines() if line.endswith(": benchmark"))
                if not names or ("benchmarks" in metadata and names != metadata["benchmarks"]):
                    raise RuntimeError(f"benchmark cases differ for {variant}")
                metadata["benchmarks"] = names
                metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")
            orders = [("fork", "candidate", "upstream"), ("candidate", "upstream", "fork"), ("upstream", "fork", "candidate")]
            runtime_environment = dict(environment, DISABLE_RUSQLITE_BENCHMARK="1", CRITERION_HOME=str(output / "alternating"))
            for repeat in range(1, args.repeats + 1):
                for variant in orders[(repeat - 1) % len(orders)]:
                    label = f"{variant}-{repeat}"
                    print(f"{time.strftime('%H:%M:%S')} measuring {label}", flush=True)
                    with (output / f"{label}.log").open("w") as log:
                        subprocess.run([
                            str(output / "binaries" / variant / "json_benchmark"), "--bench",
                            "--warm-up-time", str(args.warmup), "--measurement-time", str(args.measurement),
                            "--sample-size", str(args.samples), "--nresamples", "10000", "--noplot",
                            "--save-baseline", label,
                        ], cwd=worktrees[variant] / "benches", env=runtime_environment,
                            stdout=log, stderr=subprocess.STDOUT, check=True)
            for variant, path in worktrees.items():
                with (output / f"allocations-{variant}.log").open("w") as log:
                    subprocess.run([
                        str(output / "binaries" / variant / "json_extract_allocations"), "--bench",
                        "--sample-count", "100", "--sample-size", "100",
                    ], cwd=path / "benches", env=environment, stdout=log, stderr=subprocess.STDOUT, check=True)
        finally:
            for path in reversed(list(worktrees.values())):
                subprocess.run(["git", "worktree", "remove", "--force", str(path)], cwd=repo, check=True)
    metadata["finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")
    summarize(output, args.repeats)


def command(arguments, cwd=None, env=None):
    return subprocess.check_output(arguments, cwd=cwd, env=env, text=True).strip()


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def build(path, output, variant, environment, jobs, toolchain):
    print(f"building {variant}", flush=True)
    with (output / f"build-{variant}.jsonl").open("w") as events, (output / f"build-{variant}.log").open("w") as log:
        subprocess.run([
            "rustup", "run", toolchain, "cargo", "bench", "--locked", "--profile", "lib-release", "-p", "turso_benches",
            "--bench", "json_benchmark", "--bench", "json_extract_allocations", "--no-run",
            "--message-format=json", "-j", str(jobs),
        ], cwd=path, env=environment, stdout=events, stderr=log, check=True)
    binaries = {}
    destination = output / "binaries" / variant
    destination.mkdir(parents=True)
    for line in (output / f"build-{variant}.jsonl").read_text().splitlines():
        event = json.loads(line)
        name = event.get("target", {}).get("name")
        if event.get("reason") == "compiler-artifact" and name in {"json_benchmark", "json_extract_allocations"} and event.get("executable"):
            target = destination / name
            shutil.copy2(event["executable"], target)
            binaries[name] = digest(target)
    if len(binaries) != 2:
        raise RuntimeError(f"missing benchmark executable for {variant}")
    return binaries


def summarize(output, repeats):
    cases = {}
    for path in sorted((output / "alternating").glob("**/estimates.json")):
        variant, separator, repeat = path.parent.name.rpartition("-")
        if not separator or variant not in {"fork", "upstream", "candidate"} or not repeat.isdigit():
            continue
        estimates = json.loads(path.read_text())
        benchmark = json.loads((path.parent / "benchmark.json").read_text())
        estimate = estimates.get("slope", estimates["mean"])
        sample = json.loads((path.parent / "sample.json").read_text())
        if not sample["times"] or len(sample["times"]) != len(sample["iters"]) or estimate["point_estimate"] <= 0:
            raise RuntimeError(f"invalid sample data in {path}")
        cases.setdefault(benchmark["full_id"], {}).setdefault(variant, []).append({
            "repeat": int(repeat), "estimate_ns": estimate["point_estimate"],
            "confidence_interval": estimate["confidence_interval"],
            "sample": sample,
        })
    if not cases:
        raise RuntimeError("no benchmark samples found")
    metadata_path = output / "metadata.json"
    if metadata_path.exists():
        metadata = json.loads(metadata_path.read_text())
        if "benchmarks" in metadata and set(cases) != set(metadata["benchmarks"]):
            raise RuntimeError("completed benchmark cases do not match the executable's case list")
    rows = []
    for name, variants in cases.items():
        if set(variants) != {"fork", "upstream", "candidate"} or any(
            sorted(run["repeat"] for run in runs) != list(range(1, repeats + 1)) for runs in variants.values()
        ):
            raise RuntimeError(f"incomplete repeated comparison for {name}")
        median = {variant: statistics.median(run["estimate_ns"] for run in runs) for variant, runs in variants.items()}
        rows.append({"benchmark": name, **{f"{variant}_median_us": value / 1000 for variant, value in median.items()},
                     "candidate_vs_fork_percent": 100 * (median["candidate"] / median["fork"] - 1),
                     "candidate_vs_upstream_percent": 100 * (median["candidate"] / median["upstream"] - 1)})
    with (output / "summary.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    (output / "samples.json").write_text(json.dumps(cases, separators=(",", ":")) + "\n")
    print(f"summarized {len(cases)} benchmarks with {repeats} repeats per revision", flush=True)


if __name__ == "__main__":
    main()
