#!/usr/bin/env python3
"""Validation-only matched FTS walltime experiment; never updates product refs."""
import csv
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

VARIANTS = {
    "base": "65a31536512e3ae906277796f520785b7d68ba93",
    "aggregate": "3e69d67052900b489ca908cc938bdb1ac44c1a75",
    "callback": "f1b896e018d6eac65f2bde5c7dc916790acf89ca",
}


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def run(args, *, cwd, env=None, log=None, timeout=2400):
    if log:
        with Path(log).open("w") as stream:
            subprocess.run(args, cwd=cwd, env=env, stdout=stream,
                           stderr=subprocess.STDOUT, check=True, timeout=timeout)
    else:
        return subprocess.check_output(args, cwd=cwd, env=env, text=True, timeout=timeout).strip()


def main():
    repo = Path(__file__).resolve().parents[2]
    root = Path(sys.argv[1]).resolve()
    root.mkdir(parents=True, exist_ok=False)
    engine = root / "engine"
    evidence = root / "evidence"
    evidence.mkdir()
    binaries = evidence / "binaries"
    binaries.mkdir()
    env = os.environ | {"CARGO_TARGET_DIR": str(root / "target"),
                        "CARGO_BUILD_JOBS": "2", "CARGO_INCREMENTAL": "0",
                        "RUSTFLAGS": ""}
    metadata = {"variants": VARIANTS, "instrument": "Criterion walltime, not Codspeed CPU simulation",
                "profile": "lib-release", "features": "fts", "jobs": 2,
                "rustc": run(["rustc", "+1.88", "-Vv"], cwd=repo),
                "host": run(["uname", "-a"], cwd=repo), "binaries": {},
                "harness_sha256": sha(repo / "benches/fts_benchmark.rs")}
    run(["git", "worktree", "add", "--detach", str(engine), VARIANTS["base"]], cwd=repo)
    for label, revision in VARIANTS.items():
        run(["git", "restore", "--source=HEAD", "--", "benches/fts_benchmark.rs"], cwd=engine)
        run(["git", "checkout", "--detach", revision], cwd=engine)
        shutil.copyfile(repo / "benches/fts_benchmark.rs", engine / "benches/fts_benchmark.rs")
        assert run(["git", "rev-parse", "HEAD"], cwd=engine) == revision
        assert sha(engine / "benches/fts_benchmark.rs") == metadata["harness_sha256"]
        (evidence / f"{label}-harness.patch").write_text(run(["git", "diff", "--", "benches/fts_benchmark.rs"], cwd=engine))
        build_json = evidence / f"{label}-build.jsonl"
        with build_json.open("w") as stdout, (evidence / f"{label}-build.log").open("w") as stderr:
            subprocess.run(["cargo", "+1.88", "bench", "--no-run", "--locked",
                            "--profile", "lib-release", "-p", "turso_benches",
                            "--bench", "fts_benchmark", "--features", "fts",
                            "--message-format=json"], cwd=engine, env=env,
                           stdout=stdout, stderr=stderr, check=True, timeout=2400)
        artifacts = [json.loads(line) for line in build_json.read_text().splitlines() if line.startswith("{")]
        paths = {item["executable"] for item in artifacts
                 if item.get("reason") == "compiler-artifact" and item.get("executable")
                 and item["target"]["name"] == "fts_benchmark"}
        assert len(paths) == 1, paths
        destination = binaries / label
        shutil.copy2(paths.pop(), destination)
        metadata["binaries"][label] = {"sha256": sha(destination), "source": revision,
                                        "cargo_lock_sha256": sha(engine / "Cargo.lock")}
        (evidence / "metadata.json").write_text(json.dumps(metadata, indent=2))

    master = evidence / "master.db"
    create_env = env | {"FTS_CONTROL_CREATE": str(master)}
    run([str(binaries / "base"), "--bench"], cwd=evidence,
        env=create_env, log=evidence / "fixture-create.log", timeout=180)
    metadata["fixture_sha256"] = sha(master)
    metadata["fixture_bytes"] = master.stat().st_size
    metadata["raw_segment_id_strings"] = sorted(set(value.decode() for value in
        re.findall(rb"fts2/seg/[0-9a-f]{32}", master.read_bytes())))
    metadata["segment_note"] = "Raw physical string scan only, not an authoritative live-segment count; full fixture SHA fixes all physical bytes."
    (evidence / "metadata.json").write_text(json.dumps(metadata, indent=2))
    orders = [["base", "aggregate", "control", "callback"],
              ["callback", "control", "aggregate", "base"],
              ["aggregate", "base", "callback", "control"]]
    records = []
    reference = None
    for rotation, order in enumerate(orders):
        for position, label in enumerate(order):
            trial = evidence / f"trial-{rotation}-{position}-{label}"
            trial.mkdir()
            working = root / "working"
            if working.exists():
                shutil.rmtree(working)
            working.mkdir()
            fixture = working / "fts_bench.db"
            shutil.copyfile(master, fixture)
            assert sha(fixture) == metadata["fixture_sha256"]
            binary = binaries / ("base" if label == "control" else label)
            trial_env = env | {"FTS_CONTROL_FIXTURE": str(fixture),
                               "FTS_CONTROL_METADATA": str(trial),
                               "CRITERION_HOME": str(trial / "criterion")}
            run([str(binary), "--bench", "sparse_top_10_query", "--warm-up-time", "1",
                 "--measurement-time", "2", "--sample-size", "20", "--noplot"],
                cwd=working, env=trial_env, log=trial / "run.log", timeout=180)
            estimates = list((trial / "criterion").rglob("new/estimates.json"))
            assert len(estimates) == 1, estimates
            sample = json.loads(estimates[0].with_name("sample.json").read_text())
            assert len(sample["times"]) == 20, sample.keys()
            current = {name: sha(trial / f"{name}.txt") for name in ["documents", "schema", "plan"]}
            if reference is None:
                reference = current
            assert current == reference, (label, current, reference)
            assert sha(master) == metadata["fixture_sha256"]
            assert sha(fixture) == metadata["fixture_sha256"], "read-only trial changed main bytes"
            estimate = json.loads(estimates[0].read_text())
            row = {"rotation": rotation, "position": position, "variant": label,
                   "binary_sha256": sha(binary), "fixture_sha256": sha(fixture), **current,
                   "mean_ns": estimate["mean"]["point_estimate"],
                   "median_ns": estimate["median"]["point_estimate"]}
            records.append(row)
            with (evidence / "summary.csv").open("w") as stream:
                writer = csv.DictWriter(stream, fieldnames=list(row))
                writer.writeheader()
                writer.writerows(records)
    assert len(records) == 12
    metadata["verified_trials"] = len(records)
    metadata["logical_hashes"] = reference
    (evidence / "metadata.json").write_text(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    main()
