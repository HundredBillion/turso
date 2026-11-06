#!/usr/bin/env python3
"""Build identical mixed probes at one path, preserve binaries, then run screening."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tempfile
import time


def output(command, cwd=None, env=None):
    return subprocess.check_output(command, cwd=cwd, env=env, text=True).strip()


def digest(data):
    return hashlib.sha256(data).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--candidate", default="ad54b7aaa6bf7b3933d4971ba039f14bab21e3d6")
    parser.add_argument("--fork", default="4ccf547a400777307a834b554f467c8868830f5c")
    parser.add_argument("--upstream", default="2709799dad2ebb0f1749cfa62fa28514f568da28")
    parser.add_argument("--requests", type=int, default=4000)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    if not 1 <= args.requests <= 100000 or not 1 <= args.repeats <= 10:
        parser.error("invalid measurement bounds")
    destination = args.output.resolve()
    destination.mkdir(parents=True, exist_ok=False)
    repo = Path(output(["git", "rev-parse", "--show-toplevel"]))
    revisions = {name: output(["git", "rev-parse", "--verify", "--end-of-options", f"{revision}^{{commit}}"], repo)
                 for name, revision in [("upstream", args.upstream), ("fork", args.fork), ("candidate", args.candidate)]}
    fixtures = {name: (repo / name).read_bytes() for name in [
        "benches/examples/cache_application_probe.rs", "benches/examples/cache_mixed_write_probe.rs",
        "tests/integration/unreliable_io.rs"]}
    lock = (repo / "Cargo.lock").read_bytes()
    environment = os.environ | {"RUSTUP_TOOLCHAIN": "1.88.0", "CARGO_INCREMENTAL": "0",
        "CARGO_PROFILE_LIB_RELEASE_OPT_LEVEL": "3", "CARGO_PROFILE_LIB_RELEASE_DEBUG": "0",
        "CARGO_PROFILE_LIB_RELEASE_CODEGEN_UNITS": "16", "CARGO_PROFILE_LIB_RELEASE_LTO": "false",
        "CARGO_PROFILE_LIB_RELEASE_PANIC": "unwind"}
    metadata = {"fixture_revision": output(["git", "rev-parse", "HEAD"], repo),
        "fixture_sha256": {name: digest(data) for name, data in fixtures.items()},
        "runtime_revisions": revisions, "lock_sha256": digest(lock), "platform": platform.platform(),
        "profile": "lib-release,opt-level3,debug0,codegen-units16,ltofalse,panicunwind,incremental0",
        "rustc": output(["rustup", "run", "1.88.0", "rustc", "-Vv"], repo), "binaries": {},
        "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    metadata_path = destination / "build-metadata.json"
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")
    if Path("/proc/cpuinfo").exists():
        (destination / "cpuinfo.txt").write_text(Path("/proc/cpuinfo").read_text())
    binaries = destination / "binaries"
    binaries.mkdir()
    with tempfile.TemporaryDirectory(prefix="turso-mixed-controls-") as directory:
        temporary = Path(directory)
        canonical = temporary / "source"
        environment["CARGO_TARGET_DIR"] = str(temporary / "target")
        added = False
        try:
            for label, revision in revisions.items():
                if not added:
                    subprocess.run(["git", "worktree", "add", "--detach", str(canonical), revision], cwd=repo, check=True)
                    added = True
                else:
                    subprocess.run(["git", "reset", "--hard", revision], cwd=canonical, check=True)
                if (canonical / "Cargo.lock").read_bytes() != lock:
                    raise RuntimeError(f"{label} dependency lock differs")
                for name, data in fixtures.items():
                    target = canonical / name
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(data)
                with (destination / f"build-{label}.log").open("w") as log:
                    for action in ["test", "build"]:
                        subprocess.run(["rustup", "run", "1.88.0", "cargo", action, "--locked", "--profile", "lib-release",
                                        "-p", "turso_benches", "--example", "cache_mixed_write_probe", "-j", "2"],
                                       cwd=canonical, env=environment, stdout=log, stderr=subprocess.STDOUT, check=True)
                binary = binaries / label
                shutil.copy2(temporary / "target/lib-release/examples/cache_mixed_write_probe", binary)
                metadata["binaries"][label] = {"path": str(binary), "sha256": digest(binary.read_bytes())}
                metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")
        finally:
            if added:
                subprocess.run(["git", "worktree", "remove", "--force", str(canonical)], cwd=repo, check=True)
    metadata["builds_finished_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")
    command = ["python3", str(repo / "scripts/run_cache_mixed_probe.py"), "--output", str(destination / "screening"),
               "--requests", str(args.requests), "--repeats", str(args.repeats)]
    for label in revisions:
        command.extend(["--binary", f"{label}={binaries / label}"])
    subprocess.run(command, cwd=repo, check=True)


if __name__ == "__main__":
    main()
