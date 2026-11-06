import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

repo = Path.cwd()
out = repo / 'application-results'
out.mkdir()
canonical = Path('/tmp/turso-cache-application-source')
target = Path('/tmp/turso-cache-application-target')
assert not canonical.exists() and not target.exists()
revisions = {'upstream': '2709799dad2ebb0f1749cfa62fa28514f568da28', 'fork': '4ccf547a400777307a834b554f467c8868830f5c', 'candidate': 'c4808695adcbe8fbd6f240d34910f7fd3a3febb6'}
fixture = Path('benches/examples/cache_application_probe.rs')
env = dict(os.environ, RUSTUP_TOOLCHAIN='1.88.0', CARGO_TARGET_DIR=str(target), CARGO_INCREMENTAL='0', CARGO_PROFILE_LIB_RELEASE_OPT_LEVEL='3', CARGO_PROFILE_LIB_RELEASE_DEBUG='0', CARGO_PROFILE_LIB_RELEASE_CODEGEN_UNITS='16', CARGO_PROFILE_LIB_RELEASE_LTO='false', CARGO_PROFILE_LIB_RELEASE_PANIC='unwind')
def command(args):
    return subprocess.check_output(args,text=True).strip()
metadata = {'revisions':revisions, 'fixture_sha256':hashlib.sha256((repo/fixture).read_bytes()).hexdigest(), 'toolchain':command(['rustc','-Vv']), 'profile':'lib-release,opt3,debug0,codegen16,lto=false,panic=unwind','binaries':{}}
(out/'cpu.txt').write_text(command(['lscpu']))
(out/'build-metadata.json').write_text(json.dumps(metadata,indent=2))
subprocess.run(['git','worktree','add','--detach',str(canonical),revisions['upstream']],check=True)
try:
    for label, revision in revisions.items():
        subprocess.run(['git','reset','--hard',revision],cwd=canonical,check=True)
        (canonical/fixture).parent.mkdir(parents=True,exist_ok=True)
        shutil.copyfile(repo/fixture,canonical/fixture)
        assert (canonical/'Cargo.lock').read_bytes() == (repo/'Cargo.lock').read_bytes()
        with (out/f'build-{label}.log').open('w') as log:
            subprocess.run(['cargo','build','--locked','-p','turso_benches','--example','cache_application_probe','--profile','lib-release','-j','2'],cwd=canonical,env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
        binary = out/'binaries'/label
        binary.parent.mkdir(exist_ok=True)
        shutil.copyfile(target/'lib-release/examples/cache_application_probe',binary)
        binary.chmod(0o755)
        metadata['binaries'][label] = hashlib.sha256(binary.read_bytes()).hexdigest()
        (out/'build-metadata.json').write_text(json.dumps(metadata,indent=2))
finally:
    subprocess.run(['git','worktree','remove','--force',str(canonical)],check=True)
