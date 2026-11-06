import csv
import importlib.util
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess

repo = Path.cwd()
out = (repo / 'canonical-results').resolve()
out.mkdir()
spec = importlib.util.spec_from_file_location('comparison', repo / 'scripts/compare_json_benchmarks.py')
comparison = importlib.util.module_from_spec(spec)
spec.loader.exec_module(comparison)
canonical = Path('/tmp/turso-json-canonical-source')
target = Path('/tmp/turso-json-canonical-target')
assert not canonical.exists() and not target.exists()
revisions = {'fork': '4ccf547a400777307a834b554f467c8868830f5c',
             'candidate': 'c4808695adcbe8fbd6f240d34910f7fd3a3febb6',
             'static': 'ad54b7aaa',
             'upstream': '2709799dad2ebb0f1749cfa62fa28514f568da28'}
env = dict(os.environ, RUSTUP_TOOLCHAIN='1.88.0', CARGO_INCREMENTAL='0', CARGO_TARGET_DIR=str(target),
           CARGO_PROFILE_LIB_RELEASE_OPT_LEVEL='3', CARGO_PROFILE_LIB_RELEASE_DEBUG='0',
           CARGO_PROFILE_LIB_RELEASE_CODEGEN_UNITS='16', CARGO_PROFILE_LIB_RELEASE_LTO='false',
           CARGO_PROFILE_LIB_RELEASE_PANIC='unwind')
env['RUSTC'] = comparison.command(['rustup', 'which', '--toolchain', '1.88.0', 'rustc'])
fixtures = ['benches/Cargo.toml', 'benches/json_benchmark.rs', 'benches/json_extract_allocations.rs']
metadata = {'cpu': subprocess.check_output(['lscpu'], text=True), 'revisions': revisions, 'fixtures': {p: comparison.digest(repo/p) for p in fixtures},
            'toolchain': comparison.command([env['RUSTC'], '-Vv']), 'environment': {k:v for k,v in env.items() if k.startswith('CARGO_') or k in ('RUSTUP_TOOLCHAIN','RUSTC')}, 'binaries': {}}
(out/'metadata.json').write_text(json.dumps(metadata, indent=2))
subprocess.run(['git','worktree','add','--detach',str(canonical),revisions['fork']],check=True)
try:
    for label, revision in revisions.items():
        subprocess.run(['git','reset','--hard',revision],cwd=canonical,check=True)
        for fixture in fixtures:
            shutil.copyfile(repo/fixture,canonical/fixture)
        assert (canonical/'Cargo.lock').read_bytes() == (repo/'Cargo.lock').read_bytes()
        metadata['binaries'][label] = comparison.build(canonical,out,label,env,2,'1.88.0')
        (out/'metadata.json').write_text(json.dumps(metadata, indent=2))
    filt = 'JSONB Size - (Small|Medium)/Limbo|JSON Patch - (Simple Property Update|Remove Property)/Limbo|JSON Extract/(1024|102400)/(first|multiple)'
    runtime = dict(env, DISABLE_RUSQLITE_BENCHMARK='1', CRITERION_HOME=str(out/'alternating'))
    labels = list(revisions)
    for repeat in range(1,5):
        order = labels[repeat-1:]+labels[:repeat-1]
        for label in order:
            name=f'{label}-{repeat}'
            print('measuring',name,flush=True)
            with (out/f'{name}.log').open('w') as log:
                subprocess.run([str(out/'binaries'/label/'json_benchmark'), filt, '--bench', '--warm-up-time','1','--measurement-time','2','--sample-size','50','--nresamples','10000','--noplot','--save-baseline',name],cwd=canonical/'benches',env=runtime,stdout=log,stderr=subprocess.STDOUT,check=True)
finally:
    subprocess.run(['git','worktree','remove','--force',str(canonical)],check=True)
rows=[]
for estimate in (out/'alternating').rglob('estimates.json'):
    label=estimate.parent.name
    if label.rsplit('-',1)[0] not in revisions: continue
    variant,repeat=label.rsplit('-',1)
    e=json.loads(estimate.read_text())['median']
    benchmark=json.loads((estimate.parent/'benchmark.json').read_text())['full_id']
    rows.append({'benchmark':benchmark,'variant':variant,'repeat':int(repeat),'median_ns':e['point_estimate'],'lower_ns':e['confidence_interval']['lower_bound'],'upper_ns':e['confidence_interval']['upper_bound']})
assert len(rows)==8*4*4,len(rows)
with (out/'summary.csv').open('w') as f:
    writer=csv.DictWriter(f,fieldnames=list(rows[0]));writer.writeheader();writer.writerows(sorted(rows,key=lambda r:(r['benchmark'],r['variant'],r['repeat'])))
