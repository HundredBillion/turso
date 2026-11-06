import csv, hashlib, json, os, subprocess
from pathlib import Path
out=Path('artifact-comparison-results').resolve();out.mkdir()
inputs={f'{origin}_{variant}':Path('comparison-inputs')/origin/'binaries'/variant/'json_benchmark' for origin in ['original','canonical'] for variant in ['fork','candidate']}
metadata={'source_runs':{'original':42243464027,'canonical':42250171490}, 'binaries':{},'cpu':subprocess.check_output(['lscpu'],text=True)}
for label,path in inputs.items():
    path.chmod(0o755)
    metadata['binaries'][label]=hashlib.sha256(path.read_bytes()).hexdigest()
(out/'metadata.json').write_text(json.dumps(metadata,indent=2))
env=dict(os.environ,DISABLE_RUSQLITE_BENCHMARK='1',CRITERION_HOME=str(out/'alternating'))
labels=list(inputs)
filt='JSONB Size - (Small|Medium)/Limbo|JSON Patch - (Simple Property Update|Remove Property)/Limbo|JSON Extract/(1024|102400)/(first|multiple)'
for repeat in range(1,5):
    order=labels[repeat-1:]+labels[:repeat-1]
    for label in order:
        name=f'{label}-{repeat}';print(name,flush=True)
        with (out/f'{name}.log').open('w') as log:
            subprocess.run([str(inputs[label].resolve()),filt,'--bench','--warm-up-time','1','--measurement-time','2','--sample-size','50','--nresamples','10000','--noplot','--save-baseline',name],cwd='benches',env=env,stdout=log,stderr=subprocess.STDOUT,check=True)
rows=[]
for estimate in (out/'alternating').rglob('estimates.json'):
    label=estimate.parent.name
    if label.rsplit('-',1)[0] not in inputs:continue
    variant,repeat=label.rsplit('-',1);e=json.loads(estimate.read_text())['median'];benchmark=json.loads((estimate.parent/'benchmark.json').read_text())['full_id']
    rows.append({'benchmark':benchmark,'variant':variant,'repeat':int(repeat),'median_ns':e['point_estimate'],'lower_ns':e['confidence_interval']['lower_bound'],'upper_ns':e['confidence_interval']['upper_bound']})
assert len(rows)==8*4*4,len(rows)
with (out/'summary.csv').open('w') as f:
    w=csv.DictWriter(f,fieldnames=list(rows[0]));w.writeheader();w.writerows(sorted(rows,key=lambda r:(r['benchmark'],r['variant'],r['repeat'])))
