#!/usr/bin/env python3
"""Actual-process native offline operator checks; no private fixture constructors."""
import argparse
import hashlib
import json
import os
import select
import sqlite3
import time
from pathlib import Path
import subprocess
import tempfile


def snapshot(directory):
    return {str(p.relative_to(directory)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in directory.rglob('*') if p.is_file()}


def crash_cases(test_binary, directory, run):
    listing = subprocess.run([str(test_binary), '--list', '--ignored'], capture_output=True,
                             text=True, timeout=20, check=True).stdout
    print(json.dumps({'fixture_binary': str(test_binary),
                      'sha256': hashlib.sha256(test_binary.read_bytes()).hexdigest()}), flush=True)

    def selector(suffix):
        matches = [line.removesuffix(': test') for line in listing.splitlines()
                   if line.endswith('::' + suffix + ': test')]
        assert len(matches) == 1, (suffix, matches)
        return matches[0]

    child_test = selector('shared_downgrade_crash_child')
    phases = ['intent', 'commit', 'checkpoint', 'cleanup-start', 'cleanup-log',
              'cleanup-logical', 'cleanup-physical-wal', 'cleanup-physical-tshm',
              'cleanup-authority', 'cleanup-ids-a', 'cleanup-ids-b',
              'cleanup-ids-selector', 'cleanup-manifest']
    for phase in phases:
        case = directory / phase
        case.mkdir()
        ready, location = case / 'ready', case / 'location'
        env = dict(os.environ, TMPDIR=str(case), TURSO_DOWNGRADE_CRASH_PHASE=phase,
                   TURSO_DOWNGRADE_CRASH_READY=str(ready), TURSO_DOWNGRADE_CRASH_LOCATION=str(location))
        with (case / 'child.log').open('w+') as output:
            child = subprocess.Popen([str(test_binary), '--ignored', '--exact', child_test, '--nocapture'],
                                     env=env, stdout=output, stderr=subprocess.STDOUT)
            try:
                deadline = time.monotonic() + 30
                while not ready.exists():
                    assert child.poll() is None, f'fixture child exited before {phase}'
                    assert time.monotonic() < deadline, f'fixture child timed out at {phase}'
                    time.sleep(0.01)
                assert ready.read_text() == phase
                child.kill()
                assert child.wait(timeout=5) == -9
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait(timeout=5)
            output.seek(0)
            text = output.read()
            assert 'running 1 test' in text, text
            print(json.dumps({'phase': phase, 'child_test': child_test, 'signal': 9,
                              'stdout': text}), flush=True)
        lines = location.read_text().splitlines()
        assert len(lines) == 3
        path, limit, namespace = Path(lines[0]), int(lines[1]), Path(lines[2])
        assert path.resolve().is_relative_to(case.resolve())
        assert namespace.parent == path.parent
        operation = 'resume-downgrade-cleanup' if phase.startswith('cleanup-') else 'resume-downgrade-marker'
        before = path.read_bytes() if phase.startswith('cleanup-') else None
        run(path, '--shared-mvcc-offline', operation, '--all-older-workers-stopped',
            '--shared-mvcc-history-limit', str(limit))
        assert path.read_bytes()[18:20] == bytes([2, 2])
        if before is not None:
            assert path.read_bytes() == before
        assert not namespace.exists()
        assert not path.with_suffix('.db-log').exists()
        assert run(path, sql='SELECT value FROM preserved WHERE id=1').stdout.strip() == 'materialized shared row'
        assert run(path, sql='PRAGMA integrity_check').stdout.strip() == 'ok'

    upgrade_child = selector('offline_marker_crash_child')
    for point in ['committed', 'checkpointed']:
        path = directory / f'upgrade-{point}.db'
        run(path, sql="PRAGMA journal_mode=mvcc; CREATE TABLE preserved(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO preserved VALUES(1,'before');")
        env = dict(os.environ, TURSO_OFFLINE_MARKER_CRASH_PATH=str(path),
                   TURSO_OFFLINE_MARKER_CRASH_POINT=point)
        child = subprocess.run([str(test_binary), '--ignored', '--exact', upgrade_child, '--nocapture'],
                               env=env, capture_output=True, text=True, timeout=45)
        assert child.returncode == 73, (child.returncode, child.stdout, child.stderr)
        assert 'running 1 test' in child.stdout, child.stdout
        print(json.dumps({'phase': 'upgrade-' + point, 'exit': 73, 'child_test': upgrade_child}), flush=True)
        run(path, '--shared-mvcc-offline', 'resume-prepared-marker', '--all-older-workers-stopped')
        assert path.read_bytes()[18:20] == bytes([254, 254])
        assert run(path, '--shared-mvcc', sql='SELECT value FROM preserved WHERE id=1').stdout.strip() == 'before'
        run(path, '--shared-mvcc-offline', 'downgrade-existing', '--all-older-workers-stopped')
        assert run(path, sql='PRAGMA integrity_check').stdout.strip() == 'ok'


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', required=True, type=Path)
    parser.add_argument('--case', choices=['no-write', 'roundtrip', 'new', 'busy', 'crash', 'stock', 'close'], required=True)
    parser.add_argument('--test-binary', type=Path)
    args = parser.parse_args()
    binary = args.binary.resolve(strict=True)
    print(json.dumps({'binary': str(binary), 'sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
                      'case': args.case}), flush=True)

    def run(path, *options, sql=None, success=True):
        command = [str(binary), '-q', '-m', 'list', str(path), *options]
        if sql is not None:
            command.append(sql)
        result = subprocess.run(command, capture_output=True, text=True, timeout=45)
        print(json.dumps({'argv': command, 'returncode': result.returncode,
                          'stdout': result.stdout, 'stderr': result.stderr}), flush=True)
        if success is True:
            assert result.returncode == 0, result.stderr
        elif success is False:
            assert result.returncode != 0, 'unexpected successful operation'
        return result

    def offline(path, operation, *extra):
        return run(path, '--shared-mvcc-offline', operation, '--all-older-workers-stopped', *extra)

    with tempfile.TemporaryDirectory(prefix='turso-offline-cli-') as temp:
        directory = Path(temp)
        path = directory / 'data.db'
        if args.case == 'crash':
            assert args.test_binary is not None, 'crash cases require the genuine native fixture executable'
            crash_cases(args.test_binary.resolve(strict=True), directory, run)
        elif args.case == 'no-write':
            for options in [
                ['--shared-mvcc-offline', 'publish-new'],
                ['--shared-mvcc-offline', 'publish-new', '--all-older-workers-stopped'],
                ['--shared-mvcc-offline', 'upgrade-existing', '--all-older-workers-stopped', '--vfs', 'memory'],
                ['--shared-mvcc-offline', 'publish-new', '--all-older-workers-stopped', '--shared-mvcc-staging', str(directory / 'stage.db'), '--shared-mvcc-history-limit', '1'],
                ['--shared-mvcc'],
            ]:
                before = snapshot(directory)
                run(path, *options, success=False)
                assert snapshot(directory) == before, options
            before = snapshot(directory)
            run(path, '--shared-mvcc-offline', 'upgrade-existing', '--all-older-workers-stopped', sql='CREATE TABLE forbidden(x)', success=False)
            assert snapshot(directory) == before
        else:
            if args.case == 'new':
                stage = directory / 'stage.db'
                offline(path, 'publish-new', '--shared-mvcc-staging', str(stage))
                assert not stage.exists()
                assert not Path(str(stage) + '-wal').exists()
                assert not stage.with_suffix('.db-log').exists()
            else:
                if args.case == 'stock':
                    with sqlite3.connect(path) as stock:
                        stock.executescript("CREATE TABLE preserved(id INTEGER PRIMARY KEY, value TEXT); CREATE UNIQUE INDEX preserved_value ON preserved(value); INSERT INTO preserved VALUES(1,'before'); CREATE TABLE typed(i INTEGER, r REAL, t TEXT, b BLOB, z); CREATE INDEX typed_text ON typed(t);")
                        stock.execute('INSERT INTO typed VALUES(?,?,?,?,?)', (-17, 1.25, 'héllo', b'\x00\xff', None))
                        stock.commit()
                        schema = stock.execute("SELECT name,sql FROM sqlite_schema WHERE name IN ('preserved','preserved_value','typed','typed_text') ORDER BY name").fetchall()
                    stock.close()
                    run(path, sql='PRAGMA journal_mode=mvcc;')
                else:
                    run(path, sql="PRAGMA journal_mode=mvcc; CREATE TABLE preserved(id INTEGER PRIMARY KEY, value TEXT); CREATE UNIQUE INDEX preserved_value ON preserved(value); INSERT INTO preserved VALUES(1,'before');")
                inode = path.stat().st_ino
                offline(path, 'upgrade-existing')
                assert path.stat().st_ino == inode
            assert path.read_bytes()[18:20] == bytes([254, 254])
            inode = path.stat().st_ino
            namespace = directory / f'.turso-mvcc-{path.stat().st_dev:016x}-{inode:016x}'
            assert namespace.is_dir()
            before = snapshot(directory)
            run(path, sql='SELECT 1', success=False)
            assert snapshot(directory) == before
            if args.case == 'new':
                run(path, '--shared-mvcc', sql="CREATE TABLE preserved(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO preserved VALUES(1,'before');")
            committed = run(path, '--shared-mvcc', sql="BEGIN CONCURRENT; UPDATE preserved SET value='after' WHERE id=1; INSERT INTO preserved VALUES(2,'peer'); COMMIT; SELECT 'COMMITTED_BEFORE_CLOSE';", success=None if args.case == 'close' else True)
            assert committed.stdout.strip() == 'COMMITTED_BEFORE_CLOSE', committed.stdout
            assert run(path, '--shared-mvcc', '--readonly', sql='SELECT id,value FROM preserved ORDER BY id').stdout.strip() == '1|after\n2|peer'
            if args.case == 'close':
                assert committed.returncode == 0, f'shared close failed after observed committed data: {committed.stderr}'
                print(json.dumps({'case': args.case, 'result': 'passed'}), flush=True)
                return
            if args.case == 'busy':
                worker = subprocess.Popen([str(binary), '-q', '-m', 'list', str(path), '--shared-mvcc'],
                                          stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                try:
                    worker.stdin.write(b"BEGIN CONCURRENT; SELECT 'LIVE_READER' FROM preserved WHERE id=1;\n")
                    worker.stdin.flush()
                    deadline = time.monotonic() + 15
                    output = b''
                    while b'LIVE_READER\n' not in output:
                        remaining = deadline - time.monotonic()
                        assert remaining > 0, 'worker did not acknowledge actual active transaction'
                        ready, _, _ = select.select([worker.stdout], [], [], remaining)
                        assert ready, 'worker readiness deadline'
                        chunk = os.read(worker.stdout.fileno(), 4096)
                        assert chunk, f'worker exited before readiness: {worker.poll()}'
                        output += chunk
                    # Coordination maps may change when the refused participant opens;
                    # committed durable content must not.
                    durable = lambda: {k: v for k, v in snapshot(directory).items()
                                       if not k.endswith(('authority', 'physical-tshm'))}
                    before = durable()
                    result = run(path, '--shared-mvcc-offline', 'downgrade-existing',
                                 '--all-older-workers-stopped', success=False)
                    assert 'busy' in result.stderr.lower() or 'locked' in result.stderr.lower(), result.stderr
                    assert durable() == before
                    worker.stdin.write(b"ROLLBACK;\n")
                    worker.stdin.close()
                    worker.wait(timeout=15)
                    assert worker.returncode == 0, worker.stderr.read().decode()
                finally:
                    if worker.poll() is None:
                        worker.kill()
                        worker.wait(timeout=5)
            offline(path, 'downgrade-existing')
            assert path.stat().st_ino == inode
            assert path.read_bytes()[18:20] == bytes([2, 2])
            assert not namespace.exists()
            assert not path.with_suffix('.db-log').exists()
            assert run(path, sql='SELECT id,value FROM preserved ORDER BY id').stdout.strip() == '1|after\n2|peer'
            assert run(path, sql='PRAGMA integrity_check').stdout.strip() == 'ok'
            if args.case == 'stock':
                with sqlite3.connect(path) as stock:
                    assert stock.execute('SELECT i,r,t,b,z FROM typed INDEXED BY typed_text').fetchall() == [(-17, 1.25, 'héllo', b'\x00\xff', None)]
                    assert stock.execute("SELECT name,sql FROM sqlite_schema WHERE name IN ('preserved','preserved_value','typed','typed_text') ORDER BY name").fetchall() == schema
                    assert stock.execute('PRAGMA integrity_check').fetchall() == [('ok',)]
                stock.close()
    print(json.dumps({'case': args.case, 'result': 'passed'}), flush=True)


if __name__ == '__main__':
    main()
