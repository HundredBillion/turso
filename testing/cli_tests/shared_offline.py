#!/usr/bin/env python3
"""Actual-process native offline operator checks; no private fixture constructors."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile


def snapshot(directory):
    return {str(p.relative_to(directory)): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in directory.rglob('*') if p.is_file()}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', required=True, type=Path)
    parser.add_argument('--case', choices=['no-write', 'roundtrip', 'new'], required=True)
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
        if success:
            assert result.returncode == 0, result.stderr
        else:
            assert result.returncode != 0, 'unexpected successful operation'
        return result

    def offline(path, operation, *extra):
        return run(path, '--shared-mvcc-offline', operation, '--all-older-workers-stopped', *extra)

    with tempfile.TemporaryDirectory(prefix='turso-offline-cli-') as temp:
        directory = Path(temp)
        path = directory / 'data.db'
        if args.case == 'no-write':
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
            run(path, '--shared-mvcc', sql="BEGIN CONCURRENT; UPDATE preserved SET value='after' WHERE id=1; INSERT INTO preserved VALUES(2,'peer'); COMMIT;")
            assert run(path, '--shared-mvcc', '--readonly', sql='SELECT id,value FROM preserved ORDER BY id').stdout.strip() == '1|after\n2|peer'
            offline(path, 'downgrade-existing')
            assert path.stat().st_ino == inode
            assert path.read_bytes()[18:20] == bytes([2, 2])
            assert not namespace.exists()
            assert not path.with_suffix('.db-log').exists()
            assert run(path, sql='SELECT id,value FROM preserved ORDER BY id').stdout.strip() == '1|after\n2|peer'
            assert run(path, sql='PRAGMA integrity_check').stdout.strip() == 'ok'
    print(json.dumps({'case': args.case, 'result': 'passed'}), flush=True)


if __name__ == '__main__':
    main()
