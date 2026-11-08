#!/usr/bin/env python3
"""Independent PHP process acceptance; shared mode must attest its capability."""

import argparse
import hashlib
import json
import os
import select
import signal
import subprocess
import sys
import time
import urllib.parse
import uuid
from pathlib import Path


class UnsupportedMode(RuntimeError):
    pass


class StartupRejected(RuntimeError):
    pass


class Worker:
    def __init__(self, suite, name, path=None, readonly=False, mode=None):
        self.suite = suite
        self.name = name
        self.sequence = 0
        self.pending = None
        self.buffer = bytearray()
        path = path or suite.database
        mode = mode or suite.mode
        query = {"mode": "ro" if readonly else "rwc"}
        if mode == "shared":
            query |= {"turso_mvcc": "shared", "turso_mvcc_history_limit": "67108864"}
        uri = "file:" + urllib.parse.quote(str(path), safe="/") + "?" + urllib.parse.urlencode(query)
        plain_path = suite.args.api == "sqlite3" and (mode != "shared" or suite.args.shared_open_flag)
        config = {
            "uri": str(path) if plain_path else uri,
            "api": suite.args.api,
            "engine": suite.args.engine,
            "library": suite.args.library,
            "mode": mode,
            "readonly": readonly,
            "busy_ms": suite.args.busy_ms,
            "open_flag": suite.args.shared_open_flag if mode == "shared" else 0,
        }
        self.stderr = (suite.output / f"{name}.stderr.log").open("wb")
        self.process = subprocess.Popen(
            [
                suite.args.php,
                "-d",
                "display_errors=stderr",
                str(Path(__file__).with_name("worker.php")),
                json.dumps(config),
            ],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=self.stderr,
        )
        suite.workers.append(self)
        hello = self.receive()
        suite.record("startup", worker=name, hello=hello)
        if hello.get("event") == "unsupported":
            raise UnsupportedMode(hello["reason"])
        if hello.get("event") != "ready":
            raise StartupRejected(f"{name} startup failed: {hello}")
        self.pid = hello["pid"]
        if self.pid != self.process.pid:
            raise AssertionError("PHP worker PID is not the independently spawned process")
        if suite.args.engine == "turso" and hello["identity"]["sha256"] != suite.library_hash:
            raise AssertionError("loaded Turso library hash differs from supervisor")
        assert hello["synchronous"] == [[2]], hello
        assert hello["busy_timeout"] == [[suite.args.busy_ms]], hello

    def receive(self):
        deadline = time.monotonic() + self.suite.args.deadline
        while b"\n" not in self.buffer:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not select.select([self.process.stdout], [], [], remaining)[0]:
                raise TimeoutError(f"{self.name}: worker response deadline exceeded")
            chunk = os.read(self.process.stdout.fileno(), 65536)
            if not chunk:
                raise RuntimeError(f"{self.name}: worker exited before response ({self.process.poll()})")
            self.buffer.extend(chunk)
            if len(self.buffer) > 4 * 1024 * 1024:
                raise RuntimeError("worker response exceeded bound")
        line, _, rest = self.buffer.partition(b"\n")
        self.buffer = bytearray(rest)
        return json.loads(line)

    def send(self, sql=None, **request):
        assert self.pending is None, "one outstanding command per worker"
        self.sequence += 1
        request |= {"id": self.sequence}
        if sql is not None:
            request.setdefault("op", "sql")
            request["sql"] = sql
        self.pending = self.sequence
        self.suite.record("send", worker=self.name, request=request)
        self.process.stdin.write(json.dumps(request).encode() + b"\n")
        self.process.stdin.flush()

    def result(self, ok=True):
        response = self.receive()
        assert response.get("id") == self.pending, (self.name, self.pending, response)
        self.pending = None
        self.suite.record("response", worker=self.name, response=response)
        assert response.get("attempts", 1) == 1, response
        if ok:
            assert response["ok"], response
        return response

    def sql(self, sql, parameters=None, ok=True):
        self.send(sql, parameters=parameters or [])
        return self.result(ok)

    def rows(self, sql, expected, parameters=None):
        response = self.sql(sql, parameters)
        assert response["rows"] == expected, (sql, expected, response)

    def bounded_error(self, response, allowed=(5, 6)):
        assert not response["ok"] and response["code"] & 255 in allowed, response
        assert response["duration_ns"] < (self.suite.args.busy_ms + 1000) * 1000000, response

    def kill(self):
        if self.process.poll() is None:
            os.kill(self.process.pid, signal.SIGKILL)
            self.process.wait(timeout=self.suite.args.deadline)
        self.suite.record("killed", worker=self.name, pid=self.process.pid)

    def close(self):
        if self.process.poll() is None:
            self.send(op="close")
            self.result()
            assert self.process.wait(timeout=self.suite.args.deadline) == 0
        self.stderr.close()


class Suite:
    def __init__(self, args):
        self.args = args
        self.output = args.output.resolve()
        self.output.mkdir(parents=True, exist_ok=False)
        self.database = self.output / "workers.db"
        self.mode = "shared" if args.profile in ["shared-acceptance", "sqlite-shared-negative-control"] else "wal"
        self.workers = []
        self.events = (self.output / "events.jsonl").open("w")
        self.ledger = (self.output / "acknowledgments.jsonl").open("w")
        self.library_hash = hashlib.sha256(Path(args.library).read_bytes()).hexdigest() if args.library else None
        self.record(
            "configuration", configuration=vars(args) | {"output": str(self.output)}, library_sha256=self.library_hash
        )

    def record(self, event, **values):
        self.events.write(json.dumps({"event": event, "monotonic_ns": time.monotonic_ns(), **values}) + "\n")
        self.events.flush()

    def acknowledge(self, kind, transaction, rows):
        record = {"kind": kind, "transaction": transaction, "rows": rows}
        self.ledger.write(json.dumps(record) + "\n")
        self.ledger.flush()
        os.fsync(self.ledger.fileno())
        self.record("ledger_synced", **record)

    def bootstrap(self):
        if self.mode == "shared" and self.args.engine == "turso":
            self.initialize_shared_database()
        worker = Worker(self, "bootstrap")
        if self.mode == "wal":
            worker.rows("PRAGMA journal_mode=WAL", [["wal"]])
        worker.sql("CREATE TABLE items(id INTEGER PRIMARY KEY AUTOINCREMENT, label TEXT UNIQUE, value INTEGER)")
        worker.sql("CREATE TABLE pairs(txn TEXT, slot INTEGER, payload TEXT, PRIMARY KEY(txn,slot))")
        worker.sql("INSERT INTO items(id,label,value) VALUES(1,'one',10),(2,'two',20),(3,'three',30)")
        worker.close()

    def initialize_shared_database(self):
        initializer = Worker(self, "offline-fixture", mode="wal")
        initializer.rows("PRAGMA journal_mode=WAL", [["wal"]])
        initializer.sql("PRAGMA user_version=0")
        initializer.close()
        assert all(worker.process.poll() is not None for worker in self.workers), "older fixture worker is alive"
        executable = Path(self.args.shared_admin_cli).resolve(strict=True)
        command = [
            str(executable),
            str(self.database),
            "--shared-mvcc-offline",
            "upgrade-existing",
            "--all-older-workers-stopped",
            "--shared-mvcc-history-limit",
            "67108864",
        ]
        result = subprocess.run(command, capture_output=True, text=True, timeout=self.args.deadline)
        self.record(
            "offline_initialization",
            command=command,
            cli_sha256=hashlib.sha256(executable.read_bytes()).hexdigest(),
            older_workers_stopped=True,
            returncode=result.returncode,
            stdout=result.stdout,
            stderr=result.stderr,
        )
        assert result.returncode == 0, result.stderr

    def protocol(self):
        first = Worker(self, "protocol-a")
        second = Worker(self, "protocol-b")
        assert first.pid != second.pid
        token = uuid.uuid4().hex
        for worker in [first, second]:
            worker.send(op="ping", token=token)
        for worker in [first, second]:
            assert worker.result()["rows"] == [[worker.pid, token]]
            worker.rows("SELECT count(*) FROM items", [[3]])
        syntax = first.sql("SELECT FROM", ok=False)
        first.bounded_error(syntax, (1,))
        first.rows("SELECT 42", [[42]])
        first.sql("BEGIN")
        first.sql("INSERT INTO items(id,label,value) VALUES(100,'private',100)")
        first.rows("SELECT value FROM items WHERE id=100", [[100]])
        second.rows("SELECT value FROM items WHERE id=100", [])
        first.sql("UPDATE items SET value=101 WHERE id=100")
        first.rows("SELECT value FROM items WHERE id=100", [[101]])
        first.sql("DELETE FROM items WHERE id=100")
        first.rows("SELECT value FROM items WHERE id=100", [])
        first.sql("COMMIT")
        first.close()
        second.close()

    def run(self):
        self.bootstrap()
        if self.args.profile == "turso-single-worker-control":
            self.single_worker_control()
            self.crash_recovery(hold_reader=False)
            self.record("complete", profile=self.args.profile, acceptance=False)
            return
        self.protocol()
        self.overlap_and_snapshot()
        self.rollback_and_savepoint()
        self.conflicts_and_ids()
        self.permissions()
        if self.mode == "shared":
            self.refuse_implicit_upgrade()
        self.crash_recovery()
        self.record("complete", profile=self.args.profile, acceptance=self.args.profile == "shared-acceptance")

    def overlap_and_snapshot(self):
        first, second, reader = [Worker(self, name) for name in ["overlap-a", "overlap-b", "long-reader"]]
        assert len({first.pid, second.pid, reader.pid}) == 3
        reader.sql("BEGIN")
        reader.rows("SELECT sum(value) FROM items", [[60]])
        for writer in [first, second]:
            writer.send("BEGIN")
        for writer in [first, second]:
            writer.result()
        first.sql("UPDATE items SET value=11 WHERE id=1")
        other = second.sql("UPDATE items SET value=22 WHERE id=2", ok=False)
        if self.mode == "shared":
            assert other["ok"], ("independent disjoint writes must both execute before commit", other)
        else:
            second.bounded_error(other)
            second.sql("ROLLBACK")
            self.record("wal_control_serialized_second_writer", acceptance=False)
        reader.rows("SELECT sum(value) FROM items", [[60]])
        self.record(
            "overlap_barrier", active_write_transactions=2 if self.mode == "shared" else 1, active_reader_pid=reader.pid
        )
        first.sql("COMMIT")
        if self.mode != "shared":
            second.sql("BEGIN")
            second.sql("UPDATE items SET value=22 WHERE id=2")
        second.sql("COMMIT")
        reader.rows("SELECT sum(value) FROM items", [[60]])
        fresh = Worker(self, "fresh-reader")
        fresh.rows("SELECT sum(value) FROM items", [[63]])
        checkpoint = first.sql("PRAGMA wal_checkpoint(TRUNCATE)", ok=False)
        if checkpoint["ok"]:
            assert checkpoint["rows"] and checkpoint["rows"][0][0] == 1, checkpoint
        else:
            first.bounded_error(checkpoint)
        reader.rows("SELECT sum(value) FROM items", [[60]])
        reader.sql("COMMIT")
        reader.rows("SELECT sum(value) FROM items", [[63]])
        done = first.sql("PRAGMA wal_checkpoint(TRUNCATE)")
        assert not done["rows"] or done["rows"][0][0] == 0, done
        for worker in [first, second, reader, fresh]:
            worker.close()

    def rollback_and_savepoint(self):
        first, second = [Worker(self, name) for name in ["rollback-a", "rollback-b"]]
        first.sql("BEGIN")
        first.sql("UPDATE items SET value=999 WHERE id=1")
        second.sql("BEGIN")
        result = second.sql("UPDATE items SET value=222 WHERE id=2", ok=False)
        if self.mode == "shared":
            assert result["ok"], result
            second.sql("COMMIT")
            first.sql("ROLLBACK")
        else:
            second.bounded_error(result)
            second.sql("ROLLBACK")
            first.sql("ROLLBACK")
            second.sql("BEGIN")
            second.sql("UPDATE items SET value=222 WHERE id=2")
            second.sql("COMMIT")
        second.rows("SELECT id,value FROM items ORDER BY id", [[1, 11], [2, 222], [3, 30]])
        for sql in [
            "BEGIN",
            "UPDATE items SET value=12 WHERE id=1",
            "SAVEPOINT edit",
            "UPDATE items SET value=99 WHERE id=1",
            "ROLLBACK TO edit",
            "RELEASE edit",
            "COMMIT",
        ]:
            first.sql(sql)
        second.rows("SELECT id,value FROM items ORDER BY id", [[1, 12], [2, 222], [3, 30]])
        first.close()
        second.close()

    def conflicts_and_ids(self):
        first, second = [Worker(self, name) for name in ["conflict-a", "conflict-b"]]
        cases = [
            ("UPDATE items SET value=13 WHERE id=1", "UPDATE items SET value=14 WHERE id=1", (5, 6)),
            ("DELETE FROM items WHERE id=3", "UPDATE items SET value=31 WHERE id=3", (5, 6)),
            (
                "INSERT INTO items(id,label,value) VALUES(200,'collision',1)",
                "INSERT INTO items(id,label,value) VALUES(201,'collision',2)",
                (5, 6, 19),
            ),
        ]
        for winning, conflicting, allowed in cases:
            for worker in [first, second]:
                worker.sql("BEGIN")
                worker.sql("SELECT id FROM items ORDER BY id")
            first.sql(winning)
            result = second.sql(conflicting, ok=False)
            first.sql("COMMIT")
            if result["ok"]:
                result = second.sql("COMMIT", ok=False)
            second.bounded_error(result, allowed)
            second.sql("ROLLBACK")
        first.rows(
            "SELECT id,label,value FROM items ORDER BY id", [[1, "one", 13], [2, "two", 222], [200, "collision", 1]]
        )
        for worker in [first, second]:
            worker.sql("BEGIN")
        first.sql("INSERT INTO items(label,value) VALUES('auto-a',1)")
        first_id = first.sql("SELECT last_insert_rowid()")["rows"][0][0]
        result = second.sql("INSERT INTO items(label,value) VALUES('auto-b',2)", ok=False)
        if self.mode == "shared":
            assert result["ok"], result
            second_id = second.sql("SELECT last_insert_rowid()")["rows"][0][0]
            assert first_id != second_id
            first.sql("COMMIT")
        else:
            second.bounded_error(result)
            second.sql("ROLLBACK")
            first.sql("COMMIT")
            second.sql("BEGIN")
            second.sql("INSERT INTO items(label,value) VALUES('auto-b',2)")
            second_id = second.sql("SELECT last_insert_rowid()")["rows"][0][0]
        second.sql("COMMIT")
        assert first_id != second_id and min(first_id, second_id) > 200
        self.expected_ids = sorted([first_id, second_id])
        first.close()
        second.close()
        fresh = Worker(self, "ids-reopened")
        fresh.rows(
            "SELECT id FROM items WHERE label IN ('auto-a','auto-b') ORDER BY id", [[i] for i in self.expected_ids]
        )
        sequence = fresh.sql("SELECT seq FROM sqlite_sequence WHERE name='items'")["rows"][0][0]
        assert sequence >= max(self.expected_ids)
        fresh.sql("INSERT INTO items(label,value) VALUES('after-reopen',3)")
        assert fresh.sql("SELECT last_insert_rowid()")["rows"][0][0] > max(self.expected_ids)
        fresh.close()

    def permissions(self):
        attached_path = self.output / "attached.db"
        setup = Worker(self, "attachment-setup", path=attached_path, mode="wal")
        setup.sql("CREATE TABLE attached_value(x)")
        setup.sql("INSERT INTO attached_value VALUES(7)")
        setup.close()
        before = self.durable_bytes()
        readonly = Worker(self, "readonly", readonly=True)
        readonly.rows("SELECT value FROM items WHERE id=1", [[13]])
        error = readonly.sql("UPDATE items SET value=99 WHERE id=1", ok=False)
        assert not error["ok"] and error["code"] & 255 == 8, error
        readonly.sql("CREATE TEMP TABLE private_temp(x)")
        readonly.sql("INSERT INTO private_temp VALUES(42)")
        readonly.rows("SELECT x FROM private_temp", [[42]])
        uri = "file:" + urllib.parse.quote(str(attached_path), safe="/") + "?mode=ro"
        readonly.sql("ATTACH DATABASE ? AS auxiliary", [uri])
        readonly.rows("SELECT x FROM auxiliary.attached_value", [[7]])
        error = readonly.sql("UPDATE auxiliary.attached_value SET x=8", ok=False)
        assert not error["ok"] and error["code"] & 255 == 8, error
        readonly.sql("DETACH DATABASE auxiliary")
        readonly.close()
        after = self.durable_bytes()
        self.record("readonly_bytes", before=before, after=after)
        if self.args.engine == "sqlite":
            assert all(after.get(path) == digest for path, digest in before.items())
            assert all(
                Path(path).name.endswith("-wal") and Path(path).stat().st_size == 0
                for path in after.keys() - before.keys()
            ), "unexpected SQLite control sidecar write"
        else:
            assert after == before, "read-only operations changed durable files"

    def durable_bytes(self):
        paths = []
        for root in [self.database, self.output / "attached.db"]:
            paths.extend([root, Path(str(root) + "-wal"), Path(str(root) + "-log")])
            if root.exists():
                identity = root.stat()
                directory = root.parent / f".turso-mvcc-{identity.st_dev:016x}-{identity.st_ino:016x}"
                durable = [directory / name for name in ["manifest", "logical", "physical-wal"]]
                if self.mode == "shared" and root == self.database:
                    assert all(path.is_file() for path in durable), "shared durable files are missing"
                paths.extend(durable)
        return {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths if path.exists()}

    def single_worker_control(self):
        worker = Worker(self, "single-worker")
        worker.rows("SELECT count(*) FROM items", [[3]])
        for sql in [
            "BEGIN",
            "UPDATE items SET value=11 WHERE id=1",
            "SAVEPOINT edit",
            "UPDATE items SET value=99 WHERE id=1",
            "ROLLBACK TO edit",
            "RELEASE edit",
        ]:
            worker.sql(sql)
        worker.rows("SELECT value FROM items WHERE id=1", [[11]])
        worker.sql("ROLLBACK")
        worker.rows("SELECT value FROM items WHERE id=1", [[10]])
        worker.close()

    def refuse_implicit_upgrade(self):
        fixture = self.output / "legacy-fixture"
        fixture.mkdir()
        path = fixture / "existing-ordinary.db"
        ordinary = Worker(self, "ordinary-existing", path=path, mode="wal")
        ordinary.sql("CREATE TABLE preserved(x)")
        ordinary.sql("INSERT INTO preserved VALUES(42)")
        ordinary.close()
        before = snapshot_tree(fixture)
        try:
            Worker(self, "implicit-upgrade-rejected", path=path, mode="shared")
        except (UnsupportedMode, StartupRejected) as error:
            self.workers[-1].process.wait(timeout=self.args.deadline)
            self.record("implicit_upgrade_rejected", reason=str(error))
        else:
            raise AssertionError("existing ordinary database was silently upgraded")
        assert snapshot_tree(fixture) == before, "refused upgrade changed persistent fixture subtree"

    def crash_recovery(self, hold_reader=True):
        reader = Worker(self, "crash-long-reader") if hold_reader else None
        if reader:
            reader.sql("BEGIN")
            reader.rows("SELECT count(*) FROM pairs", [[0]])
        outcomes = {}
        for case in ["acknowledged", "before-commit", "lost-ack", "commit-start"]:
            transaction = f"seed-{self.args.seed}-{case}"
            rows = [[transaction, 0, "first:" + transaction], [transaction, 1, "second:" + transaction]]
            self.acknowledge("intent", transaction, rows)
            writer = Worker(self, "crash-" + case)
            writer.sql("BEGIN")
            writer.sql("INSERT INTO pairs VALUES(?,?,?)", rows[0])
            if case == "before-commit":
                writer.kill()
                outcomes[transaction] = "absent"
                continue
            writer.sql("INSERT INTO pairs VALUES(?,?,?)", rows[1])
            if case == "acknowledged":
                writer.sql("COMMIT")
                self.acknowledge("ack", transaction, rows)
                writer.kill()
                outcomes[transaction] = "complete"
                if reader:
                    reader.rows("SELECT count(*) FROM pairs", [[0]])
            elif case == "lost-ack":
                writer.send(op="sql_then_exit", sql="COMMIT")
                assert writer.process.wait(timeout=self.args.deadline) == -signal.SIGKILL
                self.record("lost_ack", transaction=transaction, crash_point="FULL commit returned before reply")
                outcomes[transaction] = "complete"
            else:
                writer.send(op="sql_with_start", sql="COMMIT")
                event = writer.receive()
                assert event == {"event": "executing", "id": writer.pending}, event
                writer.kill()
                self.record(
                    "unreported_commit",
                    transaction=transaction,
                    crash_point="command execution announced; IO phase unknown",
                )
                outcomes[transaction] = "complete-or-absent"
        if reader:
            reader.rows("SELECT count(*) FROM pairs", [[0]])
            reader.kill()
        ledger = [json.loads(line) for line in (self.output / "acknowledgments.jsonl").read_text().splitlines()]
        recovered = Worker(self, "fresh-recovery")
        actual = recovered.sql("SELECT txn,slot,payload FROM pairs ORDER BY txn,slot")["rows"]
        acknowledged = verify_recovery(ledger, outcomes, actual)
        recovered.rows("PRAGMA integrity_check", [["ok"]])
        recovered.close()
        self.record("recovery_verified", acknowledged=sorted(acknowledged), outcomes=outcomes)

    def close(self):
        for worker in self.workers:
            if worker.process.poll() is None:
                worker.kill()
            worker.stderr.close()
        self.ledger.close()
        self.events.close()


def verify_recovery(ledger, outcomes, actual):
    intents = {}
    acknowledged = set()
    for entry in ledger:
        transaction = entry["transaction"]
        if entry["kind"] == "intent":
            assert transaction not in intents, "duplicate transaction intent"
            intents[transaction] = entry["rows"]
        else:
            assert entry["kind"] == "ack" and transaction in intents
            assert entry["rows"] == intents[transaction], "acknowledged payload differs from intent"
            assert transaction not in acknowledged, "duplicate acknowledgment"
            acknowledged.add(transaction)
    assert set(outcomes) == set(intents)
    assert {row[0] for row in actual} <= set(intents), actual
    for transaction, expected in intents.items():
        rows = [row for row in actual if row[0] == transaction]
        outcome = outcomes[transaction]
        if transaction in acknowledged or outcome == "complete":
            assert rows == expected, (transaction, rows, expected)
        elif outcome == "absent":
            assert rows == [], rows
        else:
            assert outcome == "complete-or-absent"
            assert rows in [[], expected], ("partial unreported transaction", rows, expected)
    return acknowledged


def snapshot_tree(directory):
    return {
        str(path.relative_to(directory)): "directory"
        if path.is_dir()
        else hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(directory.rglob("*"))
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--profile",
        choices=[
            "sqlite-wal-control",
            "sqlite-shared-negative-control",
            "turso-single-worker-control",
            "turso-wal-control",
            "shared-acceptance",
        ],
        required=True,
    )
    parser.add_argument("--api", choices=["pdo", "sqlite3"], default="pdo")
    parser.add_argument("--php", default="php")
    parser.add_argument("--library", default="")
    parser.add_argument(
        "--shared-admin-cli", default="", help="tested tursodb binary for explicit fixture initialization"
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--busy-ms", type=int, default=100)
    parser.add_argument("--deadline", type=float, default=5)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument(
        "--shared-open-flag",
        type=lambda value: int(value, 0),
        default=0,
        help="explicit test-only vendor flag; does not implement or imply capability",
    )
    args = parser.parse_args()
    args.engine = "sqlite" if args.profile.startswith("sqlite-") else "turso"
    if args.engine == "turso" and not args.library:
        parser.error("Turso profiles require --library with exact loaded shared library")
    if not 0 <= args.busy_ms <= 1000 or not 1 <= args.deadline <= 30:
        parser.error("busy timeout/deadline outside bounded test range")
    if args.shared_open_flag < 0 or args.shared_open_flag > 0x7FFFFFFF or args.shared_open_flag & 0x7:
        parser.error("vendor flag must fit positive C int and not change access flags")
    if args.profile == "shared-acceptance" and (args.api != "pdo" or args.shared_open_flag):
        parser.error("initial shared-mode acceptance is PDO URI only; SQLite3 flags are diagnostic, unsupported API")
    if args.profile == "shared-acceptance" and not args.shared_admin_cli:
        parser.error("shared acceptance requires --shared-admin-cli for explicit offline initialization")
    suite = Suite(args)
    try:
        suite.run()
        print(
            json.dumps({"status": "passed", "profile": args.profile, "acceptance": args.profile == "shared-acceptance"})
        )
    except UnsupportedMode as error:
        suite.record("unsupported", reason=str(error), acceptance=False)
        print(json.dumps({"status": "unsupported", "acceptance": False, "reason": str(error)}))
        return 78
    finally:
        suite.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
