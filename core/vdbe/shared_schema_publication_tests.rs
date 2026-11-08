use super::*;
use std::io::{BufRead, Write};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_schema_publication_native_independent_stores() {
    publication_orders(false, false);
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_schema_publication_native_independent_processes() {
    publication_orders(true, false);
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_schema_publication_native_sequence_domains() {
    publication_orders(false, true);
    publication_orders(true, true);
}

fn publication_orders(processes: bool, sequence: bool) {
    for same_name in [false, true] {
        for first_commit in [0, 1] {
            eprintln!("schema publication processes={processes}, same_name={same_name}, first_commit={first_commit}, sequence={sequence}");
            let directory = tempfile::tempdir().unwrap();
            let (path, limit) = crate::mvcc::database::shared_runtime_fixture(directory.path());
            let mut workers = [
                Worker::open(&path, limit, processes),
                Worker::open(&path, limit, processes),
            ];
            let names = [
                "created_a",
                if same_name { "created_a" } else { "created_b" },
            ];
            for worker in &mut workers {
                assert_eq!(worker.execute("BEGIN CONCURRENT"), "OK");
                assert_eq!(worker.query("SELECT count(*) FROM preserved"), "[1]");
            }
            for (index, worker) in workers.iter_mut().enumerate() {
                if sequence {
                    assert_eq!(
                        worker.execute(&format!(
                            "CREATE SEQUENCE {} START {}",
                            names[index],
                            index + 10
                        )),
                        "OK"
                    );
                } else {
                    assert_eq!(
                        worker.execute(&format!(
                            "CREATE TABLE {}(id INTEGER PRIMARY KEY, value INTEGER)",
                            names[index]
                        )),
                        "OK"
                    );
                    assert_eq!(
                        worker.execute(&format!(
                            "INSERT INTO {} VALUES(1,{})",
                            names[index],
                            index + 10
                        )),
                        "OK"
                    );
                }
            }
            // Both workers acknowledge staged DDL before either is permitted to commit.
            let mut acknowledged = [false; 2];
            for index in [first_commit, 1 - first_commit] {
                let result = workers[index].execute("COMMIT");
                eprintln!("schema COMMIT worker={index}: {result}");
                acknowledged[index] = result == "OK";
                if !acknowledged[index] {
                    assert!(
                        [
                            "ERR Busy",
                            "ERR SchemaConflict",
                            "ERR SchemaUpdated",
                            "ERR WriteWriteConflict"
                        ]
                        .iter()
                        .any(|expected| result.starts_with(expected)),
                        "unexpected commit error: {result}"
                    );
                    workers[index].execute("ROLLBACK");
                }
            }
            assert!(acknowledged[first_commit], "first publisher did not commit");
            assert!(
                !same_name || acknowledged.iter().filter(|ack| **ack).count() == 1,
                "two same-name schema commits acknowledged"
            );
            let mut observer = Worker::open(&path, limit, processes);
            verify_catalog(&mut observer, names, acknowledged, sequence);
            drop(observer);
            for worker in &mut workers {
                verify_catalog(worker, names, acknowledged, sequence);
            }
            drop(workers);
            // A new process/store must reconstruct exactly the acknowledged catalog and data.
            let mut reopened = Worker::open(&path, limit, processes);
            verify_catalog(&mut reopened, names, acknowledged, sequence);
        }
    }
}

fn verify_catalog(worker: &mut Worker, names: [&str; 2], acknowledged: [bool; 2], sequence: bool) {
    let count = acknowledged.iter().filter(|ack| **ack).count();
    assert_eq!(
        worker.query(
            "SELECT count(*) FROM sqlite_schema WHERE type='table' AND name LIKE '%created_%'"
        ),
        format!("[{count}]")
    );
    assert_eq!(worker.query("SELECT count(*) FROM (SELECT name FROM sqlite_schema WHERE type='table' GROUP BY name HAVING count(*)>1)"), "[0]");
    for (index, accepted) in acknowledged.into_iter().enumerate() {
        if accepted {
            let query = if sequence {
                format!("SELECT sequence_watermark_experimental('{}')", names[index])
            } else {
                format!("SELECT value FROM {} WHERE id=1", names[index])
            };
            assert_eq!(worker.query(&query), format!("[{}]", index + 10));
        } else if names[0] != names[1] {
            assert_eq!(
                worker.query(&format!(
                    "SELECT count(*) FROM sqlite_schema WHERE name LIKE '%{}'",
                    names[index]
                )),
                "[0]"
            );
        }
    }
    assert_eq!(worker.query("SELECT count(*) FROM preserved"), "[1]");
}

enum Worker {
    Local(Arc<Connection>),
    Process(ProcessWorker),
}
impl Worker {
    fn open(path: &std::path::Path, limit: u64, process: bool) -> Self {
        if process {
            Self::Process(ProcessWorker::spawn(path, limit))
        } else {
            Self::Local(open_connection(path, limit))
        }
    }
    fn execute(&mut self, sql: &str) -> String {
        match self {
            Self::Local(connection) => execute(connection, sql),
            Self::Process(child) => child.request(&format!("EXEC {sql}")),
        }
    }
    fn integrity(&mut self) -> String {
        match self {
            Self::Local(connection) => integrity(connection),
            Self::Process(child) => child.request("CHECK"),
        }
    }
    fn query(&mut self, sql: &str) -> String {
        match self {
            Self::Local(connection) => format!("{:?}", integers(connection, sql)),
            Self::Process(child) => child.request(&format!("QUERY {sql}")),
        }
    }
}
fn open_connection(path: &std::path::Path, limit: u64) -> Arc<Connection> {
    Database::open(
        Arc::new(crate::io::SyscallIO::new().unwrap()),
        path.to_str().unwrap(),
        OpenOptions::new(Arc::new(EngineDialect)).with_shared_mvcc_for_test(limit),
    )
    .unwrap()
    .connect(ConnectOpts::new())
    .unwrap()
}
fn integrity(connection: &Arc<Connection>) -> String {
    let mut rows = Vec::new();
    connection
        .prepare("PRAGMA integrity_check")
        .unwrap()
        .run_with_row_callback(|row| {
            rows.push(row.get::<&str>(0)?.to_owned());
            Ok(())
        })
        .unwrap();
    format!("{rows:?}")
}
fn execute(connection: &Arc<Connection>, sql: &str) -> String {
    match connection.execute(sql) {
        Ok(_) => "OK".into(),
        Err(error) => format!("ERR {error:?}"),
    }
}

#[test]
#[ignore = "subprocess helper; requires parent-provided fixture"]
fn shared_schema_publication_child() {
    let path = std::env::var("TURSO_SCHEMA_CHILD_PATH").unwrap();
    let limit = std::env::var("TURSO_SCHEMA_CHILD_LIMIT")
        .unwrap()
        .parse()
        .unwrap();
    let connection = open_connection(std::path::Path::new(&path), limit);
    let reply = |value: &str| {
        println!("SCHEMA_CHILD {value}");
        std::io::stdout().flush().unwrap();
    };
    reply("READY");
    for line in std::io::stdin().lock().lines() {
        let line = line.unwrap();
        if line == "CHECK" {
            reply(&integrity(&connection));
        } else if let Some(sql) = line.strip_prefix("EXEC ") {
            reply(&execute(&connection, sql));
        } else if let Some(sql) = line.strip_prefix("QUERY ") {
            reply(&format!("{:?}", integers(&connection, sql)));
        } else {
            panic!("unknown supervisor command");
        }
    }
}

struct ProcessWorker {
    child: Child,
    output: Receiver<std::io::Result<String>>,
}
impl ProcessWorker {
    fn spawn(path: &std::path::Path, limit: u64) -> Self {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "vdbe::shared_id_tests::shared_schema_publication_tests::shared_schema_publication_child", "--ignored", "--nocapture", "--test-threads=1"])
            .env("TURSO_SCHEMA_CHILD_PATH", path)
            .env("TURSO_SCHEMA_CHILD_LIMIT", limit.to_string())
            .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::inherit()).spawn().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (send, output) = mpsc::channel();
        std::thread::spawn(move || {
            for line in std::io::BufReader::new(stdout).lines() {
                if send.send(line).is_err() {
                    break;
                }
            }
        });
        let result = Self { child, output };
        assert_eq!(result.receive(), "READY");
        result
    }
    fn request(&mut self, command: &str) -> String {
        let stdin = self.child.stdin.as_mut().unwrap();
        writeln!(stdin, "{command}").unwrap();
        stdin.flush().unwrap();
        self.receive()
    }
    fn receive(&self) -> String {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let line = self
                .output
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .expect("worker response before deadline")
                .unwrap();
            if let Some((_, value)) = line.split_once("SCHEMA_CHILD ") {
                return value.to_owned();
            }
        }
    }
}
impl Drop for ProcessWorker {
    fn drop(&mut self) {
        self.child.stdin.take();
        let deadline = Instant::now() + Duration::from_secs(5);
        while matches!(self.child.try_wait(), Ok(None)) && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let deadline = Instant::now() + Duration::from_secs(5);
            while matches!(self.child.try_wait(), Ok(None)) && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_worker_native_update_conflicts() {
    worker_conflicts("update");
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_worker_native_delete_conflicts() {
    worker_conflicts("delete");
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_worker_native_unique_conflicts() {
    worker_conflicts("unique");
}

fn worker_conflicts(conflict: &str) {
    for winner in [0, 1] {
        eprintln!("worker conflict={conflict}, winner={winner}");
        let directory = tempfile::tempdir().unwrap();
        let (path, limit) = crate::mvcc::database::shared_runtime_fixture(directory.path());
        let mut setup = Worker::open(&path, limit, true);
        assert_eq!(setup.execute("CREATE TABLE worker_rows(id INTEGER PRIMARY KEY, value INTEGER, tag TEXT); CREATE UNIQUE INDEX worker_tag ON worker_rows(tag); INSERT INTO worker_rows VALUES(1,100,'original')"), "OK");
        drop(setup);
        let mut workers = [
            Worker::open(&path, limit, true),
            Worker::open(&path, limit, true),
        ];
        for worker in &mut workers {
            assert_eq!(worker.execute("BEGIN CONCURRENT"), "OK");
            assert_eq!(
                worker.query("SELECT value FROM worker_rows WHERE id=1"),
                "[100]"
            );
        }
        let sql = match conflict {
            "update" => [
                "UPDATE worker_rows SET value=10 WHERE id=1",
                "UPDATE worker_rows SET value=20 WHERE id=1",
            ],
            "delete" => [
                "DELETE FROM worker_rows WHERE id=1",
                "UPDATE worker_rows SET value=20 WHERE id=1",
            ],
            "unique" => [
                "INSERT INTO worker_rows VALUES(10,10,'duplicate')",
                "INSERT INTO worker_rows VALUES(20,20,'duplicate')",
            ],
            _ => unreachable!(),
        };
        let loser = 1 - winner;
        let first = workers[winner].execute(sql[winner]);
        eprintln!("staging winner={winner}: {first}");
        assert_eq!(first, "OK", "first conflicting write was rejected");
        let second = workers[loser].execute(sql[loser]);
        eprintln!("staging loser={loser}: {second}");
        if conflict == "unique" {
            assert_eq!(
                second, "OK",
                "distinct-primary-key inserts must reach unique-index validation"
            );
        }
        assert_eq!(workers[winner].execute("COMMIT"), "OK");
        let error = if second == "OK" {
            workers[loser].execute("COMMIT")
        } else {
            second
        };
        eprintln!("conflict result: {error}");
        assert!(
            ["ERR WriteWriteConflict", "ERR Busy", "ERR Constraint"]
                .iter()
                .any(|prefix| error.starts_with(prefix)),
            "conflicting write was not rejected: {error}"
        );
        workers[loser].execute("ROLLBACK");
        drop(workers);
        let mut fresh = Worker::open(&path, limit, true);
        assert_eq!(fresh.integrity(), "[\"ok\"]");
        match conflict {
            "unique" => {
                assert_eq!(
                    fresh.query("SELECT count(*) FROM worker_rows WHERE tag='duplicate'"),
                    "[1]"
                );
                assert_eq!(
                    fresh.query("SELECT id FROM worker_rows WHERE tag='duplicate'"),
                    format!("[{}]", (winner + 1) * 10)
                );
                assert_eq!(fresh.query("SELECT count(*) FROM worker_rows"), "[2]");
            }
            "delete" if winner == 0 => {
                assert_eq!(fresh.query("SELECT count(*) FROM worker_rows"), "[0]")
            }
            _ => assert_eq!(
                fresh.query("SELECT value FROM worker_rows WHERE id=1"),
                format!("[{}]", (winner + 1) * 10)
            ),
        }
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_worker_native_snapshot_multirow_ack_and_rolling_restart() {
    let directory = tempfile::tempdir().unwrap();
    let (path, limit) = crate::mvcc::database::shared_runtime_fixture(directory.path());
    let mut setup = Worker::open(&path, limit, true);
    assert_eq!(setup.execute("CREATE TABLE balances(id INTEGER PRIMARY KEY, amount INTEGER); INSERT INTO balances VALUES(1,100),(2,100); CREATE TABLE transfers(id INTEGER PRIMARY KEY, amount INTEGER)"), "OK");
    drop(setup);
    let main_before = std::fs::read(&path).unwrap();
    let ledger_path = directory.path().join("external-ack-ledger");
    let mut ledger = std::fs::File::create(&ledger_path).unwrap();
    ledger.sync_all().unwrap();
    std::fs::File::open(directory.path())
        .unwrap()
        .sync_all()
        .unwrap();
    let mut old_reader = Worker::open(&path, limit, true);
    assert_eq!(old_reader.execute("BEGIN CONCURRENT"), "OK");
    assert_eq!(
        old_reader.query("SELECT amount FROM balances ORDER BY id"),
        "[100, 100]"
    );
    for sequence in 1..=3 {
        let mut writer = Worker::open(&path, limit, true);
        assert_eq!(writer.execute("PRAGMA synchronous=FULL"), "OK");
        assert_eq!(writer.execute("BEGIN CONCURRENT"), "OK");
        assert_eq!(writer.execute(&format!("UPDATE balances SET amount=amount-10 WHERE id=1; UPDATE balances SET amount=amount+10 WHERE id=2; INSERT INTO transfers VALUES({sequence},10)")), "OK");
        assert_eq!(
            old_reader.query("SELECT amount FROM balances ORDER BY id"),
            "[100, 100]"
        );
        assert_eq!(old_reader.query("SELECT count(*) FROM transfers"), "[0]");
        assert_eq!(writer.execute("COMMIT"), "OK");
        writeln!(ledger, "{sequence} 10").unwrap();
        ledger.sync_all().unwrap();
        if sequence == 2 {
            let Worker::Process(child) = &mut writer else {
                unreachable!()
            };
            child.child.kill().unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            let status = loop {
                if let Some(status) = child.child.try_wait().unwrap() {
                    break status;
                }
                assert!(Instant::now() < deadline, "acknowledged worker did not die");
                std::thread::sleep(Duration::from_millis(10));
            };
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(status.signal(), Some(9));
        }
        drop(writer);
        assert_eq!(
            old_reader.query("SELECT amount FROM balances ORDER BY id"),
            "[100, 100]"
        );
        let mut fresh = Worker::open(&path, limit, true);
        assert_eq!(
            fresh.query("SELECT amount FROM balances ORDER BY id"),
            format!("[{}, {}]", 100 - sequence * 10, 100 + sequence * 10)
        );
        assert_eq!(
            fresh.query("SELECT count(*) FROM transfers"),
            format!("[{sequence}]")
        );
        assert_eq!(fresh.query("SELECT sum(amount) FROM balances"), "[200]");
    }
    assert_eq!(old_reader.execute("COMMIT"), "OK");
    drop(old_reader);
    drop(ledger);
    let acknowledged = std::fs::read_to_string(ledger_path).unwrap();
    let rows: Vec<_> = acknowledged.lines().collect();
    assert_eq!(rows, ["1 10", "2 10", "3 10"]);
    let mut recovered = Worker::open(&path, limit, true);
    assert_eq!(recovered.integrity(), "[\"ok\"]");
    assert_eq!(
        recovered.query("SELECT amount FROM balances ORDER BY id"),
        "[70, 130]"
    );
    assert_eq!(
        recovered.query("SELECT id FROM transfers ORDER BY id"),
        "[1, 2, 3]"
    );
    assert_eq!(recovered.query("SELECT sum(amount) FROM transfers"), "[30]");
    assert_eq!(
        std::fs::read(&path).unwrap(),
        main_before,
        "test crossed a physical checkpoint boundary"
    );
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_worker_native_disjoint_updates_delete_and_insert_both_commit() {
    for first_commit in [0, 1] {
        let directory = tempfile::tempdir().unwrap();
        let (path, limit) = crate::mvcc::database::shared_runtime_fixture(directory.path());
        let mut setup = Worker::open(&path, limit, true);
        assert_eq!(setup.execute("CREATE TABLE disjoint(id INTEGER PRIMARY KEY, value INTEGER); INSERT INTO disjoint VALUES(1,100),(2,200),(3,300)"), "OK");
        drop(setup);
        let mut workers = [
            Worker::open(&path, limit, true),
            Worker::open(&path, limit, true),
        ];
        for worker in &mut workers {
            assert_eq!(worker.execute("BEGIN CONCURRENT"), "OK");
            assert_eq!(worker.query("SELECT count(*) FROM disjoint"), "[3]");
        }
        assert_eq!(
            workers[0].execute(
                "UPDATE disjoint SET value=101 WHERE id=1; DELETE FROM disjoint WHERE id=3"
            ),
            "OK"
        );
        assert_eq!(
            workers[1].execute(
                "UPDATE disjoint SET value=202 WHERE id=2; INSERT INTO disjoint VALUES(4,404)"
            ),
            "OK"
        );
        for index in [first_commit, 1 - first_commit] {
            assert_eq!(workers[index].execute("COMMIT"), "OK");
        }
        drop(workers);
        let mut fresh = Worker::open(&path, limit, true);
        assert_eq!(
            fresh.query("SELECT id FROM disjoint ORDER BY id"),
            "[1, 2, 4]"
        );
        assert_eq!(
            fresh.query("SELECT value FROM disjoint ORDER BY id"),
            "[101, 202, 404]"
        );
        assert_eq!(fresh.integrity(), "[\"ok\"]");
    }
}
