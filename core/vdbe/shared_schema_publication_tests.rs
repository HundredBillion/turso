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
        if let Some(sql) = line.strip_prefix("EXEC ") {
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
