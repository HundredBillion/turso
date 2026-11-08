use super::shared_checkpoint::PublishedSharedCheckpoint;
use super::*;
use crate::mvcc::commit_manifest::{ManifestRecord, RECORD_SIZE};
use std::io::{BufRead, Write};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_idle_generation_native_reloads_after_checkpoint_and_later_commits() {
    for role in ["idle", "constructor"] {
        run_idle_generation_case(role);
    }
}

fn run_idle_generation_case(role: &str) {
    let old_process = Mutex::new(None);
    let fixture = shared_completed_checkpoint_fixture(|path| {
        let child = IdleGenerationChild::spawn(path, 64 * 1024 * 1024, role);
        let ready = child.receive();
        assert!(
            ready.starts_with("READY "),
            "old process failed to initialize: {ready}"
        );
        *old_process.lock() = Some((child, ready));
        Arc::new(crate::io::SyscallIO::new().unwrap())
    });
    assert_eq!(fixture.limit, 64 * 1024 * 1024);
    let (mut old, old_ready) = old_process.lock().take().unwrap();
    let source = fixture.proof.source_identity().unwrap();
    let target = fixture.proof.target_identity();
    assert_eq!(
        old_ready,
        format!("READY {} durable payload", source.database.generation)
    );
    assert!(target.database.generation > source.database.generation);
    let store = fixture.database.get_mv_store().as_ref().unwrap().clone();
    let runtime = store.shared_runtime.get().unwrap().clone();
    let mut finalizer = fixture.ids.finalize(fixture.guard, fixture.proof).unwrap();
    let finalized = fixture.io.block(|| Ok(finalizer.poll()?)).unwrap();
    let published = PublishedSharedCheckpoint {
        finalized,
        pause: fixture.pause,
    };
    let mut reading = published
        .begin_physical_read(store.clone(), fixture.connection.clone())
        .unwrap();
    let verified = fixture.io.block(|| reading.poll()).unwrap();
    let mut installing = verified
        .begin_local_install(store.clone(), fixture.connection.clone())
        .unwrap();
    let installed = fixture.io.block(|| installing.poll()).unwrap();
    installed.publish_ready(fixture.connection.clone()).unwrap();
    fixture
        .connection
        .execute("UPDATE preserved SET value='after first commit' WHERE id=1")
        .unwrap();
    let mut reader = IdleGenerationChild::spawn(&fixture.path, fixture.limit, "hold");
    assert_eq!(
        reader.receive(),
        format!("READY {} after first commit", target.database.generation)
    );
    fixture
        .connection
        .execute("UPDATE preserved SET value='after second commit' WHERE id=1")
        .unwrap();
    let image = std::fs::read(fixture.namespace.join("manifest")).unwrap();
    assert!(image.len() >= 2 * RECORD_SIZE);
    for slot in image[..2 * RECORD_SIZE].chunks_exact(RECORD_SIZE) {
        let record = ManifestRecord::decode(slot).unwrap().unwrap();
        assert_eq!(record.log_generation(), target.database.generation);
        assert!(!record.requires_recovery());
        assert!(
            record.prefix().end_offset > 0,
            "empty completed record was not overwritten"
        );
    }
    let guard = runtime.files.authority.try_acquire().unwrap();
    let prefix = guard.published().unwrap();
    assert!(prefix.committed_timestamp > target.database.checkpoint_timestamp);
    let foreign_snapshots = guard.live_registrations().unwrap();
    assert_eq!(foreign_snapshots.len(), 1);
    guard.finish().unwrap();
    let bytes = durable_files(fixture.directory.path());
    old.send("read");
    assert_eq!(
        old.receive(),
        "VALUE after second commit",
        "idle old-generation process failed to refresh a nonempty newer generation"
    );
    reader.send("read");
    assert_eq!(
        reader.receive(),
        "VALUE after first commit",
        "refresh changed a foreign reader snapshot"
    );
    let guard = runtime.files.authority.try_acquire().unwrap();
    assert_eq!(
        guard.published().unwrap(),
        prefix,
        "local reload rewound publication"
    );
    assert_eq!(
        guard.live_registrations().unwrap(),
        foreign_snapshots,
        "local reload changed foreign registrations"
    );
    guard.finish().unwrap();
    assert_eq!(
        durable_files(fixture.directory.path()),
        bytes,
        "read-only local reload changed durable files"
    );
    old.send("exit");
    reader.send("exit");
    assert!(old.wait().success());
    assert!(reader.wait().success());
}

#[test]
#[ignore = "subprocess helper; requires a parent-provided shared fixture"]
fn shared_idle_generation_native_child() {
    let path = std::env::var("TURSO_IDLE_GENERATION_PATH").unwrap();
    let limit = std::env::var("TURSO_IDLE_GENERATION_LIMIT")
        .unwrap()
        .parse()
        .unwrap();
    let role = std::env::var("TURSO_IDLE_GENERATION_ROLE").unwrap();
    let hold = role == "hold";
    let io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
    let database = crate::Database::open(
        io,
        &path,
        crate::OpenOptions::new(Arc::new(crate::EngineDialect))
            .flags(if hold {
                crate::OpenFlags::ReadOnly
            } else {
                crate::OpenFlags::None
            })
            .with_shared_mvcc_for_test(limit),
    )
    .unwrap();
    let connection = database.connect(crate::ConnectOpts::new()).unwrap();
    if hold {
        connection.execute("BEGIN CONCURRENT").unwrap();
    }
    let value = read_value(&connection).unwrap();
    let store = database.get_mv_store().as_ref().unwrap().clone();
    let runtime = store.shared_runtime.get().unwrap();
    let generation = runtime
        .replay
        .lock()
        .as_ref()
        .unwrap()
        .identity()
        .unwrap()
        .database
        .generation;
    if !hold {
        assert!(store.txs.is_empty());
    }
    reply(&format!("READY {generation} {value}"));
    for line in std::io::stdin().lock().lines() {
        match line.unwrap().as_str() {
            "read" => match if role == "constructor" {
                database
                    .connect(crate::ConnectOpts::new())
                    .and_then(|fresh| read_value(&fresh))
            } else {
                read_value(&connection)
            } {
                Ok(value) => reply(&format!("VALUE {value}")),
                Err(error) => reply(&format!("ERROR {error:?}")),
            },
            "exit" => {
                if hold {
                    connection.execute("ROLLBACK").unwrap();
                }
                break;
            }
            other => panic!("unknown parent command: {other}"),
        }
    }
}

fn read_value(connection: &Arc<Connection>) -> Result<String> {
    let mut values = Vec::new();
    connection
        .prepare("SELECT value FROM preserved WHERE id=1")?
        .run_with_row_callback(|row| {
            values.push(row.get::<&str>(0)?.to_owned());
            Ok(())
        })?;
    assert_eq!(values.len(), 1);
    Ok(values.pop().unwrap())
}
fn reply(message: &str) {
    println!("IDLE_GENERATION {message}");
    std::io::stdout().flush().unwrap();
}
fn durable_files(root: &Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
    fn visit(root: &Path, path: &Path, files: &mut std::collections::BTreeMap<PathBuf, Vec<u8>>) {
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                visit(root, &path, files);
            } else if !matches!(
                path.file_name().and_then(|name| name.to_str()),
                Some("authority" | "physical-tshm")
            ) {
                files.insert(
                    path.strip_prefix(root).unwrap().to_owned(),
                    std::fs::read(path).unwrap(),
                );
            }
        }
    }
    let mut files = std::collections::BTreeMap::new();
    visit(root, root, &mut files);
    files
}
struct IdleGenerationChild {
    child: Child,
    output: Receiver<std::io::Result<String>>,
}
impl IdleGenerationChild {
    fn spawn(path: &Path, limit: u64, role: &str) -> Self {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "mvcc::database::shared_runtime::shared_idle_generation_tests::shared_idle_generation_native_child", "--ignored", "--nocapture", "--test-threads=1"])
            .env("TURSO_IDLE_GENERATION_PATH", path)
            .env("TURSO_IDLE_GENERATION_LIMIT", limit.to_string())
            .env("TURSO_IDLE_GENERATION_ROLE", role)
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
        Self { child, output }
    }
    fn send(&mut self, command: &str) {
        let stdin = self.child.stdin.as_mut().unwrap();
        writeln!(stdin, "{command}").unwrap();
        stdin.flush().unwrap();
    }
    fn receive(&self) -> String {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let line = self
                .output
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .expect("child acknowledgement before deadline")
                .unwrap();
            if let Some((_, message)) = line.split_once("IDLE_GENERATION ") {
                return message.to_owned();
            }
        }
    }
    fn wait(&mut self) -> std::process::ExitStatus {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "child did not exit before deadline"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
impl Drop for IdleGenerationChild {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let deadline = Instant::now() + Duration::from_secs(5);
            while matches!(self.child.try_wait(), Ok(None)) && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
}
