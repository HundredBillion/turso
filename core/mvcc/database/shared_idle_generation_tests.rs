use super::shared_checkpoint::PublishedSharedCheckpoint;
use super::*;
use crate::mvcc::commit_manifest::{ManifestRecord, RECORD_SIZE, SLOT_SIZE};
use crate::Value;
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

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_idle_generation_native_cancel_partial_replay_then_retry_latest() {
    run_idle_generation_case("cancel");
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_idle_generation_native_cancel_physical_inspection_before_and_after_callback() {
    for role in [
        "physical-1-before",
        "physical-1-after",
        "physical-2-before",
        "physical-2-after",
    ] {
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
    if role == "cancel" {
        fixture
            .connection
            .execute("UPDATE preserved SET value=replace(hex(zeroblob(8000)),'0','x') WHERE id=1")
            .unwrap();
    } else {
        fixture
            .connection
            .execute("UPDATE preserved SET value='after second commit' WHERE id=1")
            .unwrap();
    }
    let image = std::fs::read(fixture.namespace.join("manifest")).unwrap();
    assert!(image.len() >= 2 * SLOT_SIZE);
    for slot in image[..2 * SLOT_SIZE].chunks_exact(SLOT_SIZE) {
        let record = ManifestRecord::decode(&slot[..RECORD_SIZE])
            .unwrap()
            .unwrap();
        assert_eq!(record.log_generation(), target.database.generation);
        assert!(!record.requires_recovery());
        assert!(
            record.prefix().end_offset > 0,
            "empty completed record was not overwritten"
        );
    }
    if role == "cancel" || role.starts_with("physical-") {
        old.send("cancel");
        let cancelled = old.receive();
        assert!(cancelled.starts_with("CANCELLED "), "{cancelled}");
        fixture
            .connection
            .execute("UPDATE preserved SET value='after second commit' WHERE id=1")
            .unwrap();
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
    let (io, delayed) = if role == "cancel" || role.starts_with("physical-") {
        let canonical = std::fs::canonicalize(&path).unwrap();
        let id = crate::io::get_file_id(canonical.to_str().unwrap()).unwrap();
        let namespace = crate::mvcc::mode_transition::namespace_for(&canonical, id).unwrap();
        let read_path = if role == "cancel" {
            namespace.join("logical")
        } else {
            canonical
        };
        let (io, control) = super::shared_connection_tests::delaying_native_read(read_path);
        (io, Some(control))
    } else {
        (
            Arc::new(crate::io::SyscallIO::new().unwrap()) as Arc<dyn crate::IO>,
            None,
        )
    };
    let database = crate::Database::open(
        io.clone(),
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
    let source_function = if !hold {
        connection.register_internal_vtab(LocalFunction).unwrap();
        Some(assert_local_function(&connection))
    } else {
        None
    };
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
    let mut cancelled = None;
    for line in std::io::stdin().lock().lines() {
        match line.unwrap().as_str() {
            "cancel" => {
                let control = delayed.as_ref().unwrap();
                let physical = role.starts_with("physical-");
                let before_callback = !role.ends_with("-after");
                let read_number = if physical {
                    role.split('-').nth(1).unwrap().parse().unwrap()
                } else {
                    3
                };
                control.delay_after_reads(read_number);
                let previous_frontier = store.last_committed_tx_ts.load(Ordering::Acquire);
                let previous_roots = store.table_id_to_rootpage.load_full();
                let previous_schema = database.schema.lock().clone();
                let previous_replay = runtime.replay.lock().as_ref().unwrap().clone();
                let mut open = runtime.begin_open().unwrap();
                io.block(|| open.poll_manifest()).unwrap();
                let latest = open
                    .loaded
                    .as_ref()
                    .unwrap()
                    .record()
                    .prefix()
                    .committed_timestamp;
                let (waiter, held) = {
                    let mut attempts = 0;
                    loop {
                        attempts += 1;
                        assert!(attempts < 1000, "reload did not reach delayed logical read");
                        match open
                            .poll_current_prefix(&store, &connection, crate::MAIN_DB_ID, None)
                            .unwrap()
                        {
                            IOResult::IO(waiter) => {
                                if let Some(held) = control.try_take_pending() {
                                    break (waiter, held);
                                }
                                waiter.wait(io.as_ref()).unwrap();
                            }
                            IOResult::Done(()) => panic!("reload bypassed delayed logical read"),
                        }
                    }
                };
                let partial = store.last_committed_tx_ts.load(Ordering::Acquire);
                let base = store.durable_txid_max.load(Ordering::Acquire);
                if physical {
                    assert_eq!(partial, previous_frontier);
                    assert!(Arc::ptr_eq(
                        &previous_roots,
                        &store.table_id_to_rootpage.load_full()
                    ));
                    assert!(Arc::ptr_eq(&previous_schema, &database.schema.lock()));
                    assert!(Arc::ptr_eq(
                        &previous_replay,
                        runtime.replay.lock().as_ref().unwrap()
                    ));
                } else {
                    assert!(
                        base < partial && partial < latest,
                        "base={base} partial={partial} latest={latest}"
                    );
                    assert!(runtime.replay.lock().is_none());
                }
                if !before_callback {
                    held.complete();
                }
                drop(waiter);
                drop(open);
                assert!(matches!(
                    runtime.files.authority.try_acquire(),
                    Err(LimboError::Busy)
                ));
                assert!(matches!(runtime.begin_open(), Err(LimboError::Busy)));
                if before_callback {
                    held.complete();
                }
                assert!(matches!(
                    runtime.files.authority.try_acquire(),
                    Err(LimboError::Busy)
                ));
                assert!(matches!(runtime.begin_open(), Err(LimboError::Busy)));
                drop(held);
                let guard = runtime.files.authority.try_acquire().unwrap();
                drop(guard);
                if !physical {
                    cancelled = Some((
                        base,
                        partial,
                        latest,
                        store.clock.get_timestamp(crate::mvcc::clock::no_op),
                    ));
                }
                reply(&format!(
                    "CANCELLED base={base} partial={partial} latest={latest}"
                ));
            }
            "read" => match if role == "constructor" {
                database
                    .connect(crate::ConnectOpts::new())
                    .and_then(|fresh| read_value(&fresh))
            } else {
                read_value(&connection)
            } {
                Ok(value) => {
                    if let Some(original) = source_function.as_ref() {
                        let current = assert_local_function(&connection);
                        assert!(
                            Arc::ptr_eq(original, &current),
                            "local reload replaced a caller-defined function instance"
                        );
                    }
                    if let Some((base, partial, old_latest, clock)) = cancelled.take() {
                        let identity = runtime.replay.lock().as_ref().unwrap().identity().unwrap();
                        assert_eq!(identity.database.checkpoint_timestamp, base);
                        assert!(identity.prefix.committed_timestamp > old_latest);
                        assert!(store.clock.get_timestamp(crate::mvcc::clock::no_op) > clock);
                        assert_eq!(
                            store.last_committed_tx_ts.load(Ordering::Acquire),
                            identity.prefix.committed_timestamp
                        );
                        eprintln!("retry physical={base} partial={partial} old_latest={old_latest} selected_latest={}", identity.prefix.committed_timestamp);
                    }
                    reply(&format!("VALUE {value}"));
                }
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
pub(super) struct IdleGenerationChild {
    child: Child,
    output: Receiver<std::io::Result<String>>,
}
impl IdleGenerationChild {
    pub(super) fn spawn(path: &Path, limit: u64, role: &str) -> Self {
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
    pub(super) fn send(&mut self, command: &str) {
        let stdin = self.child.stdin.as_mut().unwrap();
        writeln!(stdin, "{command}").unwrap();
        stdin.flush().unwrap();
    }
    pub(super) fn receive(&self) -> String {
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
    pub(super) fn wait(&mut self) -> std::process::ExitStatus {
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

#[derive(Debug)]
struct LocalFunction;
impl crate::vtab::InternalVirtualTable for LocalFunction {
    fn name(&self) -> String {
        "source_only_function".into()
    }
    fn sql(&self) -> String {
        "CREATE TABLE source_only_function(value INTEGER)".into()
    }
    fn open(
        &self,
        _: Arc<Connection>,
    ) -> Result<Arc<RwLock<dyn crate::vtab::InternalVirtualTableCursor>>> {
        Ok(Arc::new(RwLock::new(LocalFunctionCursor)))
    }
    fn best_index(
        &self,
        constraints: &[turso_ext::ConstraintInfo],
        _: &[turso_ext::OrderByInfo],
    ) -> std::result::Result<turso_ext::IndexInfo, turso_ext::ResultCode> {
        Ok(turso_ext::IndexInfo {
            idx_num: 0,
            idx_str: None,
            order_by_consumed: false,
            estimated_cost: 1.0,
            estimated_rows: 1,
            constraint_usages: constraints
                .iter()
                .map(|_| turso_ext::ConstraintUsage {
                    argv_index: None,
                    omit: false,
                })
                .collect(),
        })
    }
}
struct LocalFunctionCursor;
impl crate::vtab::InternalVirtualTableCursor for LocalFunctionCursor {
    fn next(&mut self) -> Result<bool> {
        Ok(false)
    }
    fn rowid(&self) -> i64 {
        1
    }
    fn column(&self, _: usize) -> Result<Value> {
        Ok(Value::from_i64(42))
    }
    fn filter(&mut self, _: &[Value], _: Option<String>, _: i32) -> Result<bool> {
        Ok(true)
    }
}
fn assert_local_function(connection: &Arc<Connection>) -> Arc<crate::vtab::VirtualTable> {
    assert!(
        connection
            .db
            .schema
            .lock()
            .has_table(NamespaceId::ROOT, "source_only_function"),
        "local reload dropped a source-only function"
    );
    let mut values = Vec::new();
    connection
        .prepare("SELECT value FROM source_only_function")
        .unwrap()
        .run_with_row_callback(|row| {
            values.push(row.get::<i64>(0)?);
            Ok(())
        })
        .unwrap();
    assert_eq!(values, [42]);
    let table = connection
        .db
        .schema
        .lock()
        .get_table(NamespaceId::ROOT, "source_only_function")
        .unwrap();
    let Table::Virtual(function) = table.as_ref() else {
        panic!("source-only function changed kind")
    };
    function.clone()
}
