use super::*;
use crate::io::{File, FileId, FileSyncType};
use crate::sync::atomic::{AtomicBool, Ordering};
use crate::sync::Mutex;
use crate::{Buffer, Completion, Result};
use std::path::Path;
use std::sync::mpsc;
use std::time::Duration;

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_id_lifecycle_io_native_mutations_retire_before_reset_acknowledges() {
    for scope in ["reservation", "named"] {
        for operation in ["write", "sync", "truncate"] {
            for reset in [true, false] {
                check_retirement(scope, "ids", operation, reset);
            }
        }
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_id_lifecycle_io_native_sequence_commit_preserves_saved_outer() {
    for operation in ["write", "sync"] {
        for reset in [true, false] {
            check_retirement("sequence", "manifest", operation, reset);
        }
    }
}

fn check_retirement(
    scope: &'static str,
    target: &'static str,
    operation: &'static str,
    reset: bool,
) {
    eprintln!("physical ID lifecycle {scope}/{target}/{operation}/reset={reset}");
    let directory = tempfile::tempdir().unwrap();
    let (path, limit) = crate::mvcc::database::shared_runtime_fixture(directory.path());
    let options = OpenOptions::new(Arc::new(EngineDialect)).with_shared_mvcc_for_test(limit);
    let peer = Database::open(
        Arc::new(crate::io::SyscallIO::new().unwrap()),
        path.to_str().unwrap(),
        options.clone(),
    )
    .unwrap()
    .connect(ConnectOpts::new())
    .unwrap();
    if scope == "sequence" {
        peer.execute("CREATE SEQUENCE io_sequence").unwrap();
    }
    let control = Arc::new(Control {
        armed: AtomicBool::new(false),
        pending: Mutex::new(None),
        operation,
    });
    let worker_control = control.clone();
    let (events, receive) = mpsc::channel();
    let (commands, command) = mpsc::channel();
    let worker = std::thread::spawn(move || {
        let io: Arc<dyn crate::IO> = Arc::new(DeferredIo {
            inner: Arc::new(crate::io::SyscallIO::new().unwrap()),
            control: worker_control.clone(),
            target,
        });
        let first = Database::open(io, path.to_str().unwrap(), options)
            .unwrap()
            .connect(ConnectOpts::new())
            .unwrap();
        first.execute("BEGIN CONCURRENT").unwrap();
        first
            .execute("INSERT INTO preserved(id,value) VALUES(66,'prior')")
            .unwrap();
        let outer = first.get_mv_tx_id_for_db(0).unwrap();
        let snapshot = first.mv_store().as_ref().unwrap().read_snapshot_ts(outer);
        if scope == "named" {
            first.execute("SAVEPOINT scope").unwrap();
            first
                .execute("INSERT INTO preserved(id,value) VALUES(77,'scoped')")
                .unwrap();
        }
        let sql = match scope {
            "named" => "ROLLBACK TO scope",
            "sequence" => "INSERT INTO preserved(id,value) VALUES(77,nextval('io_sequence'))",
            _ => "INSERT INTO preserved(id,value) VALUES(77,'abandoned')",
        };
        let mut statement = first.prepare(sql).unwrap();
        worker_control.armed.store(true, Ordering::Release);
        let mut captured = false;
        for _ in 0..10000 {
            let step = statement.step().unwrap();
            if worker_control.pending.lock().is_some() {
                assert!(matches!(step, crate::StepResult::IO));
                captured = true;
                break;
            }
            match step {
                crate::StepResult::IO => first.get_pager().io.step().unwrap(),
                other => panic!("expected delayed mutation, got {other:?}"),
            }
        }
        assert!(captured);
        events.send("captured").unwrap();
        command.recv_timeout(Duration::from_secs(20)).unwrap();
        if reset {
            statement
                .reset()
                .expect("reset must finish only scoped rollback");
        }
        drop(statement);
        assert_eq!(first.get_mv_tx_id_for_db(0), Some(outer));
        assert_eq!(
            first.mv_store().as_ref().unwrap().read_snapshot_ts(outer),
            snapshot
        );
        assert_eq!(
            integers(&first, "SELECT id FROM preserved WHERE id>=66"),
            [66]
        );
        events.send("retired").unwrap();
        command.recv_timeout(Duration::from_secs(20)).unwrap();
        first.execute("COMMIT").unwrap();
        events.send("committed").unwrap();
    });
    assert_eq!(
        receive.recv_timeout(Duration::from_secs(20)).unwrap(),
        "captured"
    );
    let held = control.pending.lock().take().unwrap();
    let peer_insert = "INSERT INTO preserved(id,value) VALUES(77,'peer')";
    assert!(matches!(
        peer.execute(peer_insert),
        Err(crate::LimboError::Busy)
    ));
    commands.send(()).unwrap();
    held.complete();
    assert!(
        matches!(peer.execute(peer_insert), Err(crate::LimboError::Busy)),
        "callback completion released physical ownership"
    );
    assert!(
        matches!(
            receive.recv_timeout(Duration::from_millis(20)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ),
        "reset/drop acknowledged before backend completion retirement"
    );
    drop(held);
    assert_eq!(
        receive.recv_timeout(Duration::from_secs(20)).unwrap(),
        "retired"
    );
    peer.execute(peer_insert).unwrap();
    assert!(matches!(
        peer.execute("INSERT INTO preserved(id,value) VALUES(66,'peer')"),
        Err(crate::LimboError::Busy)
    ));
    if scope == "sequence" {
        assert_eq!(
            integers(
                &peer,
                "SELECT sequence_watermark_experimental('io_sequence')"
            ),
            [2],
            "autonomous committed value lost or aborted outer sequence claim retained"
        );
    }
    commands.send(()).unwrap();
    assert_eq!(
        receive.recv_timeout(Duration::from_secs(20)).unwrap(),
        "committed"
    );
    worker.join().unwrap();
    assert_eq!(
        integers(&peer, "SELECT id FROM preserved WHERE id>=66"),
        [66, 77]
    );
}

struct Control {
    armed: AtomicBool,
    pending: Mutex<Option<HeldOperation>>,
    operation: &'static str,
}
enum Operation {
    Write(u64, Arc<Buffer>),
    Sync(FileSyncType),
    Truncate(u64),
}
struct HeldOperation {
    file: Arc<dyn File>,
    completion: Completion,
    operation: Operation,
}
impl HeldOperation {
    fn complete(&self) {
        if self.completion.finished() {
            return;
        }
        let result = match &self.operation {
            Operation::Write(offset, buffer) => {
                self.file
                    .pwrite(*offset, buffer.clone(), self.completion.clone())
            }
            Operation::Sync(kind) => self.file.sync(self.completion.clone(), *kind),
            Operation::Truncate(size) => self.file.truncate(*size, self.completion.clone()),
        };
        drop(result.unwrap());
        assert!(self.completion.finished());
    }
}
impl Drop for HeldOperation {
    fn drop(&mut self) {
        self.complete();
    }
}
struct DeferredFile {
    inner: Arc<dyn File>,
    control: Arc<Control>,
}
impl DeferredFile {
    fn capture(
        &self,
        name: &str,
        operation: Operation,
        completion: Completion,
    ) -> Option<Completion> {
        if self.control.operation == name && self.control.armed.swap(false, Ordering::AcqRel) {
            assert!(self
                .control
                .pending
                .lock()
                .replace(HeldOperation {
                    file: self.inner.clone(),
                    completion: completion.clone(),
                    operation,
                })
                .is_none());
            Some(completion)
        } else {
            None
        }
    }
}
impl File for DeferredFile {
    fn file_id(&self) -> Result<Option<FileId>> {
        self.inner.file_id()
    }
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.inner.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.inner.unlock_file()
    }
    fn size(&self) -> Result<u64> {
        self.inner.size()
    }
    fn pread(&self, offset: u64, completion: Completion) -> Result<Completion> {
        self.inner.pread(offset, completion)
    }
    fn pwrite(
        &self,
        offset: u64,
        buffer: Arc<Buffer>,
        completion: Completion,
    ) -> Result<Completion> {
        match self.capture(
            "write",
            Operation::Write(offset, buffer.clone()),
            completion.clone(),
        ) {
            Some(pending) => Ok(pending),
            None => self.inner.pwrite(offset, buffer, completion),
        }
    }
    fn sync(&self, completion: Completion, kind: FileSyncType) -> Result<Completion> {
        match self.capture("sync", Operation::Sync(kind), completion.clone()) {
            Some(pending) => Ok(pending),
            None => self.inner.sync(completion, kind),
        }
    }
    fn truncate(&self, size: u64, completion: Completion) -> Result<Completion> {
        match self.capture("truncate", Operation::Truncate(size), completion.clone()) {
            Some(pending) => Ok(pending),
            None => self.inner.truncate(size, completion),
        }
    }
}
struct DeferredIo {
    inner: Arc<dyn crate::IO>,
    control: Arc<Control>,
    target: &'static str,
}
impl crate::Clock for DeferredIo {
    fn current_time_monotonic(&self) -> crate::MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> crate::WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}
impl crate::IO for DeferredIo {
    fn open_file(
        &self,
        path: &str,
        flags: crate::OpenFlags,
        direct: bool,
    ) -> Result<Arc<dyn File>> {
        let inner = self.inner.open_file(path, flags, direct)?;
        let name = Path::new(path)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("");
        if (self.target == "ids" && matches!(name, "ids-a" | "ids-b")) || name == self.target {
            Ok(Arc::new(DeferredFile {
                inner,
                control: self.control.clone(),
            }))
        } else {
            Ok(inner)
        }
    }
    fn remove_file(&self, path: &str) -> Result<()> {
        self.inner.remove_file(path)
    }
    fn supports_shared_wal_coordination(&self) -> bool {
        self.inner.supports_shared_wal_coordination()
    }
    fn step(&self) -> Result<()> {
        std::thread::yield_now();
        self.inner.step()
    }
}
