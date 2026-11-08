use super::*;
use crate::io::{File, FileId, FileSyncType};
use crate::sync::atomic::{AtomicBool, Ordering};
use crate::sync::Mutex;
use crate::{Buffer, Completion, Result};
use std::path::Path;

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_watermark_io_native_reset_drop_resume_retains_physical_reads() {
    for target in ["shared.db", "manifest", "ids-selector"] {
        for cancel in [
            "reset_before",
            "reset_after",
            "drop_before",
            "drop_after",
            "resume",
        ] {
            let directory = tempfile::tempdir().unwrap();
            let (path, limit) = crate::mvcc::database::shared_runtime_fixture(directory.path());
            let control = Arc::new(ReadControl::default());
            let io: Arc<dyn crate::IO> = Arc::new(DeferredIo {
                inner: Arc::new(crate::io::SyscallIO::new().unwrap()),
                control: control.clone(),
                target,
            });
            let options =
                OpenOptions::new(Arc::new(EngineDialect)).with_shared_mvcc_for_test(limit);
            let database = Database::open(io, path.to_str().unwrap(), options.clone()).unwrap();
            let first = database.connect(ConnectOpts::new()).unwrap();
            first.execute("CREATE SEQUENCE delayed_sequence").unwrap();
            let peer = Database::open(
                Arc::new(crate::io::SyscallIO::new().unwrap()),
                path.to_str().unwrap(),
                options,
            )
            .unwrap()
            .connect(ConnectOpts::new())
            .unwrap();
            let before = watermark_durable_files(directory.path());
            let mut statement = Some(
                first
                    .prepare("SELECT sequence_watermark_experimental('delayed_sequence')")
                    .unwrap(),
            );
            control.armed.store(true, Ordering::Release);
            let mut captured = None;
            for _ in 0..1000 {
                let step = statement.as_mut().unwrap().step().unwrap();
                if let Some(read) = control.pending.lock().take() {
                    assert!(matches!(step, crate::StepResult::IO));
                    captured = Some(read);
                    break;
                }
                match step {
                    crate::StepResult::IO => first.get_pager().io.step().unwrap(),
                    other => panic!("{target}/{cancel}: expected a physical read, got {other:?}"),
                }
            }
            let held =
                captured.unwrap_or_else(|| panic!("{target}/{cancel}: no physical read captured"));
            if cancel.ends_with("after") {
                held.complete();
            }
            if cancel.starts_with("reset") {
                statement.as_mut().unwrap().reset().unwrap();
            }
            if cancel.starts_with("drop") {
                drop(statement.take());
            }
            let write = "INSERT INTO preserved(id,value) VALUES(90,'peer')";
            assert!(
                matches!(peer.execute(write), Err(crate::LimboError::Busy)),
                "{target}/{cancel}: released authority before physical completion"
            );
            if !cancel.ends_with("after") {
                held.complete();
            }
            assert!(
                matches!(peer.execute(write), Err(crate::LimboError::Busy)),
                "{target}/{cancel}: callback released backend ownership"
            );
            drop(held);
            assert_eq!(watermark_durable_files(directory.path()), before);
            if cancel == "resume" {
                let mut rows = 0;
                statement
                    .as_mut()
                    .unwrap()
                    .run_with_row_callback(|row| {
                        assert_eq!(row.get_values().next().unwrap().as_int(), Some(1));
                        rows += 1;
                        Ok(())
                    })
                    .unwrap();
                assert_eq!(rows, 1);
            }
            peer.execute(write).unwrap_or_else(|error| {
                panic!("{target}/{cancel}: retired read retained ownership: {error}")
            });
            if cancel.starts_with("reset") {
                let mut rows = 0;
                statement
                    .as_mut()
                    .unwrap()
                    .run_with_row_callback(|row| {
                        assert_eq!(row.get_values().next().unwrap().as_int(), Some(1));
                        rows += 1;
                        Ok(())
                    })
                    .unwrap();
                assert_eq!(rows, 1);
            } else {
                assert_eq!(
                    integers(
                        &first,
                        "SELECT sequence_watermark_experimental('delayed_sequence')"
                    ),
                    [1]
                );
            }
            assert_eq!(first.get_mv_tx_id_for_db(0), None);
        }
    }
}

#[derive(Default)]
struct ReadControl {
    armed: AtomicBool,
    pending: Mutex<Option<HeldRead>>,
}
struct HeldRead {
    file: Arc<dyn File>,
    offset: u64,
    completion: Completion,
}
impl HeldRead {
    fn complete(&self) {
        drop(
            self.file
                .pread(self.offset, self.completion.clone())
                .unwrap(),
        );
        assert!(self.completion.finished());
    }
}
struct DeferredFile {
    inner: Arc<dyn File>,
    control: Arc<ReadControl>,
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
        if self.control.armed.swap(false, Ordering::AcqRel) {
            assert!(self
                .control
                .pending
                .lock()
                .replace(HeldRead {
                    file: self.inner.clone(),
                    offset,
                    completion: completion.clone()
                })
                .is_none());
            Ok(completion)
        } else {
            self.inner.pread(offset, completion)
        }
    }
    fn pwrite(
        &self,
        offset: u64,
        buffer: Arc<Buffer>,
        completion: Completion,
    ) -> Result<Completion> {
        self.inner.pwrite(offset, buffer, completion)
    }
    fn sync(&self, completion: Completion, kind: FileSyncType) -> Result<Completion> {
        self.inner.sync(completion, kind)
    }
    fn truncate(&self, size: u64, completion: Completion) -> Result<Completion> {
        self.inner.truncate(size, completion)
    }
}
struct DeferredIo {
    inner: Arc<dyn crate::IO>,
    control: Arc<ReadControl>,
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
        if Path::new(path).file_name().and_then(|name| name.to_str()) == Some(self.target) {
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
        self.inner.step()
    }
}
