use super::*;
use crate::mvcc::database::{shared_runtime_fixture, SharedMvccRuntime};
use crate::storage::shared_wal_coordination::SharedWalRecoveryLocks;
use crate::util::IOExt;
use crate::{OpenFlags, IO};

#[derive(Clone)]
enum NativeOperation {
    Read(u64),
    Write(u64, Arc<Buffer>),
    Sync(FileSyncType),
    Truncate(u64),
}
struct HeldNativeOperation {
    file: Arc<dyn File>,
    completion: Completion,
    operation: NativeOperation,
}
impl HeldNativeOperation {
    fn complete(&self) {
        let result = match &self.operation {
            NativeOperation::Read(offset) => self.file.pread(*offset, self.completion.clone()),
            NativeOperation::Write(offset, buffer) => {
                self.file
                    .pwrite(*offset, buffer.clone(), self.completion.clone())
            }
            NativeOperation::Sync(kind) => self.file.sync(self.completion.clone(), *kind),
            NativeOperation::Truncate(size) => self.file.truncate(*size, self.completion.clone()),
        }
        .unwrap();
        assert!(result.succeeded(), "fixture uses synchronous native I/O");
    }
}
struct DeferredNativeFile {
    inner: Arc<dyn File>,
    pending: Mutex<Vec<Arc<HeldNativeOperation>>>,
}
impl DeferredNativeFile {
    fn submit(&self, completion: Completion, operation: NativeOperation) -> Result<Completion> {
        self.pending.lock().push(Arc::new(HeldNativeOperation {
            file: self.inner.clone(),
            completion: completion.clone(),
            operation,
        }));
        Ok(completion)
    }
}
impl File for DeferredNativeFile {
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
        self.submit(completion, NativeOperation::Read(offset))
    }
    fn pwrite(
        &self,
        offset: u64,
        buffer: Arc<Buffer>,
        completion: Completion,
    ) -> Result<Completion> {
        self.submit(completion, NativeOperation::Write(offset, buffer))
    }
    fn sync(&self, completion: Completion, kind: FileSyncType) -> Result<Completion> {
        self.submit(completion, NativeOperation::Sync(kind))
    }
    fn truncate(&self, size: u64, completion: Completion) -> Result<Completion> {
        self.submit(completion, NativeOperation::Truncate(size))
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem and OFD locks"]
fn physical_wal_recovery_native_binding_retains_both_authorities_until_backend_retirement() {
    for operation in 0..5 {
        for callback_before_drop in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let (path, limit) = shared_runtime_fixture(directory.path());
            let canonical = path.canonicalize().unwrap();
            let before = std::fs::read(&path).unwrap();
            let io: Arc<dyn IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
            let raw = io
                .open_file(canonical.to_str().unwrap(), OpenFlags::NoLock, false)
                .unwrap();
            let id = raw.file_id().unwrap().unwrap();
            let backend = Arc::new(DeferredNativeFile {
                inner: raw,
                pending: Default::default(),
            });
            let runtime = SharedMvccRuntime::open_existing(
                io.clone(),
                backend.clone(),
                id,
                &canonical,
                limit,
                false,
            )
            .unwrap();
            let mapping = runtime.physical_coordination();
            let mut opening = runtime.begin_checkpoint_recovery().unwrap();
            let selection = io.block(|| opening.poll()).unwrap();
            let physical = SharedWalRecoveryLocks::acquire(mapping.clone()).unwrap();
            let bindings = runtime
                .bind_physical_recovery_files(selection.manifest.guard().unwrap(), &physical)
                .unwrap();
            let file = runtime.main_file();
            let header = Arc::new(Buffer::new(before[..512].to_vec()));
            let caller = match operation {
                0 => file.pread(0, Completion::new_read(header, |_| None)),
                1 => file.pwrite(0, header, Completion::new_write(|_| {})),
                2 => file.sync(Completion::new_sync(|_| {}), FileSyncType::FullFsync),
                3 => file.truncate(before.len() as u64, Completion::new_trunc(|_| {})),
                _ => file.pwritev(
                    0,
                    vec![
                        Arc::new(Buffer::new(before[..256].to_vec())),
                        Arc::new(Buffer::new(before[256..512].to_vec())),
                    ],
                    Completion::new_write(|_| {}),
                ),
            }
            .unwrap();
            let held = std::mem::take(&mut *backend.pending.lock());
            assert!(!held.is_empty());
            if callback_before_drop {
                for pending in &held {
                    pending.complete();
                }
            }
            caller.abort();
            drop(caller);
            drop(bindings);
            drop(physical);
            drop(selection);
            drop(opening);
            assert!(
                mapping
                    .register_db_file_reader(mapping.owner_record())
                    .is_none(),
                "physical reader admitted before backend retirement, operation {operation}"
            );
            assert!(matches!(
                SharedWalRecoveryLocks::acquire(mapping.clone()),
                Err(LimboError::Busy)
            ));
            assert!(matches!(runtime.begin_open(), Err(LimboError::Busy)));
            assert!(matches!(
                crate::Database::open(
                    io.clone(),
                    canonical.to_str().unwrap(),
                    crate::OpenOptions::new(Arc::new(crate::EngineDialect))
                        .with_shared_mvcc_for_test(limit)
                ),
                Err(LimboError::Busy)
            ));
            if !callback_before_drop {
                for pending in &held {
                    pending.complete();
                }
            }
            assert!(mapping
                .register_db_file_reader(mapping.owner_record())
                .is_none());
            assert!(matches!(runtime.begin_open(), Err(LimboError::Busy)));
            drop(held);
            drop(SharedWalRecoveryLocks::acquire(mapping.clone()).unwrap());
            assert!(runtime.begin_open().is_ok());
            assert_eq!(std::fs::read(&path).unwrap(), before);
            let database = crate::Database::open(
                io.clone(),
                canonical.to_str().unwrap(),
                crate::OpenOptions::new(Arc::new(crate::EngineDialect))
                    .with_shared_mvcc_for_test(limit),
            )
            .unwrap();
            let connection = database.connect(crate::ConnectOpts::new()).unwrap();
            let mut rows = Vec::new();
            connection
                .prepare("SELECT value FROM preserved WHERE id=1")
                .unwrap()
                .run_with_row_callback(|row| {
                    rows.push(row.get::<&str>(0)?.to_owned());
                    Ok(())
                })
                .unwrap();
            assert_eq!(rows, ["durable payload"]);
        }
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem and OFD locks"]
fn physical_wal_recovery_native_binding_rejects_foreign_mapping_and_authority() {
    let left_directory = tempfile::tempdir().unwrap();
    let right_directory = tempfile::tempdir().unwrap();
    let (left_path, left_limit) = shared_runtime_fixture(left_directory.path());
    let (right_path, right_limit) = shared_runtime_fixture(right_directory.path());
    let io: Arc<dyn IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
    let open_runtime = |path: &std::path::Path, limit| {
        let canonical = path.canonicalize().unwrap();
        let raw = io
            .open_file(canonical.to_str().unwrap(), OpenFlags::NoLock, false)
            .unwrap();
        let id = raw.file_id().unwrap().unwrap();
        SharedMvccRuntime::open_existing(io.clone(), raw, id, &canonical, limit, false).unwrap()
    };
    let left = open_runtime(&left_path, left_limit);
    let right = open_runtime(&right_path, right_limit);
    let mut left_open = left.begin_checkpoint_recovery().unwrap();
    let left_selection = io.block(|| left_open.poll()).unwrap();
    let mut right_open = right.begin_checkpoint_recovery().unwrap();
    let right_selection = io.block(|| right_open.poll()).unwrap();
    let id = left.main_file().file_id().unwrap().unwrap();
    let namespace =
        crate::mvcc::mode_transition::namespace_for(&left_path.canonicalize().unwrap(), id)
            .unwrap();
    let substitute = Arc::new(
        crate::storage::shared_wal_coordination::MappedSharedWalCoordination::open_existing(
            &io,
            &namespace.join("physical-tshm"),
            64,
        )
        .unwrap()
        .unwrap(),
    );
    assert_eq!(
        substitute.file_id().unwrap(),
        left.physical_coordination().file_id().unwrap()
    );
    assert!(!Arc::ptr_eq(&substitute, &left.physical_coordination()));
    let foreign = SharedWalRecoveryLocks::acquire(substitute).unwrap();
    assert!(
        matches!(left.bind_physical_recovery_files(left_selection.manifest.guard().unwrap(), &foreign), Err(LimboError::InvalidArgument(message)) if message.contains("changed owner or native file"))
    );
    assert!(matches!(
        left.main_file()
            .sync(Completion::new_sync(|_| {}), FileSyncType::FullFsync),
        Err(LimboError::Busy)
    ));
    drop(foreign);
    let physical = SharedWalRecoveryLocks::acquire(left.physical_coordination()).unwrap();
    assert!(
        matches!(left.bind_physical_recovery_files(right_selection.manifest.guard().unwrap(), &physical), Err(LimboError::InvalidArgument(message)) if message.contains("retained runtime files"))
    );
    assert!(matches!(
        left.main_file()
            .sync(Completion::new_sync(|_| {}), FileSyncType::FullFsync),
        Err(LimboError::Busy)
    ));
    let bindings = left
        .bind_physical_recovery_files(left_selection.manifest.guard().unwrap(), &physical)
        .unwrap();
    assert_eq!(bindings.len(), 2);
    drop(bindings);
    drop(physical);
}
