use super::*;
use crate::io::{FileSyncType, SharedWalLockKind, SharedWalMappedRegion};
use crate::{Buffer, Completion};

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_io_native_cancel_retains_authority_until_manifest_read_retires() {
    for cancel_at in 0..3 {
        let directory = tempfile::tempdir().unwrap();
        let (path, limit) = shared_runtime_fixture(directory.path());
        let control = Arc::new(RecoveryIoControl::default());
        let io: Arc<dyn crate::IO> = Arc::new(RecoveryIo {
            inner: Arc::new(crate::io::SyscallIO::new().unwrap()),
            control: control.clone(),
        });
        let database = crate::Database::open(
            io.clone(),
            path.to_str().unwrap(),
            crate::OpenOptions::new(Arc::new(crate::EngineDialect))
                .with_shared_mvcc_for_test(limit),
        )
        .unwrap();
        let connection = database.connect(crate::ConnectOpts::new()).unwrap();
        let store = database.get_mv_store().as_ref().unwrap().clone();
        let runtime = store.shared_runtime.get().unwrap();
        let mut initial = runtime.begin_checkpoint_recovery().unwrap();
        let selected = io.block(|| initial.poll()).unwrap();
        let previous = selected.manifest.record();
        drop(selected);
        drop(initial);
        let manifest_path = runtime.files.namespace.join("manifest");
        let logical_path = runtime.files.namespace.join("logical");
        let manifest_before = std::fs::read(&manifest_path).unwrap();
        let logical_before = std::fs::read(&logical_path).unwrap();

        control.delay_manifest_read.store(true, Ordering::Release);
        let mut recovery = runtime.begin_checkpoint_recovery().unwrap();
        let waiter = match recovery.poll().unwrap() {
            IOResult::IO(waiter) => waiter,
            IOResult::Done(_) => panic!("recovery bypassed the delayed manifest read"),
        };
        let held = control.pending_read.lock().take().unwrap();
        assert!(!held.completion.finished());
        if cancel_at != 0 {
            held.complete();
        }
        drop(waiter);
        if cancel_at == 2 {
            let selected = io.block(|| recovery.poll()).unwrap();
            assert_eq!(selected.manifest.record(), previous);
            drop(selected);
        }
        drop(recovery);
        assert!(runtime.gate.try_enter_admission().is_ok());
        assert!(
            matches!(runtime.files.authority.try_acquire(), Err(LimboError::Busy)),
            "cancelled manifest read released authority before backend retirement (phase {cancel_at})"
        );
        assert!(matches!(runtime.begin_open(), Err(LimboError::Busy)));
        if cancel_at == 0 {
            held.complete();
        }
        assert!(held.completion.finished());
        assert!(
            matches!(runtime.files.authority.try_acquire(), Err(LimboError::Busy)),
            "manifest callback released its retained authority before backend retirement (phase {cancel_at})"
        );
        drop(held);
        let guard = runtime.files.authority.try_acquire().unwrap();
        assert!(guard.requires_recovery());
        assert!(guard.published().is_err());
        drop(guard);
        assert_eq!(std::fs::read(&manifest_path).unwrap(), manifest_before);
        assert_eq!(std::fs::read(&logical_path).unwrap(), logical_before);

        connection
            .execute("UPDATE preserved SET value='after cancelled recovery' WHERE id=1")
            .unwrap();
        let manifest_after_commit = std::fs::read(&manifest_path).unwrap();
        let mut retry = runtime.begin_checkpoint_recovery().unwrap();
        let selected = io.block(|| retry.poll()).unwrap();
        let fresh = selected.manifest.record();
        assert!(fresh.sequence() > previous.sequence());
        assert!(fresh.prefix().committed_timestamp > previous.prefix().committed_timestamp);
        assert!(fresh.prefix().end_offset > previous.prefix().end_offset);
        assert_eq!(
            std::fs::read(&manifest_path).unwrap(),
            manifest_after_commit
        );
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_io_native_slot_unlock_error_poison_prevents_authority_reuse() {
    let directory = tempfile::tempdir().unwrap();
    let (path, limit) = shared_runtime_fixture(directory.path());
    let control = Arc::new(RecoveryIoControl::default());
    let io: Arc<dyn crate::IO> = Arc::new(RecoveryIo {
        inner: Arc::new(crate::io::SyscallIO::new().unwrap()),
        control: control.clone(),
    });
    let database = crate::Database::open(
        io,
        path.to_str().unwrap(),
        crate::OpenOptions::new(Arc::new(crate::EngineDialect)).with_shared_mvcc_for_test(limit),
    )
    .unwrap();
    let store = database.get_mv_store().as_ref().unwrap().clone();
    let runtime = store.shared_runtime.get().unwrap();
    let manifest_path = runtime.files.namespace.join("manifest");
    let before = std::fs::read(&manifest_path).unwrap();
    control.fail_slot_unlock.store(true, Ordering::Release);
    let error = match runtime.begin_checkpoint_recovery() {
        Ok(_) => panic!("recovery ignored a failed native slot unlock"),
        Err(error) => error,
    };
    assert!(
        matches!(error, LimboError::LockingError(message) if message == "injected recovery slot unlock failure")
    );
    assert!(runtime.gate.try_enter_admission().is_ok());
    assert!(
        matches!(runtime.files.authority.try_acquire(), Err(LimboError::InvalidArgument(message)) if message.contains("lost lock ownership")),
        "slot unlock failure allowed authority reuse"
    );
    assert!(matches!(
        runtime.begin_checkpoint_recovery(),
        Err(LimboError::InvalidArgument(_))
    ));
    assert_eq!(std::fs::read(&manifest_path).unwrap(), before);
}

struct HeldManifestRead {
    file: Arc<dyn File>,
    offset: u64,
    completion: Completion,
}

impl HeldManifestRead {
    fn complete(&self) {
        drop(
            self.file
                .pread(self.offset, self.completion.clone())
                .unwrap(),
        );
        assert!(self.completion.finished());
    }
}

#[derive(Default)]
struct RecoveryIoControl {
    delay_manifest_read: crate::sync::atomic::AtomicBool,
    fail_slot_unlock: crate::sync::atomic::AtomicBool,
    pending_read: Mutex<Option<HeldManifestRead>>,
}

struct RecoveryFile {
    inner: Arc<dyn File>,
    control: Arc<RecoveryIoControl>,
    manifest: bool,
}

impl File for RecoveryFile {
    fn file_id(&self) -> Result<Option<FileId>> {
        self.inner.file_id()
    }
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.inner.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.inner.unlock_file()
    }
    fn pread(&self, offset: u64, completion: Completion) -> Result<Completion> {
        if self.manifest
            && self
                .control
                .delay_manifest_read
                .swap(false, Ordering::AcqRel)
        {
            assert!(self
                .control
                .pending_read
                .lock()
                .replace(HeldManifestRead {
                    file: self.inner.clone(),
                    offset,
                    completion: completion.clone(),
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
    fn size(&self) -> Result<u64> {
        self.inner.size()
    }
    fn truncate(&self, size: u64, completion: Completion) -> Result<Completion> {
        self.inner.truncate(size, completion)
    }
    fn shared_wal_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: SharedWalLockKind,
    ) -> Result<()> {
        self.inner.shared_wal_lock_byte(offset, exclusive, kind)
    }
    fn shared_wal_try_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: SharedWalLockKind,
    ) -> Result<bool> {
        self.inner.shared_wal_try_lock_byte(offset, exclusive, kind)
    }
    fn shared_wal_unlock_byte(&self, offset: u64, kind: SharedWalLockKind) -> Result<()> {
        if !self.manifest
            && (2..66).contains(&offset)
            && self.control.fail_slot_unlock.swap(false, Ordering::AcqRel)
        {
            return Err(LimboError::LockingError(
                "injected recovery slot unlock failure".into(),
            ));
        }
        self.inner.shared_wal_unlock_byte(offset, kind)
    }
    fn shared_wal_set_len(&self, size: u64) -> Result<()> {
        self.inner.shared_wal_set_len(size)
    }
    fn shared_wal_map(&self, offset: u64, size: usize) -> Result<Box<dyn SharedWalMappedRegion>> {
        self.inner.shared_wal_map(offset, size)
    }
}

struct RecoveryIo {
    inner: Arc<dyn crate::IO>,
    control: Arc<RecoveryIoControl>,
}

impl crate::Clock for RecoveryIo {
    fn current_time_monotonic(&self) -> crate::MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> crate::WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl crate::IO for RecoveryIo {
    fn open_file(
        &self,
        path: &str,
        flags: crate::OpenFlags,
        direct: bool,
    ) -> Result<Arc<dyn File>> {
        let file = self.inner.open_file(path, flags, direct)?;
        let name = Path::new(path).file_name().and_then(|name| name.to_str());
        if matches!(name, Some("manifest" | "authority")) {
            Ok(Arc::new(RecoveryFile {
                inner: file,
                control: self.control.clone(),
                manifest: name == Some("manifest"),
            }))
        } else {
            Ok(file)
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
