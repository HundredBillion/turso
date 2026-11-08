use super::*;
use crate::io::FileId;

pub(crate) struct SharedWalRecoveryLocks {
    mapping: Arc<MappedSharedWalCoordination>,
    file_id: FileId,
    offsets: Vec<u64>,
}

impl SharedWalRecoveryLocks {
    pub(crate) fn acquire(mapping: Arc<MappedSharedWalCoordination>) -> Result<Arc<Self>> {
        if !mapping.uses_linux_ofd_locking() {
            return Err(LimboError::InvalidArgument(
                "physical WAL recovery requires native Linux OFD locks".into(),
            ));
        }
        let file_id = mapping.file.file_id()?.ok_or_else(|| {
            LimboError::InvalidArgument("physical WAL recovery requires native storage".into())
        })?;
        let mut offsets = Vec::new();
        offsets.try_reserve_exact(mapping.header().reader_slot_count as usize + 2)?;
        let mut local = mapping.local_lock_state.lock();
        if local.recovery_poisoned {
            return Err(LimboError::InvalidArgument(
                "physical WAL recovery lost native lock ownership".into(),
            ));
        }
        if local.recovery_held
            || local.writer_lock_held
            || local.checkpoint_lock_held
            || local.reader_locks.iter().any(|count| *count != 0)
        {
            return Err(LimboError::Busy);
        }
        let wanted = [CHECKPOINT_LOCK_OFFSET, WRITER_LOCK_OFFSET]
            .into_iter()
            .chain(
                (0..mapping.header().reader_slot_count)
                    .map(MappedSharedWalCoordination::process_lock_offset_for_reader),
            );
        for offset in wanted {
            let result = mapping
                .file
                .shared_wal_try_lock_byte(offset, true, mapping.lock_kind());
            let error = match result {
                Ok(true) => {
                    offsets.push(offset);
                    continue;
                }
                Ok(false) => LimboError::Busy,
                Err(error) => error,
            };
            let cleanup = release_offsets(&mapping, &offsets);
            if let Err(cleanup) = cleanup {
                local.recovery_poisoned = true;
                return Err(cleanup);
            }
            return Err(error);
        }
        local.recovery_held = true;
        drop(local);
        Ok(Arc::new(Self {
            mapping,
            file_id,
            offsets,
        }))
    }

    pub(crate) fn validate_mapping(
        &self,
        mapping: &Arc<MappedSharedWalCoordination>,
    ) -> Result<()> {
        let local = self.mapping.local_lock_state.lock();
        if !Arc::ptr_eq(&self.mapping, mapping)
            || self.mapping.file.file_id()? != Some(self.file_id)
            || !local.recovery_held
            || local.recovery_poisoned
        {
            return Err(LimboError::InvalidArgument(
                "physical WAL recovery exclusion changed owner or native file".into(),
            ));
        }
        Ok(())
    }
}

impl Drop for SharedWalRecoveryLocks {
    fn drop(&mut self) {
        let mut local = self.mapping.local_lock_state.lock();
        if let Err(error) = release_offsets(&self.mapping, &self.offsets) {
            local.recovery_poisoned = true;
            tracing::error!(%error, "failed releasing physical WAL recovery locks");
        }
        local.recovery_held = false;
    }
}

fn release_offsets(mapping: &MappedSharedWalCoordination, offsets: &[u64]) -> Result<()> {
    let mut first_error = None;
    for offset in offsets.iter().rev() {
        if let Err(error) = mapping
            .file
            .shared_wal_unlock_byte(*offset, mapping.lock_kind())
        {
            first_error.get_or_insert(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
mod tests {
    use super::*;

    #[test]
    #[ignore = "requires native Linux OFD locks"]
    fn physical_wal_recovery_native_owns_all_slots_until_drop() {
        let directory = tempfile::tempdir().unwrap();
        let io: Arc<dyn IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        let mapping = Arc::new(
            MappedSharedWalCoordination::create_or_open(&io, &directory.path().join("tshm"), 64)
                .unwrap(),
        );
        let owner = mapping.owner_record();
        let recovery = SharedWalRecoveryLocks::acquire(mapping.clone()).unwrap();
        assert!(Arc::ptr_eq(&recovery.mapping, &mapping));
        assert!(mapping.register_reader(owner, 1).is_none());
        assert!(mapping.register_db_file_reader(owner).is_none());
        assert!(!mapping.try_acquire_writer(owner));
        assert!(!mapping.try_acquire_checkpoint(owner));
        assert!(mapping.writer_or_checkpoint_lock_active());
        assert!(mapping.checkpoint_lock_active());
        assert!(mapping.has_active_db_file_reader());
        assert_eq!(mapping.min_active_reader_frame(), Some(0));
        let foreign_io: Arc<dyn IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        let foreign = foreign_io
            .open_file(
                directory.path().join("tshm").to_str().unwrap(),
                OpenFlags::NoLock,
                false,
            )
            .unwrap();
        for offset in 1..67 {
            assert!(
                !foreign
                    .shared_wal_try_lock_byte(offset, true, mapping.lock_kind())
                    .unwrap(),
                "native recovery byte {offset} escaped exclusion"
            );
        }
        assert!(matches!(
            SharedWalRecoveryLocks::acquire(mapping.clone()),
            Err(LimboError::Busy)
        ));
        drop(recovery);
        let reader = mapping.register_reader(owner, 1).unwrap();
        mapping.unregister_reader(reader);
        assert!(mapping.try_acquire_writer(owner));
        mapping.release_writer(owner);
        assert!(mapping.try_acquire_checkpoint(owner));
        mapping.release_checkpoint(owner);
        drop(SharedWalRecoveryLocks::acquire(mapping).unwrap());
    }

    #[test]
    #[ignore = "requires native Linux OFD locks"]
    fn physical_wal_recovery_native_stale_probe_preserves_new_local_owner() {
        for (role, offset) in [
            ("writer", WRITER_LOCK_OFFSET),
            ("checkpoint", CHECKPOINT_LOCK_OFFSET),
        ] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("tshm");
            let mapping = native_mapping(&path);
            let sampled_owned = mapping.with_local_lock_state(|local| {
                if offset == WRITER_LOCK_OFFSET {
                    local.writer_lock_held
                } else {
                    local.checkpoint_lock_held
                }
            });
            assert!(!sampled_owned);
            let participant = Participant::acquire(&mapping, role);
            assert!(
                mapping.byte_lock_is_held(offset, sampled_owned),
                "stale probe released the newly acquired local {role}"
            );
            let io: Arc<dyn IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
            let foreign = io
                .open_file(path.to_str().unwrap(), OpenFlags::NoLock, false)
                .unwrap();
            assert!(!foreign
                .shared_wal_try_lock_byte(offset, true, mapping.lock_kind())
                .unwrap());
            participant.assert_unchanged(&mapping);
            participant.release(&mapping);
        }
    }

    enum Participant {
        Writer,
        Checkpoint,
        Reader(SharedReaderSlot),
        SharedReader(SharedReaderSlot),
    }
    impl Participant {
        fn acquire(mapping: &MappedSharedWalCoordination, role: &str) -> Self {
            let owner = mapping.owner_record();
            match role {
                "writer" => {
                    assert!(mapping.try_acquire_writer(owner));
                    Self::Writer
                }
                "checkpoint" => {
                    assert!(mapping.try_acquire_checkpoint(owner));
                    Self::Checkpoint
                }
                "wal-reader" => Self::Reader(mapping.register_reader(owner, 7).unwrap()),
                "main-reader" => {
                    Self::SharedReader(mapping.register_db_file_reader(owner).unwrap())
                }
                _ => panic!("unknown physical participant"),
            }
        }
        fn release(self, mapping: &MappedSharedWalCoordination) {
            match self {
                Self::Writer => mapping.release_writer(mapping.owner_record()),
                Self::Checkpoint => mapping.release_checkpoint(mapping.owner_record()),
                Self::Reader(slot) => mapping.unregister_reader(slot),
                Self::SharedReader(slot) => mapping.unregister_reader_for_snapshot(slot),
            }
        }
        fn assert_unchanged(&self, mapping: &MappedSharedWalCoordination) {
            match self {
                Self::Writer => assert_eq!(mapping.writer_owner(), Some(mapping.owner_record())),
                Self::Checkpoint => {
                    assert_eq!(mapping.checkpoint_owner(), Some(mapping.owner_record()))
                }
                Self::Reader(slot) | Self::SharedReader(slot) => {
                    assert_eq!(mapping.reader_owner(slot.slot_index), Some(slot.owner))
                }
            }
        }
    }

    fn native_mapping(path: &Path) -> Arc<MappedSharedWalCoordination> {
        let io: Arc<dyn IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        Arc::new(MappedSharedWalCoordination::create_or_open(&io, path, 64).unwrap())
    }

    #[test]
    #[ignore = "requires native Linux OFD locks"]
    fn physical_wal_recovery_native_local_owners_remain_valid_until_release() {
        for role in ["writer", "checkpoint", "wal-reader", "main-reader"] {
            let directory = tempfile::tempdir().unwrap();
            let mapping = native_mapping(&directory.path().join("tshm"));
            let participant = Participant::acquire(&mapping, role);
            assert!(matches!(
                SharedWalRecoveryLocks::acquire(mapping.clone()),
                Err(LimboError::Busy)
            ));
            participant.assert_unchanged(&mapping);
            participant.release(&mapping);
            drop(SharedWalRecoveryLocks::acquire(mapping.clone()).unwrap());
            Participant::acquire(&mapping, role).release(&mapping);
        }
    }

    struct NativeChild(std::process::Child);
    impl NativeChild {
        fn spawn(path: &Path, role: &str) -> Self {
            use std::io::BufRead;
            let mut child = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "storage::shared_wal_coordination::recovery::tests::physical_wal_recovery_child", "--ignored", "--nocapture", "--test-threads=1"])
                .env("TURSO_PHYSICAL_RECOVERY_PATH", path)
                .env("TURSO_PHYSICAL_RECOVERY_ROLE", role)
                .stdin(std::process::Stdio::null()).stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::inherit()).spawn().unwrap();
            let stdout = child.stdout.take().unwrap();
            let (send, receive) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                for line in std::io::BufReader::new(stdout).lines() {
                    if send.send(line).is_err() {
                        break;
                    }
                }
            });
            let child = Self(child);
            let deadline = Instant::now() + Duration::from_secs(15);
            loop {
                let line = receive
                    .recv_timeout(deadline.checked_duration_since(Instant::now()).unwrap())
                    .unwrap()
                    .unwrap();
                if line.contains("PHYSICAL_PARTICIPANT_READY") {
                    return child;
                }
            }
        }
        fn kill(&mut self) {
            use std::os::unix::process::ExitStatusExt;
            self.0.kill().unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if let Some(status) = self.0.try_wait().unwrap() {
                    assert_eq!(status.signal(), Some(9));
                    return;
                }
                assert!(Instant::now() < deadline, "physical owner failed to exit");
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
    impl Drop for NativeChild {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    #[test]
    #[ignore = "requires native Linux OFD locks"]
    fn physical_wal_recovery_native_foreign_owners_block_until_sigkill() {
        for role in ["writer", "checkpoint", "wal-reader", "main-reader"] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("tshm");
            let mapping = native_mapping(&path);
            let mut child = NativeChild::spawn(&path, role);
            assert!(matches!(
                SharedWalRecoveryLocks::acquire(mapping.clone()),
                Err(LimboError::Busy)
            ));
            child.kill();
            drop(SharedWalRecoveryLocks::acquire(mapping.clone()).unwrap());
            Participant::acquire(&mapping, role).release(&mapping);
        }
    }

    #[derive(Default)]
    struct LockFaults {
        tries: AtomicU64,
        fail_try: AtomicU64,
        busy_try: AtomicU64,
        unlocks: AtomicU64,
        fail_unlock: AtomicU64,
    }
    struct FaultFile {
        inner: Arc<dyn File>,
        faults: Arc<LockFaults>,
    }
    impl File for FaultFile {
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
            self.inner.pread(offset, completion)
        }
        fn pwrite(
            &self,
            offset: u64,
            buffer: Arc<crate::Buffer>,
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
        fn size(&self) -> Result<u64> {
            self.inner.size()
        }
        fn shared_wal_try_lock_byte(
            &self,
            offset: u64,
            exclusive: bool,
            kind: SharedWalLockKind,
        ) -> Result<bool> {
            let ordinal = self.faults.tries.fetch_add(1, Ordering::AcqRel) + 1;
            if self.faults.fail_try.load(Ordering::Acquire) == ordinal {
                return Err(LimboError::LockingError(
                    "injected physical acquisition error".into(),
                ));
            }
            if self.faults.busy_try.load(Ordering::Acquire) == ordinal {
                return Ok(false);
            }
            self.inner.shared_wal_try_lock_byte(offset, exclusive, kind)
        }
        fn shared_wal_unlock_byte(&self, offset: u64, kind: SharedWalLockKind) -> Result<()> {
            if offset != PROCESS_LIFETIME_LOCK_OFFSET {
                let ordinal = self.faults.unlocks.fetch_add(1, Ordering::AcqRel) + 1;
                if self.faults.fail_unlock.load(Ordering::Acquire) == ordinal {
                    return Err(LimboError::LockingError(
                        "injected physical unlock error".into(),
                    ));
                }
            }
            self.inner.shared_wal_unlock_byte(offset, kind)
        }
    }
    fn faulty_mapping(path: &Path, faults: Arc<LockFaults>) -> Arc<MappedSharedWalCoordination> {
        let io: Arc<dyn IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        let mut mapping = MappedSharedWalCoordination::create_or_open(&io, path, 64).unwrap();
        mapping.file = Arc::new(FaultFile {
            inner: mapping.file.clone(),
            faults,
        });
        Arc::new(mapping)
    }
    fn foreign_file(path: &Path) -> Arc<dyn File> {
        let io: Arc<dyn IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        io.open_file(path.to_str().unwrap(), OpenFlags::NoLock, false)
            .unwrap()
    }
    fn assert_poisoned(mapping: &Arc<MappedSharedWalCoordination>) {
        assert!(
            matches!(SharedWalRecoveryLocks::acquire(mapping.clone()), Err(LimboError::InvalidArgument(message)) if message.contains("lost native lock ownership"))
        );
        assert!(!mapping.try_acquire_writer(mapping.owner_record()));
        assert!(!mapping.try_acquire_checkpoint(mapping.owner_record()));
        assert!(mapping.register_reader(mapping.owner_record(), 1).is_none());
        assert!(mapping
            .register_db_file_reader(mapping.owner_record())
            .is_none());
    }

    #[test]
    #[ignore = "requires native Linux OFD locks"]
    fn physical_wal_recovery_native_acquisition_failures_release_only_owned_prefix() {
        for busy in [false, true] {
            for at in 1..=66 {
                let directory = tempfile::tempdir().unwrap();
                let path = directory.path().join("tshm");
                let faults = Arc::new(LockFaults::default());
                let mapping = faulty_mapping(&path, faults.clone());
                if busy {
                    faults.busy_try.store(at, Ordering::Release);
                } else {
                    faults.fail_try.store(at, Ordering::Release);
                }
                let result = SharedWalRecoveryLocks::acquire(mapping.clone());
                if busy {
                    assert!(matches!(result, Err(LimboError::Busy)));
                } else {
                    assert!(
                        matches!(result, Err(LimboError::LockingError(message)) if message == "injected physical acquisition error")
                    );
                }
                assert_eq!(faults.unlocks.load(Ordering::Acquire), at - 1);
                assert!(!mapping.local_lock_state.lock().recovery_poisoned);
                let foreign = foreign_file(&path);
                for offset in 1..67 {
                    assert!(foreign
                        .shared_wal_try_lock_byte(offset, true, mapping.lock_kind())
                        .unwrap());
                    foreign
                        .shared_wal_unlock_byte(offset, mapping.lock_kind())
                        .unwrap();
                }
                drop(SharedWalRecoveryLocks::acquire(mapping).unwrap());
            }
        }
    }

    #[test]
    #[ignore = "requires native Linux OFD locks"]
    fn physical_wal_recovery_native_unlock_failures_poison_incomplete_cleanup_and_drop() {
        for fail_at in 1..=66 {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("tshm");
            let faults = Arc::new(LockFaults::default());
            let mapping = faulty_mapping(&path, faults.clone());
            let recovery = SharedWalRecoveryLocks::acquire(mapping.clone()).unwrap();
            let failed_offset = recovery.offsets[66 - fail_at as usize];
            faults.fail_unlock.store(fail_at, Ordering::Release);
            drop(recovery);
            assert_eq!(faults.unlocks.load(Ordering::Acquire), 66);
            assert_poisoned(&mapping);
            let foreign = foreign_file(&path);
            for offset in 1..67 {
                let acquired = foreign
                    .shared_wal_try_lock_byte(offset, true, mapping.lock_kind())
                    .unwrap();
                assert_eq!(acquired, offset != failed_offset);
                if acquired {
                    foreign
                        .shared_wal_unlock_byte(offset, mapping.lock_kind())
                        .unwrap();
                }
            }
            drop(mapping);
            assert!(foreign
                .shared_wal_try_lock_byte(failed_offset, true, SharedWalLockKind::LinuxOfd)
                .unwrap());
            foreign
                .shared_wal_unlock_byte(failed_offset, SharedWalLockKind::LinuxOfd)
                .unwrap();
        }
        for failed_acquisition in 2..=66 {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("tshm");
            let faults = Arc::new(LockFaults::default());
            let mapping = faulty_mapping(&path, faults.clone());
            faults.fail_try.store(failed_acquisition, Ordering::Release);
            faults.fail_unlock.store(1, Ordering::Release);
            assert!(
                matches!(SharedWalRecoveryLocks::acquire(mapping.clone()), Err(LimboError::LockingError(message)) if message == "injected physical unlock error")
            );
            assert_eq!(
                faults.unlocks.load(Ordering::Acquire),
                failed_acquisition - 1
            );
            assert_poisoned(&mapping);
        }
    }

    #[test]
    #[ignore = "subprocess helper; requires parent physical participant parameters"]
    fn physical_wal_recovery_child() {
        use std::io::Write;
        let path = std::env::var("TURSO_PHYSICAL_RECOVERY_PATH").unwrap();
        let role = std::env::var("TURSO_PHYSICAL_RECOVERY_ROLE").unwrap();
        let mapping = native_mapping(Path::new(&path));
        let _participant = Participant::acquire(&mapping, &role);
        println!("PHYSICAL_PARTICIPANT_READY");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    }
}
