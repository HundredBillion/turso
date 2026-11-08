use super::*;
use crate::mvcc::shared_ids::io_protocol::checkpoint_prepare::finalize::FinalizedIds;

#[allow(dead_code)]
pub(in crate::mvcc) struct FinalizedRecoveredCheckpoint {
    runtime: Arc<SharedMvccRuntime>,
    parts: PhysicalBaseParts,
    finalized: FinalizedIds,
    pause: CheckpointPause,
}
#[allow(dead_code)]
impl FinalizedRecoveredCheckpoint {
    pub(in crate::mvcc) fn guard(&self) -> &AuthorityGuard {
        self.finalized.guard()
    }
    pub(in crate::mvcc) fn proof(&self) -> &CompletedCheckpointProof {
        self.finalized.proof()
    }
}

#[allow(dead_code)]
pub(in crate::mvcc) struct RecoveryIdFinalizationState {
    confirmed: Option<RecoveredCheckpointConfirmation>,
}
#[allow(dead_code)]
impl RecoveredCheckpointConfirmation {
    pub(in crate::mvcc) fn begin_id_finalization(self) -> Result<RecoveryIdFinalizationState> {
        Ok(RecoveryIdFinalizationState {
            confirmed: Some(self),
        })
    }
}
#[allow(dead_code)]
impl RecoveryIdFinalizationState {
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<FinalizedRecoveredCheckpoint> {
        self.confirmed.take();
        Err(LimboError::InvalidArgument(
            "recovered checkpoint ID finalization is not implemented".into(),
        )
        .into())
    }
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
mod tests {
    use super::super::tests::fault_fixture_with_seed;
    use super::*;
    use crate::io::FileId;
    use crate::mvcc::shared_authority::SharedMvccAuthority;
    use crate::sync::atomic::{AtomicUsize, Ordering};
    use std::io::Write;
    use std::os::unix::fs::MetadataExt;
    use std::path::Path;

    #[derive(Default)]
    struct EpochTrace {
        owner: Mutex<
            Option<(
                crate::sync::Weak<SharedMvccAuthority>,
                crate::sync::Weak<SharedMvccRuntime>,
                u64,
            )>,
        >,
        erasures: AtomicUsize,
    }
    impl EpochTrace {
        fn check_replay(&self) {
            if let Some((_, runtime, _)) = self.owner.lock().as_ref() {
                assert!(
                    runtime.upgrade().unwrap().replay.lock().is_none(),
                    "ID mutation retained old runtime replay"
                );
            }
        }
        fn before_erase(&self) {
            self.check_replay();
            let owner = self.owner.lock();
            let (authority, _, maximum) = owner.as_ref().unwrap();
            let (epoch, ready, _) = authority
                .upgrade()
                .unwrap()
                .recovery_state_for_test()
                .unwrap();
            assert!(
                ready && epoch > *maximum,
                "old ID bank erased before retained owner epoch reconciliation"
            );
            self.erasures.fetch_add(1, Ordering::AcqRel);
        }
    }
    struct EpochFile {
        inner: Arc<dyn File>,
        trace: Arc<EpochTrace>,
    }
    impl File for EpochFile {
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
            bytes: Arc<crate::Buffer>,
            completion: Completion,
        ) -> Result<Completion> {
            self.trace.check_replay();
            self.inner.pwrite(offset, bytes, completion)
        }
        fn sync(&self, completion: Completion, kind: FileSyncType) -> Result<Completion> {
            self.trace.check_replay();
            assert!(matches!(kind, FileSyncType::FullFsync));
            self.inner.sync(completion, kind)
        }
        fn truncate(&self, size: u64, completion: Completion) -> Result<Completion> {
            if size == 0 {
                self.trace.before_erase();
            }
            self.inner.truncate(size, completion)
        }
    }
    struct EpochIo {
        inner: Arc<dyn crate::IO>,
        trace: Arc<EpochTrace>,
    }
    impl crate::Clock for EpochIo {
        fn current_time_monotonic(&self) -> crate::MonotonicInstant {
            self.inner.current_time_monotonic()
        }
        fn current_time_wall_clock(&self) -> crate::WallClockInstant {
            self.inner.current_time_wall_clock()
        }
    }
    impl crate::IO for EpochIo {
        fn open_file(
            &self,
            path: &str,
            flags: crate::OpenFlags,
            direct: bool,
        ) -> Result<Arc<dyn File>> {
            let inner = self.inner.open_file(path, flags, direct)?;
            if matches!(
                Path::new(path).file_name().and_then(|name| name.to_str()),
                Some("ids-a" | "ids-b" | "ids-selector")
            ) {
                Ok(Arc::new(EpochFile {
                    inner,
                    trace: self.trace.clone(),
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

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn checkpoint_recovery_finalize_native_fresh_incarnation_preserves_epoch_without_admission() {
        let (_directory, path, namespace, earlier, old_maximum, target) = {
            let mut old = fault_fixture_with_seed(false, true);
            let inspected = old.inspected.take().unwrap();
            let maximum = inspected
                .ids
                .max_owner_epoch()
                .expect("genuine owner epoch missing");
            let target = inspected.ids.identity();
            drop(inspected);
            (
                old._directory,
                old.path,
                old.namespace,
                old.earlier_coordination,
                maximum,
                target,
            )
        };
        let paths = [
            path.clone(),
            namespace.join("manifest"),
            namespace.join("logical"),
            namespace.join("physical-wal"),
            namespace.join("ids-a"),
            namespace.join("ids-b"),
            namespace.join("ids-selector"),
        ];
        let before = paths.each_ref().map(|path| std::fs::read(path).unwrap());
        let authority_path = namespace.join("authority");
        let old_metadata = std::fs::metadata(&authority_path).unwrap();
        assert_eq!(old_metadata.len(), earlier.len() as u64);
        let old_id = (old_metadata.dev(), old_metadata.ino());
        let mut restore = std::fs::OpenOptions::new()
            .write(true)
            .open(&authority_path)
            .unwrap();
        restore.write_all(&earlier).unwrap();
        restore.sync_all().unwrap();
        drop(restore);
        let metadata = std::fs::metadata(&authority_path).unwrap();
        assert_eq!((metadata.dev(), metadata.ino()), old_id);
        assert_eq!(
            paths.each_ref().map(|path| std::fs::read(path).unwrap()),
            before
        );
        let trace = Arc::new(EpochTrace::default());
        let io: Arc<dyn crate::IO> = Arc::new(EpochIo {
            inner: Arc::new(crate::io::SyscallIO::new().unwrap()),
            trace: trace.clone(),
        });
        let canonical = std::fs::canonicalize(&path).unwrap();
        let main = io
            .open_file(canonical.to_str().unwrap(), crate::OpenFlags::NoLock, false)
            .unwrap();
        let id = main.file_id().unwrap().unwrap();
        let metadata = std::fs::metadata(&path).unwrap();
        assert_eq!(
            id,
            FileId {
                dev: metadata.dev(),
                ino: metadata.ino()
            }
        );
        let runtime = SharedMvccRuntime::open_existing(
            io.clone(),
            main,
            id,
            &canonical,
            64 * 1024 * 1024,
            false,
        )
        .unwrap();
        *trace.owner.lock() = Some((
            Arc::downgrade(&runtime.files.authority),
            Arc::downgrade(&runtime),
            old_maximum,
        ));
        let (epoch, ready, next_timestamp) =
            runtime.files.authority.recovery_state_for_test().unwrap();
        assert!(!ready, "fixture retained an old authority incarnation");
        assert!(
            epoch <= old_maximum,
            "fixture does not require retained owner reconciliation"
        );
        let mut opening = runtime.begin_checkpoint_recovery().unwrap();
        let selected = io.block(|| Ok(opening.poll()?)).unwrap();
        let mut physical = selected
            .begin_physical_read(crate::OpenOptions::new(Arc::new(crate::EngineDialect)))
            .unwrap();
        let physical = io.block(|| Ok(physical.poll()?)).unwrap();
        let mut loading = physical.begin_id_inspection().unwrap();
        let inspected = io.block(|| Ok(loading.poll()?)).unwrap();
        assert_eq!(inspected.ids.max_owner_epoch(), Some(old_maximum));
        let mut confirming = inspected.begin_confirmation().unwrap();
        let confirmed = io.block(|| Ok(confirming.poll()?)).unwrap();
        assert_eq!(confirmed.proof().source_identity(), None);
        assert_eq!(confirmed.proof().target_identity(), target);
        assert_eq!(confirmed.max_owner_epoch(), Some(old_maximum));
        assert!(confirmed.guard().requires_recovery());
        let mut finalizing = confirmed.begin_id_finalization().unwrap();
        let finalized = io.block(|| Ok(finalizing.poll()?)).unwrap();
        let (epoch, ready, after_timestamp) =
            runtime.files.authority.recovery_state_for_test().unwrap();
        assert_eq!(trace.erasures.load(Ordering::Acquire), 1);
        assert!(ready);
        assert!(epoch > old_maximum);
        assert!(after_timestamp >= next_timestamp);
        assert!(after_timestamp > target.prefix.committed_timestamp);
        assert!(runtime.replay.lock().is_none());
        assert_eq!(finalized.guard().published().unwrap(), target.prefix);
        assert!(finalized.proof().matches_lease(finalized.guard()));
        assert!(matches!(
            runtime.gate.try_enter_admission(),
            Err(LimboError::Busy)
        ));
        assert!(matches!(
            runtime.files.authority.try_acquire(),
            Err(LimboError::Busy)
        ));
        for index in [0, 1, 2, 3] {
            assert_eq!(std::fs::read(&paths[index]).unwrap(), before[index]);
        }
    }
}
