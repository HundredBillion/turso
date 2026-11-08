use super::shared_authority::{AuthorityGuard, PublicationLease};
use crate::io::{File, FileId, FileSyncType};
use crate::sync::{Arc, Weak};
use crate::{Buffer, Completion, LimboError, Result};

pub(super) struct ReadPinnedFile {
    inner: Arc<dyn File>,
    lease: Weak<PublicationLease>,
}
impl ReadPinnedFile {
    pub(super) fn new(inner: Arc<dyn File>, guard: &AuthorityGuard) -> Result<Arc<Self>> {
        guard.published()?;
        let lease = guard.io_lease();
        PublicationLease::pin(&lease)?;
        Ok(Arc::new(Self { inner, lease }))
    }

    pub(super) fn for_manifest_replay(
        inner: Arc<dyn File>,
        manifest: &super::commit_manifest::LoadedManifest,
    ) -> Result<Arc<Self>> {
        if manifest.record().requires_recovery() {
            return Err(LimboError::InvalidArgument(
                "logical replay requires a normal manifest".into(),
            ));
        }
        let lease = manifest.guard()?.io_lease();
        PublicationLease::pin(&lease)?;
        Ok(Arc::new(Self { inner, lease }))
    }
}
impl File for ReadPinnedFile {
    fn file_id(&self) -> Result<Option<FileId>> {
        self.inner.file_id()
    }
    fn lock_file(&self, _: bool) -> Result<()> {
        Err(LimboError::ReadOnly)
    }
    fn unlock_file(&self) -> Result<()> {
        Err(LimboError::ReadOnly)
    }
    fn size(&self) -> Result<u64> {
        self.inner.size()
    }
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        let pin = PublicationLease::pin(&self.lease).inspect_err(|_| c.abort())?;
        let caller = c.clone();
        let physical = Completion::new_read(c.as_read().buf_arc(), move |result| {
            // The physical callback retains the pin even after caller cancellation or completion.
            let _pin = &pin;
            match result {
                Ok((_, count)) => caller.complete(count),
                Err(error) => caller.error(error),
            }
            None
        });
        let _physical = self.inner.pread(pos, physical).inspect_err(|_| c.abort())?;
        Ok(c)
    }
    fn pwrite(&self, _: u64, _: Arc<Buffer>, _: Completion) -> Result<Completion> {
        Err(LimboError::ReadOnly)
    }
    fn sync(&self, _: Completion, _: FileSyncType) -> Result<Completion> {
        Err(LimboError::ReadOnly)
    }
    fn truncate(&self, _: u64, _: Completion) -> Result<Completion> {
        Err(LimboError::ReadOnly)
    }
}

#[cfg(all(test, not(shuttle)))]
mod tests {
    use super::*;
    use crate::mvcc::shared_authority::{authority_for_test, PublishedPrefix};
    use crate::sync::atomic::{AtomicBool, Ordering};
    use crate::sync::Mutex;

    #[derive(Default)]
    struct DelayedFile {
        locked: AtomicBool,
        pending: Mutex<Option<Completion>>,
        fail_submission: AtomicBool,
    }
    impl File for DelayedFile {
        fn file_id(&self) -> Result<Option<FileId>> {
            Ok(Some(FileId { dev: 1, ino: 3 }))
        }
        fn lock_file(&self, _: bool) -> Result<()> {
            Ok(())
        }
        fn unlock_file(&self) -> Result<()> {
            Ok(())
        }
        fn size(&self) -> Result<u64> {
            Ok(4)
        }
        fn pread(&self, _: u64, c: Completion) -> Result<Completion> {
            assert!(self.pending.lock().replace(c.clone()).is_none());
            if self.fail_submission.load(Ordering::Acquire) {
                return Err(LimboError::InternalError(
                    "injected submission failure".into(),
                ));
            }
            Ok(c)
        }
        fn pwrite(&self, _: u64, _: Arc<Buffer>, _: Completion) -> Result<Completion> {
            panic!("unexpected mutation")
        }
        fn sync(&self, _: Completion, _: FileSyncType) -> Result<Completion> {
            panic!("unexpected mutation")
        }
        fn truncate(&self, _: u64, _: Completion) -> Result<Completion> {
            panic!("unexpected mutation")
        }
        fn shared_wal_try_lock_byte(
            &self,
            _: u64,
            _: bool,
            _: crate::io::SharedWalLockKind,
        ) -> Result<bool> {
            Ok(self
                .locked
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok())
        }
        fn shared_wal_unlock_byte(&self, _: u64, _: crate::io::SharedWalLockKind) -> Result<()> {
            self.locked.store(false, Ordering::Release);
            Ok(())
        }
    }

    #[test]
    fn read_pin_survives_caller_abort_and_physical_callback_until_retirement() {
        for abort in [false, true] {
            let backend = Arc::new(DelayedFile::default());
            let authority = authority_for_test(backend.clone());
            let mut guard = authority.try_acquire().unwrap();
            guard.recover(PublishedPrefix::default()).unwrap();
            let file = ReadPinnedFile::new(backend.clone(), &guard).unwrap();
            let caller = file
                .pread(
                    0,
                    Completion::new_read(Arc::new(Buffer::new_temporary(4)), |_| None),
                )
                .unwrap();
            if abort {
                caller.abort();
            }
            drop(file);
            drop(guard);
            assert!(
                matches!(authority.try_acquire(), Err(LimboError::Busy)),
                "backend still owns read"
            );
            backend.pending.lock().as_ref().unwrap().complete(4);
            assert!(
                matches!(authority.try_acquire(), Err(LimboError::Busy)),
                "callback does not retire physical Arc"
            );
            backend.pending.lock().take();
            assert!(authority.try_acquire().unwrap().requires_recovery());
        }
    }
    #[test]
    fn read_pin_covers_submission_error_and_rejects_stale_lease_and_mutations() {
        let backend = Arc::new(DelayedFile::default());
        let authority = authority_for_test(backend.clone());
        let mut guard = authority.try_acquire().unwrap();
        guard.recover(PublishedPrefix::default()).unwrap();
        let file = ReadPinnedFile::new(backend.clone(), &guard).unwrap();
        assert_eq!(file.file_id().unwrap(), backend.file_id().unwrap());
        assert!(matches!(
            file.pwrite(
                0,
                Arc::new(Buffer::new_temporary(4)),
                Completion::new_write(|_| {})
            ),
            Err(LimboError::ReadOnly)
        ));
        assert!(matches!(
            file.sync(Completion::new_sync(|_| {}), FileSyncType::FullFsync),
            Err(LimboError::ReadOnly)
        ));
        assert!(matches!(
            file.truncate(0, Completion::new_trunc(|_| {})),
            Err(LimboError::ReadOnly)
        ));
        backend.fail_submission.store(true, Ordering::Release);
        assert!(file
            .pread(
                0,
                Completion::new_read(Arc::new(Buffer::new_temporary(4)), |_| None)
            )
            .is_err());
        drop(guard);
        assert!(matches!(authority.try_acquire(), Err(LimboError::Busy)));
        backend.pending.lock().as_ref().unwrap().complete(4);
        assert!(matches!(authority.try_acquire(), Err(LimboError::Busy)));
        backend.pending.lock().take();
        let mut recovered = authority.try_acquire().unwrap();
        recovered.recover(PublishedPrefix::default()).unwrap();
        assert!(matches!(
            file.pread(
                0,
                Completion::new_read(Arc::new(Buffer::new_temporary(4)), |_| None)
            ),
            Err(LimboError::Busy)
        ));
        assert!(backend.pending.lock().is_none());
        recovered.finish().unwrap();
    }
}
