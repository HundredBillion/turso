//! Shared file mutations retain publication ownership through backend completion retirement.

use super::shared_authority::{AuthorityGuard, PublicationLease};
use crate::io::{CompletionGroup, File, FileId, FileSyncType};
use crate::sync::{Arc, Mutex, Weak};
use crate::{Buffer, Completion, CompletionError, LimboError, Result};

pub(super) struct GuardedFile {
    inner: Arc<dyn File>,
    lease: Mutex<LeaseSlot>,
}

struct LeaseSlot {
    lease: Weak<PublicationLease>,
    bound: bool,
}

pub(super) struct FileBinding {
    file: Arc<GuardedFile>,
    lease: Weak<PublicationLease>,
}

impl GuardedFile {
    pub(super) fn new(inner: Arc<dyn File>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            lease: Mutex::new(LeaseSlot {
                lease: Weak::new(),
                bound: false,
            }),
        })
    }

    pub(super) fn bind(self: &Arc<Self>, guard: &AuthorityGuard) -> Result<FileBinding> {
        let mut current = self.lease.lock();
        if current.lease.upgrade().is_some() {
            return Err(LimboError::Busy);
        }
        let lease = guard.io_lease();
        PublicationLease::pin(&lease)?;
        current.lease = lease.clone();
        current.bound = true;
        Ok(FileBinding {
            file: self.clone(),
            lease,
        })
    }

    fn pin(&self) -> Result<super::shared_authority::IoPin> {
        let slot = self.lease.lock();
        if !slot.bound {
            return Err(LimboError::Busy);
        }
        PublicationLease::pin(&slot.lease)
    }

    fn completion(
        &self,
        caller: &Completion,
        expected: Option<usize>,
        kind: CompletionKind,
    ) -> Result<Completion> {
        let pin = self.pin()?;
        let caller = caller.clone();
        let callback = move |result: std::result::Result<i32, CompletionError>| {
            // The backend owns this Fn capture until its last completion reference retires.
            let _pin = &pin;
            match result {
                Ok(bytes)
                    if expected.is_some_and(|expected| usize::try_from(bytes) != Ok(expected)) =>
                {
                    caller.error(CompletionError::ShortWrite)
                }
                Ok(bytes) if bytes < 0 => caller.error(CompletionError::IOError(
                    std::io::ErrorKind::Other,
                    "negative guarded file operation result",
                )),
                Ok(bytes) => caller.complete(bytes),
                Err(error) => caller.error(error),
            }
        };
        Ok(match kind {
            CompletionKind::Write => Completion::new_write(callback),
            CompletionKind::Sync => Completion::new_sync(callback),
            CompletionKind::Truncate => Completion::new_trunc(callback),
        })
    }
}

enum CompletionKind {
    Write,
    Sync,
    Truncate,
}

impl Drop for FileBinding {
    fn drop(&mut self) {
        let mut current = self.file.lease.lock();
        if Weak::ptr_eq(&current.lease, &self.lease) {
            current.bound = false;
        }
    }
}

impl File for GuardedFile {
    fn file_id(&self) -> Result<Option<FileId>> {
        self.inner.file_id()
    }
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.inner.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.inner.unlock_file()
    }
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        self.inner.pread(pos, c)
    }
    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
        let expected = buffer.len();
        if expected > i32::MAX as usize || pos.checked_add(expected as u64).is_none() {
            return Err(LimboError::TooBig);
        }
        let physical = self.completion(&c, Some(expected), CompletionKind::Write)?;
        let _physical = self.inner.pwrite(pos, buffer, physical)?;
        Ok(c)
    }
    fn pwritev(&self, pos: u64, buffers: Vec<Arc<Buffer>>, c: Completion) -> Result<Completion> {
        let total = buffers
            .iter()
            .try_fold(0usize, |total, buffer| total.checked_add(buffer.len()))
            .ok_or(LimboError::TooBig)?;
        if total > i32::MAX as usize || pos.checked_add(total as u64).is_none() {
            return Err(LimboError::TooBig);
        }
        let pin = self.pin()?;
        let caller = c.clone();
        let mut group = CompletionGroup::new(move |result| {
            let _pin = &pin;
            match result {
                Ok(_) => caller.complete(total as i32),
                Err(error) => caller.error(error),
            }
        });
        let mut offset = pos;
        // Validate each physical write; the default vectored fanout does not aggregate child errors.
        for buffer in buffers {
            let bytes = buffer.len();
            let child = Completion::new_write(|_| {});
            group.add(&child);
            if let Err(error) = self.pwrite(offset, buffer, child) {
                group.cancel();
                c.abort();
                return Err(error);
            }
            offset += bytes as u64;
        }
        let _group = group.build();
        Ok(c)
    }
    fn sync(&self, c: Completion, kind: FileSyncType) -> Result<Completion> {
        let physical = self.completion(&c, None, CompletionKind::Sync)?;
        let _physical = self.inner.sync(physical, kind)?;
        Ok(c)
    }
    fn size(&self) -> Result<u64> {
        self.inner.size()
    }
    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        let physical = self.completion(&c, None, CompletionKind::Truncate)?;
        let _physical = self.inner.truncate(len, physical)?;
        Ok(c)
    }
}

#[cfg(all(test, not(shuttle)))]
mod tests {
    use super::*;
    use crate::io::SharedWalLockKind;
    use crate::mvcc::shared_authority::{authority_for_test, PublishedPrefix};
    use crate::sync::atomic::{AtomicBool, Ordering};
    use crate::sync::Mutex;
    use crate::LimboError;

    enum Operation {
        Write(u64, Arc<Buffer>),
        Sync,
        Truncate(u64),
    }
    struct Pending {
        completion: Completion,
        operation: Operation,
    }
    struct SubmitBarrier {
        entered: std::sync::mpsc::SyncSender<()>,
        release: std::sync::mpsc::Receiver<()>,
    }
    #[derive(Default)]
    struct DelayedFile {
        locked: AtomicBool,
        pending: Mutex<Vec<Pending>>,
        bytes: Mutex<Vec<u8>>,
        syncs: crate::sync::atomic::AtomicUsize,
        fail_submission: crate::sync::atomic::AtomicUsize,
        submissions: crate::sync::atomic::AtomicUsize,
        fail_unlock: AtomicBool,
        submit_barrier: Mutex<Option<SubmitBarrier>>,
    }
    impl DelayedFile {
        fn submit(&self, c: Completion, operation: Operation) -> Result<Completion> {
            let barrier = self.submit_barrier.lock().take();
            if let Some(SubmitBarrier { entered, release }) = barrier {
                entered.send(()).unwrap();
                release
                    .recv_timeout(std::time::Duration::from_secs(5))
                    .unwrap();
            }
            let submission = self.submissions.fetch_add(1, Ordering::SeqCst) + 1;
            if self.fail_submission.load(Ordering::SeqCst) == submission {
                return Err(LimboError::InternalError(
                    "injected submission error".into(),
                ));
            }
            self.pending.lock().push(Pending {
                completion: c.clone(),
                operation,
            });
            Ok(c)
        }
        fn complete_at(&self, index: usize, result: i32) {
            let pending = self.pending.lock();
            let operation = &pending[index];
            if result >= 0 {
                match &operation.operation {
                    Operation::Write(offset, buffer) => {
                        let mut bytes = self.bytes.lock();
                        let count = (result as usize).min(buffer.len());
                        let end = *offset as usize + count;
                        if bytes.len() < end {
                            bytes.resize(end, 0);
                        }
                        bytes[*offset as usize..end].copy_from_slice(&buffer.as_slice()[..count]);
                    }
                    Operation::Sync => {
                        self.syncs.fetch_add(1, Ordering::SeqCst);
                    }
                    Operation::Truncate(len) => self.bytes.lock().resize(*len as usize, 0),
                }
            }
            operation.completion.complete(result);
        }
        fn complete_without_retiring(&self, result: i32) {
            let count = self.pending.lock().len();
            for index in 0..count {
                self.complete_at(index, result);
            }
        }
        fn retire(&self) {
            self.pending.lock().clear();
        }
    }
    impl File for DelayedFile {
        fn file_id(&self) -> Result<Option<FileId>> {
            Ok(Some(FileId { dev: 1, ino: 2 }))
        }
        fn lock_file(&self, _exclusive: bool) -> Result<()> {
            Ok(())
        }
        fn unlock_file(&self) -> Result<()> {
            Ok(())
        }
        fn pread(&self, _pos: u64, c: Completion) -> Result<Completion> {
            c.complete(0);
            Ok(c)
        }
        fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, c: Completion) -> Result<Completion> {
            self.submit(c, Operation::Write(pos, buffer))
        }
        fn sync(&self, c: Completion, _kind: FileSyncType) -> Result<Completion> {
            self.submit(c, Operation::Sync)
        }
        fn size(&self) -> Result<u64> {
            Ok(self.bytes.lock().len() as u64)
        }
        fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
            self.submit(c, Operation::Truncate(len))
        }
        fn shared_wal_try_lock_byte(
            &self,
            _offset: u64,
            _exclusive: bool,
            _kind: SharedWalLockKind,
        ) -> Result<bool> {
            Ok(self
                .locked
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok())
        }
        fn shared_wal_unlock_byte(&self, _offset: u64, _kind: SharedWalLockKind) -> Result<()> {
            if self.fail_unlock.load(Ordering::SeqCst) {
                return Err(LimboError::InternalError("injected unlock failure".into()));
            }
            self.locked.store(false, Ordering::Release);
            Ok(())
        }
    }

    #[test]
    fn backend_retention_outlives_caller_abort_and_guard_drop() {
        for operation in 0..4 {
            let backend = Arc::new(DelayedFile::default());
            *backend.bytes.lock() = vec![7; 8];
            let authority = authority_for_test(backend.clone());
            let mut guard = authority.try_acquire().unwrap();
            guard.recover(PublishedPrefix::default()).unwrap();
            let file = GuardedFile::new(backend.clone());
            let binding = file.bind(&guard).unwrap();
            let buffer = Arc::new(Buffer::new(vec![1; 16]));
            let completion = match operation {
                0 => file.pwrite(0, buffer, Completion::new_write(|_| {})),
                1 => file.pwritev(
                    0,
                    vec![buffer.clone(), buffer],
                    Completion::new_write(|_| {}),
                ),
                2 => file.sync(Completion::new_sync(|_| {}), FileSyncType::Fsync),
                _ => file.truncate(0, Completion::new_trunc(|_| {})),
            }
            .unwrap();
            completion.abort();
            drop(completion);
            drop(binding);
            drop(file);
            drop(guard);
            assert!(
                matches!(authority.try_acquire(), Err(LimboError::Busy)),
                "publication lock released before backend retired operation {operation}"
            );
            assert_eq!(*backend.bytes.lock(), vec![7; 8]);
            backend.complete_without_retiring(if operation <= 1 { 16 } else { 0 });
            match operation {
                0 => assert_eq!(*backend.bytes.lock(), vec![1; 16]),
                1 => assert_eq!(*backend.bytes.lock(), vec![1; 32]),
                2 => assert_eq!(backend.syncs.load(Ordering::SeqCst), 1),
                _ => assert!(backend.bytes.lock().is_empty()),
            }
            assert!(
                matches!(authority.try_acquire(), Err(LimboError::Busy)),
                "callback invocation prematurely released backend pin"
            );
            backend.retire();
            let mut recovered = authority.try_acquire().unwrap();
            assert!(recovered.requires_recovery());
            recovered.recover(PublishedPrefix::default()).unwrap();
            recovered.finish().unwrap();
        }
    }
    fn initialized(
        backend: &Arc<DelayedFile>,
    ) -> (
        Arc<crate::mvcc::shared_authority::SharedMvccAuthority>,
        AuthorityGuard,
    ) {
        let authority = authority_for_test(backend.clone());
        let mut guard = authority.try_acquire().unwrap();
        guard.recover(PublishedPrefix::default()).unwrap();
        (authority, guard)
    }

    #[test]
    fn finish_rejects_backend_pins_even_after_successful_callback() {
        let backend = Arc::new(DelayedFile::default());
        let (authority, guard) = initialized(&backend);
        let file = GuardedFile::new(backend.clone());
        let _binding = file.bind(&guard).unwrap();
        let completion = file
            .pwrite(
                0,
                Arc::new(Buffer::new(vec![1; 4])),
                Completion::new_write(|_| {}),
            )
            .unwrap();
        assert!(!guard.io_retired());
        backend.complete_without_retiring(4);
        assert!(completion.succeeded());
        assert!(!guard.io_retired());
        assert!(matches!(guard.finish(), Err(LimboError::Busy)));
        assert!(matches!(authority.try_acquire(), Err(LimboError::Busy)));
        assert!(matches!(
            file.sync(Completion::new_sync(|_| {}), FileSyncType::Fsync),
            Err(LimboError::Busy)
        ));
        backend.retire();
        let mut guard = authority.try_acquire().unwrap();
        assert!(guard.requires_recovery());
        guard.recover(PublishedPrefix::default()).unwrap();
        guard.finish().unwrap();
        let guard = authority.try_acquire().unwrap();
        assert!(!guard.requires_recovery());
        guard.finish().unwrap();
    }

    #[test]
    fn binding_scope_rejects_mutation_and_cannot_disable_new_binding() {
        let backend = Arc::new(DelayedFile::default());
        let (authority, guard) = initialized(&backend);
        let file = GuardedFile::new(backend.clone());
        assert!(matches!(
            file.truncate(0, Completion::new_trunc(|_| {})),
            Err(LimboError::Busy)
        ));
        assert_eq!(file.file_id().unwrap(), Some(FileId { dev: 1, ino: 2 }));
        assert_eq!(file.size().unwrap(), 0);
        assert!(file
            .pread(
                0,
                Completion::new_read(Arc::new(Buffer::new(vec![0; 4])), |_| None)
            )
            .unwrap()
            .succeeded());
        let old = file.bind(&guard).unwrap();
        assert!(matches!(file.bind(&guard), Err(LimboError::Busy)));
        guard.finish().unwrap();
        let guard = authority.try_acquire().unwrap();
        let new = file.bind(&guard).unwrap();
        drop(old);
        let completion = file
            .sync(Completion::new_sync(|_| {}), FileSyncType::Fsync)
            .unwrap();
        backend.complete_without_retiring(0);
        backend.retire();
        assert!(completion.succeeded());
        drop(new);
        assert!(matches!(
            file.sync(Completion::new_sync(|_| {}), FileSyncType::Fsync),
            Err(LimboError::Busy)
        ));
        guard.finish().unwrap();
    }

    #[test]
    fn dropped_binding_cannot_replace_retained_backend_lease() {
        let backend = Arc::new(DelayedFile::default());
        let (_, guard) = initialized(&backend);
        let file = GuardedFile::new(backend.clone());
        let binding = file.bind(&guard).unwrap();
        let completion = file
            .sync(Completion::new_sync(|_| {}), FileSyncType::Fsync)
            .unwrap();
        completion.abort();
        drop(binding);
        drop(guard);
        let other = Arc::new(DelayedFile::default());
        let (_, other_guard) = initialized(&other);
        assert!(matches!(file.bind(&other_guard), Err(LimboError::Busy)));
        backend.complete_without_retiring(0);
        assert!(matches!(file.bind(&other_guard), Err(LimboError::Busy)));
        backend.retire();
        let _binding = file.bind(&other_guard).unwrap();
        other_guard.finish().unwrap();
    }

    #[test]
    fn short_negative_and_failed_vector_children_reach_caller() {
        for result in [-1, 0, 3, 5] {
            let backend = Arc::new(DelayedFile::default());
            let (_, guard) = initialized(&backend);
            let file = GuardedFile::new(backend.clone());
            let _binding = file.bind(&guard).unwrap();
            let buffer = Arc::new(Buffer::new(vec![2; 4]));
            let completion = file
                .pwritev(
                    0,
                    vec![buffer.clone(), buffer],
                    Completion::new_write(|_| {}),
                )
                .unwrap();
            backend.complete_at(0, result);
            backend.complete_at(1, 4);
            assert_eq!(completion.get_error(), Some(CompletionError::ShortWrite));
            drop(guard);
            assert!(backend.locked.load(Ordering::Acquire));
            backend.retire();
            assert!(!backend.locked.load(Ordering::Acquire));
        }
        let backend = Arc::new(DelayedFile::default());
        let (_, guard) = initialized(&backend);
        let file = GuardedFile::new(backend.clone());
        let _binding = file.bind(&guard).unwrap();
        let completion = file
            .pwrite(
                0,
                Arc::new(Buffer::new(vec![1; 4])),
                Completion::new_write(|_| {}),
            )
            .unwrap();
        backend.pending.lock()[0]
            .completion
            .error(CompletionError::Aborted);
        assert_eq!(completion.get_error(), Some(CompletionError::Aborted));
        backend.retire();
        drop(guard);
    }

    #[test]
    fn vector_submission_failure_and_overflow_keep_submitted_children_safe() {
        let backend = Arc::new(DelayedFile::default());
        let (authority, guard) = initialized(&backend);
        let file = GuardedFile::new(backend.clone());
        let _binding = file.bind(&guard).unwrap();
        let buffer = Arc::new(Buffer::new(vec![2; 4]));
        assert!(matches!(
            file.pwritev(
                u64::MAX,
                vec![buffer.clone()],
                Completion::new_write(|_| {})
            ),
            Err(LimboError::TooBig)
        ));
        assert_eq!(backend.submissions.load(Ordering::SeqCst), 0);
        backend.fail_submission.store(2, Ordering::SeqCst);
        let caller = Completion::new_write(|_| {});
        assert!(file
            .pwritev(0, vec![buffer.clone(), buffer], caller.clone())
            .is_err());
        assert!(caller.failed());
        drop(guard);
        assert!(matches!(authority.try_acquire(), Err(LimboError::Busy)));
        backend.complete_without_retiring(4);
        assert!(matches!(authority.try_acquire(), Err(LimboError::Busy)));
        backend.retire();
        assert!(authority.try_acquire().unwrap().requires_recovery());
    }

    #[test]
    fn pin_precedes_backend_submission_and_survives_concurrent_guard_drop() {
        let backend = Arc::new(DelayedFile::default());
        let (authority, guard) = initialized(&backend);
        let file = GuardedFile::new(backend.clone());
        let binding = file.bind(&guard).unwrap();
        let (entered_tx, entered_rx) = std::sync::mpsc::sync_channel(0);
        let (release_tx, release_rx) = std::sync::mpsc::sync_channel(0);
        *backend.submit_barrier.lock() = Some(SubmitBarrier {
            entered: entered_tx,
            release: release_rx,
        });
        let submitted = std::thread::spawn(move || {
            file.pwrite(
                0,
                Arc::new(Buffer::new(vec![1; 4])),
                Completion::new_write(|_| {}),
            )
            .unwrap()
        });
        entered_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        drop(binding);
        drop(guard);
        assert!(backend.pending.lock().is_empty());
        assert!(matches!(authority.try_acquire(), Err(LimboError::Busy)));
        release_tx.send(()).unwrap();
        let caller = submitted.join().unwrap();
        caller.abort();
        backend.complete_without_retiring(4);
        assert!(matches!(authority.try_acquire(), Err(LimboError::Busy)));
        backend.retire();
        assert!(authority.try_acquire().unwrap().requires_recovery());
    }

    #[test]
    fn empty_vector_needs_binding_but_leaves_no_retained_pin() {
        let backend = Arc::new(DelayedFile::default());
        let (_, guard) = initialized(&backend);
        let file = GuardedFile::new(backend.clone());
        assert!(matches!(
            file.pwritev(0, vec![], Completion::new_write(|_| {})),
            Err(LimboError::Busy)
        ));
        let _binding = file.bind(&guard).unwrap();
        assert!(file
            .pwritev(0, vec![], Completion::new_write(|_| {}))
            .unwrap()
            .succeeded());
        assert!(backend.pending.lock().is_empty());
        assert!(guard.io_retired());
        guard.finish().unwrap();
    }

    #[test]
    fn failed_unlock_poisoning_preserves_fail_closed_authority() {
        for pending in [false, true] {
            let backend = Arc::new(DelayedFile::default());
            let (authority, guard) = initialized(&backend);
            let file = GuardedFile::new(backend.clone());
            let _binding = file.bind(&guard).unwrap();
            backend.fail_unlock.store(true, Ordering::SeqCst);
            if pending {
                let caller = file
                    .sync(Completion::new_sync(|_| {}), FileSyncType::Fsync)
                    .unwrap();
                drop(guard);
                backend.complete_without_retiring(0);
                backend.retire();
                assert!(caller.succeeded());
            } else {
                assert!(guard.finish().is_err());
            }
            assert!(backend.locked.load(Ordering::Acquire));
            assert!(matches!(
                authority.try_acquire(),
                Err(LimboError::InvalidArgument(_))
            ));
        }
    }
    #[cfg(target_os = "linux")]
    fn real_authority(
        path: &std::path::Path,
    ) -> Arc<crate::mvcc::shared_authority::SharedMvccAuthority> {
        let io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        crate::mvcc::shared_authority::SharedMvccAuthority::open(
            &io,
            path.to_str().unwrap(),
            FileId { dev: 1, ino: 2 },
            1_000_000,
        )
        .unwrap()
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn guarded_file_peer_process() {
        let Ok(path) = std::env::var("TURSO_GUARDED_FILE_PEER_PATH") else {
            return;
        };
        let authority = real_authority(std::path::Path::new(&path));
        if std::env::var("TURSO_GUARDED_FILE_PEER_MODE").unwrap() == "busy" {
            assert!(matches!(authority.try_acquire(), Err(LimboError::Busy)));
        } else {
            let mut guard = authority.try_acquire().unwrap();
            assert!(guard.requires_recovery());
            guard.recover(PublishedPrefix::default()).unwrap();
            guard.finish().unwrap();
        }
    }

    #[cfg(target_os = "linux")]
    fn peer(path: &std::path::Path, mode: &str) {
        use std::process::{Command, Stdio};
        use std::time::{Duration, Instant};
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "mvcc::guarded_file::tests::guarded_file_peer_process",
                "--nocapture",
            ])
            .env("TURSO_GUARDED_FILE_PEER_PATH", path)
            .env("TURSO_GUARDED_FILE_PEER_MODE", mode)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("guarded-file peer exceeded deadline");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn process_ofd_lock_survives_abort_until_backend_retirement() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("authority");
        let authority = real_authority(&path);
        let mut guard = authority.try_acquire().unwrap();
        guard.recover(PublishedPrefix::default()).unwrap();
        let backend = Arc::new(DelayedFile::default());
        let file = GuardedFile::new(backend.clone());
        let binding = file.bind(&guard).unwrap();
        let caller = file
            .pwrite(
                0,
                Arc::new(Buffer::new(vec![3; 4])),
                Completion::new_write(|_| {}),
            )
            .unwrap();
        caller.abort();
        drop(caller);
        drop(binding);
        drop(file);
        drop(guard);
        peer(&path, "busy");
        backend.complete_without_retiring(4);
        peer(&path, "busy");
        backend.retire();
        peer(&path, "recover");
        let guard = authority.try_acquire().unwrap();
        assert!(!guard.requires_recovery());
        guard.finish().unwrap();
    }
}
