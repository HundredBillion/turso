use crate::io::{File, FileId, SharedWalLockKind};
use crate::sync::atomic::{AtomicBool, Ordering};
use crate::sync::Arc;
use crate::{LimboError, OpenFlags, Result, IO};
use std::path::Path;

// Authority bytes 0/1 and 2..66 belong to lifetime/publication and snapshot slots.
const ADMISSION_LOCK: u64 = 66;
const LOCK_KIND: SharedWalLockKind = SharedWalLockKind::LinuxOfd;

pub(super) struct CheckpointAdmissionGate {
    admission_file: Arc<dyn File>,
    pause_file: Arc<dyn File>,
    admission_owned: AtomicBool,
    pause_owned: AtomicBool,
    poisoned: AtomicBool,
}

pub(super) struct AdmissionGuard {
    gate: Arc<CheckpointAdmissionGate>,
}

pub(super) struct CheckpointPause {
    gate: Arc<CheckpointAdmissionGate>,
}

impl CheckpointPause {
    pub(super) fn belongs_to(&self, gate: &Arc<CheckpointAdmissionGate>) -> bool {
        Arc::ptr_eq(&self.gate, gate)
            && gate.pause_owned.load(Ordering::Acquire)
            && !gate.poisoned.load(Ordering::Acquire)
    }
}

impl CheckpointAdmissionGate {
    /// The caller supplies native I/O and the identity of a retained, validated authority.
    /// Each open must create an independent open-file description for OFD exclusion.
    pub(super) fn open_existing(
        io: &Arc<dyn IO>,
        path: &Path,
        expected_authority_id: FileId,
    ) -> Result<Arc<Self>> {
        if !cfg!(all(target_os = "linux", target_pointer_width = "64"))
            || !io.supports_shared_wal_coordination()
        {
            return Err(LimboError::InvalidArgument(
                "shared MVCC checkpoint admission requires native 64-bit Linux I/O".into(),
            ));
        }
        let path = path
            .to_str()
            .ok_or_else(|| LimboError::InvalidArgument("authority path must be UTF-8".into()))?;
        let open = || -> Result<Arc<dyn File>> {
            let file = io.open_file(path, OpenFlags::NoLock, false)?;
            if file.file_id()? != Some(expected_authority_id) {
                return Err(LimboError::InvalidArgument(
                    "checkpoint gate authority identity changed".into(),
                ));
            }
            Ok(file)
        };
        let admission_file = open()?;
        let pause_file = open()?;
        if Arc::ptr_eq(&admission_file, &pause_file) {
            return Err(LimboError::InvalidArgument(
                "checkpoint gate requires independent native descriptors".into(),
            ));
        }
        Ok(Arc::new(Self {
            admission_file,
            pause_file,
            admission_owned: AtomicBool::new(false),
            pause_owned: AtomicBool::new(false),
            poisoned: AtomicBool::new(false),
        }))
    }

    /// Acquire before publication authority; release after registering the snapshot.
    pub(super) fn try_enter_admission(self: &Arc<Self>) -> Result<AdmissionGuard> {
        self.acquire(false)?;
        Ok(AdmissionGuard { gate: self.clone() })
    }

    /// Pauses new admissions only. Existing transactions must still be allowed to commit.
    /// The caller proves quiescence separately under publication authority, with a deadline.
    pub(super) fn try_pause_admission(self: &Arc<Self>) -> Result<CheckpointPause> {
        self.acquire(true)?;
        Ok(CheckpointPause { gate: self.clone() })
    }

    fn acquire(&self, pause: bool) -> Result<()> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(LimboError::InvalidArgument(
                "checkpoint gate lost lock ownership; close its handles".into(),
            ));
        }
        let (owned, file) = self.slot(pause);
        if owned
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(LimboError::Busy);
        }
        let acquired = file.shared_wal_try_lock_byte(ADMISSION_LOCK, pause, LOCK_KIND);
        #[cfg(test)]
        let acquired =
            if std::env::var_os("TURSO_CHECKPOINT_GATE_IGNORE_CONTENTION_CONTROL").is_some() {
                acquired.map(|_| true)
            } else {
                acquired
            };
        match acquired {
            Ok(true) => Ok(()),
            result => {
                owned.store(false, Ordering::Release);
                match result {
                    Ok(false) => Err(LimboError::Busy),
                    Err(error) => Err(error),
                    Ok(true) => unreachable!(),
                }
            }
        }
    }

    fn slot(&self, pause: bool) -> (&AtomicBool, &Arc<dyn File>) {
        if pause {
            (&self.pause_owned, &self.pause_file)
        } else {
            (&self.admission_owned, &self.admission_file)
        }
    }

    fn release(&self, pause: bool) {
        let (owned, file) = self.slot(pause);
        match file.shared_wal_unlock_byte(ADMISSION_LOCK, LOCK_KIND) {
            Ok(()) => owned.store(false, Ordering::Release),
            Err(error) => {
                self.poisoned.store(true, Ordering::Release);
                tracing::error!(%error, "failed releasing shared MVCC checkpoint admission ownership");
            }
        }
    }
}

impl Drop for AdmissionGuard {
    fn drop(&mut self) {
        self.gate.release(false);
    }
}

impl Drop for CheckpointPause {
    fn drop(&mut self) {
        self.gate.release(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checkpoint_gate_rejects_non_native_without_creation() {
        let dir = tempfile::tempdir().unwrap();
        let io: Arc<dyn IO> = Arc::new(crate::MemoryIO::new());
        assert!(CheckpointAdmissionGate::open_existing(
            &io,
            &dir.path().join("missing"),
            FileId { dev: 0, ino: 0 }
        )
        .is_err());
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
mod native_tests {
    use super::*;
    use crate::mvcc::shared_authority::{PublishedPrefix, SharedMvccAuthority};
    use std::io::{BufRead, BufReader, Write};
    use std::process::{Child, Command, Stdio};
    use std::time::{Duration, Instant};

    const DATABASE_ID: FileId = FileId { dev: 1, ino: 2 };
    const CHILD_TEST: &str = "mvcc::checkpoint_gate::native_tests::checkpoint_gate_child";

    fn io() -> Arc<dyn IO> {
        Arc::new(crate::io::SyscallIO::new().unwrap())
    }

    fn fixture(path: &Path) -> Arc<SharedMvccAuthority> {
        let authority =
            SharedMvccAuthority::open(&io(), path.to_str().unwrap(), DATABASE_ID, 1024).unwrap();
        let mut guard = authority.try_acquire().unwrap();
        guard.recover(PublishedPrefix::default()).unwrap();
        guard.reconcile_incarnation(0).unwrap();
        guard.finish().unwrap();
        authority
    }

    fn gate(path: &Path, authority: &SharedMvccAuthority) -> Arc<CheckpointAdmissionGate> {
        CheckpointAdmissionGate::open_existing(&io(), path, authority.file_id().unwrap().unwrap())
            .unwrap()
    }

    #[test]
    fn checkpoint_gate_aliases_and_local_attempts_do_not_self_upgrade() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("authority");
        let authority = fixture(&path);
        let alias = dir.path().join("alias");
        std::fs::hard_link(&path, &alias).unwrap();
        let first = gate(&path, &authority);
        let second = gate(&alias, &authority);
        let admission = first.try_enter_admission().unwrap();
        assert!(matches!(first.try_enter_admission(), Err(LimboError::Busy)));
        assert!(matches!(first.try_pause_admission(), Err(LimboError::Busy)));
        assert!(matches!(
            second.try_pause_admission(),
            Err(LimboError::Busy)
        ));
        let other_admission = second.try_enter_admission().unwrap();
        drop(admission);
        assert!(matches!(first.try_pause_admission(), Err(LimboError::Busy)));
        drop(other_admission);
        let pause = first.try_pause_admission().unwrap();
        assert!(matches!(first.try_pause_admission(), Err(LimboError::Busy)));
        assert!(matches!(first.try_enter_admission(), Err(LimboError::Busy)));
        assert!(matches!(
            second.try_enter_admission(),
            Err(LimboError::Busy)
        ));
        drop(pause);
        second.try_enter_admission().unwrap();
    }

    #[test]
    fn checkpoint_gate_concurrent_local_pause_has_one_owner() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("authority");
        let authority = fixture(&path);
        let gate = gate(&path, &authority);
        let barrier = std::sync::Barrier::new(3);
        let successes = std::sync::atomic::AtomicUsize::new(0);
        std::thread::scope(|scope| {
            for _ in 0..2 {
                scope.spawn(|| {
                    barrier.wait();
                    let pause = gate.try_pause_admission();
                    if pause.is_ok() {
                        successes.fetch_add(1, Ordering::SeqCst);
                    } else {
                        assert!(matches!(pause, Err(LimboError::Busy)));
                    }
                    barrier.wait();
                    barrier.wait();
                    drop(pause);
                });
            }
            barrier.wait();
            barrier.wait();
            let count = successes.load(Ordering::SeqCst);
            barrier.wait();
            assert_eq!(count, 1);
        });
        gate.try_pause_admission().unwrap();
    }

    #[test]
    fn checkpoint_gate_pause_allows_existing_writer_to_finish_and_drain() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("authority");
        let authority = fixture(&path);
        let gate = gate(&path, &authority);
        let mut writer = spawn(&path, "writer");
        let pause = gate.try_pause_admission().unwrap();
        assert!(matches!(gate.try_enter_admission(), Err(LimboError::Busy)));
        let guard = authority.try_acquire().unwrap();
        assert!(guard.oldest_snapshot().unwrap().is_some());
        guard.finish().unwrap();
        writer.finish();
        let guard = authority.try_acquire().unwrap();
        assert_eq!(guard.oldest_snapshot().unwrap(), None);
        guard.finish().unwrap();
        assert!(matches!(gate.try_enter_admission(), Err(LimboError::Busy)));
        drop(pause);
        gate.try_enter_admission().unwrap();
    }

    #[test]
    fn checkpoint_gate_killed_pause_owner_unblocks_admission() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("authority");
        let authority = fixture(&path);
        let gate = gate(&path, &authority);
        let mut pause_owner = spawn(&path, "pause");
        assert!(matches!(gate.try_enter_admission(), Err(LimboError::Busy)));
        let guard = authority.try_acquire().unwrap();
        assert!(!guard.requires_recovery());
        guard.finish().unwrap();
        pause_owner.0.kill().unwrap();
        pause_owner.0.wait().unwrap();
        gate.try_enter_admission().unwrap();
        gate.try_pause_admission().unwrap();
    }

    #[test]
    fn checkpoint_gate_missing_or_replaced_authority_is_not_created() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("authority");
        let authority = fixture(&path);
        let id = authority.file_id().unwrap().unwrap();
        std::fs::remove_file(&path).unwrap();
        assert!(CheckpointAdmissionGate::open_existing(&io(), &path, id).is_err());
        assert!(!path.exists());
        std::fs::write(&path, b"replacement").unwrap();
        assert!(CheckpointAdmissionGate::open_existing(&io(), &path, id).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"replacement");
    }

    #[test]
    fn checkpoint_gate_child() {
        let Ok(path) = std::env::var("TURSO_CHECKPOINT_GATE_PATH") else {
            return;
        };
        let path = Path::new(&path);
        let authority =
            SharedMvccAuthority::open_existing(&io(), path.to_str().unwrap(), DATABASE_ID, 1024)
                .unwrap();
        let gate = gate(path, &authority);
        let action = std::env::var("TURSO_CHECKPOINT_GATE_ACTION").unwrap();
        let pause;
        let snapshot;
        if action == "pause" {
            pause = Some(gate.try_pause_admission().unwrap());
            snapshot = None;
        } else {
            let admission = gate.try_enter_admission().unwrap();
            let mut guard = authority.try_acquire().unwrap();
            snapshot = Some(guard.register_snapshot().unwrap());
            guard.finish().unwrap();
            drop(admission);
            pause = None;
        }
        println!("GATE_READY");
        std::io::stdout().flush().unwrap();
        let mut command = String::new();
        std::io::stdin().read_line(&mut command).unwrap();
        if action == "writer" {
            // Existing writers use publication authority without reentering admission.
            let mut guard = authority.try_acquire().unwrap();
            guard.allocate_timestamp().unwrap();
            guard.finish().unwrap();
        }
        drop(snapshot);
        drop(pause);
    }

    fn spawn(path: &Path, action: &str) -> ChildProcess {
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", CHILD_TEST, "--nocapture"])
            .env("TURSO_CHECKPOINT_GATE_PATH", path)
            .env("TURSO_CHECKPOINT_GATE_ACTION", action)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut child = ChildProcess(child);
        let stdout = child.0.stdout.take().unwrap();
        let (send, recv) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if line.unwrap().contains("GATE_READY") {
                    let _ = send.send(());
                }
            }
        });
        recv.recv_timeout(Duration::from_secs(20)).unwrap();
        child
    }

    struct ChildProcess(Child);
    impl ChildProcess {
        fn finish(&mut self) {
            self.0
                .stdin
                .as_mut()
                .unwrap()
                .write_all(b"finish\n")
                .unwrap();
            let deadline = Instant::now() + Duration::from_secs(20);
            loop {
                if let Some(status) = self.0.try_wait().unwrap() {
                    assert!(status.success());
                    return;
                }
                assert!(Instant::now() < deadline, "gate child did not finish");
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
    impl Drop for ChildProcess {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}
