use crate::io::FileId;
use crate::sync::Arc;
use crate::{LimboError, Result, IO};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct PublishedPrefix {
    pub committed_timestamp: u64,
    pub end_offset: u64,
    pub running_crc: u32,
}

pub(super) struct SharedMvccAuthority;

pub(super) struct AuthorityGuard;

pub(super) struct SnapshotRegistration {
    pub begin_timestamp: u64,
    pub epoch: u64,
    pub prefix: PublishedPrefix,
}

impl SharedMvccAuthority {
    pub(super) fn open(
        _io: &Arc<dyn IO>,
        _path: &str,
        _database_id: FileId,
        _history_limit: u64,
    ) -> Result<Arc<Self>> {
        Err(unimplemented_authority())
    }

    pub(super) fn try_acquire(self: &Arc<Self>) -> Result<AuthorityGuard> {
        Err(unimplemented_authority())
    }
}

impl AuthorityGuard {
    pub(super) fn requires_recovery(&self) -> bool {
        true
    }

    pub(super) fn recover(&mut self, _prefix: PublishedPrefix) -> Result<()> {
        Err(unimplemented_authority())
    }

    pub(super) fn published(&self) -> Result<PublishedPrefix> {
        Err(unimplemented_authority())
    }

    pub(super) fn register_snapshot(&mut self) -> Result<SnapshotRegistration> {
        Err(unimplemented_authority())
    }

    pub(super) fn oldest_snapshot(&self) -> Result<Option<u64>> {
        Err(unimplemented_authority())
    }

    pub(super) fn finish(self) -> Result<()> {
        Err(unimplemented_authority())
    }
}

fn unimplemented_authority() -> LimboError {
    LimboError::InvalidArgument("shared MVCC authority is not implemented".into())
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::process::{Child, Command, Stdio};
    use std::time::Duration;

    const CHILD_TEST: &str = "mvcc::shared_authority::tests::authority_child";
    const DATABASE_ID: FileId = FileId { dev: 1, ino: 2 };

    #[test]
    fn authority_recovers_after_independent_owner_is_killed() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination");
        let authority = open(&path);
        initialize(&authority);
        let mut child = spawn(&path, "guard");
        assert!(matches!(authority.try_acquire(), Err(LimboError::Busy)));
        child.kill();
        let mut guard = authority.try_acquire().unwrap();
        assert!(guard.requires_recovery());
        assert!(guard.published().is_err());
        let recovered = PublishedPrefix {
            committed_timestamp: 9,
            end_offset: 128,
            running_crc: 17,
        };
        guard.recover(recovered).unwrap();
        assert_eq!(guard.published().unwrap(), recovered);
        guard.finish().unwrap();
        let guard = authority.try_acquire().unwrap();
        assert!(!guard.requires_recovery());
        assert_eq!(guard.published().unwrap(), recovered);
        guard.finish().unwrap();
    }

    #[test]
    fn authority_retains_live_snapshots_and_reclaims_dead_process_slots() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination");
        let authority = open(&path);
        initialize(&authority);
        let mut child = spawn(&path, "snapshot");
        let mut guard = authority.try_acquire().unwrap();
        let child_timestamp = guard.oldest_snapshot().unwrap().unwrap();
        let parent = guard.register_snapshot().unwrap();
        assert!(parent.begin_timestamp > child_timestamp);
        assert_eq!(guard.oldest_snapshot().unwrap(), Some(child_timestamp));
        guard.finish().unwrap();
        child.kill();
        let guard = authority.try_acquire().unwrap();
        assert!(!guard.requires_recovery());
        assert_eq!(
            guard.oldest_snapshot().unwrap(),
            Some(parent.begin_timestamp)
        );
        drop(parent);
        assert_eq!(guard.oldest_snapshot().unwrap(), None);
        guard.finish().unwrap();
    }

    #[test]
    fn authority_reopen_requires_recovery_and_local_abandonment_is_visible() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("coordination");
        let authority = open(&path);
        initialize(&authority);
        drop(authority.try_acquire().unwrap());
        let mut guard = authority.try_acquire().unwrap();
        assert!(guard.requires_recovery());
        guard.recover(PublishedPrefix::default()).unwrap();
        guard.finish().unwrap();
        drop(authority);
        let reopened = open(&path);
        assert!(reopened.try_acquire().unwrap().requires_recovery());
    }

    #[test]
    fn authority_child() {
        let Ok(path) = std::env::var("TURSO_MVCC_AUTHORITY_TEST_PATH") else {
            return;
        };
        let authority = open(std::path::Path::new(&path));
        let mut guard = authority.try_acquire().unwrap();
        assert!(!guard.requires_recovery());
        let action = std::env::var("TURSO_MVCC_AUTHORITY_TEST_ACTION").unwrap();
        let snapshot = if action == "snapshot" {
            let snapshot = guard.register_snapshot().unwrap();
            guard.finish().unwrap();
            Some(snapshot)
        } else {
            println!("AUTHORITY_READY");
            std::io::stdout().flush().unwrap();
            let mut command = String::new();
            std::io::stdin().read_line(&mut command).unwrap();
            guard.finish().unwrap();
            return;
        };
        println!("AUTHORITY_READY");
        std::io::stdout().flush().unwrap();
        let mut command = String::new();
        std::io::stdin().read_line(&mut command).unwrap();
        drop(snapshot);
    }

    fn open(path: &std::path::Path) -> Arc<SharedMvccAuthority> {
        let io: Arc<dyn IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        SharedMvccAuthority::open(&io, path.to_str().unwrap(), DATABASE_ID, 1024).unwrap()
    }

    fn initialize(authority: &Arc<SharedMvccAuthority>) {
        let mut guard = authority.try_acquire().unwrap();
        assert!(guard.requires_recovery());
        guard.recover(PublishedPrefix::default()).unwrap();
        guard.finish().unwrap();
    }

    fn spawn(path: &std::path::Path, action: &str) -> ChildProcess {
        let child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", CHILD_TEST, "--nocapture"])
            .env("TURSO_MVCC_AUTHORITY_TEST_PATH", path)
            .env("TURSO_MVCC_AUTHORITY_TEST_ACTION", action)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();
        let mut child = ChildProcess(child);
        let stdout = child.0.stdout.take().unwrap();
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if line.unwrap().contains("AUTHORITY_READY") {
                    let _ = sender.send(());
                    return;
                }
            }
        });
        receiver.recv_timeout(Duration::from_secs(20)).unwrap();
        child
    }

    struct ChildProcess(Child);

    impl ChildProcess {
        fn kill(&mut self) {
            self.0.kill().unwrap();
            self.0.wait().unwrap();
        }
    }

    impl Drop for ChildProcess {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}
