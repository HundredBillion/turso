use super::OfflinePrerequisite;
use crate::io::FileId;
use crate::mvcc::shared_authority::PublishedPrefix;
use crate::sync::Arc;
use crate::{LimboError, Result};
use std::path::Path;

pub(crate) struct PreparedMarkerRecovery;

impl PreparedMarkerRecovery {
    pub(super) fn open(_: &Path, _: u64, _: OfflinePrerequisite) -> Result<Arc<Self>> {
        Err(LimboError::InvalidArgument(
            "prepared recovery validation is not implemented".into(),
        ))
    }
    pub(crate) fn path(&self) -> &Path {
        unreachable!()
    }
    pub(crate) fn database_id(&self) -> FileId {
        unreachable!()
    }
    pub(super) fn prefix(&self) -> PublishedPrefix {
        unreachable!()
    }
    pub(crate) fn timestamp(&self) -> u64 {
        self.prefix().committed_timestamp
    }
    pub(crate) fn revalidate(&self) -> Result<()> {
        unreachable!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mvcc::mode_transition::ordinary::{
        lock_exclusive, open_native, OrdinaryOfflineBackend,
    };
    use crate::mvcc::mode_transition::{
        identity, namespace_for, prepare_namespace, OfflineBackend,
    };

    const LIMIT: u64 = 64 * 1024 * 1024;

    fn prepared() -> (tempfile::TempDir, std::path::PathBuf, u64) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("prepared.db");
        {
            let io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
            let db = crate::Database::open_file(
                io,
                path.to_str().unwrap(),
                Arc::new(crate::EngineDialect),
            )
            .unwrap();
            let conn = db.connect(crate::ConnectOpts::new()).unwrap();
            conn.execute("PRAGMA journal_mode=mvcc; CREATE TABLE kept(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO kept VALUES(1,'durable')").unwrap();
            conn.close().unwrap();
        }
        let mut backend =
            OrdinaryOfflineBackend::open(&path, OfflinePrerequisite::AllOlderWorkersStopped)
                .unwrap();
        let timestamp = backend.recover_materialize_sync().unwrap();
        let floor = backend.materialized_logical_floor().unwrap();
        let id = identity(&open_native(&path).unwrap()).unwrap();
        prepare_namespace(
            &namespace_for(&path, id).unwrap(),
            id,
            timestamp,
            floor,
            LIMIT,
        )
        .unwrap();
        drop(backend);
        (dir, path, timestamp)
    }

    #[test]
    #[cfg_attr(
        not(target_os = "linux"),
        ignore = "requires native Linux ext-family filesystem"
    )]
    fn marker_recovery_token_retains_main_exclusion_and_initial_proofs() {
        let (_dir, path, timestamp) = prepared();
        let token =
            PreparedMarkerRecovery::open(&path, LIMIT, OfflinePrerequisite::AllOlderWorkersStopped)
                .unwrap();
        assert_eq!(token.timestamp(), timestamp);
        token.revalidate().unwrap();
        let competitor = open_native(&path).unwrap();
        assert!(lock_exclusive(&competitor).is_err());
        drop(token);
        lock_exclusive(&competitor).unwrap();
    }
}
