use super::{OfflineBackend, OfflinePrerequisite};
use crate::{LimboError, Result};
use std::path::Path;

pub(super) struct OrdinaryOfflineBackend;

impl OrdinaryOfflineBackend {
    pub(super) fn open(_path: &Path, _: OfflinePrerequisite) -> Result<Self> {
        Err(LimboError::InvalidArgument(
            "ordinary offline backend is not implemented".into(),
        ))
    }
}

impl OfflineBackend for OrdinaryOfflineBackend {
    fn path(&self) -> &Path {
        unreachable!()
    }
    fn recover_materialize_sync(&mut self) -> Result<u64> {
        unreachable!()
    }
    fn materialized_logical_floor(&mut self) -> Result<i64> {
        unreachable!()
    }
    fn commit_marker_checkpoint_sync(&mut self, _: u8) -> Result<()> {
        unreachable!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::Arc;

    #[test]
    #[cfg_attr(
        not(target_os = "linux"),
        ignore = "requires native Linux ext-family filesystem"
    )]
    fn offline_backend_upgrades_real_mvcc_data() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ordinary-mvcc.db");
        let io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        let db = crate::Database::open_file(
            io.clone(),
            path.to_str().unwrap(),
            Arc::new(crate::EngineDialect),
        )
        .unwrap();
        let conn = db.connect(crate::ConnectOpts::new()).unwrap();
        conn.execute("PRAGMA journal_mode=mvcc; CREATE TABLE kept(id INTEGER PRIMARY KEY, value TEXT); CREATE INDEX kept_value ON kept(value); INSERT INTO kept VALUES(1,'retained')").unwrap();
        conn.close().unwrap();
        drop(conn);
        drop(db);
        let backend =
            OrdinaryOfflineBackend::open(&path, OfflinePrerequisite::AllOlderWorkersStopped)
                .unwrap();
        let mut transition = super::super::OfflineModeChange::new(
            backend,
            OfflinePrerequisite::AllOlderWorkersStopped,
            64 * 1024 * 1024,
        );
        transition.upgrade().unwrap();
        drop(transition);
        super::super::validate_shared_open(&path).unwrap();
        let db = crate::Database::open(
            io,
            path.to_str().unwrap(),
            crate::OpenOptions::new(Arc::new(crate::EngineDialect))
                .with_shared_mvcc_for_test(64 * 1024 * 1024),
        )
        .unwrap();
        let conn = db.connect(crate::ConnectOpts::new()).unwrap();
        let mut rows = Vec::new();
        conn.prepare("SELECT value FROM kept INDEXED BY kept_value WHERE value='retained'")
            .unwrap()
            .run_with_row_callback(|row| {
                rows.push(row.get::<&str>(0)?.to_owned());
                Ok(())
            })
            .unwrap();
        assert_eq!(rows, ["retained"]);
    }
}
