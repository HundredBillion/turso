#[cfg(test)]
mod tests {
    use crate::mvcc::mode_transition::ordinary::OrdinaryOfflineBackend;
    use crate::mvcc::mode_transition::{
        validate_shared_open, OfflineModeChange, OfflinePrerequisite,
    };
    use std::path::PathBuf;

    const LIMIT: u64 = 64 * 1024 * 1024;

    #[test]
    #[cfg_attr(
        not(target_os = "linux"),
        ignore = "requires native Linux ext-family filesystem"
    )]
    fn staging_cleanup_fresh_publication_removes_owned_ordinary_sidecars() {
        let dir = tempfile::tempdir().unwrap();
        let staging = dir.path().join("staging.db");
        let destination = dir.path().join("published.db");
        {
            let io: crate::sync::Arc<dyn crate::IO> =
                crate::sync::Arc::new(crate::io::SyscallIO::new().unwrap());
            let db = crate::Database::open_file(
                io,
                staging.to_str().unwrap(),
                crate::sync::Arc::new(crate::EngineDialect),
            )
            .unwrap();
            let conn = db.connect(crate::ConnectOpts::new()).unwrap();
            conn.execute("PRAGMA journal_mode=mvcc; CREATE TABLE kept(id INTEGER PRIMARY KEY)")
                .unwrap();
            conn.close().unwrap();
        }
        let backend =
            OrdinaryOfflineBackend::open(&staging, OfflinePrerequisite::AllOlderWorkersStopped)
                .unwrap();
        OfflineModeChange::new(backend, OfflinePrerequisite::AllOlderWorkersStopped, LIMIT)
            .publish_new(&destination)
            .unwrap();
        validate_shared_open(&destination).unwrap();
        assert!(!staging.exists());
        for path in [
            PathBuf::from(format!("{}-wal", staging.display())),
            staging.with_extension("db-log"),
        ] {
            assert!(
                !path.exists(),
                "owned staging sidecar remained after publication: {}",
                path.display()
            );
        }
    }
}
