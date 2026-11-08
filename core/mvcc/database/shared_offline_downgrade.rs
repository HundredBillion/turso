use super::shared_checkpoint::SharedCheckpointPublication;
use crate::dialect::EngineDialect;
use crate::mvcc::mode_transition::{downgrade_marker, OfflinePrerequisite};
use crate::sync::Arc;
use crate::{ConnectOpts, Database, IOExt, LimboError, OpenFlags, OpenOptions, Result, IO};
use std::path::Path;

pub(in crate::mvcc) fn downgrade_existing(
    path: &Path,
    history_limit: u64,
    prerequisite: OfflinePrerequisite,
) -> Result<()> {
    let path = path
        .to_str()
        .ok_or_else(|| LimboError::InvalidArgument("database path is not UTF-8".into()))?;
    let io: Arc<dyn IO> = Arc::new(crate::io::SyscallIO::new()?);
    let options = OpenOptions::new(Arc::new(EngineDialect))
        .flags(OpenFlags::None)
        .with_shared_mvcc(history_limit);
    let database = Database::open(io.clone(), path, options)?;
    let connection = database.connect(ConnectOpts::new())?;
    let store = database.get_mv_store().as_ref().cloned().ok_or_else(|| {
        LimboError::InvalidArgument("offline downgrade requires a shared store".into())
    })?;
    let mut publication = SharedCheckpointPublication::new(store.clone(), connection.clone())?;
    let published = io.block(|| publication.poll())?;
    drop(publication);
    let mut reading = published.begin_physical_read(store.clone(), connection.clone())?;
    let verified = io.block(|| reading.poll())?;
    drop(reading);
    let mut installing = verified.begin_local_install(store, connection.clone())?;
    let installed = io.block(|| installing.poll())?;
    drop(installing);
    let mut preparing = installed.begin_downgrade(connection, prerequisite)?;
    let intent = io.block(|| preparing.poll())?;
    drop(preparing);
    downgrade_marker::commit_marker(intent)?.cleanup()
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
mod tests {
    use super::*;

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn shared_offline_operator_native_close_preserves_acknowledged_rows() {
        let directory = tempfile::tempdir().unwrap();
        let (path, limit) = crate::mvcc::database::shared_runtime_fixture(directory.path());
        let io: Arc<dyn IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        let options = OpenOptions::new(Arc::new(EngineDialect)).with_shared_mvcc(limit);
        let database = Database::open(io.clone(), path.to_str().unwrap(), options.clone()).unwrap();
        let connection = database.connect(ConnectOpts::new()).unwrap();
        connection
            .execute("UPDATE preserved SET value='acknowledged before close' WHERE id=1")
            .unwrap();
        let before = std::fs::read(&path).unwrap();
        connection
            .close()
            .expect("shared close must not attempt an unowned physical WAL sync");
        assert!(connection.is_closed());
        connection.close().unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), before);
        drop(connection);
        drop(database);
        let database = Database::open(
            io,
            path.to_str().unwrap(),
            options.flags(OpenFlags::ReadOnly),
        )
        .unwrap();
        let connection = database.connect(ConnectOpts::new()).unwrap();
        let mut values = Vec::new();
        connection
            .prepare("SELECT value FROM preserved WHERE id=1")
            .unwrap()
            .run_with_row_callback(|row| {
                values.push(row.get::<String>(0)?);
                Ok(())
            })
            .unwrap();
        assert_eq!(values, ["acknowledged before close"]);
        connection.close().unwrap();
    }
}
