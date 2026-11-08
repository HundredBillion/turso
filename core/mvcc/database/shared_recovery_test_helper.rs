use super::*;

pub fn recovered_checkpoint_for_test(
    directory: &Path,
    options: crate::OpenOptions,
) -> Result<(Arc<crate::Database>, Arc<Connection>)> {
    let path = directory.join("recovered.db");
    let timestamp = materialize_marked_fixture_with_sql(&path,
        "CREATE TABLE recovered_items(id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT);          INSERT INTO recovered_items VALUES(1,'original'),(9,'deleted');          DELETE FROM recovered_items WHERE id=9;          CREATE SEQUENCE recovered_sequence START WITH 41; SELECT nextval('recovered_sequence')",
    );
    let history_limit = 64 * 1024 * 1024;
    crate::mvcc::mode_transition::prepare_test_namespace(&path, timestamp, history_limit)?;
    let path = path
        .canonicalize()
        .map_err(|error| LimboError::InvalidArgument(error.to_string()))?;
    let io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new()?);
    let main = io.open_file(
        path.to_str()
            .ok_or_else(|| LimboError::InvalidArgument("test path must be UTF-8".into()))?,
        crate::OpenFlags::NoLock,
        false,
    )?;
    let identity = main.file_id()?.ok_or_else(|| {
        LimboError::InvalidArgument("test main has no native file identity".into())
    })?;
    let runtime =
        SharedMvccRuntime::open_existing(io.clone(), main, identity, &path, history_limit, false)?;
    let mut selecting = runtime.begin_checkpoint_recovery()?;
    let selected = io.block(|| selecting.poll())?;
    let mut physical = selected.begin_physical_read(options)?;
    let physical = io.block(|| physical.poll())?;
    let mut inspecting = physical.begin_id_inspection()?;
    let inspected = io.block(|| inspecting.poll())?;
    let mut confirming = inspected.begin_confirmation()?;
    let confirmed = io.block(|| confirming.poll())?;
    let mut finalizing = confirmed.begin_id_finalization()?;
    let finalized = io.block(|| finalizing.poll())?;
    let mut reclaiming = finalized.begin_log_reclamation()?;
    let reclaimed = io.block(|| reclaiming.poll())?;
    let mut bootstrap = reclaimed.begin_bootstrap()?;
    Ok(io.block(|| bootstrap.poll())?.into_parts())
}

pub fn hold_shared_prepare_authority_for_test(
    connection: &Arc<Connection>,
) -> Result<Box<dyn FnOnce() -> Result<()> + Send>> {
    let store = connection
        .db
        .get_mv_store()
        .as_ref()
        .cloned()
        .ok_or_else(|| LimboError::InvalidArgument("test connection requires MVCC".into()))?;
    let runtime = store.shared_runtime.get().ok_or_else(|| {
        LimboError::InvalidArgument("test connection requires shared MVCC".into())
    })?;
    let guard = runtime.files.authority.try_acquire()?;
    Ok(Box::new(move || guard.finish()))
}
