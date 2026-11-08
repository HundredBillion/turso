use super::*;

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_connection_native_pause_rejects_connection_before_header_io() {
    let directory = tempfile::tempdir().unwrap();
    let (path, limit) = shared_runtime_fixture(directory.path());
    let io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
    let database = crate::Database::open(
        io,
        path.to_str().unwrap(),
        crate::OpenOptions::new(Arc::new(crate::EngineDialect)).with_shared_mvcc_for_test(limit),
    )
    .unwrap();
    let store = database.get_mv_store().as_ref().unwrap().clone();
    let runtime = store.shared_runtime.get().unwrap();
    let pause = runtime.gate.try_pause_admission().unwrap();
    assert!(
        matches!(
            database.connect(crate::ConnectOpts::new()),
            Err(LimboError::Busy)
        ),
        "a paused checkpoint admitted a new shared connection"
    );
    drop(pause);
    let connection = database.connect(crate::ConnectOpts::new()).unwrap();
    connection.execute("SELECT value FROM preserved").unwrap();
}
