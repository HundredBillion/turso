use super::*;

fn completed_files(finalize_ids: bool) -> (tempfile::TempDir, PathBuf, PathBuf, u64) {
    let fixture = crate::mvcc::database::shared_completed_checkpoint_fixture(|_| {
        Arc::new(crate::io::SyscallIO::new().unwrap())
    });
    let crate::mvcc::database::SharedCompletedCheckpointFixture {
        proof,
        guard,
        ids,
        pause,
        database,
        connection,
        io,
        path,
        namespace,
        limit,
        directory,
    } = fixture;
    let logical = namespace.join("logical");
    assert!(std::fs::metadata(&logical).unwrap().len() > 0);
    if finalize_ids {
        let mut finalizing = ids.finalize(guard, proof).unwrap();
        let finalized = io.block(|| Ok(finalizing.poll()?)).unwrap();
        drop(finalized);
        drop(finalizing);
    } else {
        drop(ids);
        drop(proof);
        drop(guard);
    }
    drop(connection);
    drop(database);
    drop(pause);
    drop(io);
    (directory, path, logical, limit)
}

fn open_and_check(path: &Path, limit: u64) -> Arc<crate::Database> {
    let io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
    let database = crate::Database::open(
        io,
        path.to_str().unwrap(),
        crate::OpenOptions::new(Arc::new(crate::EngineDialect)).with_shared_mvcc_for_test(limit),
    )
    .unwrap();
    let connection = database.connect(crate::ConnectOpts::new()).unwrap();
    let mut values = Vec::new();
    connection
        .prepare("SELECT value FROM preserved WHERE id=1")
        .unwrap()
        .run_with_row_callback(|row| {
            values.push(row.get::<&str>(0)?.to_owned());
            Ok(())
        })
        .unwrap();
    assert_eq!(values, ["materialized shared row"]);
    connection.execute("UPDATE preserved SET value='opened through normal recovery' WHERE id=1; INSERT INTO preserved VALUES(2,'new public-open row')").unwrap();
    database
}

#[test]
#[ignore = "requires native shared MVCC files"]
fn shared_open_recovery_native_completed_pending_ids_opens() {
    let (_directory, path, _logical, limit) = completed_files(false);
    let database = open_and_check(&path, limit);
    let peer_io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
    let peer = crate::Database::open(
        peer_io,
        path.to_str().unwrap(),
        crate::OpenOptions::new(database.dialect()).with_shared_mvcc_for_test(limit),
    )
    .unwrap();
    let reader = peer.connect(crate::ConnectOpts::new()).unwrap();
    let mut values = Vec::new();
    reader
        .prepare("SELECT value FROM preserved ORDER BY id")
        .unwrap()
        .run_with_row_callback(|row| {
            values.push(row.get::<&str>(0)?.to_owned());
            Ok(())
        })
        .unwrap();
    assert_eq!(
        values,
        ["opened through normal recovery", "new public-open row"]
    );
}

#[test]
#[ignore = "requires native shared MVCC files"]
fn shared_open_recovery_native_finalized_ids_reclaims_old_log() {
    let (_directory, path, logical, limit) = completed_files(true);
    let io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
    let database = crate::Database::open(
        io,
        path.to_str().unwrap(),
        crate::OpenOptions::new(Arc::new(crate::EngineDialect)).with_shared_mvcc_for_test(limit),
    )
    .unwrap();
    assert_eq!(
        std::fs::metadata(logical).unwrap().len(),
        0,
        "normal shared open retained obsolete checkpoint log bytes"
    );
    let connection = database.connect(crate::ConnectOpts::new()).unwrap();
    connection
        .execute("UPDATE preserved SET value='finalized IDs recovered' WHERE id=1")
        .unwrap();
}

#[test]
#[ignore = "requires native shared MVCC files"]
fn shared_open_recovery_native_healthy_open_keeps_live_reader() {
    for nonempty_prefix in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let (path, limit) = crate::mvcc::database::shared_runtime_fixture(directory.path());
        let io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        let options = crate::OpenOptions::new(Arc::new(crate::EngineDialect))
            .with_shared_mvcc_for_test(limit);
        let first = crate::Database::open(io, path.to_str().unwrap(), options.clone()).unwrap();
        let reader = first.connect(crate::ConnectOpts::new()).unwrap();
        if nonempty_prefix {
            reader
                .execute("UPDATE preserved SET value='healthy nonempty prefix' WHERE id=1")
                .unwrap();
        }
        reader
            .execute("BEGIN; SELECT value FROM preserved WHERE id=1")
            .unwrap();
        let fresh_io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        let fresh = crate::Database::open(fresh_io, path.to_str().unwrap(), options).unwrap();
        let second = fresh.connect(crate::ConnectOpts::new()).unwrap();
        second
            .execute("SELECT value FROM preserved WHERE id=1")
            .unwrap();
        let mut values = Vec::new();
        reader
            .prepare("SELECT value FROM preserved WHERE id=1")
            .unwrap()
            .run_with_row_callback(|row| {
                values.push(row.get::<&str>(0)?.to_owned());
                Ok(())
            })
            .unwrap();
        assert_eq!(
            values,
            [if nonempty_prefix {
                "healthy nonempty prefix"
            } else {
                "durable payload"
            }]
        );
        reader.execute("ROLLBACK").unwrap();
        second
            .execute("UPDATE preserved SET value='healthy peer still writes' WHERE id=1")
            .unwrap();
    }
}
