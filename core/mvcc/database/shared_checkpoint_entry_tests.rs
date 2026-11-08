use super::*;

fn fixture() -> (tempfile::TempDir, Arc<crate::Database>, Arc<Connection>) {
    let directory = tempfile::tempdir().unwrap();
    let (path, limit) = shared_runtime_fixture(directory.path());
    let io: Arc<dyn crate::IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
    let options =
        crate::OpenOptions::new(Arc::new(crate::EngineDialect)).with_shared_mvcc_for_test(limit);
    let db = crate::Database::open(io, path.to_str().unwrap(), options).unwrap();
    let connection = db.connect(crate::ConnectOpts::new()).unwrap();
    (directory, db, connection)
}

fn payload(connection: &Arc<Connection>) -> Vec<String> {
    let mut values = Vec::new();
    connection
        .prepare("SELECT value FROM preserved WHERE id=1")
        .unwrap()
        .run_with_row_callback(|row| {
            values.push(row.get::<&str>(0)?.to_owned());
            Ok(())
        })
        .unwrap();
    values
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_checkpoint_entry_native_api_reclaims_and_preserves_idle_peer() {
    let (_directory, db, connection) = fixture();
    let idle = db.connect(crate::ConnectOpts::new()).unwrap();
    assert_eq!(payload(&idle), ["durable payload"]);
    connection
        .execute("UPDATE preserved SET value='checkpoint payload' WHERE id=1")
        .unwrap();
    let store = db.get_mv_store().as_ref().unwrap().clone();
    let runtime = store.shared_runtime.get().unwrap();
    assert!(
        std::fs::metadata(runtime.files.namespace.join("logical"))
            .unwrap()
            .len()
            > 0
    );
    let result = connection.checkpoint(CheckpointMode::Full).unwrap();
    assert!(result.everything_backfilled());
    assert_eq!(
        std::fs::metadata(runtime.files.namespace.join("logical"))
            .unwrap()
            .len(),
        0
    );
    assert_eq!(payload(&idle), ["checkpoint payload"]);
    idle.execute("UPDATE preserved SET value='after checkpoint' WHERE id=1")
        .unwrap();
    assert_eq!(payload(&connection), ["after checkpoint"]);
    for mode in [
        CheckpointMode::Passive {
            upper_bound_inclusive: None,
        },
        CheckpointMode::Full,
        CheckpointMode::Restart,
        CheckpointMode::Truncate {
            upper_bound_inclusive: None,
        },
    ] {
        connection
            .execute("UPDATE preserved SET value='each API mode' WHERE id=1")
            .unwrap();
        assert!(connection.checkpoint(mode).unwrap().everything_backfilled());
        assert_eq!(payload(&idle), ["each API mode"]);
    }
    let before = std::fs::read(runtime.files.namespace.join("manifest")).unwrap();
    let logical = std::fs::read(runtime.files.namespace.join("logical")).unwrap();
    for mode in [
        CheckpointMode::Passive {
            upper_bound_inclusive: Some(1),
        },
        CheckpointMode::Truncate {
            upper_bound_inclusive: Some(1),
        },
    ] {
        assert!(matches!(
            connection.checkpoint(mode),
            Err(LimboError::InvalidArgument(_))
        ));
    }
    let reader = db
        .connect(crate::ConnectOpts::new().with_readonly(true))
        .unwrap();
    assert!(matches!(
        reader.checkpoint(CheckpointMode::Full),
        Err(LimboError::ReadOnly)
    ));
    assert!(matches!(
        reader.execute("PRAGMA wal_checkpoint"),
        Err(LimboError::ReadOnly)
    ));
    assert_eq!(
        std::fs::read(runtime.files.namespace.join("manifest")).unwrap(),
        before
    );
    assert_eq!(
        std::fs::read(runtime.files.namespace.join("logical")).unwrap(),
        logical
    );
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_checkpoint_entry_native_sql_repeats_after_commits() {
    let (_directory, _db, connection) = fixture();
    for value in ["PASSIVE", "FULL", "RESTART", "TRUNCATE"] {
        connection
            .execute(format!("UPDATE preserved SET value='{value}' WHERE id=1"))
            .unwrap();
        let mut rows = Vec::new();
        connection
            .prepare(format!("PRAGMA wal_checkpoint({value})"))
            .unwrap()
            .run_with_row_callback(|row| {
                rows.push((row.get::<i64>(0)?, row.get::<i64>(1)?, row.get::<i64>(2)?));
                Ok(())
            })
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, 0);
        assert!(rows[0].1 >= 0);
        assert_eq!(rows[0].1, rows[0].2);
        assert_eq!(payload(&connection), [value]);
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_checkpoint_entry_native_busy_preserves_reader() {
    let (_directory, db, connection) = fixture();
    let reader = db.connect(crate::ConnectOpts::new()).unwrap();
    reader.execute("BEGIN").unwrap();
    assert_eq!(payload(&reader), ["durable payload"]);
    connection
        .execute("UPDATE preserved SET value='while reading' WHERE id=1")
        .unwrap();
    assert!(matches!(
        connection.checkpoint(CheckpointMode::Full),
        Err(LimboError::Busy)
    ));
    assert_eq!(payload(&reader), ["durable payload"]);
    connection
        .execute("UPDATE preserved SET value='after busy' WHERE id=1")
        .unwrap();
    reader.execute("COMMIT").unwrap();
    connection.checkpoint(CheckpointMode::Full).unwrap();
    assert_eq!(payload(&reader), ["after busy"]);
}
