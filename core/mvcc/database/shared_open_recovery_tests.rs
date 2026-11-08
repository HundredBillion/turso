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
    drop(connection);
    drop(database);
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
fn shared_open_recovery_native_empty_prefix_ignores_unacknowledged_tail() {
    for kind in ["complete", "corrupt", "torn"] {
        let (_directory, path, logical, limit) = completed_files(true);
        let mut tail = std::fs::read(&logical).unwrap();
        match kind {
            "complete" => {}
            "corrupt" => {
                *tail.last_mut().unwrap() ^= 0x80;
            }
            "torn" => tail.truncate(17),
            _ => unreachable!(),
        }
        std::fs::write(&logical, &tail).unwrap();
        std::fs::File::open(&logical).unwrap().sync_all().unwrap();
        let physical_wal = logical.parent().unwrap().join("physical-wal");
        let before = [&path, &logical, &physical_wal].map(|path| std::fs::read(path).unwrap());
        let options = crate::OpenOptions::new(Arc::new(crate::EngineDialect))
            .with_shared_mvcc_for_test(limit)
            .flags(crate::OpenFlags::ReadOnly);
        let io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        let database = crate::Database::open(io, path.to_str().unwrap(), options.clone()).unwrap();
        let connection = database.connect(crate::ConnectOpts::new()).unwrap();
        connection
            .execute("BEGIN; SELECT value FROM preserved WHERE id=1")
            .unwrap();
        let peer_io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        let peer = crate::Database::open(peer_io, path.to_str().unwrap(), options).unwrap();
        let reader = peer.connect(crate::ConnectOpts::new()).unwrap();
        let mut values = Vec::new();
        reader
            .prepare("SELECT value FROM preserved WHERE id=1")
            .unwrap()
            .run_with_row_callback(|row| {
                values.push(row.get::<&str>(0)?.to_owned());
                Ok(())
            })
            .unwrap();
        assert_eq!(values, ["materialized shared row"]);
        assert_eq!(
            [&path, &logical, &physical_wal].map(|path| std::fs::read(path).unwrap()),
            before,
            "healthy readonly empty-prefix open changed content for {kind} tail"
        );
        connection.execute("ROLLBACK").unwrap();
    }
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
            .execute("UPDATE preserved SET value='healthy peer still writes' WHERE id=1; CREATE TABLE allocated_after_probe(id INTEGER PRIMARY KEY); INSERT INTO allocated_after_probe DEFAULT VALUES")
            .unwrap();
    }
}

#[test]
#[ignore = "requires native shared MVCC files"]
fn shared_open_selector_native_matching_prepared_ids_are_readonly_facts() {
    use crate::mvcc::shared_ids::io_protocol::open_selection::InitialIdRoute;
    let fixture = crate::mvcc::database::shared_completed_checkpoint_fixture(|_| {
        Arc::new(crate::io::SyscallIO::new().unwrap())
    });
    let runtime = fixture
        .database
        .get_mv_store()
        .as_ref()
        .unwrap()
        .shared_runtime
        .get()
        .unwrap()
        .clone();
    let paths = [
        fixture.path.clone(),
        fixture.namespace.join("manifest"),
        fixture.namespace.join("logical"),
        fixture.namespace.join("physical-wal"),
        fixture.namespace.join("ids-selector"),
        fixture.namespace.join("ids-a"),
        fixture.namespace.join("ids-b"),
    ];
    let before = paths.each_ref().map(|path| std::fs::read(path).unwrap());
    let mut loading = ManifestLoad::new(
        runtime.files.manifest.clone(),
        fixture.guard,
        runtime.database_id,
    )
    .unwrap();
    let manifest = fixture.io.block(|| Ok(loading.poll()?)).unwrap();
    let mut probing = runtime.ids.inspect_open_selector(manifest).unwrap();
    let inspected = fixture.io.block(|| Ok(probing.poll()?)).unwrap();
    let (manifest, route) = inspected.into_parts();
    assert_eq!(route, InitialIdRoute::CompletedCheckpoint);
    assert!(manifest.guard().unwrap().io_retired());
    assert_eq!(
        paths.each_ref().map(|path| std::fs::read(path).unwrap()),
        before
    );
    drop(fixture.connection);
    drop(fixture.database);
    drop(fixture.ids);
    drop(fixture.proof);
    drop(manifest);
    drop(fixture.pause);
}

#[test]
#[ignore = "requires native shared MVCC files"]
fn shared_open_selector_native_healthy_handoff_keeps_bindings_free() {
    use crate::mvcc::shared_ids::io_protocol::open_selection::InitialIdRoute;
    let directory = tempfile::tempdir().unwrap();
    let (path, limit) = crate::mvcc::database::shared_runtime_fixture(directory.path());
    let io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
    let database = crate::Database::open(
        io.clone(),
        path.to_str().unwrap(),
        crate::OpenOptions::new(Arc::new(crate::EngineDialect)).with_shared_mvcc_for_test(limit),
    )
    .unwrap();
    let connection = database.connect(crate::ConnectOpts::new()).unwrap();
    connection.execute("UPDATE preserved SET value='selector probe acknowledged' WHERE id=1; BEGIN; SELECT value FROM preserved").unwrap();
    let store = database.get_mv_store().as_ref().unwrap().clone();
    let runtime = store.shared_runtime.get().unwrap().clone();
    let identity = runtime.replay.lock().as_ref().unwrap().identity().unwrap();
    assert!(identity.prefix.end_offset > 0);
    let admission = runtime.gate.try_enter_admission().unwrap();
    let guard = runtime.files.authority.try_acquire().unwrap();
    assert!(!guard.live_registrations().unwrap().is_empty());
    let mut loading =
        ManifestLoad::new(runtime.files.manifest.clone(), guard, runtime.database_id).unwrap();
    let manifest = io.block(|| Ok(loading.poll()?)).unwrap();
    let mut probing = runtime.ids.inspect_open_selector(manifest).unwrap();
    let inspected = io.block(|| Ok(probing.poll()?)).unwrap();
    let (manifest, route) = inspected.into_parts();
    assert_eq!(route, InitialIdRoute::Healthy);
    let mut ids = runtime
        .ids
        .load(manifest.into_guard().unwrap(), identity)
        .unwrap();
    let ids = io.block(|| Ok(ids.poll()?)).unwrap();
    assert!(!ids.has_pending_checkpoint());
    ids.into_guard().finish().unwrap();
    drop(admission);
    connection.execute("ROLLBACK; CREATE TABLE allocated_after_selector(id INTEGER PRIMARY KEY); INSERT INTO allocated_after_selector DEFAULT VALUES").unwrap();
}
