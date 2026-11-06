use super::*;
use crate::io::OpenFlags;

fn open_shared(path: &std::path::Path, limit: u64) -> Arc<crate::Database> {
    crate::Database::open(
        Arc::new(crate::io::PlatformIO::new().unwrap()),
        path.to_str().unwrap(),
        crate::OpenOptions::new(Arc::new(crate::EngineDialect))
            .flags(OpenFlags::None)
            .with_shared_mvcc_for_test(limit),
    )
    .unwrap()
}

fn payload(connection: &Arc<Connection>) -> String {
    let mut values = Vec::new();
    connection
        .prepare("SELECT value FROM preserved WHERE id=1")
        .unwrap()
        .run_with_row_callback(|row| {
            values.push(row.get::<&str>(0)?.to_owned());
            Ok(())
        })
        .unwrap();
    assert_eq!(values.len(), 1);
    values.pop().unwrap()
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_commit_native_preserves_snapshots_and_recovers_acknowledged_writes() {
    let dir = tempfile::tempdir().unwrap();
    let (path, limit) = shared_runtime_fixture(dir.path());
    let original = std::fs::read(&path).unwrap();
    {
        let first = open_shared(&path, limit);
        let second = open_shared(&path, limit);
        let a = first.connect(crate::ConnectOpts::new()).unwrap();
        let b = second.connect(crate::ConnectOpts::new()).unwrap();
        a.execute("BEGIN CONCURRENT").unwrap();
        assert_eq!(payload(&a), "durable payload");
        b.execute("UPDATE preserved SET value='peer' WHERE id=1")
            .unwrap();
        assert_eq!(payload(&a), "durable payload");
        a.execute("ROLLBACK").unwrap();
        assert_eq!(payload(&a), "peer");
        a.execute("UPDATE preserved SET value='local' WHERE id=1")
            .unwrap();
        assert_eq!(payload(&b), "local");
        a.execute("INSERT INTO preserved VALUES(2,'explicit row id')")
            .unwrap();
        assert_eq!(payload(&a), "local");
    }
    assert_eq!(
        std::fs::read(&path).unwrap(),
        original,
        "logical commits must not checkpoint main storage"
    );
    let recovered = open_shared(&path, limit);
    let connection = recovered.connect(crate::ConnectOpts::new()).unwrap();
    assert_eq!(payload(&connection), "local");
    let mut count = 0;
    connection
        .prepare("SELECT id FROM preserved ORDER BY id")
        .unwrap()
        .run_with_row_callback(|_| {
            count += 1;
            Ok(())
        })
        .unwrap();
    assert_eq!(count, 2);
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_commit_native_conflicting_writers_fail_before_second_publication() {
    let dir = tempfile::tempdir().unwrap();
    let (path, limit) = shared_runtime_fixture(dir.path());
    let first = open_shared(&path, limit);
    let second = open_shared(&path, limit);
    let a = first.connect(crate::ConnectOpts::new()).unwrap();
    let b = second.connect(crate::ConnectOpts::new()).unwrap();
    a.execute("BEGIN CONCURRENT; UPDATE preserved SET value='winner' WHERE id=1")
        .unwrap();
    b.execute("BEGIN CONCURRENT; UPDATE preserved SET value='loser' WHERE id=1")
        .unwrap();
    a.execute("COMMIT").unwrap();
    assert!(matches!(
        b.execute("COMMIT"),
        Err(LimboError::WriteWriteConflict)
    ));
    let _ = b.execute("ROLLBACK");
    assert_eq!(payload(&b), "winner");
}
