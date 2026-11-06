//! Native opcode coverage; these fixtures require the private shared runtime.
use crate::sync::Arc;
use crate::{ConnectOpts, Connection, Database, EngineDialect, OpenOptions};

fn pair() -> (tempfile::TempDir, Arc<Connection>, Arc<Connection>) {
    let directory = tempfile::tempdir().unwrap();
    let (path, limit) = crate::mvcc::database::shared_runtime_fixture(directory.path());
    let open = || {
        let io: Arc<dyn crate::IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
        let db = Database::open(
            io,
            path.to_str().unwrap(),
            OpenOptions::new(Arc::new(EngineDialect)).with_shared_mvcc_for_test(limit),
        )
        .unwrap();
        db.connect(ConnectOpts::new()).unwrap()
    };
    let first = open();
    let second = open();
    (directory, first, second)
}
fn integers(connection: &Arc<Connection>, sql: &str) -> Vec<i64> {
    let mut statement = connection.prepare(sql).unwrap();
    let mut values = Vec::new();
    statement
        .run_with_row_callback(|row| {
            values.push(row.get_values().next().unwrap().as_int().unwrap());
            Ok(())
        })
        .unwrap();
    values
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_id_opcode_native_reserves_rows_across_independent_stores() {
    let (_directory, first, second) = pair();
    first.execute("BEGIN CONCURRENT").unwrap();
    second.execute("BEGIN CONCURRENT").unwrap();
    first
        .execute("INSERT INTO preserved(value) VALUES('first')")
        .unwrap();
    second
        .execute("INSERT INTO preserved(value) VALUES('second')")
        .unwrap();
    let a = integers(&first, "SELECT id FROM preserved WHERE value='first'")[0];
    let b = integers(&second, "SELECT id FROM preserved WHERE value='second'")[0];
    assert_ne!(a, b, "independent stores allocated the same live rowid");
    first.execute("COMMIT").unwrap();
    second.execute("COMMIT").unwrap();
    assert_eq!(integers(&first, "SELECT count(*) FROM preserved"), [3]);
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_id_opcode_native_explicit_claim_excludes_generated_row() {
    let (_directory, first, second) = pair();
    first.execute("BEGIN CONCURRENT").unwrap();
    second.execute("BEGIN CONCURRENT").unwrap();
    first
        .execute("INSERT INTO preserved(id,value) VALUES(100,'explicit')")
        .unwrap();
    second
        .execute("INSERT INTO preserved(value) VALUES('generated')")
        .unwrap();
    let id = integers(&second, "SELECT id FROM preserved WHERE value='generated'")[0];
    assert!(id > 100, "allocator ignored a peer's live explicit key");
    first.execute("COMMIT").unwrap();
    second.execute("COMMIT").unwrap();
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_id_opcode_native_reserves_logical_tables_before_schema_commit() {
    let (_directory, first, second) = pair();
    first.execute("BEGIN CONCURRENT").unwrap();
    second.execute("BEGIN CONCURRENT").unwrap();
    first
        .execute("CREATE TABLE first_table(id INTEGER PRIMARY KEY)")
        .unwrap();
    second
        .execute("CREATE TABLE second_table(id INTEGER PRIMARY KEY)")
        .unwrap();
    let a = integers(
        &first,
        "SELECT rootpage FROM sqlite_schema WHERE name='first_table'",
    )[0];
    let b = integers(
        &second,
        "SELECT rootpage FROM sqlite_schema WHERE name='second_table'",
    )[0];
    assert!(a < 0 && b < 0);
    assert_ne!(a, b, "independent stores allocated the same logical table");
    first.execute("ROLLBACK").unwrap();
    second.execute("ROLLBACK").unwrap();
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_id_opcode_native_rejects_process_local_sequence_watermark() {
    let (_directory, first, _) = pair();
    first.execute("CREATE SEQUENCE shared_sequence").unwrap();
    let error = first
        .execute("SELECT sequence_watermark_experimental('shared_sequence')")
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("requires a global live-claim reader"),
        "{error}"
    );
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_id_opcode_native_sequence_preserves_saved_outer_owner() {
    let (_directory, first, second) = pair();
    first.execute("CREATE SEQUENCE owner_sequence").unwrap();
    first.execute("BEGIN CONCURRENT").unwrap();
    first
        .execute("INSERT INTO preserved(id,value) VALUES(100,'outer')")
        .unwrap();
    let outer = first.get_mv_tx_id_for_db(0).unwrap();
    let store = first.mv_store().as_ref().unwrap().clone();
    let snapshot = store.read_snapshot_ts(outer);
    let allocated = integers(&first, "SELECT nextval('owner_sequence')")[0];
    assert_eq!(first.get_mv_tx_id_for_db(0), Some(outer));
    assert_eq!(store.read_snapshot_ts(outer), snapshot);
    assert!(store.is_tx_rollbackable(outer));
    assert_eq!(
        integers(&first, "SELECT id FROM preserved WHERE value='outer'"),
        [100]
    );
    assert!(integers(&second, "SELECT id FROM preserved WHERE value='outer'").is_empty());
    let later = integers(&second, "SELECT nextval('owner_sequence')")[0];
    assert!(later > allocated);
    first.execute("ROLLBACK").unwrap();
    assert!(integers(&second, "SELECT id FROM preserved WHERE value='outer'").is_empty());
    assert!(integers(&second, "SELECT nextval('owner_sequence')")[0] > later);
}
