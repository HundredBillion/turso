//! Native opcode coverage; these fixtures require the private shared runtime.
use crate::sync::Arc;
use crate::{ConnectOpts, Connection, Database, EngineDialect, OpenOptions};

fn pair() -> (tempfile::TempDir, Arc<Connection>, Arc<Connection>) {
    pair_with_setup(None)
}
fn pair_with_setup(setup: Option<&str>) -> (tempfile::TempDir, Arc<Connection>, Arc<Connection>) {
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
    if let Some(sql) = setup {
        first.execute(sql).unwrap();
    }
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
    let (_directory, first, second) = pair_with_setup(Some("CREATE SEQUENCE owner_sequence"));
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

#[derive(Debug)]
struct AfterClaimYield(std::sync::atomic::AtomicBool);
impl crate::mvcc::yield_points::YieldInjector for AfterClaimYield {
    fn should_yield(&self, _: u64, _: u64, point: crate::mvcc::yield_points::YieldPoint) -> bool {
        use crate::mvcc::yield_hooks::YieldPointMarker;
        point == super::execute::TransactionYieldPoint::AfterSharedRowClaim.point()
            && self.0.swap(false, std::sync::atomic::Ordering::SeqCst)
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_id_opcode_native_claim_yield_resume_reset_drop() {
    for outcome in ["resume", "reset", "drop"] {
        let (_directory, first, second) = pair();
        first.execute("BEGIN CONCURRENT").unwrap();
        first
            .execute("INSERT INTO preserved(id,value) VALUES(66,'prior')")
            .unwrap();
        let outer = first.get_mv_tx_id_for_db(0).unwrap();
        first.set_yield_injector(Some(Arc::new(AfterClaimYield(
            std::sync::atomic::AtomicBool::new(true),
        ))));
        let mut statement = first
            .prepare("INSERT INTO preserved(id,value) VALUES(77,'parked')")
            .unwrap();
        let mut parked = false;
        for _ in 0..10000 {
            match statement.step().unwrap() {
                crate::StepResult::Yield => {
                    parked = true;
                    break;
                }
                crate::StepResult::IO => statement.get_pager().io.step().unwrap(),
                other => panic!("expected claim boundary, got {other:?}"),
            }
        }
        assert!(parked, "did not reach durable claim boundary");
        first.set_yield_injector(None);
        assert_eq!(first.get_mv_tx_id_for_db(0), Some(outer));
        assert!(integers(&second, "SELECT id FROM preserved WHERE id=77").is_empty());
        let error = second
            .execute("INSERT INTO preserved(id,value) VALUES(77,'peer')")
            .unwrap_err();
        assert!(
            matches!(error, crate::LimboError::Busy),
            "{outcome}: {error:?}"
        );
        match outcome {
            "resume" => statement.run_ignore_rows().unwrap(),
            "reset" => statement.reset().unwrap(),
            "drop" => (),
            _ => unreachable!(),
        }
        drop(statement);
        assert_eq!(first.get_mv_tx_id_for_db(0), Some(outer));
        let error = second
            .execute("INSERT INTO preserved(id,value) VALUES(66,'peer')")
            .unwrap_err();
        assert!(
            matches!(error, crate::LimboError::Busy),
            "prior claim lost on {outcome}: {error:?}"
        );
        first.execute("ROLLBACK").unwrap();
        second
            .execute("INSERT INTO preserved(id,value) VALUES(66,'peer'),(77,'peer')")
            .unwrap();
        assert_eq!(
            integers(&second, "SELECT count(*) FROM preserved WHERE id IN(66,77)"),
            [2]
        );
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_id_stale_prepare_native_peer_sequence_ddl() {
    let (_directory, first, second) = pair();
    first.execute("CREATE SEQUENCE peer_sequence").unwrap();
    second.execute("SELECT id FROM preserved").unwrap();
    let prepared = second.prepare("SELECT nextval('peer_sequence')");
    assert!(
        prepared.is_ok(),
        "idle peer did not import committed sequence DDL before prepare: {:?}",
        prepared.err()
    );
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_id_opcode_native_autoincrement_uses_durable_sequence() {
    let (_directory, first, second) = pair_with_setup(Some(
        "CREATE TABLE auto_ids(id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT)",
    ));
    first.execute("BEGIN CONCURRENT").unwrap();
    second.execute("BEGIN CONCURRENT").unwrap();
    first
        .execute("INSERT INTO auto_ids(value) VALUES('first')")
        .unwrap();
    second
        .execute("INSERT INTO auto_ids(value) VALUES('second')")
        .unwrap();
    let a = integers(&first, "SELECT id FROM auto_ids WHERE value='first'")[0];
    let b = integers(&second, "SELECT id FROM auto_ids WHERE value='second'")[0];
    assert!(
        b > a,
        "independent AUTOINCREMENT stores reused a sequence value"
    );
    first.execute("ROLLBACK").unwrap();
    second.execute("COMMIT").unwrap();
    first
        .execute("INSERT INTO auto_ids(value) VALUES('after rollback')")
        .unwrap();
    assert!(
        integers(
            &first,
            "SELECT id FROM auto_ids WHERE value='after rollback'"
        )[0] > b
    );
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_id_opcode_native_updated_key_claim_blocks_peer_until_rollback() {
    let (_directory, first, second) = pair();
    first.execute("BEGIN CONCURRENT").unwrap();
    first
        .execute("UPDATE preserved SET id=99 WHERE id=1")
        .unwrap();
    assert!(matches!(
        second.execute("INSERT INTO preserved(id,value) VALUES(99,'peer')"),
        Err(crate::LimboError::Busy)
    ));
    assert_eq!(integers(&first, "SELECT id FROM preserved"), [99]);
    first.execute("ROLLBACK").unwrap();
    second
        .execute("INSERT INTO preserved(id,value) VALUES(99,'peer')")
        .unwrap();
    assert_eq!(integers(&second, "SELECT count(*) FROM preserved"), [2]);
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_id_opcode_native_replace_retains_key_claim_until_rollback() {
    let (_directory, first, second) = pair();
    first.execute("BEGIN CONCURRENT").unwrap();
    first
        .execute("INSERT INTO preserved(id,value) VALUES(99,'initial')")
        .unwrap();
    first
        .execute("INSERT OR REPLACE INTO preserved(id,value) VALUES(99,'replacement')")
        .unwrap();
    assert_eq!(
        integers(
            &first,
            "SELECT count(*) FROM preserved WHERE id=99 AND value='replacement'"
        ),
        [1]
    );
    assert!(matches!(
        second.execute("INSERT INTO preserved(id,value) VALUES(99,'peer')"),
        Err(crate::LimboError::Busy)
    ));
    first.execute("ROLLBACK").unwrap();
    second
        .execute("INSERT INTO preserved(id,value) VALUES(99,'peer')")
        .unwrap();
    assert_eq!(integers(&second, "SELECT count(*) FROM preserved"), [2]);
}
