use super::*;
use crate::mvcc::database::shared_id_oracle::SharedIdOracle;
use crate::mvcc::shared_ids::journal::Domain;
use crate::mvcc::shared_ids::policy::{CommittedIdOracle, IdFact, IdQuery};
use crate::namespace::NamespaceId;

fn query(
    oracle: &mut dyn CommittedIdOracle,
    guard: &crate::mvcc::shared_authority::AuthorityGuard,
    io: &dyn crate::IO,
    query: IdQuery,
) -> IdFact {
    loop {
        match oracle.poll(guard, &query).unwrap() {
            IOResult::Done(fact) => return fact,
            IOResult::IO(waiter) => io.wait_for_completion(waiter.0).unwrap(),
        }
    }
}

fn root(connection: &Connection, name: &str) -> i64 {
    let schema = connection.schema.read();
    schema
        .get_table(NamespaceId::ROOT, name)
        .unwrap()
        .btree()
        .unwrap()
        .root_page
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_id_oracle_native_reads_committed_rows_and_cleans_each_query() {
    let directory = tempfile::tempdir().unwrap();
    let (path, limit) = shared_runtime_fixture(directory.path());
    let io: Arc<dyn crate::IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
    let db = crate::Database::open(
        io.clone(),
        path.to_str().unwrap(),
        crate::OpenOptions::new(Arc::new(crate::EngineDialect)).with_shared_mvcc_for_test(limit),
    )
    .unwrap();
    let writer = db.connect(crate::ConnectOpts::new()).unwrap();
    let outer = db.connect(crate::ConnectOpts::new()).unwrap();
    let store = db.get_mv_store().as_ref().unwrap().clone();
    let checkpoint_timestamp = store.last_committed_tx_ts.load(Ordering::Acquire);
    writer.execute("CREATE TABLE negative(id INTEGER PRIMARY KEY); INSERT INTO negative VALUES(-5); CREATE TABLE empty_table(id INTEGER PRIMARY KEY); CREATE TABLE maximum(id INTEGER PRIMARY KEY); INSERT INTO maximum VALUES(9223372036854775807)").unwrap();
    outer
        .execute("BEGIN CONCURRENT; SELECT * FROM preserved")
        .unwrap();
    writer
        .execute("INSERT INTO preserved VALUES(99,'latest committed')")
        .unwrap();
    outer.execute("INSERT INTO preserved VALUES(500,'outer only'); DELETE FROM preserved WHERE id=1; CREATE TABLE pending(id INTEGER PRIMARY KEY); INSERT INTO pending VALUES(700)").unwrap();
    let runtime = store.shared_runtime.get().unwrap();
    let guard = runtime.files.authority.try_acquire().unwrap();
    let mut load =
        ManifestLoad::new(runtime.files.manifest.clone(), guard, runtime.database_id).unwrap();
    let loaded = loop {
        match load.poll().unwrap() {
            IOResult::Done(loaded) => break loaded,
            IOResult::IO(waiter) => io.wait_for_completion(waiter.0).unwrap(),
        }
    };
    let identity = RecoveryIdentity {
        database: DatabaseGeneration {
            generation: loaded.record().log_generation(),
            checkpoint_timestamp,
        },
        prefix: loaded.record().prefix(),
    };
    let guard = loaded.into_guard().unwrap();
    let domain = |connection: &Connection, name| Domain {
        generation: identity.database.generation,
        table: i64::from(store.get_table_id_from_root_page(root(connection, name))),
        sequence: false,
    };
    let preserved = domain(&outer, "preserved");
    let pending = domain(&outer, "pending");
    let outer_id = outer.get_mv_tx();
    let transactions = store.txs.iter().count();
    let registrations = guard.live_registrations().unwrap().len();
    let mut oracle = SharedIdOracle::new(store.clone(), outer.clone(), identity);
    assert!(guard.io_retired(), "lazy oracle must not pin the ID loader");
    for (domain, expected) in [
        (preserved, Some(99)),
        (domain(&writer, "negative"), Some(-5)),
        (domain(&writer, "empty_table"), None),
        (domain(&writer, "maximum"), Some(i64::MAX)),
        (pending, None),
    ] {
        let fact = query(
            &mut oracle,
            &guard,
            io.as_ref(),
            IdQuery::RowFacts {
                domain,
                autoincrement: false,
            },
        );
        assert!(
            matches!(fact, IdFact::RowFacts { maximum, autoincrement_floor: 0 } if maximum == expected),
            "{fact:?}, expected {expected:?}"
        );
        assert_eq!(store.txs.iter().count(), transactions);
        assert_eq!(outer.get_mv_tx(), outer_id);
        assert!(outer.get_pager().holds_read_lock());
        assert!(
            guard.io_retired(),
            "completed query must not pin the next append"
        );
    }
    for (domain, value, expected) in [
        (preserved, 1, true),
        (preserved, 99, true),
        (preserved, 500, false),
        (pending, 700, false),
    ] {
        let fact = query(
            &mut oracle,
            &guard,
            io.as_ref(),
            IdQuery::RowExists { domain, value },
        );
        assert!(matches!(fact, IdFact::RowExists(exists) if exists == expected));
    }
    assert!(
        matches!(query(&mut oracle, &guard, io.as_ref(), IdQuery::LogicalFloor), IdFact::LogicalFloor(floor) if floor < -1)
    );
    assert_eq!(guard.live_registrations().unwrap().len(), registrations);
    assert!(
        matches!(oracle.poll(&guard, &IdQuery::RowFacts { domain: preserved, autoincrement: true }), Err(LimboError::InvalidArgument(message)) if message.contains("implicit sequence"))
    );
    assert_eq!(store.txs.iter().count(), transactions);
    assert_eq!(outer.get_mv_tx(), outer_id);
    assert!(guard.io_retired());
    drop(oracle);
    guard.finish().unwrap();
    outer.execute("ROLLBACK").unwrap();
}
