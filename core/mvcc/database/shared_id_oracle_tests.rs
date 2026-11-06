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
    for pending_ddl in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let (path, limit) = shared_runtime_fixture(directory.path());
        let io: Arc<dyn crate::IO> = Arc::new(crate::io::PlatformIO::new().unwrap());
        let db = crate::Database::open(
            io.clone(),
            path.to_str().unwrap(),
            crate::OpenOptions::new(Arc::new(crate::EngineDialect))
                .with_shared_mvcc_for_test(limit),
        )
        .unwrap();
        let writer = db.connect(crate::ConnectOpts::new()).unwrap();
        let outer = db.connect(crate::ConnectOpts::new()).unwrap();
        let store = db.get_mv_store().as_ref().unwrap().clone();
        let checkpoint_timestamp = store.last_committed_tx_ts.load(Ordering::Acquire);
        writer.execute("CREATE TABLE negative(id INTEGER PRIMARY KEY); INSERT INTO negative VALUES(-5); CREATE TABLE empty_table(id INTEGER PRIMARY KEY); CREATE TABLE maximum(id INTEGER PRIMARY KEY); INSERT INTO maximum VALUES(9223372036854775807)").unwrap();
        if pending_ddl {
            writer
                .execute("INSERT INTO preserved VALUES(99,'latest committed')")
                .unwrap();
        }
        outer.execute("BEGIN CONCURRENT").unwrap();
        outer.execute("SELECT * FROM preserved").unwrap();
        if !pending_ddl {
            writer
                .execute("INSERT INTO preserved VALUES(99,'latest committed')")
                .unwrap();
        }
        outer
            .execute("INSERT INTO preserved VALUES(500,'outer only')")
            .expect("outer insert");
        outer
            .execute("DELETE FROM preserved WHERE id=1")
            .expect("outer delete");
        if pending_ddl {
            outer
                .execute("CREATE TABLE pending(id INTEGER PRIMARY KEY)")
                .expect("current snapshot DDL");
            outer
                .execute("INSERT INTO pending VALUES(700)")
                .expect("pending table insert");
        }
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
        let pending = pending_ddl.then(|| domain(&outer, "pending"));
        let outer_id = outer.get_mv_tx();
        let transactions = store.txs.iter().count();
        let registrations = guard.live_registrations().unwrap().len();
        let mut oracle = SharedIdOracle::new(store.clone(), outer.clone(), identity);
        assert!(guard.io_retired(), "lazy oracle must not pin the ID loader");
        let mut maxima = vec![
            (preserved, Some(99)),
            (domain(&writer, "negative"), Some(-5)),
            (domain(&writer, "empty_table"), None),
            (domain(&writer, "maximum"), Some(i64::MAX)),
        ];
        if let Some(pending) = pending {
            maxima.push((pending, None));
        }
        for (domain, expected) in maxima {
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
        let mut membership = vec![
            (preserved, 1, true),
            (preserved, 2, false),
            (preserved, 99, true),
            (preserved, 500, false),
        ];
        if let Some(pending) = pending {
            membership.push((pending, 700, false));
        }
        for (domain, value, expected) in membership {
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
}

struct HeldRead {
    file: Arc<dyn File>,
    offset: u64,
    completion: crate::Completion,
}
#[derive(Default)]
struct ReadControl {
    armed: crate::sync::atomic::AtomicBool,
    pending: Mutex<Option<HeldRead>>,
}
struct DeferredMainFile {
    inner: Arc<dyn File>,
    control: Arc<ReadControl>,
}
impl File for DeferredMainFile {
    fn file_id(&self) -> Result<Option<FileId>> {
        self.inner.file_id()
    }
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.inner.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.inner.unlock_file()
    }
    fn size(&self) -> Result<u64> {
        self.inner.size()
    }
    fn pread(&self, offset: u64, completion: crate::Completion) -> Result<crate::Completion> {
        if self.control.armed.load(Ordering::Acquire) {
            assert!(self
                .control
                .pending
                .lock()
                .replace(HeldRead {
                    file: self.inner.clone(),
                    offset,
                    completion: completion.clone(),
                })
                .is_none());
            Ok(completion)
        } else {
            self.inner.pread(offset, completion)
        }
    }
    fn pwrite(
        &self,
        offset: u64,
        buffer: Arc<crate::Buffer>,
        completion: crate::Completion,
    ) -> Result<crate::Completion> {
        self.inner.pwrite(offset, buffer, completion)
    }
    fn sync(
        &self,
        completion: crate::Completion,
        kind: crate::io::FileSyncType,
    ) -> Result<crate::Completion> {
        self.inner.sync(completion, kind)
    }
    fn truncate(&self, size: u64, completion: crate::Completion) -> Result<crate::Completion> {
        self.inner.truncate(size, completion)
    }
}
struct DeferredMainIo {
    inner: Arc<dyn crate::IO>,
    main: PathBuf,
    control: Arc<ReadControl>,
}
impl crate::Clock for DeferredMainIo {
    fn current_time_monotonic(&self) -> crate::MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> crate::WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}
impl crate::IO for DeferredMainIo {
    fn open_file(
        &self,
        path: &str,
        flags: crate::io::OpenFlags,
        direct: bool,
    ) -> Result<Arc<dyn File>> {
        let file = self.inner.open_file(path, flags, direct)?;
        if Path::new(path) == self.main {
            Ok(Arc::new(DeferredMainFile {
                inner: file,
                control: self.control.clone(),
            }))
        } else {
            Ok(file)
        }
    }
    fn remove_file(&self, path: &str) -> Result<()> {
        self.inner.remove_file(path)
    }
    fn supports_shared_wal_coordination(&self) -> bool {
        self.inner.supports_shared_wal_coordination()
    }
    fn step(&self) -> Result<()> {
        self.inner.step()
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_id_oracle_native_cancel_holds_authority_until_physical_read_retires() {
    for abort in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let (path, limit) = shared_runtime_fixture(directory.path());
        let control = Arc::new(ReadControl::default());
        let io: Arc<dyn crate::IO> = Arc::new(DeferredMainIo {
            inner: Arc::new(crate::io::PlatformIO::new().unwrap()),
            main: path.clone(),
            control: control.clone(),
        });
        let db = crate::Database::open(
            io.clone(),
            path.to_str().unwrap(),
            crate::OpenOptions::new(Arc::new(crate::EngineDialect))
                .with_shared_mvcc_for_test(limit),
        )
        .unwrap();
        let outer = db.connect(crate::ConnectOpts::new()).unwrap();
        outer
            .execute("BEGIN CONCURRENT; SELECT * FROM preserved")
            .unwrap();
        let store = db.get_mv_store().as_ref().unwrap().clone();
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
                checkpoint_timestamp: store.last_committed_tx_ts.load(Ordering::Acquire),
            },
            prefix: loaded.record().prefix(),
        };
        let guard = loaded.into_guard().unwrap();
        let outer_id = outer.get_mv_tx();
        let transactions = store.txs.iter().count();
        let mut oracle = SharedIdOracle::new(store.clone(), outer.clone(), identity);
        control.armed.store(true, Ordering::Release);
        let waiter = match oracle.poll(&guard, &IdQuery::LogicalFloor).unwrap() {
            IOResult::IO(waiter) => waiter,
            IOResult::Done(fact) => panic!("oracle bypassed deferred main read: {fact:?}"),
        };
        assert!(control.pending.lock().is_some());
        assert_eq!(store.txs.iter().count(), transactions + 1);
        if abort {
            waiter.0.abort();
        }
        drop(waiter);
        drop(oracle);
        assert_eq!(store.txs.iter().count(), transactions);
        assert_eq!(outer.get_mv_tx(), outer_id);
        assert!(outer.get_pager().holds_read_lock());
        assert!(!guard.io_retired());
        drop(guard);
        assert!(matches!(
            runtime.files.authority.try_acquire(),
            Err(LimboError::Busy)
        ));
        let held = control.pending.lock().take().unwrap();
        drop(
            held.file
                .pread(held.offset, held.completion.clone())
                .unwrap(),
        );
        assert!(held.completion.finished());
        assert!(
            matches!(runtime.files.authority.try_acquire(), Err(LimboError::Busy)),
            "callback execution must not release the backend pin"
        );
        drop(held);
        let mut recovered = runtime.files.authority.try_acquire().unwrap();
        assert!(recovered.requires_recovery());
        recovered.recover(identity.prefix).unwrap();
        recovered.finish().unwrap();
        control.armed.store(false, Ordering::Release);
        outer.execute("ROLLBACK").unwrap();
    }
}
