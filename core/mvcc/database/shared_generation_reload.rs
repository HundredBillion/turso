use super::shared_runtime::CurrentBaseEvidence;
use super::*;
use crate::mvcc::commit_manifest::LoadedManifest;
use crate::mvcc::shared_authority::PublishedPrefix;

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReloadOrigin {
    CompletedCheckpoint,
    Current(PublishedPrefix),
}

pub(in crate::mvcc) struct PreparedGenerationReload<A: ConcurrentAllocator> {
    roots: Arc<SkipMap<MVTableId, RootEntry, BasicComparator, A>>,
    source_roots: Arc<SkipMap<MVTableId, RootEntry, BasicComparator, A>>,
    header: DatabaseHeader,
    checkpoint_timestamp: u64,
    logical_floor: i64,
    source_frontier: u64,
    source_schema_generation: u64,
    origin: ReloadOrigin,
}

impl<C: LogicalClock, A: ConcurrentAllocator> MvStore<C, A> {
    pub(in crate::mvcc) fn prepare_generation_reload(
        &self,
        header: DatabaseHeader,
        schema: &Schema,
        checkpoint_timestamp: u64,
    ) -> Result<PreparedGenerationReload<A>> {
        self.prepare_generation_map(
            header,
            schema,
            checkpoint_timestamp,
            ReloadOrigin::CompletedCheckpoint,
        )
    }

    pub(in crate::mvcc) fn prepare_current_generation_reload(
        &self,
        evidence: &CurrentBaseEvidence,
        manifest: &LoadedManifest,
    ) -> Result<PreparedGenerationReload<A>> {
        evidence.validate_closed_store(self, manifest)?;
        self.prepare_generation_map(
            evidence.header(),
            evidence.schema(),
            evidence.identity().database.checkpoint_timestamp,
            ReloadOrigin::Current(evidence.identity().prefix),
        )
    }

    fn prepare_generation_map(
        &self,
        header: DatabaseHeader,
        schema: &Schema,
        checkpoint_timestamp: u64,
        origin: ReloadOrigin,
    ) -> Result<PreparedGenerationReload<A>> {
        let source_roots = self.table_id_to_rootpage.load_full();
        let pages = i64::from(header.database_size.get());
        let source_frontier = self.last_committed_tx_ts.load(Ordering::Acquire);
        let source_schema_generation = self.schema_generation.load(Ordering::Acquire);
        if pages == 0
            || header.read_version.0 != SHARED_MVCC_VERSION
            || header.write_version.0 != SHARED_MVCC_VERSION
            || match origin {
                ReloadOrigin::CompletedCheckpoint => checkpoint_timestamp < source_frontier,
                ReloadOrigin::Current(prefix) => {
                    checkpoint_timestamp > prefix.committed_timestamp
                        || source_frontier > prefix.committed_timestamp
                }
            }
            || checkpoint_timestamp >= crate::mvcc::shared_authority::MAX_TIMESTAMP
            || source_schema_generation == u64::MAX
        {
            return Err(LimboError::Corrupt(
                "invalid materialized generation metadata".into(),
            ));
        }
        let old_floor = self.recovered_logical_id_floor()?;
        let logical_floor = old_floor.min(-pages);
        logical_floor.checked_sub(1).ok_or_else(|| {
            LimboError::Corrupt("materialized logical ID floor is exhausted".into())
        })?;
        let roots = SkipMap::new_in(self.alloc.clone());
        let physical_roots = schema
            .all_tables()
            .filter_map(|table| match table.as_ref() {
                Table::BTree(table) => Some(table.root_page),
                _ => None,
            })
            .chain(
                schema
                    .all_indexes()
                    .filter(|index| index.is_btree_backed())
                    .map(|index| index.root_page),
            );
        for root in physical_roots {
            if root <= 0 || root > pages || roots.contains_key(&MVTableId::from(-root)) {
                return Err(LimboError::Corrupt(
                    "invalid or duplicate materialized physical root".into(),
                ));
            }
            roots.try_insert(MVTableId::from(-root), RootEntry::live(Some(root as u64)))?;
        }
        if !roots.contains_key(&SQLITE_SCHEMA_MVCC_TABLE_ID) {
            return Err(LimboError::Corrupt(
                "materialized schema root is absent".into(),
            ));
        }
        Ok(PreparedGenerationReload {
            roots: Arc::new(roots),
            source_roots,
            header,
            checkpoint_timestamp,
            logical_floor,
            source_frontier,
            source_schema_generation,
            origin,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mvcc::database::tests::MvccTestDb;
    use crate::schema::BTreeTable;
    use crate::storage::sqlite3_ondisk::RawVersion;

    fn header() -> DatabaseHeader {
        let mut header = DatabaseHeader::default();
        header.database_size = 42.into();
        header.read_version = RawVersion(SHARED_MVCC_VERSION);
        header.write_version = header.read_version;
        header
    }

    fn schema(root: i64) -> Schema {
        let mut schema = Schema::new();
        let table =
            BTreeTable::from_sql("CREATE TABLE reloaded(id INTEGER PRIMARY KEY)", root).unwrap();
        schema.insert_table(
            NamespaceId::ROOT,
            "reloaded".into(),
            Arc::new(Table::BTree(Arc::new(table))),
        );
        schema
    }

    #[test]
    fn shared_generation_reload_prepares_complete_canonical_map_without_publication() {
        let db = MvccTestDb::new();
        let store = &db.mvcc_store;
        store.insert_table_id_to_rootpage(MVTableId::from(-90), Some(42));
        let previous = store.table_id_to_rootpage.load_full();
        let prepared = store
            .prepare_generation_reload(header(), &schema(7), 100)
            .unwrap();
        assert!(Arc::ptr_eq(&previous, &prepared.source_roots));
        assert_eq!(prepared.roots.len(), 2);
        assert_eq!(
            prepared
                .roots
                .get(&MVTableId::from(-7))
                .unwrap()
                .value()
                .root_page,
            Some(7)
        );
        assert!(prepared.roots.get(&MVTableId::from(-90)).is_none());
        assert_eq!(prepared.logical_floor, -90);
        assert_eq!(prepared.checkpoint_timestamp, 100);
        assert!(Arc::ptr_eq(
            &previous,
            &store.table_id_to_rootpage.load_full()
        ));
        assert_eq!(store.current_root_page(&MVTableId::from(-90)), Some(42));
    }

    #[test]
    fn shared_generation_reload_rejects_nonphysical_or_duplicate_roots_without_publication() {
        let db = MvccTestDb::new();
        let store = &db.mvcc_store;
        let previous = store.table_id_to_rootpage.load_full();
        for root in [-7, 0, 43, 1] {
            assert!(matches!(
                store.prepare_generation_reload(header(), &schema(root), 100),
                Err(LimboError::Corrupt(_))
            ));
            assert!(Arc::ptr_eq(
                &previous,
                &store.table_id_to_rootpage.load_full()
            ));
        }
    }
}

impl<A: ConcurrentAllocator> PreparedGenerationReload<A> {
    pub(in crate::mvcc) fn install<C: LogicalClock>(
        self,
        store: &MvStore<C, A>,
        proof: &crate::mvcc::commit_manifest::checkpoint_publication::CompletedCheckpointProof,
        guard: &crate::mvcc::shared_authority::AuthorityGuard,
    ) -> Result<()> {
        let runtime = store.shared_runtime.get().ok_or_else(|| {
            LimboError::InvalidArgument("generation reload requires shared runtime".into())
        })?;
        let target = proof.target_identity();
        if self.origin != ReloadOrigin::CompletedCheckpoint
            || !runtime.owns_authority(guard)
            || !proof.matches_lease(guard)
            || guard.published()? != target.prefix
            || target.database.checkpoint_timestamp != self.checkpoint_timestamp
            || target.prefix.committed_timestamp != self.checkpoint_timestamp
            || target.prefix.end_offset != 0
            || target.prefix.running_crc != 0
            || proof.materialized_floor() > -i64::from(self.header.database_size.get())
        {
            return Err(LimboError::InvalidArgument(
                "generation reload requires matching completed checkpoint".into(),
            ));
        }
        if !guard.io_retired() || !guard.live_registrations()?.is_empty() {
            return Err(LimboError::Busy);
        }
        self.publish_local_map(store, proof.materialized_floor())
    }

    pub(in crate::mvcc) fn install_current<C: LogicalClock>(
        self,
        store: &MvStore<C, A>,
        evidence: &CurrentBaseEvidence,
        manifest: &LoadedManifest,
    ) -> Result<()> {
        evidence.validate_closed_store(store, manifest)?;
        if self.origin != ReloadOrigin::Current(evidence.identity().prefix)
            || self.checkpoint_timestamp != evidence.identity().database.checkpoint_timestamp
            || self.source_frontier > evidence.identity().prefix.committed_timestamp
        {
            return Err(LimboError::InvalidArgument(
                "local reload preparation differs from its selected physical base".into(),
            ));
        }
        self.publish_local_map(store, evidence.logical_floor())
    }

    fn publish_local_map<C: LogicalClock>(
        self,
        store: &MvStore<C, A>,
        physical_floor: i64,
    ) -> Result<()> {
        if store.experimental_mvcc_passive_checkpoint
            || store
                .checkpoint_in_progress
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
        {
            return Err(LimboError::Busy);
        }
        if !store.blocking_checkpoint_lock.write() {
            store.checkpoint_in_progress.store(false, Ordering::Release);
            return Err(LimboError::Busy);
        }
        struct ReloadGate<'a> {
            lock: &'a TursoRwLock,
            checkpoint: &'a AtomicBool,
        }
        impl Drop for ReloadGate<'_> {
            fn drop(&mut self) {
                self.lock.unlock();
                self.checkpoint.store(false, Ordering::Release);
            }
        }
        let _gate = ReloadGate {
            lock: &store.blocking_checkpoint_lock,
            checkpoint: &store.checkpoint_in_progress,
        };
        if !store.txs.is_empty()
            || store.exclusive_tx.load(Ordering::Acquire) != NO_EXCLUSIVE_TX
            || store.gc_in_progress.load(Ordering::Acquire)
            || store.checkpoint_publish_in_progress.load(Ordering::Acquire)
        {
            return Err(LimboError::Busy);
        }
        if !Arc::ptr_eq(&self.source_roots, &store.table_id_to_rootpage.load_full())
            || store.last_committed_tx_ts.load(Ordering::Acquire) != self.source_frontier
            || store.schema_generation.load(Ordering::Acquire) != self.source_schema_generation
        {
            return Err(LimboError::InvalidArgument(
                "generation reload preparation became stale".into(),
            ));
        }
        let increment = |value: u64| {
            value
                .checked_add(1)
                .ok_or_else(|| LimboError::Corrupt("generation reload counter exhausted".into()))
        };
        let schema_generation = increment(self.source_schema_generation)?;
        let table_epoch = increment(store.table_rows_epoch.load(Ordering::Acquire))?;
        let index_epoch = increment(store.index_rows_epoch.load(Ordering::Acquire))?;
        let next_table = self
            .logical_floor
            .min(physical_floor)
            .checked_sub(1)
            .ok_or_else(|| LimboError::Corrupt("generation reload logical IDs exhausted".into()))?;
        store.clock.publish_observed(self.checkpoint_timestamp, || {
            store.rows.clear();
            store.index_rows.clear();
            store.finalized_tx_states.clear();
            store.checkpoint_dirty_table_keys.clear();
            store.checkpoint_dirty_index_keys.clear();
            store.table_id_to_last_rowid.write().clear();
            store.sequence_watermarks.lock().clear();
            store.sequence_allocations.lock().clear();
            store.table_id_to_rootpage.store(self.roots);
            store.next_table_id.fetch_min(next_table, Ordering::SeqCst);
            store.global_header.write().replace(self.header);
            *store.backfill_floor.write() = WalPos::ORIGIN;
            store
                .durable_txid_max
                .store(self.checkpoint_timestamp, Ordering::Release);
            store
                .last_committed_tx_ts
                .store(self.checkpoint_timestamp, Ordering::Release);
            store
                .last_committed_schema_change_ts
                .store(self.checkpoint_timestamp, Ordering::Release);
            store
                .last_global_header_ts
                .store(self.checkpoint_timestamp, Ordering::Release);
            store
                .schema_generation
                .store(schema_generation, Ordering::Release);
            store.table_rows_epoch.store(table_epoch, Ordering::Release);
            store.index_rows_epoch.store(index_epoch, Ordering::Release);
            store
                .checkpoint_full_scan_generation
                .store(0, Ordering::Release);
            store
                .checkpoint_full_scan_min_snapshot_ts
                .store(0, Ordering::Release);
            store.live_version_count_approx.store(0, Ordering::Release);
            store.live_versions_at_last_gc.store(0, Ordering::Release);
            *store.gc_table_cursor.lock() = None;
            *store.gc_index_cursor.lock() = None;
            store.gc_last_lwm.store(u64::MAX, Ordering::Release);
            Ok(())
        })
    }
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_generation_reload_native_installs_only_under_completed_checkpoint_authority() {
    use crate::util::IOExt;
    let fixture = super::shared_runtime::shared_completed_checkpoint_fixture(|_| {
        Arc::new(crate::io::PlatformIO::new().unwrap())
    });
    let store = fixture.database.get_mv_store().as_ref().unwrap().clone();
    let header = fixture
        .io
        .block(|| fixture.connection.get_pager().with_header(|header| *header))
        .unwrap();
    let schema = fixture.connection.schema.read().clone();
    let target = fixture
        .proof
        .target_identity()
        .database
        .checkpoint_timestamp;
    store.insert_table_id_to_rootpage(MVTableId::from(-900), Some(1));
    let old_roots = store.table_id_to_rootpage.load_full();
    let tx_ids = store.tx_ids.load(Ordering::Acquire);
    let before_clock = store.clock.get_timestamp(crate::mvcc::clock::no_op);
    let generation = store.schema_generation.load(Ordering::Acquire);
    let prepared = store
        .prepare_generation_reload(header, &schema, target)
        .unwrap();
    fixture
        .connection
        .get_pager()
        .reset_shared_generation()
        .unwrap();
    prepared
        .install(&store, &fixture.proof, &fixture.guard)
        .unwrap();
    assert!(!Arc::ptr_eq(
        &old_roots,
        &store.table_id_to_rootpage.load_full()
    ));
    assert!(store.current_root_page(&MVTableId::from(-900)).is_none());
    assert!(old_roots.get(&MVTableId::from(-900)).is_some());
    assert!(store.rows.is_empty());
    assert!(store.index_rows.is_empty());
    assert!(store.finalized_tx_states.is_empty());
    assert_eq!(store.tx_ids.load(Ordering::Acquire), tx_ids);
    assert!(store.clock.get_timestamp(crate::mvcc::clock::no_op) > before_clock);
    assert_eq!(
        store.schema_generation.load(Ordering::Acquire),
        generation + 1
    );
    assert_eq!(store.last_committed_tx_ts.load(Ordering::Acquire), target);
    assert_eq!(store.durable_txid_max.load(Ordering::Acquire), target);
    assert!(fixture.guard.io_retired());
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_generation_reload_native_rejects_stale_or_busy_preparation() {
    use crate::util::IOExt;
    for case in 0..5 {
        let fixture = super::shared_runtime::shared_completed_checkpoint_fixture(|_| {
            Arc::new(crate::io::PlatformIO::new().unwrap())
        });
        let store = fixture.database.get_mv_store().as_ref().unwrap().clone();
        let header = fixture
            .io
            .block(|| fixture.connection.get_pager().with_header(|header| *header))
            .unwrap();
        let schema = fixture.connection.schema.read().clone();
        let target = fixture
            .proof
            .target_identity()
            .database
            .checkpoint_timestamp;
        let prepared = store
            .prepare_generation_reload(header, &schema, target + u64::from(case == 0))
            .unwrap();
        let old_map = store.table_id_to_rootpage.load_full();
        let mut expected_map = old_map.clone();
        let pending = if case == 1 {
            Some(
                crate::mvcc::shared_authority::PublicationLease::pin(&fixture.guard.io_lease())
                    .unwrap(),
            )
        } else {
            None
        };
        if case == 2 {
            assert!(store.blocking_checkpoint_lock.read());
        }
        if case == 3 {
            expected_map = prepared.roots.clone();
            store.table_id_to_rootpage.store(expected_map.clone());
        }
        let generation = store.schema_generation.load(Ordering::Acquire);
        let tx_ids = store.tx_ids.load(Ordering::Acquire);
        let foreign = (case == 4).then(|| {
            super::shared_runtime::shared_completed_checkpoint_fixture(|_| {
                Arc::new(crate::io::PlatformIO::new().unwrap())
            })
        });
        let authority = foreign.as_ref().unwrap_or(&fixture);
        let error = prepared
            .install(&store, &authority.proof, &authority.guard)
            .unwrap_err();
        if case == 1 || case == 2 {
            assert!(matches!(error, LimboError::Busy));
        } else {
            assert!(matches!(error, LimboError::InvalidArgument(_)));
        }
        assert!(Arc::ptr_eq(
            &expected_map,
            &store.table_id_to_rootpage.load_full()
        ));
        assert_eq!(store.schema_generation.load(Ordering::Acquire), generation);
        assert_eq!(store.tx_ids.load(Ordering::Acquire), tx_ids);
        assert!(!store.checkpoint_in_progress.load(Ordering::Acquire));
        if case == 2 {
            store.blocking_checkpoint_lock.unlock();
        }
        drop(pending);
    }
}
