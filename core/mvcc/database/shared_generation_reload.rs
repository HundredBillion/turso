use super::*;

pub(in crate::mvcc) struct PreparedGenerationReload<A: ConcurrentAllocator> {
    roots: Arc<SkipMap<MVTableId, RootEntry, BasicComparator, A>>,
    source_roots: Arc<SkipMap<MVTableId, RootEntry, BasicComparator, A>>,
    header: DatabaseHeader,
    checkpoint_timestamp: u64,
    logical_floor: i64,
    source_frontier: u64,
    source_schema_generation: u64,
}

impl<C: LogicalClock, A: ConcurrentAllocator> MvStore<C, A> {
    pub(in crate::mvcc) fn prepare_generation_reload(
        &self,
        header: DatabaseHeader,
        schema: &Schema,
        checkpoint_timestamp: u64,
    ) -> Result<PreparedGenerationReload<A>> {
        let source_roots = self.table_id_to_rootpage.load_full();
        let pages = i64::from(header.database_size.get());
        let source_frontier = self.last_committed_tx_ts.load(Ordering::Acquire);
        let source_schema_generation = self.schema_generation.load(Ordering::Acquire);
        if pages == 0
            || header.read_version.0 != SHARED_MVCC_VERSION
            || header.write_version.0 != SHARED_MVCC_VERSION
            || checkpoint_timestamp < source_frontier
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
