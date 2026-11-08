use super::*;

impl<C: LogicalClock, A: ConcurrentAllocator> MvStore<C, A> {
    pub(super) fn prepare_peer_sequence_schema(
        &self,
        schema: &mut Schema,
        prior: &Schema,
        frame: &PreparedRecoveryFrame,
    ) -> Result<()> {
        for (backing, name) in schema.sequence_backing_table_names() {
            let table = schema
                .get_btree_table(NamespaceId::ROOT, &backing)
                .ok_or_else(|| {
                    LimboError::Corrupt("sequence backing table is not a btree".into())
                })?;
            let table_id = self.get_table_id_from_root_page(table.root_page);
            let integer_key = |id: &RowID| match id.row_id {
                RowKey::Int(value) => Ok(value),
                _ => Err(LimboError::Corrupt(
                    "sequence backing row has a non-integer key".into(),
                )),
            };
            let mut deleted = HashSet::default();
            let mut row = None;
            for op in frame.decoded_frame.iter().rev() {
                match op {
                    StreamingResult::DeleteTableRow { rowid, .. } if rowid.table_id == table_id => {
                        deleted.try_reserve(1)?;
                        deleted.insert(integer_key(rowid)?);
                    }
                    StreamingResult::UpsertTableRow { row: candidate, .. }
                        if candidate.id.table_id == table_id =>
                    {
                        if !deleted.contains(&integer_key(&candidate.id)?) {
                            row = Some(candidate);
                            break;
                        }
                    }
                    _ => {}
                }
            }
            if let Some(row) = row {
                let record = ImmutableRecord::from_bin_record(crate::types::value_blob_from_slice(
                    row.payload(),
                )?);
                schema.install_sequence_descriptor_from_record(&name, &record)?;
                continue;
            }
            let sequence = prior
                .sequence_backing_table_names()
                .into_iter()
                .find_map(|(old_backing, old_name)| {
                    let old_table = prior.get_btree_table(NamespaceId::ROOT, &old_backing)?;
                    if self.get_table_id_from_root_page(old_table.root_page) != table_id {
                        return None;
                    }
                    let (namespace, short_name) = prior.demangle_name(&old_name);
                    prior
                        .has_sequence(namespace, short_name)
                        .then(|| prior.get_sequence(namespace, short_name).unwrap().clone())
                })
                .ok_or_else(|| {
                    LimboError::Corrupt(format!(
                        "new sequence \"{name}\" has no committed descriptor row"
                    ))
                })?;
            let short_name = schema.demangle_name(&name).1;
            let sequence = if sequence.name == short_name {
                sequence
            } else {
                let mut renamed = sequence.as_ref().clone();
                short_name.clone_into(&mut renamed.name);
                Arc::new(renamed)
            };
            schema.insert_sequence_under_mangled_name(&name, sequence);
        }
        Ok(())
    }
}
