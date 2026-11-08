use super::*;
use crate::io::FileId;

pub(crate) struct SharedWalRecoveryLocks {
    mapping: Arc<MappedSharedWalCoordination>,
    file_id: FileId,
    offsets: Vec<u64>,
}

impl SharedWalRecoveryLocks {
    pub(crate) fn acquire(mapping: Arc<MappedSharedWalCoordination>) -> Result<Arc<Self>> {
        if !mapping.uses_linux_ofd_locking() {
            return Err(LimboError::InvalidArgument(
                "physical WAL recovery requires native Linux OFD locks".into(),
            ));
        }
        let file_id = mapping.file.file_id()?.ok_or_else(|| {
            LimboError::InvalidArgument("physical WAL recovery requires native storage".into())
        })?;
        let mut offsets = Vec::new();
        offsets.try_reserve_exact(mapping.header().reader_slot_count as usize + 2)?;
        let mut local = mapping.local_lock_state.lock();
        if local.recovery_poisoned {
            return Err(LimboError::InvalidArgument(
                "physical WAL recovery lost native lock ownership".into(),
            ));
        }
        if local.recovery_held
            || local.writer_lock_held
            || local.checkpoint_lock_held
            || local.reader_locks.iter().any(|count| *count != 0)
        {
            return Err(LimboError::Busy);
        }
        let wanted = [CHECKPOINT_LOCK_OFFSET, WRITER_LOCK_OFFSET]
            .into_iter()
            .chain(
                (0..mapping.header().reader_slot_count)
                    .map(MappedSharedWalCoordination::process_lock_offset_for_reader),
            );
        for offset in wanted {
            let result = mapping
                .file
                .shared_wal_try_lock_byte(offset, true, mapping.lock_kind());
            let error = match result {
                Ok(true) => {
                    offsets.push(offset);
                    continue;
                }
                Ok(false) => LimboError::Busy,
                Err(error) => error,
            };
            let cleanup = release_offsets(&mapping, &offsets);
            if let Err(cleanup) = cleanup {
                local.recovery_poisoned = true;
                return Err(cleanup);
            }
            return Err(error);
        }
        local.recovery_held = true;
        drop(local);
        Ok(Arc::new(Self {
            mapping,
            file_id,
            offsets,
        }))
    }

    pub(crate) fn validate_mapping(
        &self,
        mapping: &Arc<MappedSharedWalCoordination>,
    ) -> Result<()> {
        let local = self.mapping.local_lock_state.lock();
        if !Arc::ptr_eq(&self.mapping, mapping)
            || self.mapping.file.file_id()? != Some(self.file_id)
            || !local.recovery_held
            || local.recovery_poisoned
        {
            return Err(LimboError::InvalidArgument(
                "physical WAL recovery exclusion changed owner or native file".into(),
            ));
        }
        Ok(())
    }
}

impl Drop for SharedWalRecoveryLocks {
    fn drop(&mut self) {
        let mut local = self.mapping.local_lock_state.lock();
        if let Err(error) = release_offsets(&self.mapping, &self.offsets) {
            local.recovery_poisoned = true;
            tracing::error!(%error, "failed releasing physical WAL recovery locks");
        }
        local.recovery_held = false;
    }
}

fn release_offsets(mapping: &MappedSharedWalCoordination, offsets: &[u64]) -> Result<()> {
    let mut first_error = None;
    for offset in offsets.iter().rev() {
        if let Err(error) = mapping
            .file
            .shared_wal_unlock_byte(*offset, mapping.lock_kind())
        {
            first_error.get_or_insert(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
mod tests {
    use super::*;

    #[test]
    #[ignore = "requires native Linux OFD locks"]
    fn physical_wal_recovery_native_owns_all_slots_until_drop() {
        let directory = tempfile::tempdir().unwrap();
        let io: Arc<dyn IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        let mapping = Arc::new(
            MappedSharedWalCoordination::create_or_open(&io, &directory.path().join("tshm"), 64)
                .unwrap(),
        );
        let owner = mapping.owner_record();
        let recovery = SharedWalRecoveryLocks::acquire(mapping.clone()).unwrap();
        assert!(Arc::ptr_eq(&recovery.mapping, &mapping));
        assert!(mapping.register_reader(owner, 1).is_none());
        assert!(mapping.register_db_file_reader(owner).is_none());
        assert!(!mapping.try_acquire_writer(owner));
        assert!(!mapping.try_acquire_checkpoint(owner));
        assert!(matches!(
            SharedWalRecoveryLocks::acquire(mapping.clone()),
            Err(LimboError::Busy)
        ));
        drop(recovery);
        let reader = mapping.register_reader(owner, 1).unwrap();
        mapping.unregister_reader(reader);
        assert!(mapping.try_acquire_writer(owner));
        mapping.release_writer(owner);
        assert!(mapping.try_acquire_checkpoint(owner));
        mapping.release_checkpoint(owner);
        drop(SharedWalRecoveryLocks::acquire(mapping).unwrap());
    }
}
