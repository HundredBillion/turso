use super::*;

pub(crate) struct SharedWalRecoveryLocks {
    mapping: Arc<MappedSharedWalCoordination>,
}

impl SharedWalRecoveryLocks {
    pub(crate) fn acquire(mapping: Arc<MappedSharedWalCoordination>) -> Result<Arc<Self>> {
        let _ = mapping;
        Err(LimboError::InvalidArgument(
            "physical WAL recovery exclusion is not implemented".into(),
        ))
    }
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
