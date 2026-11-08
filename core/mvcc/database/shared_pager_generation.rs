use super::*;
use crate::mvcc::commit_manifest::checkpoint_publication::CompletedCheckpointProof;
use crate::mvcc::commit_manifest::ManifestRecord;
use crate::mvcc::shared_authority::AuthorityGuard;
use crate::storage::pager::SharedPagerGeneration;

impl SharedMvccRuntime {
    pub(super) fn captured_pager_generation(
        &self,
        record: ManifestRecord,
    ) -> Result<SharedPagerGeneration> {
        let identity = self
            .replay
            .lock()
            .as_ref()
            .ok_or(LimboError::Busy)?
            .identity()?;
        if record.requires_recovery()
            || record.database_id() != self.database_id
            || record.log_generation() != identity.database.generation
        {
            return Err(LimboError::Busy);
        }
        Ok(SharedPagerGeneration {
            database_id: self.database_id,
            generation: identity.database.generation,
            checkpoint_timestamp: identity.database.checkpoint_timestamp,
        })
    }

    pub(in crate::mvcc::database) fn certify_current_pager(
        &self,
        connection: &Connection,
        captured: Option<SharedPagerGeneration>,
        guard: &AuthorityGuard,
    ) -> Result<()> {
        self.certify_database_pager(&connection.db, &connection.get_pager(), captured, guard)
    }

    pub(in crate::mvcc::database) fn certify_database_pager(
        &self,
        database: &crate::Database,
        pager: &Pager,
        captured: Option<SharedPagerGeneration>,
        guard: &AuthorityGuard,
    ) -> Result<()> {
        let identity = self
            .replay
            .lock()
            .as_ref()
            .ok_or(LimboError::Busy)?
            .identity()?;
        self.validate_oracle_identity(guard, identity)?;
        if !guard.io_retired() {
            return Err(LimboError::Busy);
        }
        let generation = SharedPagerGeneration {
            database_id: self.database_id,
            generation: identity.database.generation,
            checkpoint_timestamp: identity.database.checkpoint_timestamp,
        };
        if captured.is_some_and(|captured| captured != generation) {
            return Err(LimboError::Busy);
        }
        database.validate_shared_pager_owner(pager, self)?;
        pager.ensure_shared_generation(generation)
    }

    pub(in crate::mvcc) fn certify_completed_pager(
        &self,
        connection: &Connection,
        proof: &CompletedCheckpointProof,
        guard: &AuthorityGuard,
    ) -> Result<()> {
        let target = proof.target_identity();
        if self.replay.lock().is_some()
            || !self.owns_authority(guard)
            || !proof.matches_lease(guard)
            || guard.published()? != target.prefix
            || !guard.incarnation_ready()?
            || target.database.checkpoint_timestamp != target.prefix.committed_timestamp
            || target.prefix.end_offset != 0
            || target.prefix.running_crc != 0
        {
            return Err(LimboError::InvalidArgument(
                "pager requires installed completed checkpoint authority".into(),
            ));
        }
        if !guard.io_retired() || !guard.live_registrations()?.is_empty() {
            return Err(LimboError::Busy);
        }
        let pager = connection.get_pager();
        connection.db.validate_shared_pager_owner(&pager, self)?;
        pager.reset_shared_generation()?;
        pager.ensure_shared_generation(SharedPagerGeneration {
            database_id: self.database_id,
            generation: target.database.generation,
            checkpoint_timestamp: target.database.checkpoint_timestamp,
        })
    }
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
mod tests {
    use super::*;

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn shared_pager_generation_native_completed_certification_keeps_admission_closed() {
        let fixture =
            shared_completed_checkpoint_fixture(|_| Arc::new(crate::io::SyscallIO::new().unwrap()));
        let store = fixture.database.get_mv_store().as_ref().unwrap().clone();
        let runtime = store.shared_runtime.get().unwrap();
        let pager = fixture.connection.get_pager();
        assert!(
            runtime
                .certify_completed_pager(&fixture.connection, &fixture.proof, &fixture.guard)
                .is_err(),
            "completion certified a pager while the previous replay context remained ready"
        );
        let header = fixture
            .io
            .block(|| pager.with_header(|header| *header))
            .unwrap();
        let schema = fixture.connection.schema.read().clone();
        let target = fixture.proof.target_identity();
        let prepared = store
            .prepare_generation_reload(header, &schema, target.database.checkpoint_timestamp)
            .unwrap();
        let previous_replay = runtime.replay.lock().take().unwrap();
        pager.reset_shared_generation().unwrap();
        prepared
            .install(&store, &fixture.proof, &fixture.guard)
            .unwrap();
        let (before, completion) = fixture.io.block(|| pager.read_page(1)).unwrap();
        if let Some(completion) = completion {
            fixture.io.wait_for_completion(completion).unwrap();
        }
        assert!(fixture.guard.io_retired());
        let foreign = crate::Database::open_file(
            Arc::new(crate::MemoryIO::new()),
            ":memory:",
            Arc::new(crate::EngineDialect),
        )
        .unwrap()
        .connect(crate::ConnectOpts::new())
        .unwrap();
        assert!(runtime
            .certify_completed_pager(&foreign, &fixture.proof, &fixture.guard)
            .is_err());
        assert!(Arc::ptr_eq(&before, &pager.cache_get(1).unwrap().unwrap()));
        pager.begin_read_tx().unwrap();
        assert!(matches!(
            runtime.certify_completed_pager(&fixture.connection, &fixture.proof, &fixture.guard),
            Err(LimboError::Busy)
        ));
        assert!(pager.holds_read_lock());
        assert!(Arc::ptr_eq(&before, &pager.cache_get(1).unwrap().unwrap()));
        pager.end_read_tx();
        runtime
            .certify_completed_pager(&fixture.connection, &fixture.proof, &fixture.guard)
            .unwrap();
        assert!(
            pager.cache_get(1).unwrap().is_none(),
            "completed target retained an earlier pager cache"
        );
        assert!(runtime.replay.lock().is_none());
        assert!(matches!(runtime.begin_open(), Err(LimboError::Busy)));
        fixture.guard.finish().unwrap();
        let replacement = runtime.files.authority.try_acquire().unwrap();
        assert!(
            runtime
                .certify_completed_pager(&fixture.connection, &fixture.proof, &replacement)
                .is_err(),
            "a replacement authority lease reused an earlier completed proof"
        );
        assert!(runtime.replay.lock().is_none());
        drop(replacement);
        drop(previous_replay);
    }
}
