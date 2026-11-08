use super::*;
use crate::database::shared_physical::SharedPhysicalConnection;
use crate::mvcc::database::shared_bootstrap::{PhysicalBaseParts, PhysicalBaseReadState};
use crate::mvcc::read_pinned_file::ReadPinnedFile;
use crate::mvcc::shared_ids::io_protocol::current_generation::{
    CurrentGenerationIdLoad, CurrentIdBase,
};
use crate::storage::database::DatabaseFile;

pub(in crate::mvcc) struct InspectedCurrentBase {
    runtime: Arc<SharedMvccRuntime>,
    database: Arc<crate::Database>,
    manifest: LoadedManifest,
    ids: CurrentIdBase,
    parts: PhysicalBaseParts,
}

pub(super) struct CurrentBaseInspection {
    runtime: Arc<SharedMvccRuntime>,
    database: Arc<crate::Database>,
    ids_load: Option<CurrentGenerationIdLoad>,
    ids: Option<CurrentIdBase>,
    manifest_load: Option<ManifestLoad>,
    manifest: Option<LoadedManifest>,
    opening: Option<SharedPhysicalConnection>,
    reader: Option<Arc<Connection>>,
    reading: PhysicalBaseReadState,
    parts: Option<PhysicalBaseParts>,
    finished: bool,
}

impl CurrentBaseInspection {
    pub(super) fn new(
        runtime: Arc<SharedMvccRuntime>,
        database: Arc<crate::Database>,
        manifest: LoadedManifest,
    ) -> Result<Self> {
        validate_selection(&runtime, &database, &manifest)?;
        let ids_load = runtime.ids.inspect_current_base(manifest)?;
        Ok(Self {
            runtime,
            database,
            ids_load: Some(ids_load),
            ids: None,
            manifest_load: None,
            manifest: None,
            opening: None,
            reader: None,
            reading: Default::default(),
            parts: None,
            finished: false,
        })
    }

    pub(super) fn poll(&mut self) -> IOResultOr<InspectedCurrentBase> {
        if self.finished {
            return Err(
                LimboError::InvalidArgument("current base inspection has ended".into()).into(),
            );
        }
        let result = self.poll_inner();
        if result.is_err() {
            self.finished = true;
            self.ids_load = None;
            self.manifest_load = None;
            self.manifest = None;
            self.opening = None;
            self.reader = None;
        }
        result
    }

    fn poll_inner(&mut self) -> IOResultOr<InspectedCurrentBase> {
        if let Some(load) = self.ids_load.as_mut() {
            let loaded = return_if_io!(load.poll());
            self.ids_load = None;
            let (ids, guard) = loaded.suspend();
            self.ids = Some(ids);
            self.manifest_load = Some(ManifestLoad::new(
                self.runtime.files.manifest.clone(),
                guard,
                self.runtime.database_id,
            )?);
        }
        if let Some(load) = self.manifest_load.as_mut() {
            let manifest = return_if_io!(load.poll());
            self.manifest_load = None;
            validate_selection(&self.runtime, &self.database, &manifest)?;
            self.ids.as_ref().unwrap().validate_selection(&manifest)?;
            self.manifest = Some(manifest);
        }
        if self.reader.is_none() {
            if self.opening.is_none() {
                let manifest = self.manifest.as_ref().unwrap();
                let file =
                    ReadPinnedFile::for_manifest_replay(self.runtime.files.main.clone(), manifest)?;
                let options = crate::OpenOptions::new(self.database.dialect().clone())
                    .db_opts(self.database.opts)
                    .allocators(self.database.allocators.clone())
                    .storage(Arc::new(DatabaseFile::new(file)));
                self.opening = Some(SharedPhysicalConnection::new(
                    self.database.io.clone(),
                    self.database.path.clone(),
                    options,
                )?);
            }
            self.reader = Some(return_if_io!(self.opening.as_mut().unwrap().poll()));
            self.opening = None;
        }
        if self.parts.is_none() {
            self.parts = Some(return_if_io!(self
                .reading
                .poll(self.reader.as_ref().unwrap())));
        }
        let manifest = self.manifest.as_ref().unwrap();
        if !manifest.guard()?.io_retired() {
            return Ok(IOResult::IO(crate::types::IOCompletions(
                manifest.guard()?.retirement_completion(),
            )));
        }
        let inspected = InspectedCurrentBase {
            runtime: self.runtime.clone(),
            database: self.database.clone(),
            manifest: self.manifest.take().unwrap(),
            ids: self.ids.take().unwrap(),
            parts: self.parts.take().unwrap(),
        };
        inspected.validate()?;
        self.finished = true;
        self.reader = None;
        Ok(IOResult::Done(inspected))
    }
}

impl InspectedCurrentBase {
    pub(in crate::mvcc) fn identity(&self) -> RecoveryIdentity {
        self.ids.identity()
    }
    pub(in crate::mvcc) fn logical_floor(&self) -> i64 {
        self.ids.logical_floor()
    }
    pub(in crate::mvcc) fn guard(&self) -> Result<&crate::mvcc::shared_authority::AuthorityGuard> {
        self.manifest.guard()
    }
    pub(in crate::mvcc) fn database(&self) -> &Arc<crate::Database> {
        &self.database
    }
    pub(in crate::mvcc) fn parts(&self) -> &PhysicalBaseParts {
        &self.parts
    }

    pub(in crate::mvcc) fn validate(&self) -> Result<()> {
        validate_selection(&self.runtime, &self.database, &self.manifest)?;
        self.ids.validate_selection(&self.manifest)?;
        let identity = self.ids.identity();
        let header = &self.parts.header;
        let pages = u64::from(header.database_size.get());
        let size = self.runtime.files.main.size()?;
        let page_size = u64::from(header.page_size.get());
        if pages == 0
            || page_size == 0
            || header.read_version.0 != SHARED_MVCC_VERSION
            || header.write_version.0 != SHARED_MVCC_VERSION
        {
            return Err(LimboError::Corrupt(
                "invalid current physical database header".into(),
            ));
        }
        if self.parts.persistent_timestamp != identity.database.checkpoint_timestamp {
            return Err(LimboError::Corrupt(
                "physical checkpoint timestamp differs from active IDs".into(),
            ));
        }
        if self.ids.logical_floor() > -i64::from(header.database_size.get()) {
            return Err(LimboError::Corrupt(
                "current ID floor does not cover physical database pages".into(),
            ));
        }
        if pages
            .checked_mul(page_size)
            .is_none_or(|required| size < required)
            || size % page_size != 0
        {
            return Err(LimboError::Corrupt(
                "current physical database is truncated or unaligned".into(),
            ));
        }
        self.database
            .validate_shared_base_layout(&self.runtime, header)?;
        if !self.manifest.guard()?.io_retired() {
            return Err(LimboError::Busy);
        }
        Ok(())
    }

    pub(in crate::mvcc) fn into_parts(
        self,
    ) -> (
        Arc<SharedMvccRuntime>,
        Arc<crate::Database>,
        LoadedManifest,
        CurrentIdBase,
        PhysicalBaseParts,
    ) {
        (
            self.runtime,
            self.database,
            self.manifest,
            self.ids,
            self.parts,
        )
    }
}

fn validate_selection(
    runtime: &SharedMvccRuntime,
    database: &crate::Database,
    manifest: &LoadedManifest,
) -> Result<()> {
    database.validate_shared_runtime(runtime)?;
    if !manifest.belongs_to_file(&runtime.files.manifest)
        || manifest.record().database_id() != runtime.database_id
        || manifest.record().requires_recovery()
        || !runtime.owns_authority(manifest.guard()?)
        || runtime.files.main.file_id()? != Some(runtime.database_id)
        || runtime.files.manifest.file_id()?.is_none()
        || runtime.files.physical_wal.file_id()?.is_none()
        || runtime.files.physical_wal.size()? != 0
    {
        return Err(LimboError::InvalidArgument(
            "current base requires its retained normal manifest and empty physical WAL".into(),
        ));
    }
    Ok(())
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
mod tests {
    use super::*;
    use crate::IO;

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn current_base_native_inspection_preserves_local_and_foreign_read_state() {
        let directory = tempfile::tempdir().unwrap();
        let (path, limit) = shared_runtime_fixture(directory.path());
        let io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        let database = crate::Database::open(
            io.clone(),
            path.to_str().unwrap(),
            crate::OpenOptions::new(Arc::new(crate::EngineDialect))
                .with_shared_mvcc_for_test(limit),
        )
        .unwrap();
        let writer = database.connect(crate::ConnectOpts::new()).unwrap();
        writer
            .execute("UPDATE preserved SET value='logical only' WHERE id=1")
            .unwrap();
        let reader = database.connect(crate::ConnectOpts::new()).unwrap();
        reader.execute("BEGIN CONCURRENT").unwrap();
        reader.execute("SELECT value FROM preserved").unwrap();
        let store = database.get_mv_store().as_ref().unwrap().clone();
        let runtime = store.shared_runtime.get().unwrap().clone();
        let replay = runtime.replay.lock().clone().unwrap();
        let identity = replay.identity().unwrap();
        assert!(identity.prefix.committed_timestamp > identity.database.checkpoint_timestamp);
        let roots = store.table_id_to_rootpage.load_full();
        let schema = database.schema.lock().clone();
        let outer = reader.get_mv_tx();
        let clock = store.clock.get_timestamp(crate::mvcc::clock::no_op);
        let mut open = runtime.begin_open().unwrap();
        io.block(|| open.poll_manifest()).unwrap();
        let snapshots = open
            .loaded
            .as_ref()
            .unwrap()
            .guard()
            .unwrap()
            .live_registrations()
            .unwrap();
        assert_eq!(snapshots.len(), 1);
        let mut inspecting = CurrentBaseInspection::new(
            runtime.clone(),
            database.clone(),
            open.loaded.take().unwrap(),
        )
        .unwrap();
        let inspected = io.block(|| inspecting.poll()).unwrap();
        assert_eq!(inspected.identity(), identity);
        assert_eq!(
            inspected.parts().persistent_timestamp,
            identity.database.checkpoint_timestamp
        );
        assert_eq!(
            inspected.guard().unwrap().live_registrations().unwrap(),
            snapshots
        );
        assert!(Arc::ptr_eq(&roots, &store.table_id_to_rootpage.load_full()));
        assert!(Arc::ptr_eq(&schema, &database.schema.lock()));
        assert!(Arc::ptr_eq(
            &replay,
            runtime.replay.lock().as_ref().unwrap()
        ));
        assert_eq!(reader.get_mv_tx(), outer);
        assert_eq!(
            store.clock.get_timestamp(crate::mvcc::clock::no_op),
            clock + 1
        );
        let (_, _, manifest, _, _) = inspected.into_parts();
        manifest.into_guard().unwrap().finish().unwrap();
        drop(open);
        reader.execute("ROLLBACK").unwrap();
    }

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn current_base_native_rejects_same_bytes_from_a_different_manifest_file() {
        let directory = tempfile::tempdir().unwrap();
        let (path, limit) = shared_runtime_fixture(directory.path());
        let io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        let database = crate::Database::open(
            io,
            path.to_str().unwrap(),
            crate::OpenOptions::new(Arc::new(crate::EngineDialect))
                .with_shared_mvcc_for_test(limit),
        )
        .unwrap();
        let store = database.get_mv_store().as_ref().unwrap().clone();
        let runtime = store.shared_runtime.get().unwrap().clone();
        let bytes = std::fs::read(runtime.files.namespace.join("manifest")).unwrap();
        let memory = crate::MemoryIO::new();
        let forged = memory
            .open_file("same-byte-manifest", crate::OpenFlags::Create, false)
            .unwrap();
        drop(
            forged
                .pwrite(
                    0,
                    Arc::new(crate::Buffer::new(bytes)),
                    crate::Completion::new_write(|_| {}),
                )
                .unwrap(),
        );
        let guard = runtime.files.authority.try_acquire().unwrap();
        let mut load = ManifestLoad::new(forged, guard, runtime.database_id).unwrap();
        let manifest = memory.block(|| Ok(load.poll()?)).unwrap();
        assert!(CurrentBaseInspection::new(runtime, database, manifest).is_err());
    }
}
