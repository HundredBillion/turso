use super::shared_checkpoint_recovery::CheckpointRecoverySelection;
use super::*;
use crate::database::shared_physical::SharedPhysicalConnection;
use crate::database::shared_recovered::RecoveryOpenConfig;
use crate::mvcc::database::shared_bootstrap::{PhysicalBaseParts, PhysicalBaseReadState};
use crate::mvcc::read_pinned_file::ReadPinnedFile;

#[allow(dead_code)]
pub(in crate::mvcc) struct InspectedRecoveryBase {
    parts: PhysicalBaseParts,
    selection: CheckpointRecoverySelection,
    config: Arc<RecoveryOpenConfig>,
}

impl InspectedRecoveryBase {
    pub(in crate::mvcc) fn into_parts(
        self,
    ) -> (
        PhysicalBaseParts,
        CheckpointRecoverySelection,
        Arc<RecoveryOpenConfig>,
    ) {
        (self.parts, self.selection, self.config)
    }
}

#[allow(dead_code)]
pub(in crate::mvcc) struct RecoveryPhysicalRead {
    selection: Option<CheckpointRecoverySelection>,
    config: Arc<RecoveryOpenConfig>,
    opening: Option<SharedPhysicalConnection>,
    reader: Option<Arc<Connection>>,
    state: PhysicalBaseReadState,
    parts: Option<PhysicalBaseParts>,
    finished: bool,
}

impl CheckpointRecoverySelection {
    #[allow(dead_code)]
    pub(in crate::mvcc) fn begin_physical_read(
        self,
        options: crate::OpenOptions,
    ) -> Result<RecoveryPhysicalRead> {
        let runtime = &self.runtime;
        let guard = self.manifest.guard()?;
        if !runtime.owns_authority(guard)
            || self.manifest.record().database_id() != runtime.database_id
            || runtime.files.main.file_id()? != Some(runtime.database_id)
        {
            return Err(LimboError::InvalidArgument(
                "checkpoint inspection requires the selected runtime owner".into(),
            ));
        }
        if runtime.files.physical_wal.size()? != 0 {
            return Err(LimboError::InvalidArgument(
                "physical WAL recovery is required before main-only inspection".into(),
            ));
        }
        let file = ReadPinnedFile::new_for_checkpoint_recovery(runtime.files.main.clone(), guard)?;
        let path = runtime.path.to_str().ok_or_else(|| {
            LimboError::InvalidArgument("shared database path must be UTF-8".into())
        })?;
        let config = RecoveryOpenConfig::capture(options)?;
        config.validate_history(runtime.files.authority.configured_history_limit())?;
        let opening = config.open_physical(runtime.io.clone(), path.to_owned(), file)?;
        Ok(RecoveryPhysicalRead {
            selection: Some(self),
            config,
            opening: Some(opening),
            reader: None,
            state: Default::default(),
            parts: None,
            finished: false,
        })
    }
}

#[allow(dead_code)]
impl RecoveryPhysicalRead {
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<InspectedRecoveryBase> {
        let result = self.poll_inner();
        if result.is_err() {
            self.abort();
        }
        result
    }

    fn poll_inner(&mut self) -> IOResultOr<InspectedRecoveryBase> {
        if self.finished {
            return Err(LimboError::InvalidArgument("physical inspection has ended".into()).into());
        }
        if self.reader.is_none() && self.parts.is_none() {
            self.reader = Some(return_if_io!(self.opening.as_mut().unwrap().poll()));
            self.opening = None;
        }
        if self.parts.is_none() {
            self.parts = Some(return_if_io!(self
                .state
                .poll(self.reader.as_ref().unwrap())));
            self.reader = None;
        }
        let selection = self.selection.as_ref().unwrap();
        validate_parts(self.parts.as_ref().unwrap(), selection)?;
        let guard = selection.manifest.guard()?;
        guard.ensure_no_snapshot_locks()?;
        if !guard.io_retired() {
            return Ok(IOResult::IO(IOCompletions(guard.retirement_completion())));
        }
        self.finished = true;
        Ok(IOResult::Done(InspectedRecoveryBase {
            parts: self.parts.take().unwrap(),
            selection: self.selection.take().unwrap(),
            config: self.config.clone(),
        }))
    }

    fn abort(&mut self) {
        self.state = Default::default();
        self.reader = None;
        self.opening = None;
        self.parts = None;
        self.selection = None;
        self.finished = true;
    }
}

impl Drop for RecoveryPhysicalRead {
    fn drop(&mut self) {
        self.abort();
    }
}

fn validate_parts(
    parts: &PhysicalBaseParts,
    selection: &CheckpointRecoverySelection,
) -> Result<()> {
    let header = &parts.header;
    let record = selection.manifest.record();
    let bytes = selection.runtime.files.main.size()?;
    let size = u64::from(header.page_size.get());
    let pages = i64::from(header.database_size.get());
    if header.magic != *b"SQLite format 3\0"
        || header.read_version.0 != SHARED_MVCC_VERSION
        || header.write_version.0 != SHARED_MVCC_VERSION
        || crate::PageSize::new(header.page_size.get()).is_none()
        || !header
            .page_size
            .has_valid_reserved_space(header.reserved_space)
        || pages <= 0
        || bytes % size != 0
        || bytes / size < pages as u64
        || parts.persistent_timestamp > record.prefix().committed_timestamp
        || record
            .checkpoint_intent()
            .is_some_and(|intent| parts.persistent_timestamp < intent.source_checkpoint_timestamp)
        || (record.checkpoint_intent().is_none()
            && record.prefix().end_offset == 0
            && parts.persistent_timestamp != record.prefix().committed_timestamp)
    {
        return Err(LimboError::Corrupt(
            "invalid shared recovery physical facts".into(),
        ));
    }
    let mut roots = HashSet::default();
    for root in parts
        .schema
        .all_tables()
        .filter_map(|table| match table.as_ref() {
            crate::schema::Table::BTree(table) => Some(table.root_page),
            _ => None,
        })
        .chain(
            parts
                .schema
                .all_indexes()
                .filter(|index| index.is_btree_backed())
                .map(|index| index.root_page),
        )
    {
        roots.try_reserve(1)?;
        if root <= 0 || root > pages || !roots.insert(root) {
            return Err(LimboError::Corrupt(
                "invalid shared recovery root layout".into(),
            ));
        }
    }
    if !roots.contains(&1) {
        return Err(LimboError::Corrupt(
            "shared recovery lacks schema root".into(),
        ));
    }
    Ok(())
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
mod tests {
    use super::*;

    #[test]
    #[ignore = "requires native Linux ext-family filesystem"]
    fn checkpoint_recovery_physical_native_inspects_without_recovering_authority() {
        let fixture =
            shared_prepared_checkpoint_fixture(|_| Arc::new(crate::io::PlatformIO::new().unwrap()));
        let runtime = fixture
            .database
            .get_mv_store()
            .as_ref()
            .unwrap()
            .shared_runtime
            .get()
            .unwrap()
            .clone();
        let io = fixture.io.clone();
        drop(fixture.prepared);
        drop(fixture.token);
        drop(fixture.pause);
        drop(fixture.materialized);
        drop(fixture.connection);
        drop(fixture.database);
        let main = std::fs::read(&fixture.path).unwrap();
        let manifest = std::fs::read(fixture.namespace.join("manifest")).unwrap();
        let mut opening = runtime.begin_checkpoint_recovery().unwrap();
        let selected = io.block(|| opening.poll()).unwrap();
        let expected = selected.manifest.record().prefix().committed_timestamp;
        assert!(selected.manifest.record().checkpoint_intent().is_some());
        assert!(selected.manifest.guard().unwrap().requires_recovery());
        let mut reading = selected
            .begin_physical_read(crate::OpenOptions::new(Arc::new(crate::EngineDialect)))
            .unwrap();
        let inspected = io.block(|| reading.poll()).unwrap();
        assert_eq!(inspected.parts.persistent_timestamp, expected);
        assert!(inspected
            .parts
            .schema
            .get_table(crate::namespace::NamespaceId::ROOT, "preserved")
            .is_some());
        assert!(inspected
            .selection
            .manifest
            .guard()
            .unwrap()
            .requires_recovery());
        assert!(inspected
            .selection
            .manifest
            .guard()
            .unwrap()
            .published()
            .is_err());
        assert!(matches!(
            runtime.gate.try_enter_admission(),
            Err(LimboError::Busy)
        ));
        assert!(reading.poll().is_err());
        assert_eq!(std::fs::read(&fixture.path).unwrap(), main);
        assert_eq!(
            std::fs::read(fixture.namespace.join("manifest")).unwrap(),
            manifest
        );
    }
}
