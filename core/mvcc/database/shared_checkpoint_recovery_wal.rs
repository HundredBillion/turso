use super::*;
use crate::mvcc::shared_authority::AuthorityGuard;
use crate::storage::shared_wal_coordination::SharedWalRecoveryLocks;

impl SharedMvccRuntime {
    pub(in crate::mvcc) fn bind_physical_recovery_files(
        &self,
        guard: &AuthorityGuard,
        physical: &Arc<SharedWalRecoveryLocks>,
    ) -> Result<Vec<FileBinding>> {
        if !self.owns_authority(guard)
            || self.files.main.file_id()? != Some(self.database_id)
            || self.files.physical_wal.file_id()?.is_none()
            || self.files.physical_coordination.file_id()?.is_none()
        {
            return Err(LimboError::InvalidArgument(
                "physical recovery binding requires its retained runtime files".into(),
            ));
        }
        guard.ensure_no_snapshot_locks()?;
        let mut bindings = Vec::new();
        for file in [&self.main, &self.wal] {
            physical.validate_mapping(&self.files.physical_coordination)?;
            bindings.push(file.bind_physical_recovery(guard, physical.clone())?);
        }
        Ok(bindings)
    }
}

use super::shared_checkpoint_recovery_intent::RecoveredCheckpointIntent;
use crate::database::shared_physical::SharedPhysicalConnection;
use crate::mvcc::commit_manifest::checkpoint_publication::CheckpointIntentInspection;
use crate::mvcc::commit_manifest::ManifestRecord;
use crate::mvcc::database::shared_bootstrap::{PhysicalBaseParts, PhysicalBaseReadState};
use crate::mvcc::shared_authority::{IoPin, PublicationLease};
use crate::storage::sqlite3_ondisk::{BuildSharedWal, WalHeader, WAL_MAGIC_BE, WAL_MAGIC_LE};
use crate::storage::wal::WalFileShared;
use crate::sync::atomic::AtomicUsize;
use crate::sync::RwLock;

pub(in crate::mvcc) struct RecoveredIntentPhysicalBase {
    original_options: crate::OpenOptions,
    parts: PhysicalBaseParts,
    intent: RecoveredCheckpointIntent,
}
impl RecoveredIntentPhysicalBase {
    pub(in crate::mvcc) fn record(&self) -> ManifestRecord {
        self.intent.record()
    }
    pub(in crate::mvcc) fn guard(&self) -> &AuthorityGuard {
        self.intent.guard()
    }
}

#[derive(Clone)]
pub(in crate::mvcc) struct RecoveryWalEvidence {
    runtime: Arc<SharedMvccRuntime>,
    physical: Arc<SharedWalRecoveryLocks>,
    record: ManifestRecord,
    lease: Weak<PublicationLease>,
    main_id: FileId,
    wal_id: FileId,
    manifest_id: FileId,
    coordination_id: FileId,
}
impl RecoveryWalEvidence {
    fn new(
        intent: &RecoveredCheckpointIntent,
        physical: Arc<SharedWalRecoveryLocks>,
    ) -> Result<Self> {
        let runtime = intent.retained_runtime().clone();
        let native_id = |file: &Arc<dyn File>| {
            file.file_id()?.ok_or_else(|| {
                LimboError::InvalidArgument(
                    "physical recovery requires native retained files".into(),
                )
            })
        };
        let evidence =
            Self {
                main_id: native_id(&runtime.files.main)?,
                wal_id: native_id(&runtime.files.physical_wal)?,
                manifest_id: native_id(&runtime.files.manifest)?,
                coordination_id: runtime.files.physical_coordination.file_id()?.ok_or_else(
                    || {
                        LimboError::InvalidArgument(
                            "physical recovery requires native coordination".into(),
                        )
                    },
                )?,
                record: intent.record(),
                lease: intent.guard().io_lease(),
                runtime,
                physical,
            };
        evidence.validate_guard(intent.guard())?;
        Ok(evidence)
    }
    fn validate_files(&self) -> Result<()> {
        if self.runtime.files.main.file_id()? != Some(self.main_id)
            || self.main_id != self.runtime.database_id
            || self.runtime.files.physical_wal.file_id()? != Some(self.wal_id)
            || self.runtime.files.manifest.file_id()? != Some(self.manifest_id)
            || self.runtime.files.physical_coordination.file_id()? != Some(self.coordination_id)
            || self.record.database_id() != self.runtime.database_id
            || self.record.checkpoint_intent().is_none()
            || self.record.downgrade_intent().is_some()
        {
            return Err(LimboError::InvalidArgument(
                "physical recovery retained identities changed".into(),
            ));
        }
        self.physical
            .validate_mapping(&self.runtime.files.physical_coordination)?;
        PublicationLease::pin(&self.lease)?;
        Ok(())
    }
    pub(in crate::mvcc) fn validate_guard(&self, guard: &AuthorityGuard) -> Result<()> {
        self.validate_files()?;
        if !self.runtime.owns_authority(guard) || !Weak::ptr_eq(&self.lease, &guard.io_lease()) {
            return Err(LimboError::InvalidArgument(
                "physical recovery authority lease changed".into(),
            ));
        }
        guard.ensure_no_snapshot_locks()
    }
    pub(in crate::mvcc) fn inspection_file(
        &self,
        file: &Arc<dyn File>,
        record: ManifestRecord,
        guard: &AuthorityGuard,
    ) -> Result<(Arc<GuardedFile>, FileBinding)> {
        self.validate_guard(guard)?;
        if !Arc::ptr_eq(file, &self.runtime.files.manifest) || record != self.record {
            return Err(LimboError::InvalidArgument(
                "physical recovery inspection changed manifest".into(),
            ));
        }
        let file = GuardedFile::new(file.clone(), &self.runtime.files.authority);
        let binding = file.bind_physical_recovery(guard, self.physical.clone())?;
        Ok((file, binding))
    }
}

pub(crate) struct RecoveredPhysicalWalEvidence {
    source: RecoveryWalEvidence,
    scanned: Arc<RwLock<WalFileShared>>,
    header: crate::storage::sqlite3_ondisk::DatabaseHeader,
    main_bytes: u64,
}
impl RecoveredPhysicalWalEvidence {
    pub(crate) fn empty_wal_state(
        &self,
        physical: &Arc<SharedWalRecoveryLocks>,
    ) -> Result<(WalHeader, (u32, u32), u64)> {
        self.source.validate_files()?;
        if !Arc::ptr_eq(physical, &self.source.physical)
            || self.source.runtime.files.physical_wal.size()? != 0
            || self.source.runtime.files.main.size()? != self.main_bytes
            || self.main_bytes % u64::from(self.header.page_size.get()) != 0
            || self.main_bytes / u64::from(self.header.page_size.get())
                < u64::from(self.header.database_size.get())
        {
            return Err(LimboError::InvalidArgument(
                "physical recovery output changed native extent".into(),
            ));
        }
        let wal = self.scanned.read();
        if wal.metadata.max_frame.load(Ordering::Acquire) != 0
            || wal.metadata.nbackfills.load(Ordering::Acquire) != 0
            || wal
                .runtime
                .file
                .as_ref()
                .is_none_or(|file| !Arc::ptr_eq(file, &self.source.runtime.physical_wal_file()))
        {
            return Err(LimboError::InvalidArgument(
                "physical recovery output is not the actual empty WAL".into(),
            ));
        }
        let mut header = *wal.metadata.wal_header.lock();
        header.page_size = self.header.page_size.get();
        Ok((
            header,
            wal.metadata.last_checksum,
            wal.metadata.transaction_count.load(Ordering::Acquire),
        ))
    }
    fn sync_owner(&self, guard: &AuthorityGuard) -> Result<RecoveryWalSyncEvidence> {
        self.source.validate_guard(guard)?;
        Ok(RecoveryWalSyncEvidence {
            source: self.source.clone(),
            _physical: self.source.physical.clone(),
            _publication: PublicationLease::pin(&self.source.lease)?,
        })
    }
}
pub(crate) struct RecoveryWalSyncEvidence {
    source: RecoveryWalEvidence,
    _physical: Arc<SharedWalRecoveryLocks>,
    _publication: IoPin,
}
impl RecoveryWalSyncEvidence {
    pub(crate) fn validate(&self, physical: &Arc<SharedWalRecoveryLocks>) -> Result<()> {
        self.source.validate_files()?;
        if !Arc::ptr_eq(physical, &self._physical) {
            return Err(LimboError::InvalidArgument(
                "physical recovery sync changed native owner".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum Phase {
    Acquire,
    Scan,
    CommitHeader,
    OpenEffective,
    ReadEffective,
    InspectIntent,
    Copy,
    OpenMain,
    ReadMain,
    InspectFinal,
    Repair,
    Sync,
    Retire,
    Done,
}
struct CommitRead {
    buffer: Arc<crate::Buffer>,
    completion: Completion,
    bytes: Arc<AtomicUsize>,
}
pub(in crate::mvcc) struct CheckpointPhysicalWalRecovery {
    runtime: Arc<SharedMvccRuntime>,
    options: crate::OpenOptions,
    phase: Phase,
    physical: Option<Arc<SharedWalRecoveryLocks>>,
    bindings: Vec<FileBinding>,
    scan: Option<BuildSharedWal>,
    scanned: Option<Arc<RwLock<WalFileShared>>>,
    commit_read: Option<CommitRead>,
    commit_pages: Option<u32>,
    nonempty_uncommitted: bool,
    opening: Option<SharedPhysicalConnection>,
    reader: Option<Arc<Connection>>,
    read: PhysicalBaseReadState,
    expected: Option<PhysicalBaseParts>,
    actual: Option<PhysicalBaseParts>,
    pager: Option<Arc<Pager>>,
    inspection: Option<CheckpointIntentInspection>,
    evidence: Option<RecoveryWalEvidence>,
    final_evidence: Option<RecoveredPhysicalWalEvidence>,
    sync: Option<Completion>,
    intent: Option<RecoveredCheckpointIntent>,
}
impl CheckpointPhysicalWalRecovery {
    pub(super) fn new(
        intent: RecoveredCheckpointIntent,
        options: crate::OpenOptions,
    ) -> Result<Self> {
        if intent.retained_runtime().original_readonly {
            return Err(LimboError::InvalidArgument(
                "shared checkpoint recovery requires a writable participant".into(),
            ));
        }
        SharedPhysicalConnection::validate_recovery_options(
            &options,
            intent
                .retained_runtime()
                .files
                .authority
                .configured_history_limit(),
        )?;
        Ok(Self {
            runtime: intent.retained_runtime().clone(),
            options,
            phase: Phase::Acquire,
            physical: None,
            bindings: Vec::new(),
            scan: None,
            scanned: None,
            commit_read: None,
            commit_pages: None,
            nonempty_uncommitted: false,
            opening: None,
            reader: None,
            read: Default::default(),
            expected: None,
            actual: None,
            pager: None,
            inspection: None,
            evidence: None,
            final_evidence: None,
            sync: None,
            intent: Some(intent),
        })
    }
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<RecoveredIntentPhysicalBase> {
        let result = self.poll_inner();
        if result.is_err() {
            self.abort();
        }
        result
    }
    fn guard(&self) -> &AuthorityGuard {
        self.intent.as_ref().unwrap().guard()
    }
    fn open_reader(&self, wal: bool) -> Result<SharedPhysicalConnection> {
        let path = self
            .runtime
            .path
            .to_str()
            .ok_or_else(|| LimboError::InvalidArgument("shared physical path is not UTF-8".into()))?
            .to_owned();
        if wal {
            SharedPhysicalConnection::new_with_retained_wal(
                self.runtime.io.clone(),
                path,
                self.options.clone(),
                self.runtime.main_file(),
                self.scanned.as_ref().unwrap().clone(),
            )
        } else {
            SharedPhysicalConnection::new_with_retained_file(
                self.runtime.io.clone(),
                path,
                self.options.clone(),
                self.runtime.main_file(),
            )
        }
    }
    fn begin_inspection(&mut self) -> Result<()> {
        self.inspection = Some(
            self.intent
                .as_ref()
                .unwrap()
                .mirrored()
                .inspect_exact_for_physical_recovery(self.evidence.as_ref().unwrap())?,
        );
        Ok(())
    }
    fn poll_inner(&mut self) -> IOResultOr<RecoveredIntentPhysicalBase> {
        if matches!(self.phase, Phase::Done) {
            return Err(
                LimboError::InvalidArgument("physical WAL recovery has ended".into()).into(),
            );
        }
        if let Some(evidence) = &self.evidence {
            evidence.validate_guard(self.guard())?;
        }
        loop {
            match self.phase {
                Phase::Acquire => {
                    let physical =
                        SharedWalRecoveryLocks::acquire(self.runtime.physical_coordination())?;
                    let evidence =
                        RecoveryWalEvidence::new(self.intent.as_ref().unwrap(), physical.clone())?;
                    self.bindings = self
                        .runtime
                        .bind_physical_recovery_files(self.guard(), &physical)?;
                    self.scan = Some(BuildSharedWal::begin(&self.runtime.physical_wal_file())?);
                    self.physical = Some(physical);
                    self.evidence = Some(evidence);
                    self.phase = Phase::Scan;
                }
                Phase::Scan => {
                    let scanned = return_if_io!(self.scan.as_mut().unwrap().poll());
                    self.scan = None;
                    let wal = scanned.read();
                    let header = *wal.metadata.wal_header.lock();
                    let max = wal.metadata.max_frame.load(Ordering::Acquire);
                    let size = self.runtime.files.physical_wal.size()?;
                    self.nonempty_uncommitted = size != 0 && max == 0;
                    if max > 0
                        && (!matches!(header.magic, WAL_MAGIC_LE | WAL_MAGIC_BE)
                            || header.file_format != 3007000
                            || crate::PageSize::new(header.page_size).is_none())
                    {
                        return Err(LimboError::Corrupt(
                            "nonempty physical WAL lacks a supported committed image".into(),
                        )
                        .into());
                    }
                    if max > 0 {
                        let frame_size = u64::from(header.page_size)
                            .checked_add(24)
                            .ok_or(LimboError::TooBig)?;
                        let offset = max
                            .checked_sub(1)
                            .and_then(|frame| frame.checked_mul(frame_size))
                            .and_then(|offset| offset.checked_add(32))
                            .ok_or(LimboError::TooBig)?;
                        if offset.checked_add(frame_size).is_none_or(|end| end > size) {
                            return Err(LimboError::Corrupt(
                                "committed physical frame exceeds retained WAL".into(),
                            )
                            .into());
                        }
                        let buffer = Arc::new(crate::Buffer::new_temporary(24));
                        let bytes = Arc::new(AtomicUsize::new(usize::MAX));
                        let count = bytes.clone();
                        let completion = Completion::new_read(buffer.clone(), move |result| {
                            if let Ok((_, n)) = result {
                                count.store(n as usize, Ordering::Release);
                            }
                            None
                        });
                        let completion =
                            self.runtime.physical_wal_file().pread(offset, completion)?;
                        self.commit_read = Some(CommitRead {
                            buffer,
                            completion,
                            bytes,
                        });
                    }
                    drop(wal);
                    self.scanned = Some(scanned);
                    self.phase = Phase::CommitHeader;
                }
                Phase::CommitHeader => {
                    if let Some(read) = &self.commit_read {
                        if !read.completion.finished() {
                            return Ok(IOResult::IO(IOCompletions(read.completion.clone())));
                        }
                        if let Some(error) = read.completion.get_error() {
                            return Err(error.into());
                        }
                        if read.bytes.load(Ordering::Acquire) != 24 {
                            return Err(LimboError::Corrupt(
                                "short committed physical frame header".into(),
                            )
                            .into());
                        }
                        let (frame, _) = crate::storage::sqlite3_ondisk::parse_wal_frame_header(
                            read.buffer.as_slice(),
                        );
                        let wal = self.scanned.as_ref().unwrap().read();
                        let header = *wal.metadata.wal_header.lock();
                        if frame.db_size == 0
                            || frame.salt_1 != header.salt_1
                            || frame.salt_2 != header.salt_2
                            || (frame.checksum_1, frame.checksum_2) != wal.metadata.last_checksum
                        {
                            return Err(LimboError::Corrupt(
                                "committed physical frame identity changed".into(),
                            )
                            .into());
                        }
                        self.commit_pages = Some(frame.db_size);
                    }
                    self.commit_read = None;
                    self.opening = Some(self.open_reader(true)?);
                    self.phase = Phase::OpenEffective;
                }
                Phase::OpenEffective | Phase::OpenMain => {
                    self.reader = Some(return_if_io!(self.opening.as_mut().unwrap().poll()));
                    self.opening = None;
                    self.phase = if matches!(self.phase, Phase::OpenEffective) {
                        Phase::ReadEffective
                    } else {
                        Phase::ReadMain
                    };
                }
                Phase::ReadEffective => {
                    let parts = return_if_io!(self.read.poll(self.reader.as_ref().unwrap()));
                    validate_image(
                        &parts,
                        self.evidence.as_ref().unwrap().record,
                        self.runtime.files.main.size()?,
                        self.commit_pages,
                    )?;
                    if self.nonempty_uncommitted
                        && parts.persistent_timestamp
                            != self
                                .evidence
                                .as_ref()
                                .unwrap()
                                .record
                                .checkpoint_intent()
                                .unwrap()
                                .source_checkpoint_timestamp
                    {
                        return Err(LimboError::Corrupt(
                            "uncommitted physical WAL requires the exact source main".into(),
                        )
                        .into());
                    }
                    self.pager = Some(self.reader.as_ref().unwrap().get_pager());
                    self.reader = None;
                    self.expected = Some(parts);
                    self.begin_inspection()?;
                    self.phase = Phase::InspectIntent;
                }
                Phase::InspectIntent | Phase::InspectFinal => {
                    return_if_io!(self
                        .inspection
                        .as_mut()
                        .unwrap()
                        .poll(self.intent.as_ref().unwrap().guard()));
                    self.inspection = None;
                    self.phase = if matches!(self.phase, Phase::InspectIntent) {
                        Phase::Copy
                    } else {
                        Phase::Repair
                    };
                }
                Phase::Copy => {
                    return_if_io!(self.pager.as_ref().unwrap().checkpoint(
                        CheckpointMode::Truncate {
                            upper_bound_inclusive: None
                        },
                        crate::SyncMode::Full,
                        true
                    ));
                    self.pager = None;
                    self.read = Default::default();
                    self.opening = Some(self.open_reader(false)?);
                    self.phase = Phase::OpenMain;
                }
                Phase::ReadMain => {
                    let parts = return_if_io!(self.read.poll(self.reader.as_ref().unwrap()));
                    self.reader = None;
                    validate_image(
                        &parts,
                        self.evidence.as_ref().unwrap().record,
                        self.runtime.files.main.size()?,
                        None,
                    )?;
                    if self.runtime.files.physical_wal.size()? != 0
                        || !self
                            .expected
                            .as_ref()
                            .unwrap()
                            .matches_base_metadata(&parts)
                    {
                        return Err(LimboError::Corrupt(
                            "recovered main differs from committed physical image".into(),
                        )
                        .into());
                    }
                    self.actual = Some(parts);
                    self.begin_inspection()?;
                    self.phase = Phase::InspectFinal;
                }
                Phase::Repair => {
                    if !self.guard().io_retired() {
                        return Ok(IOResult::IO(IOCompletions(
                            self.guard().retirement_completion(),
                        )));
                    }
                    let evidence = RecoveredPhysicalWalEvidence {
                        source: self.evidence.as_ref().unwrap().clone(),
                        scanned: self.scanned.as_ref().unwrap().clone(),
                        header: self.actual.as_ref().unwrap().header,
                        main_bytes: self.runtime.files.main.size()?,
                    };
                    self.physical.as_ref().unwrap().install_empty(&evidence)?;
                    self.final_evidence = Some(evidence);
                    self.phase = Phase::Sync;
                }
                Phase::Sync => {
                    if self.sync.is_none() {
                        let owner = self
                            .final_evidence
                            .as_ref()
                            .unwrap()
                            .sync_owner(self.guard())?;
                        self.sync = Some(self.physical.as_ref().unwrap().begin_sync(owner)?);
                    }
                    let sync = self.sync.as_ref().unwrap();
                    if !sync.finished() {
                        return Ok(IOResult::IO(IOCompletions(sync.clone())));
                    }
                    if let Some(error) = sync.get_error() {
                        return Err(error.into());
                    }
                    self.sync = None;
                    self.phase = Phase::Retire;
                }
                Phase::Retire => {
                    if !self.guard().io_retired() {
                        return Ok(IOResult::IO(IOCompletions(
                            self.guard().retirement_completion(),
                        )));
                    }
                    self.final_evidence
                        .as_ref()
                        .unwrap()
                        .empty_wal_state(self.physical.as_ref().unwrap())?;
                    let output = RecoveredIntentPhysicalBase {
                        original_options: self.options.clone(),
                        parts: self.actual.take().unwrap(),
                        intent: self.intent.take().unwrap(),
                    };
                    self.abort();
                    return Ok(IOResult::Done(output));
                }
                Phase::Done => unreachable!(),
            }
        }
    }
    fn abort(&mut self) {
        self.inspection = None;
        self.commit_read = None;
        self.read = Default::default();
        self.reader = None;
        self.opening = None;
        self.pager = None;
        self.scan = None;
        self.scanned = None;
        self.sync = None;
        self.final_evidence = None;
        self.evidence = None;
        self.bindings.clear();
        self.physical = None;
        self.expected = None;
        self.actual = None;
        self.intent = None;
        self.phase = Phase::Done;
    }
}
impl Drop for CheckpointPhysicalWalRecovery {
    fn drop(&mut self) {
        self.abort();
    }
}

fn validate_image(
    parts: &PhysicalBaseParts,
    record: ManifestRecord,
    main_bytes: u64,
    commit_pages: Option<u32>,
) -> Result<()> {
    let header = &parts.header;
    let size = u64::from(header.page_size.get());
    let intent = record
        .checkpoint_intent()
        .ok_or_else(|| LimboError::InvalidArgument("physical recovery lost its intent".into()))?;
    if header.magic != *b"SQLite format 3\0"
        || header.read_version.0 != SHARED_MVCC_VERSION
        || header.write_version.0 != SHARED_MVCC_VERSION
        || crate::PageSize::new(header.page_size.get()).is_none()
        || !header
            .page_size
            .has_valid_reserved_space(header.reserved_space)
        || header.database_size.get() == 0
        || main_bytes < size
        || main_bytes % size != 0
        || commit_pages.is_some_and(|pages| pages != header.database_size.get())
        || (commit_pages.is_none() && main_bytes / size < u64::from(header.database_size.get()))
        || (parts.persistent_timestamp != intent.source_checkpoint_timestamp
            && parts.persistent_timestamp != record.prefix().committed_timestamp)
    {
        return Err(LimboError::Corrupt(
            "invalid exact recovery physical image".into(),
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
        if root <= 0 || root > i64::from(header.database_size.get()) || !roots.insert(root) {
            return Err(LimboError::Corrupt("invalid recovery root layout".into()));
        }
    }
    if !roots.contains(&1) {
        return Err(LimboError::Corrupt(
            "recovery physical image lacks schema root".into(),
        ));
    }
    Ok(())
}

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
#[path = "shared_checkpoint_recovery_wal_tests.rs"]
mod tests;

#[cfg(all(test, target_os = "linux", target_pointer_width = "64", not(shuttle)))]
#[path = "shared_checkpoint_wal_io_tests.rs"]
mod io_tests;
