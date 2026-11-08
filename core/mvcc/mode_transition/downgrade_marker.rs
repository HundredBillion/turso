use crate::io::{File, FileId, FileSyncType};
use crate::mvcc::commit_manifest::downgrade_publication::MirroredDowngradeIntent;
use crate::sync::Arc;
use crate::{Completion, IOExt, LimboError, Result};
use std::path::Path;

/// Retains the terminal intent, native main exclusion and admission pause through
/// every raw-open/connection lifetime. Only genuine installed checkpoints qualify.
pub(crate) struct PreparedDowngradeMarker {
    origin: MarkerOrigin,
}
enum MarkerOrigin {
    Installed(Box<MirroredDowngradeIntent>),
    Recovered(Box<super::downgrade_resume::RecoveredDowngradeSource>),
}
impl PreparedDowngradeMarker {
    fn from_intent(intent: MirroredDowngradeIntent) -> Result<Arc<Self>> {
        intent.installed_source()?.files().revalidate()?;
        let proof = Arc::new(Self {
            origin: MarkerOrigin::Installed(Box::new(intent)),
        });
        proof.revalidate()?;
        Ok(proof)
    }
    pub(crate) fn revalidate(&self) -> Result<()> {
        match &self.origin {
            MarkerOrigin::Installed(intent) => intent
                .installed_source()?
                .validate_marker(intent.guard(), intent.record()),
            MarkerOrigin::Recovered(source) => source.revalidate(),
        }
    }
    fn files(&self) -> &super::downgrade_files::DowngradeFiles {
        match &self.origin {
            MarkerOrigin::Installed(intent) => intent.installed_source().unwrap().files(),
            MarkerOrigin::Recovered(source) => &source.files,
        }
    }
    fn record(&self) -> crate::mvcc::commit_manifest::ManifestRecord {
        match &self.origin {
            MarkerOrigin::Installed(intent) => intent.record(),
            MarkerOrigin::Recovered(source) => source.record,
        }
    }
    pub(crate) fn path(&self) -> &Path {
        self.files().path()
    }
    pub(crate) fn database_id(&self) -> FileId {
        self.record().database_id()
    }
    pub(crate) fn wal_id(&self) -> FileId {
        self.record().downgrade_intent().unwrap().basename_wal_id
    }
    pub(crate) fn main_file(&self) -> Arc<dyn File> {
        self.files().main_file()
    }
    pub(crate) fn wal_file(&self) -> Arc<dyn File> {
        self.files().wal_file()
    }
    fn timestamp(&self) -> u64 {
        self.record().prefix().committed_timestamp
    }
    fn physical_floor(&self) -> i64 {
        match &self.origin {
            MarkerOrigin::Installed(intent) => intent.installed_source().unwrap().physical_floor(),
            MarkerOrigin::Recovered(source) => source.logical_floor,
        }
    }
    fn validate_layout(
        &self,
        header: &crate::storage::sqlite3_ondisk::DatabaseHeader,
        bytes: u64,
    ) -> Result<()> {
        match &self.origin {
            MarkerOrigin::Installed(intent) => intent
                .installed_source()?
                .validate_physical_layout(header, bytes),
            MarkerOrigin::Recovered(source) => source.validate_layout(header, bytes),
        }
    }
    fn mirror(&self) -> Result<()> {
        match &self.origin {
            MarkerOrigin::Installed(intent) => self.files().require_mirrored(intent.record()),
            MarkerOrigin::Recovered(source) => source.mirror(),
        }
    }
}

/// The marker is durable; namespace removal remains a separate owned phase.
pub(in crate::mvcc) struct CompletedDowngradeMarker {
    _proof: Arc<PreparedDowngradeMarker>,
}

pub(in crate::mvcc) fn commit_marker(
    intent: MirroredDowngradeIntent,
) -> Result<CompletedDowngradeMarker> {
    commit_prepared(PreparedDowngradeMarker::from_intent(intent)?)
}
pub(super) fn commit_recovered(
    source: super::downgrade_resume::RecoveredDowngradeSource,
) -> Result<CompletedDowngradeMarker> {
    commit_prepared(Arc::new(PreparedDowngradeMarker {
        origin: MarkerOrigin::Recovered(Box::new(source)),
    }))
}
fn commit_prepared(proof: Arc<PreparedDowngradeMarker>) -> Result<CompletedDowngradeMarker> {
    let db = crate::Database::open_offline_downgrade(proof.clone())?;
    if db.get_mv_store().is_some()
        || db
            .shared_wal
            .read()
            .runtime
            .frame_cache
            .lock()
            .keys()
            .any(|page| *page != 1)
    {
        return Err(invalid(
            "downgrade marker opener contains unrelated history",
        ));
    }
    let connection = db.connect(crate::ConnectOpts::new())?;
    connection.execute("PRAGMA synchronous=FULL")?;
    let pager = connection.get_pager();
    let header = pager.io.block(|| pager.with_header(|header| *header))?;
    proof.validate_layout(&header, proof.main_file().size()?)?;
    let (read, write, pages) = (
        header.read_version.0,
        header.write_version.0,
        header.database_size.get(),
    );
    if !matches!((read, write), (254, 254) | (2, 2))
        || proof.physical_floor() > -i64::from(pages.max(1))
    {
        return Err(invalid(
            "downgrade marker differs from materialized physical layout",
        ));
    }
    let mut timestamp = None;
    connection
        .prepare("SELECT v FROM __turso_internal_mvcc_meta WHERE k='persistent_tx_ts_max'")?
        .run_with_row_callback(|row| {
            let value = row.get::<i64>(0)?;
            if value < 0 || timestamp.replace(value as u64).is_some() {
                return Err(invalid("invalid materialized downgrade timestamp"));
            }
            Ok(())
        })?;
    if timestamp != Some(proof.timestamp()) {
        return Err(invalid(
            "downgrade target differs from materialized timestamp",
        ));
    }
    proof.revalidate()?;
    proof.mirror()?;
    if read == 254 {
        connection.execute("BEGIN IMMEDIATE")?;
        pager.io.block(|| {
            pager.with_header_mut(|header| {
                header.read_version = crate::storage::sqlite3_ondisk::RawVersion(2);
                header.write_version = crate::storage::sqlite3_ondisk::RawVersion(2);
            })
        })?;
        connection.execute("COMMIT")?;
    }
    connection.checkpoint(crate::CheckpointMode::Truncate {
        upper_bound_inclusive: None,
    })?;
    let sync = proof
        .main_file()
        .sync(Completion::new_sync(|_| {}), FileSyncType::FullFsync)?;
    pager.io.wait_for_completion(sync)?;
    connection.close()?;
    drop(pager);
    drop(connection);
    drop(db);
    proof.revalidate()?;
    let file = std::fs::File::open(proof.path()).map_err(super::fs_error)?;
    super::require_marker(&file, 2)?;
    if proof.wal_file().size()? != 0 {
        return Err(invalid("downgrade marker checkpoint left a nonempty WAL"));
    }
    Ok(CompletedDowngradeMarker { _proof: proof })
}
fn invalid(message: &str) -> LimboError {
    LimboError::InvalidArgument(message.into())
}
