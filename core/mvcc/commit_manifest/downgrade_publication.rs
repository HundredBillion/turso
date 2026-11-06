use super::*;

/// A component publication, not the coordinator's materialization/quiescence proof.
/// No production constructor exists until that owned proof can authorize this step.
pub(in crate::mvcc) struct DowngradeIntentPublication;

pub(in crate::mvcc) struct MirroredDowngradeIntent {
    record: ManifestRecord,
    guard: AuthorityGuard,
}

impl DowngradeIntentPublication {
    #[cfg(test)]
    pub(super) fn synthetic(_loaded: LoadedManifest, _wal_id: FileId) -> Result<Self> {
        Err(LimboError::InvalidArgument(
            "downgrade publication is not implemented".into(),
        ))
    }

    pub(in crate::mvcc) fn poll(&mut self) -> Result<IOResult<MirroredDowngradeIntent>> {
        Err(LimboError::InvalidArgument(
            "downgrade publication is not implemented".into(),
        ))
    }
}

impl MirroredDowngradeIntent {
    pub(in crate::mvcc) fn record(&self) -> ManifestRecord {
        self.record
    }
    pub(in crate::mvcc) fn guard(&self) -> &AuthorityGuard {
        &self.guard
    }
}
