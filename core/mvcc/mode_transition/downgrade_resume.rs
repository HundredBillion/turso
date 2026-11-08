use super::{downgrade_marker::CompletedDowngradeMarker, OfflinePrerequisite};
use crate::{LimboError, Result};
use std::path::Path;

pub(in crate::mvcc) fn resume_marker(
    _path: &Path,
    _history_limit: u64,
    _: OfflinePrerequisite,
) -> Result<CompletedDowngradeMarker> {
    Err(LimboError::InvalidArgument(
        "terminal downgrade recovery is not implemented".into(),
    ))
}
