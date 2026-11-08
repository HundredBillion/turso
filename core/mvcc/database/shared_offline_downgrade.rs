use crate::mvcc::mode_transition::OfflinePrerequisite;
use crate::{LimboError, Result};
use std::path::Path;

pub(in crate::mvcc) fn downgrade_existing(
    _path: &Path,
    _history_limit: u64,
    _prerequisite: OfflinePrerequisite,
) -> Result<()> {
    Err(LimboError::InvalidArgument(
        "offline downgrade orchestration is not implemented".into(),
    ))
}
