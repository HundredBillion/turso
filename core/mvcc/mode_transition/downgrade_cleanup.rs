use super::OfflinePrerequisite;
use crate::{LimboError, Result};
use std::path::Path;

pub(in crate::mvcc) fn resume_cleanup(_path: &Path, _: OfflinePrerequisite) -> Result<()> {
    Err(LimboError::InvalidArgument(
        "partial downgrade cleanup is not implemented".into(),
    ))
}
