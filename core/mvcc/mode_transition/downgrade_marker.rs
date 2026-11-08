use crate::mvcc::commit_manifest::downgrade_publication::MirroredDowngradeIntent;
use crate::{LimboError, Result};

pub(in crate::mvcc) struct CompletedDowngradeMarker;

pub(in crate::mvcc) fn commit_marker(
    _intent: MirroredDowngradeIntent,
) -> Result<CompletedDowngradeMarker> {
    Err(LimboError::InvalidArgument(
        "genuine offline downgrade marker transaction is not implemented".into(),
    ))
}
