use super::*;
use crate::mvcc::commit_manifest::checkpoint_publication::CheckpointIntentToken;

pub(in crate::mvcc) struct LoadedSourceIds {
    loaded: LoadedIds,
    intent: CheckpointIntentToken,
}

impl LoadedSourceIds {
    pub(in crate::mvcc) fn guard(&self) -> &AuthorityGuard {
        self.loaded.guard()
    }

    pub(in crate::mvcc) fn identity(&self) -> RecoveryIdentity {
        self.loaded.identity()
    }

    pub(in crate::mvcc) fn logical_floor(&self) -> i64 {
        self.loaded.selected_logical_floor()
    }
}
