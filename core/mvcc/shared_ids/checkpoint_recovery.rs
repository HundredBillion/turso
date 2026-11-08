use super::*;

#[allow(dead_code)]
pub(in crate::mvcc) struct UnrecoveredIds {
    loaded: LoadedIds,
}

#[allow(dead_code)]
impl UnrecoveredIds {
    pub(in crate::mvcc) fn guard(&self) -> &AuthorityGuard {
        self.loaded.guard()
    }

    pub(in crate::mvcc) fn identity(&self) -> RecoveryIdentity {
        self.loaded.identity()
    }

    pub(in crate::mvcc) fn logical_floor(&self) -> i64 {
        self.loaded.selected_logical_floor()
    }

    pub(in crate::mvcc) fn source_database(&self) -> Option<DatabaseGeneration> {
        self.loaded.pending_checkpoint_source()
    }
}
