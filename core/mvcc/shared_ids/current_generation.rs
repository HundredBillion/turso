use super::*;
use crate::mvcc::commit_manifest::{LoadedManifest, ManifestRecord};

pub(in crate::mvcc) struct CurrentGenerationIdLoad {
    load: IdLoad,
    record: ManifestRecord,
}

pub(in crate::mvcc) struct InspectedCurrentIds {
    loaded: LoadedIds,
    record: ManifestRecord,
}

pub(in crate::mvcc) struct CurrentIdBase {
    record: ManifestRecord,
    identity: RecoveryIdentity,
    logical_floor: i64,
    lease: crate::sync::Weak<PublicationLease>,
}

impl SharedIds {
    pub(in crate::mvcc) fn inspect_current_base(
        self: &Arc<Self>,
        manifest: LoadedManifest,
    ) -> Result<CurrentGenerationIdLoad> {
        let record = manifest.record();
        if record.database_id() != self.database_id || record.requires_recovery() {
            return Err(invalid(
                "current ID inspection requires its normal selected manifest",
            ));
        }
        let guard = manifest.into_guard()?;
        if !guard.requires_recovery() && guard.published()? != record.prefix() {
            return Err(corrupt(
                "current ID selection differs from publication authority",
            ));
        }
        let load = self.start_load(
            guard,
            IdLoadTarget::Current {
                generation: record.log_generation(),
                prefix: record.prefix(),
            },
        )?;
        Ok(CurrentGenerationIdLoad { load, record })
    }
}

impl CurrentGenerationIdLoad {
    pub(in crate::mvcc) fn poll(&mut self) -> Result<IOResult<InspectedCurrentIds>> {
        match self.load.poll()? {
            IOResult::IO(waiter) => Ok(IOResult::IO(waiter)),
            IOResult::Done(loaded) => Ok(IOResult::Done(InspectedCurrentIds {
                loaded,
                record: self.record,
            })),
        }
    }
}

impl InspectedCurrentIds {
    pub(in crate::mvcc) fn suspend(self) -> (CurrentIdBase, AuthorityGuard) {
        let base = CurrentIdBase {
            record: self.record,
            identity: self.loaded.identity(),
            logical_floor: self.loaded.selected_logical_floor(),
            lease: self.loaded.guard.io_lease(),
        };
        (base, self.loaded.into_guard())
    }
}

impl CurrentIdBase {
    pub(in crate::mvcc) fn identity(&self) -> RecoveryIdentity {
        self.identity
    }
    pub(in crate::mvcc) fn logical_floor(&self) -> i64 {
        self.logical_floor
    }

    pub(in crate::mvcc) fn validate_selection(&self, manifest: &LoadedManifest) -> Result<()> {
        if self.record != manifest.record()
            || !crate::sync::Weak::ptr_eq(&self.lease, &manifest.guard()?.io_lease())
        {
            return Err(invalid(
                "current ID base lost its original selected manifest lease",
            ));
        }
        Ok(())
    }
}
