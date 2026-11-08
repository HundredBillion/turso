//! Explicit native shared-MVCC administration. Ordinary opens never change mode.
use std::path::Path;

/// The default retained-history budget for an explicitly initialized database.
pub const DEFAULT_HISTORY_LIMIT: u64 = 64 * 1024 * 1024;

/// An operator acknowledgement, not a fact inferred from a filesystem lock.
#[derive(Clone, Copy, Debug)]
pub enum OfflinePrerequisite {
    AllOlderWorkersStopped,
}

/// Select one operation explicitly; recovery never guesses a different action.
#[derive(Clone, Copy, Debug)]
pub enum OfflineOperation<'a> {
    UpgradeExisting {
        path: &'a Path,
    },
    PublishNew {
        staging: &'a Path,
        destination: &'a Path,
    },
    ResumePreparedMarker {
        path: &'a Path,
    },
    DowngradeExisting {
        path: &'a Path,
    },
    ResumeDowngradeMarker {
        path: &'a Path,
    },
    ResumeDowngradeCleanup {
        path: &'a Path,
    },
}

/// Complete an offline operation with native identity and durability checks.
///
/// All older workers must be stopped. Live participant quiescence is still
/// verified by the protocol and may return BUSY. No SQL is silently retried.
/// A failed transition can require the corresponding explicit resume operation.
pub fn execute_offline(
    operation: OfflineOperation<'_>,
    prerequisite: OfflinePrerequisite,
    history_limit: u64,
) -> crate::Result<()> {
    #[cfg(all(host_shared_wal, unix))]
    {
        use crate::mvcc::mode_transition::{
            operation as internal, OfflinePrerequisite as Required,
        };
        let OfflinePrerequisite::AllOlderWorkersStopped = prerequisite;
        let operation = match operation {
            OfflineOperation::UpgradeExisting { path } => {
                internal::OfflineOperation::UpgradeExisting { path }
            }
            OfflineOperation::PublishNew {
                staging,
                destination,
            } => internal::OfflineOperation::PublishNew {
                staging,
                destination,
            },
            OfflineOperation::ResumePreparedMarker { path } => {
                internal::OfflineOperation::ResumePreparedMarker { path }
            }
            OfflineOperation::DowngradeExisting { path } => {
                internal::OfflineOperation::DowngradeExisting { path }
            }
            OfflineOperation::ResumeDowngradeMarker { path } => {
                internal::OfflineOperation::ResumeDowngradeMarker { path }
            }
            OfflineOperation::ResumeDowngradeCleanup { path } => {
                internal::OfflineOperation::ResumeDowngradeCleanup { path }
            }
        };
        internal::execute_offline(operation, Required::AllOlderWorkersStopped, history_limit)
    }
    #[cfg(not(all(host_shared_wal, unix)))]
    {
        let _ = (operation, prerequisite, history_limit);
        Err(crate::LimboError::InvalidArgument(
            "native shared-MVCC offline operations are unsupported on this platform".into(),
        ))
    }
}
