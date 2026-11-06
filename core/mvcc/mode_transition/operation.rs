use super::ordinary::{resume_prepared_marker, OrdinaryOfflineBackend};
use super::{OfflineModeChange, OfflinePrerequisite, Result, MIN_HISTORY_LIMIT};
use std::path::Path;

pub(in crate::mvcc) enum OfflineOperation<'a> {
    UpgradeExisting {
        path: &'a Path,
    },
    ResumePreparedMarker {
        path: &'a Path,
    },
    PublishNew {
        staging: &'a Path,
        destination: &'a Path,
    },
}

pub(in crate::mvcc) fn execute_offline(
    operation: OfflineOperation<'_>,
    prerequisite: OfflinePrerequisite,
    history_limit: u64,
) -> Result<()> {
    if history_limit < MIN_HISTORY_LIMIT {
        return Err(crate::LimboError::InvalidArgument(
            "shared MVCC history limit must be at least 32 KiB".into(),
        ));
    }
    match operation {
        OfflineOperation::UpgradeExisting { path } => {
            let backend = OrdinaryOfflineBackend::open(path, prerequisite)?;
            OfflineModeChange::new(
                backend,
                OfflinePrerequisite::AllOlderWorkersStopped,
                history_limit,
            )
            .upgrade()
        }
        OfflineOperation::ResumePreparedMarker { path } => {
            resume_prepared_marker(path, history_limit, prerequisite)
        }
        OfflineOperation::PublishNew {
            staging,
            destination,
        } => {
            let backend = OrdinaryOfflineBackend::create_staged(staging)?;
            OfflineModeChange::new(backend, prerequisite, history_limit).publish_new(destination)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offline_operation_rejects_invalid_budget_before_any_creation() {
        let dir = tempfile::tempdir().unwrap();
        let staging = dir.path().join("staging.db");
        let destination = dir.path().join("published.db");
        let error = execute_offline(
            OfflineOperation::PublishNew {
                staging: &staging,
                destination: &destination,
            },
            OfflinePrerequisite::AllOlderWorkersStopped,
            super::super::MIN_HISTORY_LIMIT - 1,
        )
        .unwrap_err();
        assert!(error.to_string().contains("at least 32 KiB"), "{error}");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }
}
