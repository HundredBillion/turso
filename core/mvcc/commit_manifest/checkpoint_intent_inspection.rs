use super::*;
use crate::mvcc::database::RecoveryWalEvidence;
use crate::mvcc::guarded_file::FileBinding;
use crate::{return_if_io, types::IOResultOr};

pub(in crate::mvcc) struct CheckpointIntentInspection {
    load: Option<ManifestLoad>,
    evidence: RecoveryWalEvidence,
    lease: crate::sync::Weak<PublicationLease>,
    expected: ManifestRecord,
    selected: bool,
    finished: bool,
    binding: Option<FileBinding>,
}
impl MirroredCheckpointIntent {
    pub(in crate::mvcc) fn inspect_exact_for_physical_recovery(
        &self,
        evidence: &RecoveryWalEvidence,
    ) -> Result<CheckpointIntentInspection> {
        let (file, binding) = evidence.inspection_file(&self.file, self.record, &self.guard)?;
        let load = ManifestLoad::start(file, None, self.record.database_id())?;
        Ok(CheckpointIntentInspection {
            load: Some(load),
            evidence: evidence.clone(),
            lease: self.guard.io_lease(),
            expected: self.record,
            selected: false,
            finished: false,
            binding: Some(binding),
        })
    }
}
impl CheckpointIntentToken {
    pub(in crate::mvcc) fn inspect_exact_for_source(
        &self,
        guard: &AuthorityGuard,
        evidence: &RecoveryWalEvidence,
    ) -> Result<CheckpointIntentInspection> {
        if !crate::sync::Weak::ptr_eq(&self.lease, &guard.io_lease()) {
            return Err(LimboError::InvalidArgument(
                "source intent changed its original lease".into(),
            ));
        }
        let (file, binding) = evidence.inspection_file(&self.file, self.record, guard)?;
        Ok(CheckpointIntentInspection {
            load: Some(ManifestLoad::start(file, None, self.record.database_id())?),
            evidence: evidence.clone(),
            lease: guard.io_lease(),
            expected: self.record,
            selected: false,
            finished: false,
            binding: Some(binding),
        })
    }
}

impl CheckpointIntentInspection {
    pub(in crate::mvcc) fn poll(&mut self, guard: &AuthorityGuard) -> IOResultOr<()> {
        let result = self.poll_inner(guard);
        if result.is_err() {
            self.abort();
        }
        result
    }
    fn poll_inner(&mut self, guard: &AuthorityGuard) -> IOResultOr<()> {
        if self.finished {
            return Err(LimboError::InvalidArgument("intent inspection has ended".into()).into());
        }
        self.evidence.validate_guard(guard)?;
        if !crate::sync::Weak::ptr_eq(&self.lease, &guard.io_lease()) {
            return Err(LimboError::InvalidArgument(
                "intent inspection resumed under another lease".into(),
            )
            .into());
        }
        if !self.selected {
            let selected = return_if_io!(self.load.as_mut().unwrap().poll());
            if selected.record() != self.expected {
                return Err(LimboError::InvalidArgument(
                    "physical recovery selected intent changed".into(),
                )
                .into());
            }
            self.load = None;
            self.selected = true;
        }
        if !guard.io_retired() {
            return Ok(IOResult::IO(IOCompletions(guard.retirement_completion())));
        }
        self.abort();
        Ok(IOResult::Done(()))
    }
    fn abort(&mut self) {
        self.load = None;
        self.binding = None;
        self.finished = true;
    }
}
impl Drop for CheckpointIntentInspection {
    fn drop(&mut self) {
        self.abort();
    }
}
