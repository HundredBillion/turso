use super::*;
use crate::database::shared_recovered::RecoveryOpenConfig;
use crate::mvcc::commit_manifest::checkpoint_publication::{
    CheckpointIntentToken, MirroredCheckpointIntent,
};
use crate::mvcc::commit_manifest::ManifestRecord;
use crate::mvcc::database::IntentSourceIdEvidence;

pub(in crate::mvcc) struct IntentSourceIdLoad {
    load: Option<IdLoad>,
    intent: Option<CheckpointIntentToken>,
    record: ManifestRecord,
    evidence: Arc<IntentSourceIdEvidence>,
}

pub(in crate::mvcc) struct LoadedSourceIds {
    loaded: LoadedIds,
    intent: CheckpointIntentToken,
    record: ManifestRecord,
    evidence: Arc<IntentSourceIdEvidence>,
    logical_floor: i64,
    max_owner_epoch: Option<u64>,
}

impl SharedIds {
    pub(in crate::mvcc) fn inspect_intent_source(
        self: &Arc<Self>,
        intent: MirroredCheckpointIntent,
        evidence: Arc<IntentSourceIdEvidence>,
    ) -> Result<IntentSourceIdLoad> {
        evidence.validate(self, intent.record(), intent.guard())?;
        let record = intent.record();
        let (intent, guard) = intent.suspend();
        let load = self.start_load(guard, IdLoadTarget::Known(evidence.identity()))?;
        Ok(IntentSourceIdLoad {
            load: Some(load),
            intent: Some(intent),
            record,
            evidence,
        })
    }
}

impl IntentSourceIdLoad {
    pub(in crate::mvcc) fn poll(&mut self) -> Result<IOResult<LoadedSourceIds>> {
        let result = self.poll_inner();
        if result.is_err() {
            self.load = None;
            self.intent = None;
        }
        result
    }

    fn poll_inner(&mut self) -> Result<IOResult<LoadedSourceIds>> {
        let load = self
            .load
            .as_mut()
            .ok_or_else(|| invalid("source ID inspection has ended"))?;
        let loaded = match load.poll()? {
            IOResult::IO(waiter) => return Ok(IOResult::IO(waiter)),
            IOResult::Done(loaded) => loaded,
        };
        self.load = None;
        self.evidence
            .validate(&loaded.files, self.record, loaded.guard())?;
        if loaded.identity() != self.evidence.identity()
            || loaded.selection.selected != loaded.selection.selector.active
            || loaded.selection.selector.active.database != self.evidence.identity().database
            || loaded
                .selection
                .selector
                .prepared
                .is_some_and(|prepared| prepared.database != self.evidence.target())
        {
            return Err(corrupt(
                "source ID selectors differ from the actual checkpoint intent",
            ));
        }
        let mut logical_floor = loaded.selected_logical_floor();
        if let Some(prepared) = loaded.selection.selector.prepared {
            let prepared_state = replay_selected(
                SelectedIds {
                    selected: prepared,
                    ..loaded.selection
                },
                [&loaded.banks[0], &loaded.banks[1]],
            )?;
            if prepared_state.logical_floor > logical_floor {
                return Err(corrupt(
                    "prepared source IDs lost the retained logical floor",
                ));
            }
            logical_floor = logical_floor.min(prepared_state.logical_floor);
        }
        if logical_floor >= 0
            || logical_floor == i64::MIN
            || logical_floor > self.evidence.physical_floor()
        {
            return Err(corrupt(
                "source IDs do not cover the retained physical extent",
            ));
        }
        let max_owner_epoch = loaded.max_owner_epoch()?;
        Ok(IOResult::Done(LoadedSourceIds {
            loaded,
            intent: self.intent.take().unwrap(),
            record: self.record,
            evidence: self.evidence.clone(),
            logical_floor,
            max_owner_epoch,
        }))
    }
}

impl LoadedSourceIds {
    pub(in crate::mvcc) fn inspect_source_manifest(
        &self,
        evidence: &crate::mvcc::database::RecoveryWalEvidence,
    ) -> Result<crate::mvcc::commit_manifest::checkpoint_publication::CheckpointIntentInspection>
    {
        self.intent.inspect_exact_for_source(self.guard(), evidence)
    }

    pub(in crate::mvcc) fn guard(&self) -> &AuthorityGuard {
        self.loaded.guard()
    }
    pub(in crate::mvcc) fn identity(&self) -> RecoveryIdentity {
        self.loaded.identity()
    }
    pub(in crate::mvcc) fn logical_floor(&self) -> i64 {
        self.logical_floor
    }
    pub(in crate::mvcc) fn max_owner_epoch(&self) -> Option<u64> {
        self.max_owner_epoch
    }
    pub(in crate::mvcc) fn validate_owner(
        &self,
        runtime: &Arc<crate::mvcc::database::SharedMvccRuntime>,
        config: &Arc<RecoveryOpenConfig>,
        record: ManifestRecord,
    ) -> Result<()> {
        if record != self.record {
            return Err(invalid("source ID record changed"));
        }
        self.evidence
            .validate_owner(runtime, config, record, self.guard())
    }
}
