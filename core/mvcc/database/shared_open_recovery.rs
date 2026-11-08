use super::*;
use crate::mvcc::database::shared_runtime::shared_checkpoint_recovery::CheckpointRecoverySelection;
use crate::mvcc::database::shared_runtime::shared_checkpoint_recovery_physical::RecoveryPhysicalRead;
use crate::mvcc::shared_ids::io_protocol::open_selection::{InitialIdRoute, OpenSelectorProbe};

pub(crate) enum SharedInitialOpenResult {
    Healthy(Box<SharedOpenState>),
    Recovered(Arc<crate::Database>),
}

pub(crate) struct SharedInitialOpen {
    phase: Phase,
    probe: Option<Box<OpenSelectorProbe>>,
    load: Option<Box<ManifestLoad>>,
    selected: Option<LoadedManifest>,
    open: Option<Box<SharedOpenState>>,
    pause: Option<CheckpointPause>,
    runtime: Arc<SharedMvccRuntime>,
    options: crate::OpenOptions,
    restarts: u8,
}

enum Phase {
    Manifest,
    Probe,
    Pause,
    Physical(Box<RecoveryPhysicalRead>),
    Ids(Box<RecoveryIdInspection>),
    Confirm(Box<RecoveryCheckpointConfirmationState>),
    Finalize(Box<RecoveryIdFinalizationState>),
    Reclaim(Box<RecoveryLogReclamation>),
    Bootstrap(Box<RecoveryBootstrap>),
    Done,
}

impl SharedMvccRuntime {
    pub(crate) fn begin_initial_open(
        self: &Arc<Self>,
        options: crate::OpenOptions,
    ) -> Result<Box<SharedInitialOpen>> {
        if !matches!(
            *self.store_claim.lock(),
            shared_store_claim::StoreClaimState::Free
        ) {
            return Err(LimboError::InvalidArgument(
                "initial open requires an unclaimed runtime".into(),
            ));
        }
        let mut open = Box::new(self.begin_read_pinned_open()?);
        let load = Box::new(open.load.take().unwrap());
        Ok(Box::new(SharedInitialOpen {
            phase: Phase::Manifest,
            probe: None,
            load: Some(load),
            selected: None,
            open: Some(open),
            pause: None,
            runtime: self.clone(),
            options,
            restarts: 0,
        }))
    }
}

impl SharedInitialOpen {
    pub(crate) fn poll(&mut self) -> IOResultOr<SharedInitialOpenResult> {
        let result = self.poll_inner();
        if result.is_err() {
            self.abort();
        }
        result
    }

    fn poll_inner(&mut self) -> IOResultOr<SharedInitialOpenResult> {
        loop {
            match &mut self.phase {
                Phase::Manifest => {
                    if self.selected.is_none() {
                        self.selected = Some(return_if_io!(self.load.as_mut().unwrap().poll()));
                        self.load = None;
                    }
                    let guard = self.selected.as_ref().unwrap().guard()?;
                    if !guard.io_retired() {
                        return Ok(IOResult::IO(crate::types::IOCompletions(
                            guard.retirement_completion(),
                        )));
                    }
                    let manifest = self.selected.take().unwrap();
                    if manifest.record().downgrade_intent().is_some() {
                        return Err(LimboError::InvalidArgument(
                            "shared open requires explicit downgrade recovery".into(),
                        )
                        .into());
                    }
                    if manifest.record().requires_recovery() {
                        return Err(LimboError::InvalidArgument(
                            "shared manifest recovery is required".into(),
                        )
                        .into());
                    }
                    self.probe = Some(Box::new(self.runtime.ids.inspect_open_selector(manifest)?));
                    self.phase = Phase::Probe;
                }
                Phase::Probe => {
                    let inspected = return_if_io!(self.probe.as_mut().unwrap().poll());
                    self.probe = None;
                    let (manifest, route) = inspected.into_parts();
                    match route {
                        InitialIdRoute::Healthy if self.pause.is_none() => {
                            let mut open = self.open.take().unwrap();
                            open.accept_selected_manifest(manifest)?;
                            self.phase = Phase::Done;
                            return Ok(IOResult::Done(SharedInitialOpenResult::Healthy(open)));
                        }
                        InitialIdRoute::Healthy => {
                            drop(manifest);
                            self.pause = None;
                            if self.restarts != 0 {
                                return Err(LimboError::Busy.into());
                            }
                            self.restarts += 1;
                            let mut open = Box::new(self.runtime.begin_read_pinned_open()?);
                            self.load = Some(Box::new(open.load.take().unwrap()));
                            self.open = Some(open);
                            self.phase = Phase::Manifest;
                        }
                        InitialIdRoute::CompletedCheckpoint => {
                            if self.runtime.original_readonly {
                                return Err(LimboError::InvalidArgument(
                                    "shared checkpoint recovery requires a writable participant"
                                        .into(),
                                )
                                .into());
                            }
                            if let Some(pause) = self.pause.take() {
                                let selection = CheckpointRecoverySelection::fresh(
                                    self.runtime.clone(),
                                    manifest,
                                    pause,
                                )?;
                                self.phase = Phase::Physical(Box::new(
                                    selection.begin_physical_read(self.options.clone())?,
                                ));
                            } else {
                                drop(manifest);
                                self.open = None;
                                self.phase = Phase::Pause;
                            }
                        }
                    }
                }
                Phase::Pause => {
                    let pause = self.runtime.gate.try_pause_admission()?;
                    let guard = self.runtime.files.authority.try_acquire()?;
                    let load = ManifestLoad::new(
                        self.runtime.files.manifest.clone(),
                        guard,
                        self.runtime.database_id,
                    )?;
                    self.pause = Some(pause);
                    self.load = Some(Box::new(load));
                    self.phase = Phase::Manifest;
                }
                Phase::Physical(reading) => {
                    let physical = return_if_io!(reading.poll());
                    self.phase = Phase::Ids(Box::new(physical.begin_id_inspection()?));
                }
                Phase::Ids(inspecting) => {
                    let inspected = return_if_io!(inspecting.poll());
                    self.phase = Phase::Confirm(Box::new(inspected.begin_confirmation()?));
                }
                Phase::Confirm(confirming) => {
                    let confirmed = return_if_io!(confirming.poll());
                    self.phase = Phase::Finalize(Box::new(confirmed.begin_id_finalization()?));
                }
                Phase::Finalize(finalizing) => {
                    let finalized = return_if_io!(finalizing.poll());
                    self.phase = Phase::Reclaim(Box::new(finalized.begin_log_reclamation()?));
                }
                Phase::Reclaim(reclaiming) => {
                    let reclaimed = return_if_io!(reclaiming.poll());
                    self.phase = Phase::Bootstrap(Box::new(reclaimed.begin_bootstrap()?));
                }
                Phase::Bootstrap(bootstrap) => {
                    let ready = return_if_io!(bootstrap.poll());
                    let (database, connection) = ready.into_parts();
                    drop(connection);
                    self.phase = Phase::Done;
                    return Ok(IOResult::Done(SharedInitialOpenResult::Recovered(database)));
                }
                Phase::Done => {
                    return Err(
                        LimboError::InvalidArgument("initial shared open has ended".into()).into(),
                    )
                }
            }
        }
    }

    fn abort(&mut self) {
        self.phase = Phase::Done;
        self.probe = None;
        self.load = None;
        self.selected = None;
        self.open = None;
        self.pause = None;
    }
}
impl Drop for SharedInitialOpen {
    fn drop(&mut self) {
        self.abort();
    }
}
