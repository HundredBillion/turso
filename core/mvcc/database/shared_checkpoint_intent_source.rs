use super::*;

pub(in crate::mvcc) struct SourceConstructionState {
    source: Option<InspectedIntentSource>,
}

pub(in crate::mvcc) struct SourceStaging {
    pub(super) connection: Arc<Connection>,
    pub(super) pager: Arc<crate::Pager>,
    pub(super) database: Arc<crate::Database>,
    pub(super) source: InspectedIntentSource,
}

impl InspectedIntentSource {
    pub(in crate::mvcc) fn begin_private_source_construction(
        self,
    ) -> Result<SourceConstructionState> {
        self.owner.require_inspected_source()?;
        self.ids
            .validate_owner(&self.runtime, &self.config, self.owner.record)?;
        Ok(SourceConstructionState { source: Some(self) })
    }
}

impl SourceConstructionState {
    pub(in crate::mvcc) fn poll(&mut self) -> IOResultOr<SourceStaging> {
        self.source = None;
        Err(
            LimboError::InvalidArgument("private source construction is not implemented".into())
                .into(),
        )
    }
}
