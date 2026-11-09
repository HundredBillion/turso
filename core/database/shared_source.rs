use super::*;
use crate::mvcc::database::{SourceConstructionEvidence, SourceConstructionPermit};

pub(crate) struct SourceDatabaseFactory {
    connection: Option<Arc<Connection>>,
    pager: Option<Arc<Pager>>,
    database: Option<Arc<Database>>,
    building: Option<Database>,
    validation: HeaderValidationState,
    permit: Arc<SourceConstructionPermit>,
    evidence: Arc<SourceConstructionEvidence>,
}

pub(crate) type SourceFactoryParts = (
    Arc<Database>,
    Arc<Connection>,
    Arc<Pager>,
    Arc<SourceConstructionEvidence>,
    crate::mvcc::database::SourceAliases,
);

impl SourceDatabaseFactory {
    pub(crate) fn new(evidence: Arc<SourceConstructionEvidence>) -> Result<Self> {
        evidence.validate()?;
        let config = evidence.config();
        let runtime = evidence.runtime();
        let io = evidence.io();
        let mut database = Database::new(
            config.options.db_opts,
            OpenFlags::NoLock,
            evidence.path()?,
            runtime.wal_path().to_str().ok_or_else(|| {
                LimboError::InvalidArgument("source WAL path must be UTF-8".into())
            })?,
            &io,
            Arc::new(DatabaseFile::new(evidence.main_file())),
            None,
            config.options.allocators.clone(),
            None,
            config.options.dialect.clone(),
            &config.options.native_extensions,
        )?;
        {
            let shared = Arc::get_mut(&mut database.shared).expect("new source Database is unique");
            shared.shared_mvcc_runtime = Some(runtime.clone());
            shared.durable_storage = Some(runtime.storage());
            shared.mv_store.store(Some(evidence.retained_store()?));
        }
        let permit = evidence.make_permit(&database)?;
        Arc::get_mut(&mut database.shared)
            .expect("new source Database is unique")
            .source_construction = Some(permit.clone());
        Ok(Self {
            connection: None,
            pager: None,
            database: None,
            building: Some(database),
            validation: Default::default(),
            permit,
            evidence,
        })
    }

    pub(crate) fn poll(&mut self) -> IOResultOr<()> {
        self.evidence.validate()?;
        if self.database.is_none() {
            let pager = return_if_io!(self.building.as_mut().unwrap().header_validation(
                &mut self.validation,
                None,
                None
            ));
            self.pager = Some(pager);
            self.database = Some(Arc::new(self.building.take().unwrap()));
        }
        let database = self.database.as_ref().unwrap();
        self.permit.validate_database(database)?;
        let pager = self.pager.as_ref().unwrap();
        let header = return_if_io!(pager.with_header(|header| *header));
        self.evidence.validate_header(&header)?;
        if self.connection.is_none() {
            self.connection = Some(database.new_connection(
                false,
                pager.clone(),
                None,
                header.default_page_cache_size.get(),
                ConnectionContext::default(),
                false,
            )?);
        }
        Ok(IOResult::Done(()))
    }

    pub(crate) fn finish(mut self) -> Result<SourceFactoryParts> {
        let database = self.database.take().ok_or(LimboError::Busy)?;
        let connection = self.connection.take().ok_or(LimboError::Busy)?;
        let pager = self.pager.take().ok_or(LimboError::Busy)?;
        self.permit.validate_database(&database)?;
        let aliases = self
            .evidence
            .capture_aliases(&database, &connection, &pager)?;
        Ok((database, connection, pager, self.evidence.clone(), aliases))
    }

    fn abort(&mut self) {
        if let HeaderValidationState::Start { init } = &mut self.validation {
            init.cancel();
        }
        self.connection = None;
        self.validation = Default::default();
        self.pager = None;
        self.database = None;
        self.building = None;
    }
}
impl Drop for SourceDatabaseFactory {
    fn drop(&mut self) {
        self.abort();
    }
}
