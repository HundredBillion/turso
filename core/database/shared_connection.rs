use super::*;
use crate::mvcc::database::{SharedMvccRuntime, SharedOpenState};
use crate::storage::pager::SharedPagerGeneration;

pub(super) struct SharedConnectState {
    database: Weak<Database>,
    open: SharedOpenState,
    init: InitState,
    opts: ConnectOpts,
    context: Option<ConnectionContext>,
    pager: Option<Arc<Pager>>,
    connection: Option<Arc<Connection>>,
    captured_generation: Option<SharedPagerGeneration>,
}

impl SharedConnectState {
    pub(super) fn new(
        database: &Arc<Database>,
        opts: ConnectOpts,
        context: ConnectionContext,
    ) -> Result<Self> {
        let runtime = database
            .shared_mvcc_runtime
            .as_ref()
            .ok_or(LimboError::Busy)?;
        if database.page_codec_id.is_some() || opts.page_codec.is_some() {
            return Err(LimboError::InvalidArgument(
                "shared connections require unencoded pages".into(),
            ));
        }
        Ok(Self {
            database: Arc::downgrade(database),
            open: runtime.begin_read_pinned_open()?,
            init: InitState::default(),
            opts,
            context: Some(context),
            pager: None,
            connection: None,
            captured_generation: None,
        })
    }

    pub(super) fn poll(&mut self, database: &Arc<Database>) -> IOResultOr<Arc<Connection>> {
        if !self.database.ptr_eq(&Arc::downgrade(database)) {
            return Err(LimboError::InvalidArgument(
                "connection resumed on another database".into(),
            )
            .into());
        }
        return_if_io!(self.open.poll_manifest());
        if self.captured_generation.is_none() {
            self.captured_generation = Some(return_if_io!(self
                .open
                .capture_connection_generation(database)));
        }
        if self.pager.is_none() {
            self.pager = Some(Arc::new(return_if_io!(database._init_nonblock(
                &mut self.init,
                self.opts.encryption_key.as_ref(),
                self.opts.page_codec.as_ref(),
            ))));
        }
        if self.connection.is_none() {
            let pager = self.pager.as_ref().unwrap();
            let cache_size =
                return_if_io!(pager.with_header(|header| header.default_page_cache_size)).get();
            self.connection = Some(database.new_connection(
                false,
                pager.clone(),
                self.opts.encryption_key.clone(),
                cache_size,
                self.context.take().unwrap(),
                self.opts.readonly || database.is_readonly(),
            )?);
        }
        return_if_io!(self.open.finish_connection(
            database.get_mv_store().as_ref().ok_or(LimboError::Busy)?,
            self.connection.as_ref().unwrap(),
            self.captured_generation.unwrap(),
        ));
        Ok(IOResult::Done(self.connection.take().unwrap()))
    }
}

impl Drop for SharedConnectState {
    fn drop(&mut self) {
        self.init.cancel();
    }
}

impl Database {
    pub(crate) fn validate_shared_pager_owner(
        &self,
        pager: &Pager,
        runtime: &SharedMvccRuntime,
    ) -> Result<()> {
        if !pager.uses_shared_state(&self.pager_state)
            || !self
                .shared_mvcc_runtime
                .as_ref()
                .is_some_and(|owner| std::ptr::eq(owner.as_ref(), runtime))
        {
            return Err(LimboError::InvalidArgument(
                "pager belongs to another shared database".into(),
            ));
        }
        Ok(())
    }
}

impl Database {
    pub(crate) fn validate_shared_runtime(&self, runtime: &SharedMvccRuntime) -> Result<()> {
        if !self
            .shared_mvcc_runtime
            .as_ref()
            .is_some_and(|owner| std::ptr::eq(owner.as_ref(), runtime))
        {
            return Err(LimboError::InvalidArgument(
                "database belongs to another shared runtime".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn validate_shared_base_layout(
        &self,
        runtime: &SharedMvccRuntime,
        header: &DatabaseHeader,
    ) -> Result<()> {
        self.validate_shared_runtime(runtime)?;
        if self.pager_state.page_size() != Some(header.page_size)
            || !header
                .page_size
                .has_valid_reserved_space(header.reserved_space)
        {
            return Err(LimboError::Corrupt(
                "selected physical base has an incompatible local page layout".into(),
            ));
        }
        Ok(())
    }
}
