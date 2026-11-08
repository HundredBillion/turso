use super::*;
use crate::mvcc::database::SharedOpenState;

pub(super) struct SharedConnectState {
    database: Weak<Database>,
    open: SharedOpenState,
    init: InitState,
    opts: ConnectOpts,
    context: Option<ConnectionContext>,
    pager: Option<Arc<Pager>>,
    connection: Option<Arc<Connection>>,
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
        if !opts.readonly.unwrap_or(database.readonly_bootstrap) {
            database.ensure_writable_open()?;
        }
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
        self.open.validate_connection_generation()?;
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
                self.opts.readonly.unwrap_or(database.readonly_bootstrap),
            )?);
        }
        return_if_io!(self.open.finish_connection());
        Ok(IOResult::Done(self.connection.take().unwrap()))
    }
}

impl Drop for SharedConnectState {
    fn drop(&mut self) {
        self.init.cancel();
    }
}
