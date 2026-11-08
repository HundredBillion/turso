use super::*;

impl ReadPersistentTxTsMaxState {
    pub(super) fn poll(&mut self, connection: &Arc<Connection>) -> IOResultOr<Option<u64>> {
        loop {
            match self {
                ReadPersistentTxTsMaxState::Start => {
                    let query_result = connection.query(format!(
                        "SELECT v FROM {MVCC_META_TABLE_NAME}
                         WHERE k = '{MVCC_META_KEY_PERSISTENT_TX_TS_MAX}'"
                    ));
                    let maybe_stmt = match query_result {
                        Ok(stmt) => stmt,
                        Err(LimboError::ParseError(msg)) if msg.contains("no such table") => {
                            return Ok(IOResult::Done(None));
                        }
                        Err(err) => {
                            return Err(LimboError::Corrupt(format!(
                                "Failed to read MVCC metadata table: {err}"
                            ))
                            .into());
                        }
                    };
                    match maybe_stmt {
                        Some(stmt) => {
                            *self = ReadPersistentTxTsMaxState::Running {
                                stmt: Box::new(stmt),
                                value: None,
                            };
                        }
                        // No statement to run — fall through to the missing-row
                        // error path (matches the blocking variant).
                        None => {
                            return Self::validate(None).map(IOResult::Done).map_err(Into::into);
                        }
                    }
                }
                ReadPersistentTxTsMaxState::Running { stmt, value } => {
                    return_if_io!(stmt.run_with_row_callback_nonblock(|row| {
                        *value = Some(row.get::<i64>(0)?);
                        Ok(())
                    }));
                    let value = *value;
                    *self = ReadPersistentTxTsMaxState::Start;
                    return Self::validate(value)
                        .map(IOResult::Done)
                        .map_err(Into::into);
                }
            }
        }
    }

    pub(super) fn validate(value: Option<i64>) -> Result<Option<u64>> {
        let value = value.ok_or_else(|| {
            LimboError::Corrupt(format!(
                "Missing MVCC metadata row for key {MVCC_META_KEY_PERSISTENT_TX_TS_MAX}"
            ))
        })?;

        if value < 0 {
            return Err(LimboError::Corrupt(format!(
                "Invalid MVCC metadata value for {MVCC_META_KEY_PERSISTENT_TX_TS_MAX}: {value}"
            )));
        }
        Ok(Some(value as u64))
    }
}
