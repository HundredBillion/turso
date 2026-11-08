use crate::app::Opts;
use anyhow::{bail, Result};
use clap::ValueEnum;
use turso_core::shared_mvcc::{
    execute_offline, OfflineOperation, OfflinePrerequisite, DEFAULT_HISTORY_LIMIT,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum Operation {
    UpgradeExisting,
    PublishNew,
    ResumePreparedMarker,
    DowngradeExisting,
    ResumeDowngradeMarker,
    ResumeDowngradeCleanup,
}

pub fn validate(opts: &Opts) -> Result<()> {
    if !opts.shared_mvcc && opts.shared_mvcc_offline.is_none() {
        return Ok(());
    }
    let path = opts
        .database
        .as_deref()
        .unwrap_or(std::path::Path::new(":memory:"));
    if path.as_os_str().is_empty()
        || path == std::path::Path::new(":memory:")
        || path.to_string_lossy().starts_with("file:")
    {
        bail!("shared MVCC requires an explicit native filesystem path, not memory or a URI");
    }
    if opts.vfs.is_some()
        || opts.mcp
        || opts.sync_server.is_some()
        || opts.sync_dir.is_some()
        || opts.experimental_encryption
        || opts.experimental_multiprocess_wal
        || opts.experimental_mvcc_passive_checkpoint
        || opts.unsafe_testing
    {
        bail!("shared MVCC cannot use custom VFS, servers, encryption, ordinary multiprocess WAL, passive checkpoint, or unsafe testing options");
    }
    #[cfg(feature = "mvcc_repl")]
    if opts.mvcc {
        bail!("shared MVCC cannot use the ordinary MVCC harness");
    }
    if let Some(operation) = opts.shared_mvcc_offline {
        if !opts.all_older_workers_stopped || opts.sql.is_some() || opts.readonly {
            bail!("offline operations require --all-older-workers-stopped and cannot execute SQL or open read-only");
        }
        if (operation == Operation::PublishNew) != opts.shared_mvcc_staging.is_some() {
            bail!("--shared-mvcc-staging is required only for publish-new");
        }
        if opts.experimental_views
            || opts.experimental_custom_types
            || opts.experimental_index_method
            || opts.experimental_autovacuum
            || opts.experimental_vacuum
            || opts.experimental_attach
            || opts.experimental_without_rowid
        {
            bail!("offline operations use native default options, not experimental shell options");
        }
    }
    Ok(())
}

pub fn run_offline(opts: &Opts) -> Result<bool> {
    let Some(operation) = opts.shared_mvcc_offline else {
        return Ok(false);
    };
    validate(opts)?;
    let path = opts
        .database
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("missing database path"))?;
    let operation = match operation {
        Operation::UpgradeExisting => OfflineOperation::UpgradeExisting { path },
        Operation::PublishNew => OfflineOperation::PublishNew {
            staging: opts.shared_mvcc_staging.as_deref().unwrap(),
            destination: path,
        },
        Operation::ResumePreparedMarker => OfflineOperation::ResumePreparedMarker { path },
        Operation::DowngradeExisting => OfflineOperation::DowngradeExisting { path },
        Operation::ResumeDowngradeMarker => OfflineOperation::ResumeDowngradeMarker { path },
        Operation::ResumeDowngradeCleanup => OfflineOperation::ResumeDowngradeCleanup { path },
    };
    execute_offline(
        operation,
        OfflinePrerequisite::AllOlderWorkersStopped,
        opts.shared_mvcc_history_limit
            .unwrap_or(DEFAULT_HISTORY_LIMIT),
    )?;
    Ok(true)
}

type Opened = (
    std::sync::Arc<dyn turso_core::IO>,
    std::sync::Arc<turso_core::Connection>,
);
pub fn open(path: &str, opts: &Opts, db_opts: turso_core::DatabaseOpts) -> Result<Opened> {
    #[cfg(unix)]
    {
        let io: std::sync::Arc<dyn turso_core::IO> =
            std::sync::Arc::new(turso_core::io::SyscallIO::new()?);
        let options =
            turso_core::OpenOptions::new(std::sync::Arc::new(turso_sqlite::SqliteDialect))
                .db_opts(db_opts)
                .flags(if opts.readonly {
                    turso_core::OpenFlags::ReadOnly
                } else {
                    turso_core::OpenFlags::None
                })
                .with_shared_mvcc(
                    opts.shared_mvcc_history_limit
                        .unwrap_or(DEFAULT_HISTORY_LIMIT),
                );
        let database = turso_core::Database::open(io.clone(), path, options)?;
        let connection = database.connect(turso_core::ConnectOpts::new())?;
        Ok((io, connection))
    }
    #[cfg(not(unix))]
    {
        let _ = (path, opts, db_opts);
        bail!("native shared MVCC is unsupported on this platform")
    }
}
