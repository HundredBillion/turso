use super::ordinary::{open_native, resume_prepared_marker, OrdinaryOfflineBackend};
use super::*;
use crate::sync::Arc;
use std::collections::BTreeMap;

const LIMIT: u64 = 64 * 1024 * 1024;

struct Prepared {
    _directory: tempfile::TempDir,
    path: PathBuf,
    namespace: PathBuf,
    timestamp: u64,
}

fn prepare(timestamp_delta: u64, floor_override: Option<i64>) -> Prepared {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("recover.db");
    {
        let io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        let db =
            crate::Database::open_file(io, path.to_str().unwrap(), Arc::new(crate::EngineDialect))
                .unwrap();
        let conn = db.connect(crate::ConnectOpts::new()).unwrap();
        conn.execute("PRAGMA journal_mode=mvcc; CREATE TABLE kept(id INTEGER PRIMARY KEY, value TEXT); CREATE INDEX kept_value ON kept(value); INSERT INTO kept VALUES(1,'durable')").unwrap();
        conn.close().unwrap();
    }
    let mut backend =
        OrdinaryOfflineBackend::open(&path, OfflinePrerequisite::AllOlderWorkersStopped).unwrap();
    let timestamp = backend.recover_materialize_sync().unwrap();
    let floor = backend.materialized_logical_floor().unwrap();
    let id = identity(&open_native(&path).unwrap()).unwrap();
    let namespace = namespace_for(&path, id).unwrap();
    prepare_namespace(
        &namespace,
        id,
        timestamp + timestamp_delta,
        floor_override.unwrap_or(floor),
        LIMIT,
    )
    .unwrap();
    drop(backend);
    assert_eq!(read_marker(&open_native(&path).unwrap()).unwrap(), 255);
    Prepared {
        _directory: directory,
        path,
        namespace,
        timestamp,
    }
}

fn durable_bytes(prepared: &Prepared) -> BTreeMap<PathBuf, Vec<u8>> {
    let mut paths = vec![
        prepared.path.clone(),
        PathBuf::from(format!("{}-wal", prepared.path.display())),
        prepared.path.with_extension("db-log"),
    ];
    paths.extend(
        [
            "manifest",
            "logical",
            "physical-wal",
            "ids-a",
            "ids-b",
            "ids-selector",
        ]
        .map(|name| prepared.namespace.join(name)),
    );
    paths
        .into_iter()
        .filter_map(|path| match fs::read(&path) {
            Ok(bytes) => Some((path, bytes)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => panic!("{}: {error}", path.display()),
        })
        .collect()
}

#[test]
#[cfg_attr(
    not(target_os = "linux"),
    ignore = "requires native Linux ext-family filesystem"
)]
fn marker_recovery_integration_prepared_255_completes_without_reinitializing() {
    let prepared = prepare(0, None);
    let before = durable_bytes(&prepared);
    resume_prepared_marker(
        &prepared.path,
        LIMIT,
        OfflinePrerequisite::AllOlderWorkersStopped,
    )
    .unwrap();
    let after = durable_bytes(&prepared);
    for (path, bytes) in &before {
        if path.starts_with(&prepared.namespace) {
            assert_eq!(
                after.get(path),
                Some(bytes),
                "namespace file changed: {}",
                path.display()
            );
        }
    }
    assert_eq!(
        read_marker(&open_native(&prepared.path).unwrap()).unwrap(),
        SHARED_MVCC_VERSION
    );
    assert_eq!(
        validate_shared_open(&prepared.path)
            .unwrap()
            .committed_timestamp,
        prepared.timestamp
    );
    let io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
    let db = crate::Database::open(
        io,
        prepared.path.to_str().unwrap(),
        crate::OpenOptions::new(Arc::new(crate::EngineDialect)).with_shared_mvcc_for_test(LIMIT),
    )
    .unwrap();
    let conn = db.connect(crate::ConnectOpts::new()).unwrap();
    let mut rows = Vec::new();
    conn.prepare("SELECT value FROM kept INDEXED BY kept_value WHERE value='durable'")
        .unwrap()
        .run_with_row_callback(|row| {
            rows.push(row.get::<String>(0)?);
            Ok(())
        })
        .unwrap();
    assert_eq!(rows, ["durable"]);
}

#[test]
#[cfg_attr(
    not(target_os = "linux"),
    ignore = "requires native Linux ext-family filesystem"
)]
fn marker_recovery_integration_rejects_valid_but_high_id_floor_unchanged() {
    let prepared = prepare(0, Some(-1));
    let before = durable_bytes(&prepared);
    let error = resume_prepared_marker(
        &prepared.path,
        LIMIT,
        OfflinePrerequisite::AllOlderWorkersStopped,
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("ID floor does not cover materialized roots"),
        "{error}"
    );
    assert_eq!(durable_bytes(&prepared), before);
    assert_eq!(
        read_marker(&open_native(&prepared.path).unwrap()).unwrap(),
        255
    );
}

#[test]
#[cfg_attr(
    not(target_os = "linux"),
    ignore = "requires native Linux ext-family filesystem"
)]
fn marker_recovery_integration_rejects_valid_but_wrong_timestamp_unchanged() {
    let prepared = prepare(1, None);
    let before = durable_bytes(&prepared);
    let error = resume_prepared_marker(
        &prepared.path,
        LIMIT,
        OfflinePrerequisite::AllOlderWorkersStopped,
    )
    .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("namespace differs from materialized database"),
        "{error}"
    );
    assert_eq!(durable_bytes(&prepared), before);
    assert_eq!(
        read_marker(&open_native(&prepared.path).unwrap()).unwrap(),
        255
    );
}
