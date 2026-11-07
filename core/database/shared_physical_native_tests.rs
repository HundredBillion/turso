use super::*;
use crate::database::shared_physical::SharedPhysicalConnection;
use crate::mvcc::read_pinned_file::ReadPinnedFile;
use crate::storage::database::DatabaseFile;

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_physical_connection_native_reads_completed_marker_without_source_mutation() {
    let fixture =
        shared_completed_checkpoint_fixture(|_| Arc::new(crate::io::PlatformIO::new().unwrap()));
    let store = fixture.database.get_mv_store().as_ref().unwrap().clone();
    let runtime = store.shared_runtime.get().unwrap();
    assert_eq!(runtime.files.physical_wal.size().unwrap(), 0);
    let before = std::fs::read(&fixture.path).unwrap();
    assert_eq!(&before[18..20], &[254, 254]);
    let schema = fixture.database.schema.lock().clone();
    let connection_schema = fixture.connection.schema.read().clone();
    let root = store.get_table_id_from_root_page(1);
    store.set_sequence_watermark("physical_reader_sentinel", 9876);
    let file = ReadPinnedFile::new(runtime.files.main.clone(), &fixture.guard).unwrap();
    let options = crate::OpenOptions::new(fixture.database.dialect().clone())
        .storage(Arc::new(DatabaseFile::new(file)));
    let mut opening = SharedPhysicalConnection::new(
        fixture.io.clone(),
        fixture.path.to_str().unwrap().into(),
        options,
    )
    .unwrap();
    let reader = fixture.io.block(|| opening.poll()).unwrap();
    let mut rows = Vec::new();
    reader
        .prepare("SELECT value FROM preserved WHERE id=1")
        .unwrap()
        .run_with_row_callback(|row| {
            rows.push(row.get::<&str>(0)?.to_owned());
            Ok(())
        })
        .unwrap();
    assert_eq!(rows, ["materialized shared row"]);
    let mut timestamps = Vec::new();
    reader
        .prepare("SELECT v FROM __turso_internal_mvcc_meta WHERE k='persistent_tx_ts_max'")
        .unwrap()
        .run_with_row_callback(|row| {
            timestamps.push(row.get::<i64>(0)?);
            Ok(())
        })
        .unwrap();
    assert_eq!(
        timestamps,
        [fixture
            .proof
            .target_identity()
            .database
            .checkpoint_timestamp as i64]
    );
    assert!(reader.db.get_mv_store().is_none());
    assert!(!Arc::ptr_eq(
        &reader.get_pager(),
        &fixture.connection.get_pager()
    ));
    assert!(Arc::ptr_eq(&fixture.database.schema.lock(), &schema));
    assert!(Arc::ptr_eq(
        &fixture.connection.schema.read(),
        &connection_schema
    ));
    assert_eq!(store.get_table_id_from_root_page(1), root);
    assert_eq!(
        store.sequence_watermark("physical_reader_sentinel"),
        Some(9876)
    );
    drop(reader);
    drop(opening);
    assert!(fixture.guard.io_retired());
    assert_eq!(std::fs::read(&fixture.path).unwrap(), before);
}
