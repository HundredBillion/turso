use super::super::shared_checkpoint_intent_fixture::{CrashPoint, IntentCrashFixture};
use super::*;
use crate::mvcc::read_pinned_file::ReadPinnedFile;
use crate::storage::sqlite3_ondisk::{BuildSharedWal, WAL_MAGIC_BE, WAL_MAGIC_LE};

fn committed_image(fixture: &IntentCrashFixture, guard: &AuthorityGuard) -> Vec<u8> {
    let file: Arc<dyn File> = ReadPinnedFile::new_for_checkpoint_recovery(
        fixture.runtime.files.physical_wal.clone(),
        guard,
    )
    .unwrap();
    let mut scan = BuildSharedWal::begin(&file).unwrap();
    let scanned = fixture.io.block(|| scan.poll()).unwrap();
    let wal = scanned.read();
    let header = *wal.metadata.wal_header.lock();
    assert!(matches!(header.magic, WAL_MAGIC_LE | WAL_MAGIC_BE));
    assert_eq!(header.file_format, 3007000);
    let page_size = crate::PageSize::new(header.page_size).unwrap().get() as usize;
    let max_frame = wal.metadata.max_frame.load(Ordering::Acquire);
    assert!(max_frame > 0);
    let bytes = std::fs::read(fixture.runtime.files.namespace.join("physical-wal")).unwrap();
    let frame_size = 24 + page_size;
    let commit_offset = 32 + (max_frame as usize - 1) * frame_size;
    let pages = u32::from_be_bytes(
        bytes[commit_offset + 4..commit_offset + 8]
            .try_into()
            .unwrap(),
    ) as usize;
    assert!(pages > 0);
    let mut expected = fixture.main_before.clone();
    expected.resize(pages * page_size, 0);
    for (&page, frames) in wal.runtime.frame_cache.lock().iter() {
        let frame = *frames.last().unwrap();
        assert!(frame <= max_frame);
        if page as usize > pages {
            continue;
        }
        let source = 32 + (frame as usize - 1) * frame_size + 24;
        let target = (page as usize - 1) * page_size;
        let payload = &bytes[source..source + page_size];
        let marker = b"acknowledged before checkpoint ";
        if payload.windows(marker.len()).any(|bytes| bytes == marker) {
            eprintln!("ACK_DATA_ROOT={page}");
        }
        expected[target..target + page_size].copy_from_slice(&bytes[source..source + page_size]);
    }
    drop(wal);
    drop(scanned);
    drop(scan);
    assert!(guard.io_retired());
    expected
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_wal_native_crash_fixtures_cover_old_partial_and_synced_main() {
    for point in [
        CrashPoint::FirstMainBackfillWrite,
        CrashPoint::PartialMainBackfill,
        CrashPoint::MainSyncedBeforeWalTruncate,
    ] {
        for commits in [1, 2] {
            let fixture = IntentCrashFixture::new(point, commits);
            let mut opening = fixture.runtime.begin_checkpoint_recovery().unwrap();
            let selected = fixture.io.block(|| opening.poll()).unwrap();
            let record = selected.manifest.record();
            assert!(record.checkpoint_intent().is_some());
            assert!(record.prefix().end_offset > 0);
            assert_eq!(
                record.prefix().end_offset,
                fixture.runtime.files.logical.size().unwrap()
            );
            assert!(selected.manifest.guard().unwrap().requires_recovery());
            assert!(selected.manifest.guard().unwrap().published().is_err());
            let expected = committed_image(&fixture, selected.manifest.guard().unwrap());
            let main = std::fs::read(&fixture.path).unwrap();
            match point {
                CrashPoint::FirstMainBackfillWrite => {
                    assert_eq!(main, fixture.main_before);
                    assert_ne!(main, expected);
                }
                CrashPoint::PartialMainBackfill => {
                    assert_ne!(main, fixture.main_before);
                    assert_ne!(main, expected);
                }
                CrashPoint::MainSyncedBeforeWalTruncate => {
                    assert_ne!(main, fixture.main_before);
                    assert_eq!(main, expected);
                }
                CrashPoint::SecondIntentWrite => unreachable!(),
            }
            let acknowledged = format!("acknowledged before checkpoint {commits}");
            assert!(expected
                .windows(acknowledged.len())
                .any(|bytes| bytes == acknowledged.as_bytes()));
            assert!(fixture.runtime.replay.lock().is_none());
            fixture.assert_durable_unchanged();
        }
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_wal_native_restores_committed_image_without_completion_or_admission() {
    for point in [
        CrashPoint::FirstMainBackfillWrite,
        CrashPoint::PartialMainBackfill,
        CrashPoint::MainSyncedBeforeWalTruncate,
    ] {
        for commits in [1, 2] {
            let fixture = IntentCrashFixture::new(point, commits);
            let mut opening = fixture.runtime.begin_checkpoint_recovery().unwrap();
            let selected = fixture.io.block(|| opening.poll()).unwrap();
            let record = selected.manifest.record();
            let expected = committed_image(&fixture, selected.manifest.guard().unwrap());
            let mut mirror = selected.begin_intent_mirror().unwrap();
            let intent = fixture.io.block(|| mirror.poll()).unwrap();
            let mut recovery = intent
                .begin_physical_wal_recovery(crate::OpenOptions::new(Arc::new(
                    crate::EngineDialect,
                )))
                .unwrap();
            let recovered = fixture.io.block(|| recovery.poll()).unwrap();
            assert_eq!(recovered.record(), record);
            assert!(recovered.guard().requires_recovery());
            assert!(recovered.guard().published().is_err());
            assert!(recovered.guard().io_retired());
            assert_eq!(
                recovered.parts.persistent_timestamp,
                record.prefix().committed_timestamp
            );
            eprintln!("PHYSICAL_RECOVERY_UNADMITTED_OUTPUT");
            let file: Arc<dyn File> = ReadPinnedFile::new_for_checkpoint_recovery(
                fixture.runtime.files.main.clone(),
                recovered.guard(),
            )
            .unwrap();
            let mut opening = SharedPhysicalConnection::new_with_retained_file(
                fixture.io.clone(),
                fixture.path.to_str().unwrap().to_owned(),
                crate::OpenOptions::new(Arc::new(crate::EngineDialect)),
                file,
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
            assert_eq!(
                rows,
                [format!("acknowledged before checkpoint {commits}")],
                "recovered data row differs from acknowledged value"
            );
            drop(reader);
            drop(opening);
            assert_eq!(std::fs::read(&fixture.path).unwrap(), expected);
            assert_eq!(fixture.runtime.files.physical_wal.size().unwrap(), 0);
            assert!(fixture.runtime.replay.lock().is_none());
            assert!(matches!(
                fixture.runtime.begin_open(),
                Err(LimboError::Busy)
            ));
            for (path, before) in &fixture.durable_before {
                if path != &fixture.path
                    && path != &fixture.runtime.files.namespace.join("physical-wal")
                {
                    assert_eq!(&std::fs::read(path).unwrap(), before);
                }
            }
        }
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_wal_native_preserves_matching_original_options_and_rejects_conflicts() {
    for case in 0..5 {
        let fixture = IntentCrashFixture::new(CrashPoint::FirstMainBackfillWrite, 1);
        let limit = fixture.runtime.files.authority.configured_history_limit();
        let mut opening = fixture.runtime.begin_checkpoint_recovery().unwrap();
        let selected = fixture.io.block(|| opening.poll()).unwrap();
        let mut mirror = selected.begin_intent_mirror().unwrap();
        let intent = fixture.io.block(|| mirror.poll()).unwrap();
        let mut options = crate::OpenOptions::new(Arc::new(crate::EngineDialect));
        options = match case {
            0 => options.with_shared_mvcc_for_test(limit),
            1 => options.with_shared_mvcc_for_test(limit + 4096),
            2 => options.wal_path("must-not-open-this-override"),
            3 => options.db_opts(crate::DatabaseOpts::new().with_multiprocess_wal(true)),
            _ => options.db_opts(
                crate::DatabaseOpts::new().with_experimental_mvcc_passive_checkpoint(true),
            ),
        };
        let result = intent.begin_physical_wal_recovery(options);
        if case == 0 {
            let mut recovery = result.unwrap();
            let recovered = fixture.io.block(|| recovery.poll()).unwrap();
            assert_eq!(
                recovered.parts.persistent_timestamp,
                recovered.record().prefix().committed_timestamp
            );
            assert!(recovered.guard().requires_recovery());
        } else {
            assert!(matches!(result, Err(LimboError::InvalidArgument(_))));
            fixture.assert_durable_unchanged();
        }
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_wal_native_original_readonly_participant_cannot_copy() {
    let fixture = IntentCrashFixture::new(CrashPoint::FirstMainBackfillWrite, 1);
    let path = fixture.path.canonicalize().unwrap();
    let limit = fixture.runtime.files.authority.configured_history_limit();
    let main = fixture
        .io
        .open_file(
            path.to_str().unwrap(),
            crate::OpenFlags::ReadOnly | crate::OpenFlags::NoLock,
            false,
        )
        .unwrap();
    let runtime = SharedMvccRuntime::open_existing(
        fixture.io.clone(),
        main.clone(),
        main.file_id().unwrap().unwrap(),
        &path,
        limit,
        true,
    )
    .unwrap();
    let mut opening = runtime.begin_checkpoint_recovery().unwrap();
    let selected = fixture.io.block(|| opening.poll()).unwrap();
    let mut mirror = selected.begin_intent_mirror().unwrap();
    let intent = fixture.io.block(|| mirror.poll()).unwrap();
    let options =
        crate::OpenOptions::new(Arc::new(crate::EngineDialect)).with_shared_mvcc_for_test(limit);
    assert!(
        matches!(intent.begin_physical_wal_recovery(options), Err(LimboError::InvalidArgument(message)) if message == "shared checkpoint recovery requires a writable participant")
    );
    fixture.assert_durable_unchanged();
}
