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
                _ => unreachable!(),
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

struct PeerProcess(std::process::Child);
impl PeerProcess {
    fn run(path: &std::path::Path, limit: u64, role: &str, commits: usize) {
        use std::io::BufRead;
        let mut process = Self(std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "mvcc::database::shared_runtime::shared_checkpoint_recovery_wal::tests::physical_wal_peer_child", "--ignored", "--nocapture", "--test-threads=1"])
            .env("TURSO_WAL_PEER_PATH", path).env("TURSO_WAL_PEER_LIMIT", limit.to_string())
            .env("TURSO_WAL_PEER_ROLE", role).env("TURSO_WAL_PEER_COMMITS", commits.to_string())
            .stdin(std::process::Stdio::null()).stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::inherit()).spawn().unwrap());
        let stdout = process.0.stdout.take().unwrap();
        let (send, receive) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            for line in std::io::BufReader::new(stdout).lines() {
                if send.send(line).is_err() {
                    break;
                }
            }
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        loop {
            let line = receive
                .recv_timeout(
                    deadline
                        .checked_duration_since(std::time::Instant::now())
                        .expect("physical peer exceeded deadline"),
                )
                .unwrap()
                .unwrap();
            if let Some((_, message)) = line.split_once("WAL_PEER ") {
                assert_eq!(message, format!("{role} ACK {commits}"));
                break;
            }
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            if let Some(status) = process.0.try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "physical peer did not exit"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}
impl Drop for PeerProcess {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_wal_native_stale_odd_map_recovers_before_fresh_readonly_peer() {
    for commits in [1, 2] {
        let fixture = IntentCrashFixture::new(CrashPoint::PartialMainBackfill, commits);
        let limit = fixture.runtime.files.authority.configured_history_limit();
        let mut opening = fixture.runtime.begin_checkpoint_recovery().unwrap();
        let selected = fixture.io.block(|| opening.poll()).unwrap();
        let mapping = fixture.runtime.physical_coordination();
        let physical = SharedWalRecoveryLocks::acquire(mapping.clone()).unwrap();
        let mut stale = mapping.try_snapshot().unwrap();
        stale.max_frame = 100;
        stale.nbackfills = 99;
        stale.checkpoint_seq = stale.checkpoint_seq.wrapping_add(40);
        stale.salt_1 = 12345;
        stale.salt_2 = 67890;
        stale.checksum_1 = 123;
        stale.checksum_2 = 456;
        mapping.install_snapshot(stale);
        mapping.record_frame(777, 9999);
        mapping.interrupt_snapshot_for_test();
        assert!(mapping.try_snapshot().is_none());
        drop(physical);
        drop(selected);
        drop(opening);
        PeerProcess::run(&fixture.path, limit, "recover", commits);
        let actual = mapping
            .try_snapshot()
            .expect("derived odd map was not repaired");
        assert_eq!(actual.max_frame, 0);
        assert_eq!(actual.nbackfills, 0);
        assert!(mapping.iter_latest_frames(0, u64::MAX).is_empty());
        assert_eq!(fixture.runtime.files.physical_wal.size().unwrap(), 0);
        PeerProcess::run(&fixture.path, limit, "observe", commits);
    }
}

#[test]
#[ignore = "subprocess helper; requires parent physical WAL peer parameters"]
fn physical_wal_peer_child() {
    use std::io::Write;
    let path = std::path::PathBuf::from(std::env::var("TURSO_WAL_PEER_PATH").unwrap());
    let limit = std::env::var("TURSO_WAL_PEER_LIMIT")
        .unwrap()
        .parse()
        .unwrap();
    let role = std::env::var("TURSO_WAL_PEER_ROLE").unwrap();
    let commits: usize = std::env::var("TURSO_WAL_PEER_COMMITS")
        .unwrap()
        .parse()
        .unwrap();
    let io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
    let flags = if role == "observe" {
        crate::OpenFlags::ReadOnly | crate::OpenFlags::NoLock
    } else {
        crate::OpenFlags::NoLock
    };
    let main = io.open_file(path.to_str().unwrap(), flags, false).unwrap();
    let runtime = SharedMvccRuntime::open_existing(
        io.clone(),
        main.clone(),
        main.file_id().unwrap().unwrap(),
        &path.canonicalize().unwrap(),
        limit,
        role == "observe",
    )
    .unwrap();
    let mut opening = runtime.begin_checkpoint_recovery().unwrap();
    let selected = io.block(|| opening.poll()).unwrap();
    let (guard_owner, inspected_owner);
    let guard = if role == "recover" {
        let mut mirror = selected.begin_intent_mirror().unwrap();
        let intent = io.block(|| mirror.poll()).unwrap();
        let mut recovery = intent
            .begin_physical_wal_recovery(crate::OpenOptions::new(Arc::new(crate::EngineDialect)))
            .unwrap();
        guard_owner = io.block(|| recovery.poll()).unwrap();
        guard_owner.guard()
    } else {
        inspected_owner = selected;
        assert!(inspected_owner
            .manifest
            .record()
            .checkpoint_intent()
            .is_some());
        inspected_owner.manifest.guard().unwrap()
    };
    let mapped = runtime
        .physical_coordination()
        .try_snapshot()
        .expect("fresh peer cannot use an odd derived map");
    assert_eq!(mapped.max_frame, 0);
    assert_eq!(mapped.nbackfills, 0);
    assert!(mapped.page_size >= 512);
    assert_eq!(runtime.files.physical_wal.size().unwrap(), 0);
    assert!(guard.requires_recovery());
    assert!(guard.published().is_err());
    let file: Arc<dyn File> =
        ReadPinnedFile::new_for_checkpoint_recovery(runtime.files.main.clone(), guard).unwrap();
    let mut physical = SharedPhysicalConnection::new_with_retained_file(
        io.clone(),
        path.to_str().unwrap().to_owned(),
        crate::OpenOptions::new(Arc::new(crate::EngineDialect)),
        file,
    )
    .unwrap();
    let reader = io.block(|| physical.poll()).unwrap();
    let mut rows = Vec::new();
    reader
        .prepare("SELECT value FROM preserved WHERE id=1")
        .unwrap()
        .run_with_row_callback(|row| {
            rows.push(row.get::<&str>(0)?.to_owned());
            Ok(())
        })
        .unwrap();
    assert_eq!(rows, [format!("acknowledged before checkpoint {commits}")]);
    assert!(runtime.replay.lock().is_none());
    println!("WAL_PEER {role} ACK {commits}");
    std::io::stdout().flush().unwrap();
}

fn precommit_points() -> [CrashPoint; 5] {
    [
        CrashPoint::BeforeWalHeader,
        CrashPoint::TornWalHeader,
        CrashPoint::BeforeWalFrame,
        CrashPoint::TornWalFrame,
        CrashPoint::BeforeWalCommit,
    ]
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_wal_precommit_native_genuine_crashes_preserve_source_main() {
    for point in precommit_points() {
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
            assert_eq!(std::fs::read(&fixture.path).unwrap(), fixture.main_before);
            let file: Arc<dyn File> = ReadPinnedFile::new_for_checkpoint_recovery(
                fixture.runtime.files.physical_wal.clone(),
                selected.manifest.guard().unwrap(),
            )
            .unwrap();
            let mut scanner = BuildSharedWal::begin(&file).unwrap();
            let scanned = fixture.io.block(|| scanner.poll()).unwrap();
            assert_eq!(scanned.read().metadata.max_frame.load(Ordering::Acquire), 0);
            let size = fixture.runtime.files.physical_wal.size().unwrap();
            match point {
                CrashPoint::BeforeWalHeader => assert_eq!(size, 0),
                CrashPoint::TornWalHeader => assert_eq!(size, 16),
                CrashPoint::BeforeWalFrame => assert_eq!(size, 32),
                CrashPoint::TornWalFrame => assert!(size > 32 && size < 32 + 24 + 4096),
                CrashPoint::BeforeWalCommit => assert!(size >= 32 + 24 + 4096),
                _ => unreachable!(),
            }
            fixture.assert_durable_unchanged();
            eprintln!("PRECOMMIT_SOURCE_UNCHANGED {point:?} parity={commits} wal_bytes={size}");
        }
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_wal_precommit_native_recovers_source_without_discarding_logical_ack() {
    for point in precommit_points() {
        for commits in [1, 2] {
            let fixture = IntentCrashFixture::new(point, commits);
            let mut opening = fixture.runtime.begin_checkpoint_recovery().unwrap();
            let selected = fixture.io.block(|| opening.poll()).unwrap();
            let record = selected.manifest.record();
            let mut mirror = selected.begin_intent_mirror().unwrap();
            let intent = fixture.io.block(|| mirror.poll()).unwrap();
            let mut recovery = intent
                .begin_physical_wal_recovery(crate::OpenOptions::new(Arc::new(
                    crate::EngineDialect,
                )))
                .unwrap();
            let recovered = fixture.io.block(|| recovery.poll()).unwrap();
            assert_eq!(recovered.record(), record);
            assert_eq!(
                recovered.parts.persistent_timestamp,
                record
                    .checkpoint_intent()
                    .unwrap()
                    .source_checkpoint_timestamp
            );
            assert!(recovered.guard().requires_recovery());
            assert!(recovered.guard().published().is_err());
            assert!(recovered.guard().io_retired());
            assert_eq!(std::fs::read(&fixture.path).unwrap(), fixture.main_before);
            assert_eq!(fixture.runtime.files.physical_wal.size().unwrap(), 0);
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
            let connection = fixture.io.block(|| opening.poll()).unwrap();
            let mut rows = Vec::new();
            connection
                .prepare("SELECT value FROM preserved WHERE id=1")
                .unwrap()
                .run_with_row_callback(|row| {
                    rows.push(row.get::<&str>(0)?.to_owned());
                    Ok(())
                })
                .unwrap();
            assert_eq!(rows, ["durable payload"]);
            for (path, bytes) in &fixture.durable_before {
                if path != &fixture.path
                    && path != &fixture.runtime.files.namespace.join("physical-wal")
                {
                    assert_eq!(&std::fs::read(path).unwrap(), bytes);
                }
            }
            assert!(fixture.runtime.replay.lock().is_none());
            eprintln!("PRECOMMIT_SOURCE_RECOVERED_ACK_RETAINED {point:?} parity={commits}");
        }
    }
}
