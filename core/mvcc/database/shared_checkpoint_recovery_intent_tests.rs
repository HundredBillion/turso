use super::super::shared_checkpoint_intent_fixture::{CrashPoint, IntentCrashFixture};
use super::*;
use crate::mvcc::commit_manifest::{ManifestRecord, RECORD_SIZE, SLOT_SIZE};

fn inspect_crash(fixture: &IntentCrashFixture, point: CrashPoint) -> CheckpointRecoverySelection {
    let mut opening = fixture.runtime.begin_checkpoint_recovery().unwrap();
    let selection = fixture.io.block(|| opening.poll()).unwrap();
    let record = selection.manifest.record();
    assert!(record.checkpoint_intent().is_some());
    assert!(record.downgrade_intent().is_none());
    assert!(record.prefix().end_offset > 0);
    assert_eq!(
        fixture.runtime.files.logical.size().unwrap(),
        record.prefix().end_offset
    );
    assert!(selection.manifest.guard().unwrap().requires_recovery());
    assert!(selection.manifest.guard().unwrap().published().is_err());
    assert!(fixture.runtime.replay.lock().is_none());
    let bytes = std::fs::read(fixture.runtime.files.namespace.join("manifest")).unwrap();
    let slots: Vec<_> = bytes
        .chunks_exact(SLOT_SIZE)
        .take(2)
        .map(|slot| {
            ManifestRecord::decode(&slot[..RECORD_SIZE])
                .unwrap()
                .unwrap()
        })
        .collect();
    assert_eq!(slots.len(), 2);
    let intents = slots
        .iter()
        .filter(|record| record.checkpoint_intent().is_some())
        .count();
    match point {
        CrashPoint::BeforeWalHeader
        | CrashPoint::TornWalHeader
        | CrashPoint::BeforeWalFrame
        | CrashPoint::TornWalFrame
        | CrashPoint::BeforeWalCommit => panic!("use the precommit WAL fixture inspector"),
        CrashPoint::SecondIntentWrite => {
            assert_eq!(intents, 1);
            assert_eq!(fixture.runtime.files.physical_wal.size().unwrap(), 0);
        }
        CrashPoint::FirstMainBackfillWrite
        | CrashPoint::PartialMainBackfill
        | CrashPoint::MainSyncedBeforeWalTruncate => {
            assert_eq!(intents, 2);
            let file: Arc<dyn File> =
                crate::mvcc::read_pinned_file::ReadPinnedFile::new_for_checkpoint_recovery(
                    fixture.runtime.files.physical_wal.clone(),
                    selection.manifest.guard().unwrap(),
                )
                .unwrap();
            let mut scan = crate::storage::sqlite3_ondisk::BuildSharedWal::begin(&file).unwrap();
            let wal = fixture.io.block(|| scan.poll()).unwrap();
            assert!(wal.read().metadata.max_frame.load(Ordering::Acquire) > 0);
            assert!(fixture.runtime.files.physical_wal.size().unwrap() > 32);
            drop(scan);
            drop(wal);
        }
    }
    match point {
        CrashPoint::SecondIntentWrite
        | CrashPoint::FirstMainBackfillWrite
        | CrashPoint::BeforeWalHeader
        | CrashPoint::TornWalHeader
        | CrashPoint::BeforeWalFrame
        | CrashPoint::TornWalFrame
        | CrashPoint::BeforeWalCommit => {
            assert_eq!(std::fs::read(&fixture.path).unwrap(), fixture.main_before);
        }
        CrashPoint::PartialMainBackfill | CrashPoint::MainSyncedBeforeWalTruncate => {
            assert_ne!(std::fs::read(&fixture.path).unwrap(), fixture.main_before);
        }
    }
    fixture.assert_durable_unchanged();
    selection
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_intent_native_crash_fixtures_keep_acknowledged_source() {
    for point in [
        CrashPoint::SecondIntentWrite,
        CrashPoint::FirstMainBackfillWrite,
    ] {
        for commits in [1, 2] {
            let fixture = IntentCrashFixture::new(point, commits);
            let selection = inspect_crash(&fixture, point);
            assert!(matches!(
                fixture.runtime.gate.try_enter_admission(),
                Err(LimboError::Busy)
            ));
            assert!(matches!(
                fixture.runtime.files.authority.try_acquire(),
                Err(LimboError::Busy)
            ));
            drop(selection);
        }
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_intent_native_mirrors_actual_crashed_selection() {
    for point in [
        CrashPoint::SecondIntentWrite,
        CrashPoint::FirstMainBackfillWrite,
    ] {
        for commits in [1, 2] {
            let fixture = IntentCrashFixture::new(point, commits);
            let selection = inspect_crash(&fixture, point);
            let expected = selection.manifest.record();
            let mut mirroring = selection.begin_intent_mirror().unwrap();
            let recovered = fixture.io.block(|| mirroring.poll()).unwrap();
            assert_eq!(recovered.record(), expected);
            assert!(recovered.record().checkpoint_intent().is_some());
            assert!(recovered.guard().requires_recovery());
            assert!(recovered.guard().published().is_err());
            assert!(recovered.guard().io_retired());
            assert!(fixture.runtime.replay.lock().is_none());
            assert!(matches!(
                fixture.runtime.gate.try_enter_admission(),
                Err(LimboError::Busy)
            ));
            assert!(matches!(
                fixture.runtime.files.authority.try_acquire(),
                Err(LimboError::Busy)
            ));
            let bytes = std::fs::read(fixture.runtime.files.namespace.join("manifest")).unwrap();
            for slot in bytes.chunks_exact(SLOT_SIZE).take(2) {
                assert_eq!(
                    ManifestRecord::decode(&slot[..RECORD_SIZE]).unwrap(),
                    Some(expected)
                );
            }
            fixture.assert_durable_unchanged();
        }
    }
}

#[path = "shared_checkpoint_intent_io_tests.rs"]
mod io_tests;
