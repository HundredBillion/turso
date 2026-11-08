use super::super::shared_checkpoint_intent_fixture::{CrashPoint, IntentCrashFixture};
use super::*;
use crate::io::{FileSyncType, SharedWalLockKind, SharedWalMappedRegion};
use crate::{Buffer, Completion};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Role {
    Main,
    Wal,
    Manifest,
    Coordination,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Operation {
    Read(u64),
    Write(u64),
    Sync(FileSyncType),
    Truncate(u64),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Event(Role, Operation);
#[derive(Clone, Copy)]
enum Fault {
    None,
    Error(usize),
    Short(usize),
    Hold(usize),
}
struct Control {
    main: Mutex<Option<PathBuf>>,
    armed: AtomicBool,
    events: Mutex<Vec<Event>>,
    fault: Mutex<Fault>,
    held: Mutex<Option<Held>>,
}
impl Control {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            main: Mutex::new(None),
            armed: AtomicBool::new(false),
            events: Mutex::new(Vec::new()),
            fault: Mutex::new(Fault::None),
            held: Mutex::new(None),
        })
    }
    fn io(self: &Arc<Self>, path: &Path) -> Arc<dyn crate::IO> {
        *self.main.lock() = Some(path.canonicalize().unwrap());
        Arc::new(ControlledIo {
            inner: Arc::new(crate::io::SyscallIO::new().unwrap()),
            control: self.clone(),
        })
    }
    fn arm(&self, fault: Fault) {
        self.events.lock().clear();
        *self.fault.lock() = fault;
        self.armed.store(true, Ordering::Release);
    }
    fn event(&self, event: Event) -> Fault {
        if !self.armed.load(Ordering::Acquire) {
            return Fault::None;
        }
        let mut events = self.events.lock();
        events.push(event);
        let index = events.len();
        match *self.fault.lock() {
            Fault::Error(at) if at == index => Fault::Error(at),
            Fault::Short(at) if at == index => Fault::Short(at),
            Fault::Hold(at) if at == index => Fault::Hold(at),
            _ => Fault::None,
        }
    }
}
enum HeldOperation {
    Read(u64),
    Write(u64, Arc<Buffer>),
    Sync(FileSyncType),
    Truncate(u64),
}
struct Held {
    file: Arc<dyn File>,
    completion: Completion,
    operation: HeldOperation,
}
impl Held {
    fn complete(&self) {
        let completed = match &self.operation {
            HeldOperation::Read(offset) => self.file.pread(*offset, self.completion.clone()),
            HeldOperation::Write(offset, buffer) => {
                self.file
                    .pwrite(*offset, buffer.clone(), self.completion.clone())
            }
            HeldOperation::Sync(kind) => self.file.sync(self.completion.clone(), *kind),
            HeldOperation::Truncate(size) => self.file.truncate(*size, self.completion.clone()),
        }
        .unwrap();
        assert!(completed.succeeded());
    }
}
struct ControlledFile {
    inner: Arc<dyn File>,
    role: Role,
    control: Arc<Control>,
}
impl ControlledFile {
    fn hold(&self, completion: Completion, operation: HeldOperation) -> Completion {
        assert!(self
            .control
            .held
            .lock()
            .replace(Held {
                file: self.inner.clone(),
                completion: completion.clone(),
                operation,
            })
            .is_none());
        completion
    }
    fn fail(&self, completion: Completion) -> Result<Completion> {
        completion.error(crate::CompletionError::Aborted);
        Ok(completion)
    }
}
impl File for ControlledFile {
    fn file_id(&self) -> Result<Option<FileId>> {
        self.inner.file_id()
    }
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.inner.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.inner.unlock_file()
    }
    fn size(&self) -> Result<u64> {
        self.inner.size()
    }
    fn pread(&self, offset: u64, completion: Completion) -> Result<Completion> {
        match self
            .control
            .event(Event(self.role, Operation::Read(offset)))
        {
            Fault::Error(_) => self.fail(completion),
            Fault::Hold(_) => Ok(self.hold(completion, HeldOperation::Read(offset))),
            _ => self.inner.pread(offset, completion),
        }
    }
    fn pwrite(
        &self,
        offset: u64,
        buffer: Arc<Buffer>,
        completion: Completion,
    ) -> Result<Completion> {
        match self
            .control
            .event(Event(self.role, Operation::Write(offset)))
        {
            Fault::Error(_) => self.fail(completion),
            Fault::Short(_) => {
                let short = Arc::new(Buffer::new_temporary(buffer.len() / 2));
                short
                    .as_mut_slice()
                    .copy_from_slice(&buffer.as_slice()[..short.len()]);
                self.inner.pwrite(offset, short, completion)
            }
            Fault::Hold(_) => Ok(self.hold(completion, HeldOperation::Write(offset, buffer))),
            _ => self.inner.pwrite(offset, buffer, completion),
        }
    }
    fn sync(&self, completion: Completion, kind: FileSyncType) -> Result<Completion> {
        match self.control.event(Event(self.role, Operation::Sync(kind))) {
            Fault::Error(_) => self.fail(completion),
            Fault::Hold(_) => Ok(self.hold(completion, HeldOperation::Sync(kind))),
            _ => self.inner.sync(completion, kind),
        }
    }
    fn truncate(&self, size: u64, completion: Completion) -> Result<Completion> {
        match self
            .control
            .event(Event(self.role, Operation::Truncate(size)))
        {
            Fault::Error(_) => self.fail(completion),
            Fault::Hold(_) => Ok(self.hold(completion, HeldOperation::Truncate(size))),
            _ => self.inner.truncate(size, completion),
        }
    }
    fn shared_wal_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: SharedWalLockKind,
    ) -> Result<()> {
        self.inner.shared_wal_lock_byte(offset, exclusive, kind)
    }
    fn shared_wal_try_lock_byte(
        &self,
        offset: u64,
        exclusive: bool,
        kind: SharedWalLockKind,
    ) -> Result<bool> {
        self.inner.shared_wal_try_lock_byte(offset, exclusive, kind)
    }
    fn shared_wal_unlock_byte(&self, offset: u64, kind: SharedWalLockKind) -> Result<()> {
        self.inner.shared_wal_unlock_byte(offset, kind)
    }
    fn shared_wal_set_len(&self, size: u64) -> Result<()> {
        self.inner.shared_wal_set_len(size)
    }
    fn shared_wal_map(&self, offset: u64, size: usize) -> Result<Box<dyn SharedWalMappedRegion>> {
        self.inner.shared_wal_map(offset, size)
    }
}
struct ControlledIo {
    inner: Arc<dyn crate::IO>,
    control: Arc<Control>,
}
impl crate::Clock for ControlledIo {
    fn current_time_monotonic(&self) -> crate::MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> crate::WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}
impl crate::IO for ControlledIo {
    fn open_file(
        &self,
        path: &str,
        flags: crate::OpenFlags,
        direct: bool,
    ) -> Result<Arc<dyn File>> {
        let file = self.inner.open_file(path, flags, direct)?;
        let path = Path::new(path);
        let role = if self.control.main.lock().as_deref() == Some(path) {
            Some(Role::Main)
        } else {
            match path.file_name().and_then(|name| name.to_str()) {
                Some("physical-wal") => Some(Role::Wal),
                Some("manifest") => Some(Role::Manifest),
                Some("physical-tshm") => Some(Role::Coordination),
                _ => None,
            }
        };
        Ok(match role {
            Some(role) => Arc::new(ControlledFile {
                inner: file,
                role,
                control: self.control.clone(),
            }),
            None => file,
        })
    }
    fn remove_file(&self, path: &str) -> Result<()> {
        self.inner.remove_file(path)
    }
    fn supports_shared_wal_coordination(&self) -> bool {
        self.inner.supports_shared_wal_coordination()
    }
    fn step(&self) -> Result<()> {
        self.inner.step()
    }
}

fn start(fixture: &IntentCrashFixture) -> CheckpointPhysicalWalRecovery {
    let mut opening = fixture.runtime.begin_checkpoint_recovery().unwrap();
    let selected = fixture.io.block(|| opening.poll()).unwrap();
    let mut mirror = selected.begin_intent_mirror().unwrap();
    let intent = fixture.io.block(|| mirror.poll()).unwrap();
    intent
        .begin_physical_wal_recovery(crate::OpenOptions::new(Arc::new(crate::EngineDialect)))
        .unwrap()
}
fn fixture(control: &Arc<Control>) -> IntentCrashFixture {
    IntentCrashFixture::with_parent_io(CrashPoint::FirstMainBackfillWrite, 1, |path| {
        control.io(path)
    })
}
fn verify_output(fixture: &IntentCrashFixture, recovered: &RecoveredIntentPhysicalBase) {
    assert!(recovered.guard().requires_recovery());
    assert!(recovered.guard().published().is_err());
    assert!(recovered.guard().io_retired());
    let file: Arc<dyn File> =
        crate::mvcc::read_pinned_file::ReadPinnedFile::new_for_checkpoint_recovery(
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
    assert_eq!(rows, ["acknowledged before checkpoint 1"]);
    assert!(fixture.runtime.replay.lock().is_none());
    assert_eq!(fixture.runtime.files.physical_wal.size().unwrap(), 0);
    for (path, bytes) in &fixture.durable_before {
        if path != &fixture.path && path != &fixture.runtime.files.namespace.join("physical-wal") {
            assert_eq!(&std::fs::read(path).unwrap(), bytes);
        }
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_wal_io_native_every_operation_error_and_short_write_retries() {
    let control = Control::new();
    let baseline = fixture(&control);
    let mut recovery = start(&baseline);
    control.arm(Fault::None);
    let output = baseline.io.block(|| recovery.poll()).unwrap();
    control.armed.store(false, Ordering::Release);
    verify_output(&baseline, &output);
    let events = control.events.lock().clone();
    eprintln!("PHYSICAL_RECOVERY_IO_TRACE {events:?}");
    assert!(events
        .iter()
        .any(|event| matches!(event, Event(Role::Main, Operation::Sync(_)))));
    assert_eq!(
        events.last(),
        Some(&Event(
            Role::Coordination,
            Operation::Sync(FileSyncType::FullFsync)
        ))
    );
    drop(output);
    drop(recovery);
    drop(baseline);
    for (index, event) in events.iter().enumerate() {
        for short in [false, true] {
            if short && !matches!(event.1, Operation::Write(_)) {
                continue;
            }
            eprintln!(
                "PHYSICAL_RECOVERY_IO_FAULT {} {event:?} short={short}",
                index + 1
            );
            let control = Control::new();
            let fixture = fixture(&control);
            let mut recovery = start(&fixture);
            control.arm(if short {
                Fault::Short(index + 1)
            } else {
                Fault::Error(index + 1)
            });
            assert!(
                fixture.io.block(|| recovery.poll()).is_err(),
                "ignored fault at {event:?}"
            );
            control.armed.store(false, Ordering::Release);
            drop(recovery);
            assert!(control.held.lock().is_none());
            let mut retry = start(&fixture);
            control.arm(Fault::None);
            let output = fixture.io.block(|| retry.poll()).unwrap();
            control.armed.store(false, Ordering::Release);
            assert_full_main_sync_before_wal_discard(&control.events.lock());
            verify_output(&fixture, &output);
        }
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_wal_io_native_cancel_before_callback_and_before_retirement() {
    let control = Control::new();
    let baseline = fixture(&control);
    let mut recovery = start(&baseline);
    control.arm(Fault::None);
    let output = baseline.io.block(|| recovery.poll()).unwrap();
    control.armed.store(false, Ordering::Release);
    let events = control.events.lock().clone();
    drop(output);
    drop(recovery);
    drop(baseline);
    for (index, event) in events.iter().enumerate() {
        for callback_first in [false, true] {
            eprintln!(
                "PHYSICAL_RECOVERY_IO_CANCEL {} {event:?} callback={callback_first}",
                index + 1
            );
            let control = Control::new();
            let fixture = fixture(&control);
            let mut recovery = start(&fixture);
            control.arm(Fault::Hold(index + 1));
            let mut polls = 0;
            let waiter = loop {
                polls += 1;
                assert!(polls < 1000, "recovery did not reach withheld backend");
                match recovery.poll().unwrap() {
                    IOResult::IO(waiter) => {
                        if control.held.lock().is_some() {
                            break waiter;
                        }
                        waiter.wait(fixture.io.as_ref()).unwrap();
                    }
                    IOResult::Done(_) => panic!("operation completed despite withheld backend"),
                }
            };
            let held = control
                .held
                .lock()
                .take()
                .expect("did not reach withheld backend");
            if callback_first {
                held.complete();
            }
            drop(waiter);
            drop(recovery);
            assert!(matches!(
                fixture.runtime.begin_open(),
                Err(LimboError::Busy)
            ));
            assert!(matches!(
                fixture.runtime.files.authority.try_acquire(),
                Err(LimboError::Busy)
            ));
            assert!(matches!(
                SharedWalRecoveryLocks::acquire(fixture.runtime.physical_coordination()),
                Err(LimboError::Busy)
            ));
            if !callback_first {
                held.complete();
            }
            assert!(held.completion.finished());
            assert!(matches!(
                fixture.runtime.begin_open(),
                Err(LimboError::Busy)
            ));
            assert!(matches!(
                SharedWalRecoveryLocks::acquire(fixture.runtime.physical_coordination()),
                Err(LimboError::Busy)
            ));
            drop(held);
            control.armed.store(false, Ordering::Release);
            let mut retry = start(&fixture);
            control.arm(Fault::None);
            let output = fixture.io.block(|| retry.poll()).unwrap();
            control.armed.store(false, Ordering::Release);
            assert_full_main_sync_before_wal_discard(&control.events.lock());
            verify_output(&fixture, &output);
        }
    }
}

fn assert_full_main_sync_before_wal_discard(events: &[Event]) {
    let sync = events
        .iter()
        .position(|event| {
            matches!(
                event,
                Event(
                    Role::Main,
                    Operation::Sync(FileSyncType::Fsync | FileSyncType::FullFsync)
                )
            )
        })
        .expect("recovery did not fully sync actual main");
    let truncate = events
        .iter()
        .position(|event| matches!(event, Event(Role::Wal, Operation::Truncate(0))))
        .expect("recovery did not truncate actual WAL");
    assert!(
        sync < truncate,
        "WAL discarded before actual main FULL barrier: {events:?}"
    );
    assert_eq!(
        events.last(),
        Some(&Event(
            Role::Coordination,
            Operation::Sync(FileSyncType::FullFsync)
        ))
    );
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_wal_precommit_native_full_main_sync_precedes_discard() {
    for point in [
        CrashPoint::BeforeWalHeader,
        CrashPoint::TornWalHeader,
        CrashPoint::BeforeWalFrame,
        CrashPoint::TornWalFrame,
        CrashPoint::BeforeWalCommit,
    ] {
        let control = Control::new();
        let fixture = IntentCrashFixture::with_parent_io(point, 1, |path| control.io(path));
        let mut recovery = start(&fixture);
        control.arm(Fault::None);
        let output = fixture.io.block(|| recovery.poll()).unwrap();
        control.armed.store(false, Ordering::Release);
        let events = control.events.lock().clone();
        eprintln!("PRECOMMIT_FULL_MAIN_BARRIER {point:?} {events:?}");
        assert_full_main_sync_before_wal_discard(&events);
        assert!(!events
            .iter()
            .any(|event| matches!(event, Event(Role::Main, Operation::Write(_)))));
        assert_eq!(std::fs::read(&fixture.path).unwrap(), fixture.main_before);
        assert!(output.guard().requires_recovery());
        assert!(output.guard().published().is_err());
    }
}
