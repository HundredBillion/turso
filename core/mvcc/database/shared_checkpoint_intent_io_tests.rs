use super::*;
use crate::io::FileSyncType;
use crate::sync::atomic::AtomicUsize;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Event {
    Read(u64),
    Write(u64),
    Sync(FileSyncType),
    Truncate(u64),
}
#[derive(Clone, Copy)]
enum Fault {
    None,
    Error(usize),
    Short(usize),
    Hold(usize),
}
struct Control {
    path: Mutex<Option<PathBuf>>,
    armed: AtomicBool,
    count: AtomicUsize,
    events: Mutex<Vec<Event>>,
    fault: Mutex<Fault>,
    held: Mutex<Option<Held>>,
}
impl Control {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            path: Mutex::new(None),
            armed: AtomicBool::new(false),
            count: AtomicUsize::new(0),
            events: Mutex::new(Vec::new()),
            fault: Mutex::new(Fault::None),
            held: Mutex::new(None),
        })
    }
    fn io(self: &Arc<Self>, path: &Path) -> Arc<dyn crate::IO> {
        let canonical = path.canonicalize().unwrap();
        let id = crate::io::get_file_id(canonical.to_str().unwrap()).unwrap();
        *self.path.lock() = Some(
            crate::mvcc::mode_transition::namespace_for(&canonical, id)
                .unwrap()
                .join("manifest"),
        );
        Arc::new(ControlledIo {
            inner: Arc::new(crate::io::SyscallIO::new().unwrap()),
            control: self.clone(),
        })
    }
    fn arm(&self, fault: Fault) {
        self.armed.store(true, Ordering::Release);
        self.count.store(0, Ordering::Release);
        self.events.lock().clear();
        *self.fault.lock() = fault;
    }
    fn event(&self, event: Event) -> Fault {
        if !self.armed.load(Ordering::Acquire) {
            return Fault::None;
        }
        self.events.lock().push(event);
        let index = self.count.fetch_add(1, Ordering::AcqRel) + 1;
        match *self.fault.lock() {
            Fault::Error(at) if at == index => Fault::Error(at),
            Fault::Short(at) if at == index => Fault::Short(at),
            Fault::Hold(at) if at == index => Fault::Hold(at),
            _ => Fault::None,
        }
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
        let inner = self.inner.open_file(path, flags, direct)?;
        if self.control.path.lock().as_deref() == Some(Path::new(path)) {
            Ok(Arc::new(ControlledFile {
                inner,
                control: self.control.clone(),
            }))
        } else {
            Ok(inner)
        }
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
struct ControlledFile {
    inner: Arc<dyn File>,
    control: Arc<Control>,
}
enum HeldOperation {
    Read(u64),
    Write(u64, Arc<crate::Buffer>),
    Sync(FileSyncType),
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
        }
        .unwrap();
        assert!(completed.succeeded());
    }
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
                operation
            })
            .is_none());
        completion
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
        match self.control.event(Event::Read(offset)) {
            Fault::Error(_) => {
                completion.error(crate::CompletionError::Aborted);
                Ok(completion)
            }
            Fault::Hold(_) => Ok(self.hold(completion, HeldOperation::Read(offset))),
            _ => self.inner.pread(offset, completion),
        }
    }
    fn pwrite(
        &self,
        offset: u64,
        buffer: Arc<crate::Buffer>,
        completion: Completion,
    ) -> Result<Completion> {
        match self.control.event(Event::Write(offset)) {
            Fault::Error(_) => {
                completion.error(crate::CompletionError::Aborted);
                Ok(completion)
            }
            Fault::Short(_) => {
                let short = Arc::new(crate::Buffer::new_temporary(RECORD_SIZE / 2));
                short
                    .as_mut_slice()
                    .copy_from_slice(&buffer.as_slice()[..RECORD_SIZE / 2]);
                self.inner.pwrite(offset, short, completion)
            }
            Fault::Hold(_) => Ok(self.hold(completion, HeldOperation::Write(offset, buffer))),
            Fault::None => self.inner.pwrite(offset, buffer, completion),
        }
    }
    fn sync(&self, completion: Completion, kind: FileSyncType) -> Result<Completion> {
        match self.control.event(Event::Sync(kind)) {
            Fault::Error(_) => {
                completion.error(crate::CompletionError::Aborted);
                Ok(completion)
            }
            Fault::Hold(_) => Ok(self.hold(completion, HeldOperation::Sync(kind))),
            _ => self.inner.sync(completion, kind),
        }
    }
    fn truncate(&self, len: u64, completion: Completion) -> Result<Completion> {
        self.control.event(Event::Truncate(len));
        self.inner.truncate(len, completion)
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_intent_native_two_full_syncs_before_owner() {
    let mut selected_slots = std::collections::BTreeSet::new();
    for commits in [1, 2] {
        let control = Control::new();
        let fixture =
            IntentCrashFixture::with_parent_io(CrashPoint::SecondIntentWrite, commits, |path| {
                control.io(path)
            });
        let selection = inspect_crash(&fixture, CrashPoint::SecondIntentWrite);
        let expected = selection.manifest.record();
        let bytes = std::fs::read(fixture.runtime.files.namespace.join("manifest")).unwrap();
        let slot = bytes
            .chunks_exact(SLOT_SIZE)
            .take(2)
            .position(|slot| {
                ManifestRecord::decode(&slot[..RECORD_SIZE]).unwrap() == Some(expected)
            })
            .unwrap();
        selected_slots.insert(slot);
        let mut mirror = selection.begin_intent_mirror().unwrap();
        control.arm(Fault::None);
        let recovered = fixture.io.block(|| mirror.poll()).unwrap();
        assert_eq!(recovered.record(), expected);
        assert_eq!(
            *control.events.lock(),
            [
                Event::Read(0),
                Event::Read(SLOT_SIZE as u64),
                Event::Write(((1 - slot) * SLOT_SIZE) as u64),
                Event::Sync(FileSyncType::FullFsync),
                Event::Write((slot * SLOT_SIZE) as u64),
                Event::Sync(FileSyncType::FullFsync)
            ]
        );
        assert!(mirror.poll().is_err());
        assert!(recovered.guard().io_retired());
        assert!(recovered.guard().requires_recovery());
        assert!(fixture.runtime.replay.lock().is_none());
        fixture.assert_durable_unchanged();
    }
    assert_eq!(selected_slots, std::collections::BTreeSet::from([0, 1]));
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_intent_native_rejects_same_bytes_from_memory_manifest() {
    let control = Control::new();
    let fixture = IntentCrashFixture::with_parent_io(CrashPoint::SecondIntentWrite, 1, |path| {
        control.io(path)
    });
    let selection = inspect_crash(&fixture, CrashPoint::SecondIntentWrite);
    let image = std::fs::read(fixture.runtime.files.namespace.join("manifest")).unwrap();
    let memory: Arc<dyn crate::IO> = Arc::new(crate::MemoryIO::new());
    let file = memory
        .open_file("substitute-manifest", crate::OpenFlags::Create, false)
        .unwrap();
    let buffer = Arc::new(crate::Buffer::new_temporary(image.len()));
    buffer.as_mut_slice().copy_from_slice(&image);
    memory
        .wait_for_completion(
            file.pwrite(0, buffer, Completion::new_write(|_| {}))
                .unwrap(),
        )
        .unwrap();
    let CheckpointRecoverySelection {
        runtime,
        manifest,
        pause,
    } = selection;
    let mut load =
        ManifestLoad::new(file, manifest.into_guard().unwrap(), runtime.database_id).unwrap();
    let substitute = memory.block(|| Ok(load.poll()?)).unwrap();
    let substitute = CheckpointRecoverySelection {
        runtime,
        manifest: substitute,
        pause,
    };
    control.arm(Fault::None);
    assert!(
        matches!(substitute.begin_intent_mirror(), Err(LimboError::InvalidArgument(message)) if message.contains("actual retained checkpoint selection"))
    );
    assert!(control.events.lock().is_empty());
    assert!(fixture.runtime.replay.lock().is_none());
    assert_eq!(
        std::fs::read(fixture.runtime.files.namespace.join("manifest")).unwrap(),
        image
    );
    fixture.assert_durable_unchanged();
    let mut opening = fixture.runtime.begin_checkpoint_recovery().unwrap();
    let selected = fixture.io.block(|| opening.poll()).unwrap();
    let mut retry = selected.begin_intent_mirror().unwrap();
    let recovered = fixture.io.block(|| retry.poll()).unwrap();
    assert!(recovered.guard().requires_recovery());
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_intent_native_io_failures_preserve_retry_and_source() {
    let faults: Vec<_> = (1..=6)
        .map(Fault::Error)
        .chain([Fault::Short(3), Fault::Short(5)])
        .collect();
    for fault in faults {
        for commits in [1, 2] {
            let control = Control::new();
            let fixture = IntentCrashFixture::with_parent_io(
                CrashPoint::SecondIntentWrite,
                commits,
                |path| control.io(path),
            );
            let selection = inspect_crash(&fixture, CrashPoint::SecondIntentWrite);
            let expected = selection.manifest.record();
            let mut mirror = selection.begin_intent_mirror().unwrap();
            control.arm(fault);
            let result = fixture.io.block(|| mirror.poll());
            match fault {
                Fault::Short(_) => assert!(
                    matches!(result, Err(LimboError::Corrupt(message)) if message.contains("short MVCC manifest write"))
                ),
                Fault::Error(_) => assert!(matches!(
                    result,
                    Err(LimboError::CompletionError(crate::CompletionError::Aborted))
                )),
                _ => unreachable!(),
            }
            assert!(mirror.poll().is_err());
            assert!(fixture.runtime.replay.lock().is_none());
            fixture.assert_durable_unchanged();
            control.arm(Fault::None);
            let mut opening = fixture.runtime.begin_checkpoint_recovery().unwrap();
            let selection = fixture.io.block(|| opening.poll()).unwrap();
            assert_eq!(selection.manifest.record(), expected);
            assert!(selection.manifest.guard().unwrap().requires_recovery());
            let mut retry = selection.begin_intent_mirror().unwrap();
            let recovered = fixture.io.block(|| retry.poll()).unwrap();
            assert_eq!(recovered.record(), expected);
            assert!(recovered.guard().requires_recovery());
            assert!(recovered.guard().published().is_err());
            fixture.assert_durable_unchanged();
        }
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn checkpoint_recovery_intent_native_cancel_retains_backend_until_retirement() {
    for at in 1..=6 {
        for callback_before_drop in [false, true] {
            for commits in [1, 2] {
                let control = Control::new();
                let fixture = IntentCrashFixture::with_parent_io(
                    CrashPoint::SecondIntentWrite,
                    commits,
                    |path| control.io(path),
                );
                let selection = inspect_crash(&fixture, CrashPoint::SecondIntentWrite);
                let expected = selection.manifest.record();
                let mut mirror = selection.begin_intent_mirror().unwrap();
                control.arm(Fault::Hold(at));
                let (waiter, held) = {
                    let mut polls = 0;
                    loop {
                        polls += 1;
                        assert!(polls < 1000, "intent mirror did not reach held I/O");
                        match mirror.poll().unwrap() {
                            IOResult::IO(waiter) => {
                                if let Some(held) = control.held.lock().take() {
                                    break (waiter, held);
                                }
                                waiter.wait(fixture.io.as_ref()).unwrap();
                            }
                            IOResult::Done(_) => {
                                panic!("intent mirror bypassed held operation {at}")
                            }
                        }
                    }
                };
                let callback_waiter = if callback_before_drop {
                    held.complete();
                    match mirror.poll().unwrap() {
                        IOResult::IO(waiter) => Some(waiter),
                        IOResult::Done(_) => panic!(
                            "intent owner returned before backend retirement at operation {at}"
                        ),
                    }
                } else {
                    None
                };
                drop(waiter);
                drop(callback_waiter);
                drop(mirror);
                assert!(fixture.runtime.replay.lock().is_none());
                assert!(matches!(
                    fixture.runtime.files.authority.try_acquire(),
                    Err(LimboError::Busy)
                ));
                assert!(matches!(
                    fixture.runtime.begin_open(),
                    Err(LimboError::Busy)
                ));
                if !callback_before_drop {
                    held.complete();
                }
                assert!(matches!(
                    fixture.runtime.files.authority.try_acquire(),
                    Err(LimboError::Busy)
                ));
                assert!(matches!(
                    fixture.runtime.begin_open(),
                    Err(LimboError::Busy)
                ));
                drop(held);
                fixture.assert_durable_unchanged();
                control.arm(Fault::None);
                let mut opening = fixture.runtime.begin_checkpoint_recovery().unwrap();
                let selection = fixture.io.block(|| opening.poll()).unwrap();
                assert_eq!(selection.manifest.record(), expected);
                let mut retry = selection.begin_intent_mirror().unwrap();
                let recovered = fixture.io.block(|| retry.poll()).unwrap();
                assert_eq!(recovered.record(), expected);
                assert!(recovered.guard().io_retired());
                assert!(recovered.guard().requires_recovery());
                assert!(recovered.guard().published().is_err());
                fixture.assert_durable_unchanged();
            }
        }
    }
}
