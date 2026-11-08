use super::*;
use crate::io::{FileSyncType, SharedWalLockKind, SharedWalMappedRegion};
use crate::{Buffer, Completion};

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_connection_native_pause_rejects_connection_before_header_io() {
    let directory = tempfile::tempdir().unwrap();
    let (path, limit) = shared_runtime_fixture(directory.path());
    let control = Arc::new(ConnectionIoControl {
        main_path: path.clone(),
        ..Default::default()
    });
    let io: Arc<dyn crate::IO> = Arc::new(ConnectionIo {
        inner: Arc::new(crate::io::SyscallIO::new().unwrap()),
        control: control.clone(),
    });
    let database = crate::Database::open(
        io,
        path.to_str().unwrap(),
        crate::OpenOptions::new(Arc::new(crate::EngineDialect)).with_shared_mvcc_for_test(limit),
    )
    .unwrap();
    let store = database.get_mv_store().as_ref().unwrap().clone();
    let runtime = store.shared_runtime.get().unwrap();
    let pause = runtime.gate.try_pause_admission().unwrap();
    let reads = control.main_reads.load(Ordering::Acquire);
    assert!(
        matches!(
            database.connect(crate::ConnectOpts::new()),
            Err(LimboError::Busy)
        ),
        "a paused checkpoint admitted a new shared connection"
    );
    assert_eq!(control.main_reads.load(Ordering::Acquire), reads);
    drop(pause);
    let connection = database.connect(crate::ConnectOpts::new()).unwrap();
    connection.execute("SELECT value FROM preserved").unwrap();
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_connection_native_cancel_keeps_header_authority_until_backend_release() {
    for read_offset in 1..=2 {
        for complete_before_drop in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let (path, limit) = shared_runtime_fixture(directory.path());
            let control = Arc::new(ConnectionIoControl {
                main_path: path.clone(),
                ..Default::default()
            });
            let io: Arc<dyn crate::IO> = Arc::new(ConnectionIo {
                inner: Arc::new(crate::io::SyscallIO::new().unwrap()),
                control: control.clone(),
            });
            let database = crate::Database::open(
                io.clone(),
                path.to_str().unwrap(),
                crate::OpenOptions::new(Arc::new(crate::EngineDialect))
                    .with_shared_mvcc_for_test(limit),
            )
            .unwrap();
            let store = database.get_mv_store().as_ref().unwrap().clone();
            let runtime = store.shared_runtime.get().unwrap();
            control.delay_at_read.store(
                control.main_reads.load(Ordering::Acquire) + read_offset,
                Ordering::Release,
            );
            let mut state = crate::ConnectAsyncState::new();
            let waiter = match database
                .connect_async(crate::ConnectOpts::new(), &mut state)
                .unwrap()
            {
                IOResult::IO(waiter) => waiter,
                IOResult::Done(_) => {
                    panic!("constructor bypassed delayed physical read {read_offset}")
                }
            };
            let held = control
                .pending_read
                .lock()
                .take()
                .expect("constructor did not submit expected header read");
            if read_offset == 2 {
                assert!(runtime.physical_coordination().has_active_db_file_reader());
            }
            if complete_before_drop {
                held.complete();
            }
            drop(waiter);
            drop(state);
            assert!(
                !runtime.physical_coordination().has_active_db_file_reader(),
                "cancelled page-one initialization retained its physical read lock"
            );
            assert!(
                matches!(runtime.files.authority.try_acquire(), Err(LimboError::Busy)),
                "cancelled header read released authority before backend release"
            );
            assert!(matches!(runtime.begin_open(), Err(LimboError::Busy)));
            if !complete_before_drop {
                held.complete();
            }
            assert!(matches!(
                runtime.files.authority.try_acquire(),
                Err(LimboError::Busy)
            ));
            drop(held);
            let guard = runtime.files.authority.try_acquire().unwrap();
            drop(guard);
            let connection = database.connect(crate::ConnectOpts::new()).unwrap();
            connection.execute("SELECT value FROM preserved").unwrap();
        }
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_connection_native_initial_open_cancel_keeps_header_authority_until_backend_release() {
    for read_offset in 1..=2 {
        for complete_before_drop in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let (path, limit) = shared_runtime_fixture(directory.path());
            let control = Arc::new(ConnectionIoControl {
                main_path: path.clone(),
                ..Default::default()
            });
            let io: Arc<dyn crate::IO> = Arc::new(ConnectionIo {
                inner: Arc::new(crate::io::SyscallIO::new().unwrap()),
                control: control.clone(),
            });
            let main = io
                .open_file(path.to_str().unwrap(), crate::OpenFlags::None, false)
                .unwrap();
            let probe = SharedMvccRuntime::open_existing(
                io.clone(),
                main.clone(),
                main.file_id().unwrap().unwrap(),
                &path,
                limit,
                false,
            )
            .unwrap();
            control.delay_at_read.store(
                control.main_reads.load(Ordering::Acquire) + read_offset,
                Ordering::Release,
            );
            let options = crate::OpenOptions::new(Arc::new(crate::EngineDialect))
                .with_shared_mvcc_for_test(limit);
            let mut state = crate::OpenDbAsyncState::new();
            let waiter = match crate::Database::open_async(
                &mut state,
                io.clone(),
                path.to_str().unwrap(),
                &options,
            )
            .unwrap()
            {
                IOResult::IO(waiter) => waiter,
                IOResult::Done(_) => {
                    panic!("initial open bypassed delayed physical read {read_offset}")
                }
            };
            let held = control
                .pending_read
                .lock()
                .take()
                .expect("initial open did not submit expected header read");
            if complete_before_drop {
                held.complete();
            }
            drop(waiter);
            drop(state);
            assert!(
                matches!(probe.files.authority.try_acquire(), Err(LimboError::Busy)),
                "cancelled initial header read released authority before backend release"
            );
            if !complete_before_drop {
                held.complete();
            }
            assert!(matches!(probe.begin_open(), Err(LimboError::Busy)));
            drop(held);
            let guard = probe.files.authority.try_acquire().unwrap();
            drop(guard);
            drop(probe);
            let database = crate::Database::open(io, path.to_str().unwrap(), options).unwrap();
            let connection = database.connect(crate::ConnectOpts::new()).unwrap();
            connection.execute("SELECT value FROM preserved").unwrap();
        }
    }
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_connection_native_initial_open_waits_for_completed_header_backend_release() {
    let directory = tempfile::tempdir().unwrap();
    let (path, limit) = shared_runtime_fixture(directory.path());
    let control = Arc::new(ConnectionIoControl {
        main_path: path.clone(),
        ..Default::default()
    });
    let io: Arc<dyn crate::IO> = Arc::new(ConnectionIo {
        inner: Arc::new(crate::io::SyscallIO::new().unwrap()),
        control: control.clone(),
    });
    control.delay_at_read.store(1, Ordering::Release);
    let options =
        crate::OpenOptions::new(Arc::new(crate::EngineDialect)).with_shared_mvcc_for_test(limit);
    let mut state = crate::OpenDbAsyncState::new();
    let waiter =
        match crate::Database::open_async(&mut state, io.clone(), path.to_str().unwrap(), &options)
            .unwrap()
        {
            IOResult::IO(waiter) => waiter,
            IOResult::Done(_) => panic!("initial open bypassed delayed header"),
        };
    let held = control.pending_read.lock().take().unwrap();
    held.complete();
    drop(waiter);
    let result =
        crate::Database::open_async(&mut state, io.clone(), path.to_str().unwrap(), &options);
    assert!(
        matches!(result, Ok(IOResult::IO(_))),
        "initial open did not wait for completed header backend release"
    );
    drop(result);
    drop(held);
    let database = io
        .block(|| {
            crate::Database::open_async(&mut state, io.clone(), path.to_str().unwrap(), &options)
        })
        .unwrap();
    database
        .connect(crate::ConnectOpts::new())
        .unwrap()
        .execute("SELECT value FROM preserved")
        .unwrap();
}

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_connection_native_active_helper_keeps_outer_transaction() {
    let directory = tempfile::tempdir().unwrap();
    let (path, limit) = shared_runtime_fixture(directory.path());
    let io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
    let database = crate::Database::open(
        io,
        path.to_str().unwrap(),
        crate::OpenOptions::new(Arc::new(crate::EngineDialect)).with_shared_mvcc_for_test(limit),
    )
    .unwrap();
    let connection = database.connect(crate::ConnectOpts::new()).unwrap();
    connection.execute("BEGIN CONCURRENT").unwrap();
    connection.execute("SELECT value FROM preserved").unwrap();
    let pager = connection.get_pager();
    let outer = connection.get_mv_tx();
    assert!(pager.holds_read_lock());
    assert!(
        matches!(
            database._connect_with_pager_and_default_cache_size(false, pager.clone(), None, 10),
            Err(LimboError::Busy)
        ),
        "private helper reused an active outer pager"
    );
    assert!(pager.holds_read_lock());
    assert_eq!(connection.get_mv_tx(), outer);
    connection.execute("SELECT value FROM preserved").unwrap();
    connection.execute("ROLLBACK").unwrap();
}

struct HeldMainRead {
    file: Arc<dyn File>,
    offset: u64,
    completion: Completion,
}

impl HeldMainRead {
    fn complete(&self) {
        drop(
            self.file
                .pread(self.offset, self.completion.clone())
                .unwrap(),
        );
        assert!(self.completion.finished());
    }
}

#[derive(Default)]
struct ConnectionIoControl {
    delay_at_read: crate::sync::atomic::AtomicUsize,
    main_reads: crate::sync::atomic::AtomicUsize,
    main_path: PathBuf,
    pending_read: Mutex<Option<HeldMainRead>>,
}

struct ConnectionFile {
    inner: Arc<dyn File>,
    control: Arc<ConnectionIoControl>,
    main: bool,
}

impl File for ConnectionFile {
    fn file_id(&self) -> Result<Option<FileId>> {
        self.inner.file_id()
    }
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.inner.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.inner.unlock_file()
    }
    fn pread(&self, offset: u64, completion: Completion) -> Result<Completion> {
        let read_number = if self.main {
            self.control.main_reads.fetch_add(1, Ordering::AcqRel) + 1
        } else {
            0
        };
        if self.main && read_number == self.control.delay_at_read.load(Ordering::Acquire) {
            assert!(self
                .control
                .pending_read
                .lock()
                .replace(HeldMainRead {
                    file: self.inner.clone(),
                    offset,
                    completion: completion.clone(),
                })
                .is_none());
            Ok(completion)
        } else {
            self.inner.pread(offset, completion)
        }
    }
    fn pwrite(
        &self,
        offset: u64,
        buffer: Arc<Buffer>,
        completion: Completion,
    ) -> Result<Completion> {
        self.inner.pwrite(offset, buffer, completion)
    }
    fn sync(&self, completion: Completion, kind: FileSyncType) -> Result<Completion> {
        self.inner.sync(completion, kind)
    }
    fn size(&self) -> Result<u64> {
        self.inner.size()
    }
    fn truncate(&self, size: u64, completion: Completion) -> Result<Completion> {
        self.inner.truncate(size, completion)
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

struct ConnectionIo {
    inner: Arc<dyn crate::IO>,
    control: Arc<ConnectionIoControl>,
}

impl crate::Clock for ConnectionIo {
    fn current_time_monotonic(&self) -> crate::MonotonicInstant {
        self.inner.current_time_monotonic()
    }
    fn current_time_wall_clock(&self) -> crate::WallClockInstant {
        self.inner.current_time_wall_clock()
    }
}

impl crate::IO for ConnectionIo {
    fn open_file(
        &self,
        path: &str,
        flags: crate::OpenFlags,
        direct: bool,
    ) -> Result<Arc<dyn File>> {
        let file = self.inner.open_file(path, flags, direct)?;
        if Path::new(path) == self.control.main_path {
            Ok(Arc::new(ConnectionFile {
                inner: file,
                control: self.control.clone(),
                main: true,
            }))
        } else {
            Ok(file)
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
