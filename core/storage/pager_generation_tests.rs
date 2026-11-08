use super::*;
use crate::io::{File, MemoryIO};
use crate::storage::database::DatabaseFile;

struct DelayedReadFile {
    inner: Arc<dyn File>,
    reads: Mutex<Vec<(u64, Completion)>>,
}
impl File for DelayedReadFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        self.inner.lock_file(exclusive)
    }
    fn unlock_file(&self) -> Result<()> {
        self.inner.unlock_file()
    }
    fn pread(&self, pos: u64, c: Completion) -> Result<Completion> {
        self.reads.lock().push((pos, c.clone()));
        Ok(c)
    }
    fn pwrite(&self, pos: u64, buf: Arc<Buffer>, c: Completion) -> Result<Completion> {
        self.inner.pwrite(pos, buf, c)
    }
    fn sync(&self, c: Completion, ty: FileSyncType) -> Result<Completion> {
        self.inner.sync(c, ty)
    }
    fn size(&self) -> Result<u64> {
        self.inner.size()
    }
    fn truncate(&self, len: u64, c: Completion) -> Result<Completion> {
        self.inner.truncate(len, c)
    }
}
fn fixture() -> (Pager, Arc<DelayedReadFile>) {
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let file = Arc::new(DelayedReadFile {
        inner: io
            .open_file("generation.db", OpenFlags::Create, false)
            .unwrap(),
        reads: Mutex::new(Vec::new()),
    });
    let buf = Arc::new(Buffer::new_temporary(4096));
    buf.as_mut_slice().fill(17);
    let _ = file.pwrite(0, buf, Completion::new_write(|_| {})).unwrap();
    let pool = BufferPool::begin_init(&io, 4096 * 128);
    pool.finalize_with_page_size(4096).unwrap();
    let shared = Arc::new(SharedPagerState::new(None));
    shared.page_size.store(4096, Ordering::SeqCst);
    let pager = Pager::new(
        Arc::new(DatabaseFile::new(file.clone())),
        None,
        io,
        PageCache::new(10),
        pool,
        shared,
    )
    .unwrap();
    pager.io_ctx.write().reset_checksum();
    (pager, file)
}
fn read(pager: &Pager) -> (PageRef, Option<Completion>) {
    match pager.read_page(1).unwrap() {
        IOResult::Done(value) => value,
        IOResult::IO(_) => panic!("unexpected spill"),
    }
}
#[test]
fn shared_generation_reset_excludes_delayed_old_read() {
    let (pager, file) = fixture();
    let (old, old_completion) = read(&pager);
    assert!(!old_completion.unwrap().finished());
    pager.reset_shared_generation().unwrap();
    let (new, new_completion) = read(&pager);
    assert!(
        !Arc::ptr_eq(&old, &new),
        "new generation reused old in-flight page"
    );
    let (pos, c) = file.reads.lock().remove(1);
    let _ = file.inner.pread(pos, c).unwrap();
    assert!(new_completion.unwrap().finished());
    assert_eq!(new.get_contents().as_ptr()[0], 17);
    let buf = Arc::new(Buffer::new_temporary(4096));
    buf.as_mut_slice().fill(29);
    let _ = file
        .inner
        .pwrite(0, buf, Completion::new_write(|_| {}))
        .unwrap();
    let (pos, c) = file.reads.lock().remove(0);
    let _ = file.inner.pread(pos, c).unwrap();
    assert_eq!(old.get_contents().as_ptr()[0], 29);
    let (again, c) = read(&pager);
    assert!(c.is_none());
    assert!(Arc::ptr_eq(&again, &new));
    assert_eq!(again.get_contents().as_ptr()[0], 17);
}

#[test]
fn shared_generation_reset_clears_pending_read_and_header_memos() {
    let (pager, file) = fixture();
    let (old, c) = pager.read_page_no_cache(1, None, false, None).unwrap();
    pager.insert_pending_read(
        1,
        PendingRead {
            page: old.clone(),
            disk_read: Some(c.clone()),
        },
    );
    *pager.header_ref_state.write() = HeaderRefState::CreateHeader {
        page: old.clone(),
        completion: Some(c),
    };
    pager.schema_cookie.store(45, Ordering::SeqCst);
    pager.reserved_space.store(12, Ordering::SeqCst);
    pager.set_auto_vacuum_mode(AutoVacuumMode::Full);
    pager.reset_shared_generation().unwrap();
    assert!(pager.pending_reads.read().is_empty());
    assert!(!pager.has_pending_reads.load(Ordering::Acquire));
    assert!(matches!(
        *pager.header_ref_state.read(),
        HeaderRefState::Start
    ));
    assert_eq!(pager.get_reserved_space(), None);
    assert_eq!(
        pager.schema_cookie.load(Ordering::SeqCst),
        Pager::SCHEMA_COOKIE_NOT_SET
    );
    assert_eq!(pager.shared.page_size.load(Ordering::SeqCst), 4096);
    assert_eq!(pager.get_auto_vacuum_mode(), AutoVacuumMode::Full);
    let (new, _) = read(&pager);
    assert!(!Arc::ptr_eq(&old, &new));
    assert_eq!(file.reads.lock().len(), 2);
}

#[test]
fn shared_generation_reset_rejections_preserve_state() {
    let mutations: &[fn(&Pager)] = &[
        |p| {
            p.dirty_pages.write().insert(1);
        },
        |p| p.syncing.store(true, Ordering::SeqCst),
        |p| p.commit_info.write().state = CommitState::WaitWrites,
        |p| p.commit_info.write().pending_sync = Some(Completion::new_yield()),
        |p| {
            p.checkpoint_state.write().phase = CheckpointPhase::Finalize {
                clear_page_cache: false,
            }
        },
        |p| {
            *p.allocate_page_state.write() =
                AllocatePageState::AllocateNewPage { current_db_size: 1 }
        },
        |p| *p.allocate_page1_state.write() = AllocatePage1State::Start,
        |p| {
            *p.free_page_state.write() = FreePageState::NewTrunk {
                page: default_page1(None),
            }
        },
        |p| {
            *p.spill_state.write() = SpillState::WritingToDisk {
                pages: Vec::new(),
                completions: Vec::new(),
            }
        },
        |p| {
            *p.cacheflush_state.write() = CacheFlushState::WalPrepareStart {
                dirty_ids: Vec::new(),
                completion: Completion::new_yield(),
            }
        },
        |p| {
            p.open_savepoint(1).unwrap();
        },
        |p| {
            p.open_subjournal().unwrap();
            p.try_use_subjournal().unwrap();
        },
    ];
    for (index, mutate) in mutations.iter().enumerate() {
        let (pager, _) = fixture();
        let (page, c) = read(&pager);
        pager.insert_pending_read(
            1,
            PendingRead {
                page: page.clone(),
                disk_read: c,
            },
        );
        pager.schema_cookie.store(45, Ordering::SeqCst);
        mutate(&pager);
        assert!(
            matches!(pager.reset_shared_generation(), Err(LimboError::Busy)),
            "case {index}"
        );
        assert_eq!(pager.page_cache.read().len(), 1, "case {index}");
        assert!(pager.has_pending_reads.load(Ordering::Acquire));
        assert!(Arc::ptr_eq(&pager.pending_reads.read()[&1].page, &page));
        assert_eq!(pager.schema_cookie.load(Ordering::SeqCst), 45);
    }
}

#[test]
fn shared_generation_reset_retains_detached_loaded_buffers_and_cache_settings() {
    let (pager, file) = fixture();
    let (old, _) = read(&pager);
    let (pos, c) = file.reads.lock().remove(0);
    let _ = file.inner.pread(pos, c).unwrap();
    pager.open_subjournal().unwrap();
    pager.set_spill_enabled(false);
    pager.reset_shared_generation().unwrap();
    assert!(old.is_loaded());
    assert_eq!(old.get_contents().as_ptr()[0], 17);
    assert_eq!(pager.page_cache.read().capacity(), 10);
    assert!(!pager.get_spill_enabled());
    assert!(pager.subjournal.read().is_some());
}

#[test]
fn shared_generation_reset_rejects_untracked_dirty_cached_page() {
    let (pager, file) = fixture();
    let (old, _) = read(&pager);
    let (pos, c) = file.reads.lock().remove(0);
    let _ = file.inner.pread(pos, c).unwrap();
    old.set_dirty();
    assert!(pager.dirty_pages.read().is_empty());
    assert!(pager.reset_shared_generation().is_err());
    assert_eq!(pager.page_cache.read().len(), 1);
    assert!(old.is_loaded() && old.is_dirty());
    assert_eq!(old.get_contents().as_ptr()[0], 17);
}

#[test]
fn shared_generation_reset_rejects_real_wal_read_and_write_locks() {
    use crate::storage::wal::{WalFile, WalFileShared};
    let (mut pager, _) = fixture();
    let shared = WalFileShared::new_shared(
        pager
            .io
            .open_file("generation.wal", OpenFlags::Create, false)
            .unwrap(),
    )
    .unwrap();
    let last = shared.read().last_checksum_and_max_frame();
    pager.wal = Some(Arc::new(WalFile::new(
        pager.io.clone(),
        shared,
        last,
        pager.buffer_pool.clone(),
    )));
    pager.begin_read_tx().unwrap();
    assert!(pager.holds_read_lock());
    assert!(matches!(
        pager.reset_shared_generation(),
        Err(LimboError::Busy)
    ));
    pager
        .wal
        .as_ref()
        .unwrap()
        .begin_write_tx(crate::storage::wal::WalAutoActions::empty())
        .unwrap();
    assert!(pager.holds_write_lock());
    assert!(matches!(
        pager.reset_shared_generation(),
        Err(LimboError::Busy)
    ));
    pager.end_write_tx();
    pager.end_read_tx();
    pager.reset_shared_generation().unwrap();
}

#[test]
fn shared_generation_reset_reloads_header_without_changing_layout() {
    use crate::util::IOExt;
    let io: Arc<dyn IO> = Arc::new(MemoryIO::new());
    let db = crate::Database::open_file(
        io.clone(),
        "header-generation.db",
        Arc::new(crate::EngineDialect),
    )
    .unwrap();
    let conn = db.connect(crate::ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE t(id INTEGER PRIMARY KEY)")
        .unwrap();
    let pager = conn.get_pager();
    let header = io
        .block(|| pager.with_header(|h| (h.schema_cookie.get(), h.reserved_space)))
        .unwrap();
    let usable = pager.usable_space();
    pager.reset_shared_generation().unwrap();
    let reloaded = io
        .block(|| pager.with_header(|h| (h.schema_cookie.get(), h.reserved_space)))
        .unwrap();
    assert_eq!(header, reloaded);
    assert_eq!(pager.usable_space(), usable);
    conn.execute("SELECT * FROM t").unwrap();
}

#[cfg(all(host_shared_wal, unix))]
#[test]
fn shared_generation_tag_resets_only_when_physical_identity_changes() {
    let source = SharedPagerGeneration {
        database_id: crate::io::FileId { dev: 1, ino: 2 },
        generation: 3,
        checkpoint_timestamp: 4,
    };
    for target in [
        SharedPagerGeneration {
            database_id: crate::io::FileId { dev: 1, ino: 9 },
            ..source
        },
        SharedPagerGeneration {
            generation: 9,
            ..source
        },
        SharedPagerGeneration {
            checkpoint_timestamp: 9,
            ..source
        },
    ] {
        let (pager, file) = fixture();
        pager.ensure_shared_generation(source).unwrap();
        let (old, completion) = read(&pager);
        let (position, backend) = file.reads.lock().remove(0);
        drop(file.inner.pread(position, backend).unwrap());
        assert!(completion.unwrap().finished());
        pager.ensure_shared_generation(source).unwrap();
        let (same, completion) = read(&pager);
        assert!(completion.is_none());
        assert!(
            Arc::ptr_eq(&old, &same),
            "same physical identity reset the pager"
        );
        pager.ensure_shared_generation(target).unwrap();
        let (new, _) = read(&pager);
        assert!(
            !Arc::ptr_eq(&old, &new),
            "changed physical identity retained cached page"
        );
    }
}

#[cfg(all(host_shared_wal, unix))]
#[test]
fn shared_generation_tag_preserves_active_pager_aliases() {
    use crate::storage::wal::{WalFile, WalFileShared};
    let source = SharedPagerGeneration {
        database_id: crate::io::FileId { dev: 1, ino: 2 },
        generation: 3,
        checkpoint_timestamp: 4,
    };
    let target = SharedPagerGeneration {
        generation: 5,
        ..source
    };
    let (mut pager, file) = fixture();
    let shared = WalFileShared::new_shared(
        pager
            .io
            .open_file("generation-alias.wal", OpenFlags::Create, false)
            .unwrap(),
    )
    .unwrap();
    let last = shared.read().last_checksum_and_max_frame();
    pager.wal = Some(Arc::new(WalFile::new(
        pager.io.clone(),
        shared,
        last,
        pager.buffer_pool.clone(),
    )));
    let pager = Arc::new(pager);
    let alias = pager.clone();
    pager.ensure_shared_generation(source).unwrap();
    pager.begin_read_tx().unwrap();
    let (old, completion) = read(&pager);
    let (position, backend) = file.reads.lock().remove(0);
    drop(file.inner.pread(position, backend).unwrap());
    assert!(completion.unwrap().finished());
    alias.ensure_shared_generation(source).unwrap();
    assert!(
        matches!(
            alias.ensure_shared_generation(target),
            Err(LimboError::Busy)
        ),
        "generation change reset a pager with an active outer reader"
    );
    assert!(pager.holds_read_lock());
    let (same, completion) = read(&pager);
    assert!(completion.is_none());
    assert!(Arc::ptr_eq(&old, &same));
    pager.end_read_tx();
    alias.ensure_shared_generation(target).unwrap();
    let (new, _) = read(&pager);
    assert!(!Arc::ptr_eq(&old, &new));
}
