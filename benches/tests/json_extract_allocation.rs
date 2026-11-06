use std::{
    alloc::{GlobalAlloc, Layout, System},
    ptr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use turso_core::{ConnectOpts, Database, LimboError, MemoryIO, Statement, StepResult};
use turso_sqlite::SqliteDialect;

struct ResultAllocationFailure;

#[global_allocator]
static ALLOCATOR: ResultAllocationFailure = ResultAllocationFailure;
static FAIL_SIZE: AtomicUsize = AtomicUsize::new(0);
static FAILURES: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for ResultAllocationFailure {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if should_fail(layout.size()) {
            ptr::null_mut()
        } else {
            unsafe { System.alloc(layout) }
        }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        if should_fail(size) {
            ptr::null_mut()
        } else {
            unsafe { System.realloc(pointer, layout, size) }
        }
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        unsafe { System.dealloc(pointer, layout) }
    }
}

fn should_fail(size: usize) -> bool {
    if size != 0
        && FAIL_SIZE
            .compare_exchange(size, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    {
        FAILURES.fetch_add(1, Ordering::SeqCst);
        true
    } else {
        false
    }
}

#[test]
fn result_allocation_failure_reaches_scalar_and_table_queries() {
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect)).expect("open database");
    let conn = db.connect(ConnectOpts::new()).expect("connect");
    conn.execute("CREATE TABLE documents(payload)")
        .expect("create table");
    conn.execute(format!("INSERT INTO documents VALUES(jsonb('{{\"value\":\"abcdefghijklmnopq\",\"padding\":\"{}\"}}'))", "x".repeat(1024)))
        .expect("insert document");
    for (index, sql) in [
        "SELECT json_extract(payload,'$.value') FROM documents",
        "SELECT value FROM documents, json_each(payload,'$.value')",
    ]
    .into_iter()
    .enumerate()
    {
        let mut statement = conn.prepare(sql).expect("prepare extraction");
        assert_eq!(
            query(&db, &mut statement).expect("warm query"),
            "abcdefghijklmnopq"
        );
        statement.reset().expect("reset warm query");
        FAIL_SIZE.store(19, Ordering::SeqCst);
        let result = query(&db, &mut statement);
        FAIL_SIZE.store(0, Ordering::SeqCst);
        assert_eq!(FAILURES.load(Ordering::SeqCst), index + 1);
        assert!(
            matches!(result, Err(LimboError::OutOfMemory)),
            "result was {result:?}"
        );
        statement.reset().expect("reset failed query");
        assert_eq!(
            query(&db, &mut statement).expect("query after allocation failure"),
            "abcdefghijklmnopq"
        );
    }
}

fn query(db: &Database, statement: &mut Statement) -> turso_core::Result<String> {
    let mut result = String::new();
    loop {
        match statement.step()? {
            StepResult::Row => {
                result = statement.row().expect("query row").get_value(0).to_string()
            }
            StepResult::Done => return Ok(result),
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => db.io.step()?,
            other => panic!("unexpected query step: {other:?}"),
        }
    }
}
