use divan::{black_box, AllocProfiler, Bencher};
use mimalloc::MiMalloc;
use std::sync::Arc;
use turso_core::{ConnectOpts, Database, MemoryIO, StepResult};
use turso_sqlite::SqliteDialect;

#[global_allocator]
static ALLOC: AllocProfiler<MiMalloc> = AllocProfiler::new(MiMalloc);

fn main() {
    divan::main();
}

#[turso_macros::divan_bench(args = [1024, 10240, 102400])]
fn scalar_extract(bencher: Bencher, size: usize) {
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(MemoryIO::new());
    let db = Database::open_file(io, ":memory:", Arc::new(SqliteDialect))
        .expect("open allocation benchmark");
    let conn = db.connect(ConnectOpts::new()).expect("connect benchmark");
    conn.execute("CREATE TABLE documents(payload)")
        .expect("create document table");
    let document = format!("{{\"first\":42,\"padding\":\"{}\"}}", "x".repeat(size - 25));
    assert_eq!(document.len(), size);
    conn.execute(format!("INSERT INTO documents VALUES(jsonb('{document}'))"))
        .expect("insert document");
    let mut statement = conn
        .prepare("SELECT json_extract(payload,'$.first') FROM documents")
        .expect("prepare extraction");
    bencher.bench_local(|| {
        loop {
            match statement.step().expect("step extraction") {
                StepResult::Row => {
                    black_box(statement.row());
                }
                StepResult::Done => break,
                StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => {
                    db.io.step().expect("complete I/O")
                }
                other => panic!("unexpected extraction step: {other:?}"),
            }
        }
        statement.reset().expect("reset extraction");
    });
}
