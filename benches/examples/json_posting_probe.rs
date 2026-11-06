#[cfg(test)]
#[allow(dead_code)]
#[path = "../../tests/integration/unreliable_io.rs"]
mod unreliable_io;

use std::{path::Path, sync::Arc, time::Instant};
use turso_core::{ConnectOpts, Connection, Database, PlatformIO, Statement, StepResult, Value};
use turso_sqlite::SqliteDialect;

const SCHEMA: &str = "CREATE TABLE documents(id INTEGER PRIMARY KEY, tenant INTEGER NOT NULL, payload BLOB NOT NULL CHECK(json_valid(payload,8) AND json_type(payload)='object')); CREATE INDEX documents_tenant ON documents(tenant)";
const POSTINGS: &str = include_str!("../fixtures/json_postings.sql");
const INDEX_QUERY: &str = "SELECT DISTINCT document_id FROM postings WHERE tenant=?1 AND mode=?2 AND path=?3 AND kind=?4 AND scalar IS ?5 ORDER BY document_id";
const SCALAR_SCAN: &str = "SELECT id FROM documents WHERE tenant=?1 AND CASE json_type(payload,?3) WHEN 'integer' THEN 'number' WHEN 'real' THEN 'number' WHEN 'true' THEN 'boolean' WHEN 'false' THEN 'boolean' ELSE json_type(payload,?3) END = ?4 AND json_extract(payload,?3) IS ?5 ORDER BY id";
const MEMBER_SCAN: &str = "SELECT d.id FROM documents d WHERE tenant=?1 AND json_type(d.payload,?3)='array' AND EXISTS(SELECT 1 FROM json_each(d.payload,?3) j WHERE CASE j.type WHEN 'integer' THEN 'number' WHEN 'real' THEN 'number' WHEN 'true' THEN 'boolean' WHEN 'false' THEN 'boolean' ELSE j.type END = ?4 AND j.atom IS ?5) ORDER BY d.id";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let count: usize = std::env::var("POSTING_ROWS")
        .unwrap_or_else(|_| "1000".into())
        .parse()?;
    let samples: usize = std::env::var("POSTING_SAMPLES")
        .unwrap_or_else(|_| "100".into())
        .parse()?;
    assert!((100..=100000).contains(&count) && (1..=10000).contains(&samples));
    let mode = std::env::var("POSTING_MODE").unwrap_or_else(|_| "all".into());
    assert!(["all", "scan", "postings", "expression"].contains(&mode.as_str()));
    println!("variant,operation,rows,samples,p50_us,p95_us,p99_us");
    for variant in ["scan", "expression", "postings"] {
        let indexed = variant == "postings";
        if mode != "all" && mode != variant {
            continue;
        }
        let directory = tempfile::tempdir()?;
        let path = directory.path().join("postings.db");
        let (db, conn) = open(&path)?;
        conn.execute(SCHEMA)?;
        if indexed {
            conn.execute(POSTINGS)?;
        }
        if variant == "expression" {
            for (name, path) in [("custom_f3", "$.custom.f3"), ("optional", "$.optional")] {
                conn.execute(format!("CREATE INDEX documents_{name} ON documents(tenant,CASE json_type(payload,'{path}') WHEN 'integer' THEN 'number' WHEN 'real' THEN 'number' WHEN 'true' THEN 'boolean' WHEN 'false' THEN 'boolean' ELSE json_type(payload,'{path}') END,json_extract(payload,'{path}'))"))?;
            }
        }
        let start = Instant::now();
        conn.execute("BEGIN")?;
        let mut insert = conn.prepare("INSERT INTO documents VALUES(?1,?2,jsonb(?3))")?;
        for id in 0..count {
            insert.bind_at(1usize.try_into()?, Value::from_i64(id as i64))?;
            insert.bind_at(2usize.try_into()?, Value::from_i64((id % 10) as i64))?;
            insert.bind_at(3usize.try_into()?, Value::build_text(document(id, false)))?;
            collect(&db, &mut insert)?;
        }
        conn.execute("COMMIT")?;
        eprintln!(
            "variant={variant} seed_ms={:.3}",
            start.elapsed().as_secs_f64() * 1000.0
        );
        if indexed {
            assert_consistent(&db, &conn)?;
        }
        for (mode, path, kind, value, scan) in [
            (
                "scalar",
                "$.custom.f3",
                "number",
                Value::from_i64(3),
                SCALAR_SCAN,
            ),
            (
                "member",
                "$.tags",
                "text",
                Value::build_text("tag3"),
                MEMBER_SCAN,
            ),
            ("scalar", "$.optional", "null", Value::Null, SCALAR_SCAN),
        ] {
            let parameters = [
                Value::from_i64(3),
                Value::build_text(mode),
                Value::build_text(path),
                Value::build_text(kind),
                value,
            ];
            let mut reference = conn.prepare(scan)?;
            bind(&mut reference, &parameters)?;
            let expected = collect(&db, &mut reference)?;
            let mut literal_scan = scan.replace("?3", &format!("'{path}'"));
            if mode == "scalar" {
                let index = if path == "$.custom.f3" {
                    "custom_f3"
                } else {
                    "optional"
                };
                literal_scan = literal_scan.replace(
                    "FROM documents WHERE",
                    &format!("FROM documents INDEXED BY documents_{index} WHERE"),
                );
            }
            let query = if indexed {
                INDEX_QUERY
            } else if variant == "expression" {
                &literal_scan
            } else {
                scan
            };
            let mut statement = conn.prepare(query)?;
            bind(&mut statement, &parameters)?;
            assert_eq!(collect(&db, &mut statement)?, expected);
            measure(
                &db,
                &conn,
                variant,
                &format!("{mode}:{path}:{kind}"),
                query,
                &parameters,
                count,
                samples,
            )?;
            let mut plan = conn.prepare(format!("EXPLAIN QUERY PLAN {query}"))?;
            eprintln!(
                "variant={variant} mode={mode} path={path} plan={:?}",
                collect(&db, &mut plan)?
            );
        }
        let mut update = conn.prepare("UPDATE documents SET payload=jsonb(?2) WHERE id=?1")?;
        let mut times = Vec::with_capacity(samples);
        for sample in 0..samples {
            let id = sample % count;
            let replacement = Value::build_text(document(id, (sample / count) % 2 == 0));
            let start = Instant::now();
            update.bind_at(1usize.try_into()?, Value::from_i64(id as i64))?;
            update.bind_at(2usize.try_into()?, replacement)?;
            collect(&db, &mut update)?;
            times.push(start.elapsed().as_nanos());
        }
        report(variant, "durable_update", count, times);
        if indexed {
            assert_consistent(&db, &conn)?;
        }
        let before = query(
            &db,
            &conn,
            "SELECT id,tenant,hex(payload) FROM documents ORDER BY id",
        )?;
        assert_eq!(
            query(&db, &conn, "PRAGMA integrity_check")?,
            vec![vec![Value::build_text("ok")]]
        );
        eprintln!(
            "variant={variant} database_bytes={} wal_bytes={}",
            std::fs::metadata(&path)?.len(),
            std::fs::metadata(path.with_extension("db-wal"))
                .map(|m| m.len())
                .unwrap_or(0)
        );
        let checkpoint = Instant::now();
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
        eprintln!(
            "variant={variant} checkpoint_us={:.3} checkpointed_database_bytes={}",
            checkpoint.elapsed().as_secs_f64() * 1e6,
            std::fs::metadata(&path)?.len()
        );
        drop(insert);
        drop(update);
        conn.close()?;
        drop(conn);
        drop(db);
        let start = Instant::now();
        let (db, conn) = open(&path)?;
        eprintln!(
            "variant={variant} reopen_configure_us={:.3}",
            start.elapsed().as_secs_f64() * 1e6
        );
        assert_eq!(
            query(
                &db,
                &conn,
                "SELECT id,tenant,hex(payload) FROM documents ORDER BY id"
            )?,
            before
        );
        if indexed {
            assert_consistent(&db, &conn)?;
        }
        conn.close()?;
    }
    Ok(())
}

fn open(path: &Path) -> turso_core::Result<(Arc<Database>, Arc<Connection>)> {
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new()?);
    let db = Database::open_file(
        io,
        path.to_str().expect("temporary path is UTF-8"),
        Arc::new(SqliteDialect),
    )?;
    let conn = db.connect(ConnectOpts::new())?;
    conn.execute("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA cache_size=-65536")?;
    Ok((db, conn))
}

fn document(id: usize, changed: bool) -> String {
    let fields = (0..16)
        .map(|field| {
            format!(
                "\"f{field}\":{}",
                (id + field + usize::from(changed && field == 3)) % 17
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let optional = if id % 3 == 0 {
        ",\"optional\":null"
    } else {
        ""
    };
    format!("{{\"custom\":{{{fields}}},\"tags\":[\"tag{}\",\"tag{}\"],\"active\":{},\"padding\":\"{}\"{optional}}}",id%7,id%7,id%2==0,"x".repeat(768))
}

fn bind(statement: &mut Statement, values: &[Value]) -> turso_core::Result<()> {
    for (index, value) in values.iter().enumerate() {
        statement.bind_at(
            (index + 1).try_into().expect("positive parameter index"),
            value.clone(),
        )?;
    }
    Ok(())
}

fn collect(db: &Database, statement: &mut Statement) -> turso_core::Result<Vec<Vec<Value>>> {
    let mut rows = Vec::new();
    loop {
        match statement.step()? {
            StepResult::Row => rows.push(
                statement
                    .row()
                    .expect("result row")
                    .get_values()
                    .cloned()
                    .collect(),
            ),
            StepResult::Done => break,
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => db.io.step()?,
            other => {
                return Err(turso_core::LimboError::InternalError(format!(
                    "unexpected probe step: {other:?}"
                )))
            }
        }
    }
    statement.reset()?;
    Ok(rows)
}

fn query(db: &Database, conn: &Arc<Connection>, sql: &str) -> turso_core::Result<Vec<Vec<Value>>> {
    collect(db, &mut conn.prepare(sql)?)
}

fn assert_consistent(db: &Database, conn: &Arc<Connection>) -> turso_core::Result<()> {
    let actual=query(db,conn,"SELECT document_id,tenant,mode,path,kind,scalar,typeof(scalar) FROM postings ORDER BY 1,2,3,4,5,6,7")?;
    let expected=query(db,conn,"SELECT d.id,d.tenant,'scalar',j.fullkey,CASE j.type WHEN 'integer' THEN 'number' WHEN 'real' THEN 'number' WHEN 'true' THEN 'boolean' WHEN 'false' THEN 'boolean' ELSE j.type END,j.atom,typeof(j.atom) FROM documents d,json_tree(d.payload) j WHERE j.type NOT IN ('object','array') UNION ALL SELECT d.id,d.tenant,'member',j.path,CASE j.type WHEN 'integer' THEN 'number' WHEN 'real' THEN 'number' WHEN 'true' THEN 'boolean' WHEN 'false' THEN 'boolean' ELSE j.type END,j.atom,typeof(j.atom) FROM documents d,json_tree(d.payload) j WHERE typeof(j.key)='integer' AND j.type NOT IN ('object','array') ORDER BY 1,2,3,4,5,6,7")?;
    assert_eq!(actual, expected, "postings differ from document scan");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn measure(
    db: &Database,
    conn: &Arc<Connection>,
    variant: &str,
    label: &str,
    sql: &str,
    parameters: &[Value],
    rows: usize,
    samples: usize,
) -> turso_core::Result<()> {
    let mut statement = conn.prepare(sql)?;
    bind(&mut statement, parameters)?;
    let expected = collect(db, &mut statement)?;
    for _ in 0..100 {
        assert_eq!(collect(db, &mut statement)?, expected);
    }
    let mut times = Vec::with_capacity(samples);
    for _ in 0..samples {
        let start = Instant::now();
        let rows = collect(db, &mut statement)?;
        let elapsed = start.elapsed().as_nanos();
        assert_eq!(rows, expected);
        times.push(elapsed);
    }
    report(variant, label, rows, times);
    Ok(())
}

fn report(variant: &str, label: &str, rows: usize, mut times: Vec<u128>) {
    times.sort_unstable();
    let n = times.len();
    let p = |percent: usize| times[(n * percent).div_ceil(100) - 1] as f64 / 1000.0;
    println!(
        "{},{label},{rows},{n},{:.3},{:.3},{:.3}",
        variant,
        p(50),
        p(95),
        p(99)
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn postings_preserve_real_and_large_integer_values() {
        let dir = tempfile::tempdir().expect("directory");
        let (db, conn) = open(&dir.path().join("types.db")).expect("open");
        conn.execute(SCHEMA).expect("schema");
        conn.execute(POSTINGS).expect("postings");
        conn.execute(r#"INSERT INTO documents VALUES(1,1,jsonb('{"n":1.0,"big":9007199254740993}')),(2,1,jsonb('{"n":true,"big":9007199254740992}'))"#).expect("insert typed documents");
        for (path, value) in [("$.n", 1), ("$.big", 9007199254740993)] {
            let parameters = [
                Value::from_i64(1),
                Value::build_text("scalar"),
                Value::build_text(path),
                Value::build_text("number"),
                Value::from_i64(value),
            ];
            let mut statement = conn.prepare(INDEX_QUERY).expect("query");
            bind(&mut statement, &parameters).expect("bind");
            assert_eq!(
                collect(&db, &mut statement).expect("typed match"),
                vec![vec![Value::from_i64(1)]]
            );
        }
        assert_consistent(&db, &conn).expect("typed consistency");
    }

    #[test]
    fn postings_recover_acknowledged_commit_after_power_loss() {
        let io = Arc::new(unreliable_io::UnreliableIo::new());
        let name = "json-postings-power-loss.db";
        let db = Database::open_file_with_flags(
            io.clone(),
            name,
            turso_core::OpenFlags::default(),
            turso_core::DatabaseOpts::new(),
            None,
            Arc::new(SqliteDialect),
        )
        .expect("open simulated database");
        let conn = db.connect(ConnectOpts::new()).expect("connect");
        conn.execute("PRAGMA synchronous=FULL")
            .expect("set durability");
        conn.execute(SCHEMA).expect("create documents");
        conn.execute(POSTINGS).expect("create postings");
        conn.execute(
            "INSERT INTO documents VALUES(1,1,jsonb('{\"n\":1}')); PRAGMA wal_checkpoint(TRUNCATE)",
        )
        .expect("durable initial state");
        io.mark_all_durable();
        conn.execute("BEGIN; UPDATE documents SET payload=jsonb('{\"n\":2,\"tags\":[1,2]}'); INSERT INTO documents VALUES(2,2,jsonb('{\"n\":3}')); COMMIT").expect("acknowledged commit");
        let expected = query(
            &db,
            &conn,
            "SELECT id,tenant,hex(payload) FROM documents ORDER BY id",
        )
        .expect("committed state");
        conn.execute("BEGIN; DELETE FROM documents")
            .expect("uncommitted deletion");
        let files = io.durable_files();
        let dir = tempfile::tempdir().expect("recovery directory");
        let path = dir.path().join("recovered.db");
        std::fs::write(&path, files.get(name).expect("durable database"))
            .expect("materialize database");
        if let Some(wal) = files.get(&format!("{name}-wal")) {
            std::fs::write(path.with_extension("db-wal"), wal).expect("materialize WAL");
        }
        let (recovered, connection) = open(&path).expect("recover durable image");
        assert_eq!(
            query(
                &recovered,
                &connection,
                "SELECT id,tenant,hex(payload) FROM documents ORDER BY id"
            )
            .expect("recovered state"),
            expected
        );
        assert_consistent(&recovered, &connection).expect("recovered postings match documents");
        assert_eq!(
            query(&recovered, &connection, "PRAGMA integrity_check").expect("integrity"),
            vec![vec![Value::build_text("ok")]]
        );
    }

    #[test]
    fn postings_serialize_two_writers_and_preserve_reader_snapshot() {
        let dir = tempfile::tempdir().expect("directory");
        let (db, first) = open(&dir.path().join("writers.db")).expect("open");
        let second = db.connect(ConnectOpts::new()).expect("second connection");
        first.execute(SCHEMA).expect("schema");
        first.execute(POSTINGS).expect("postings");
        first
            .execute("INSERT INTO documents VALUES(1,1,jsonb('{\"n\":1}'))")
            .expect("insert");
        second.execute("BEGIN").expect("reader transaction");
        let old = query(&db, &second, "SELECT scalar FROM postings WHERE path='$.n'")
            .expect("read snapshot");
        first
            .execute("BEGIN IMMEDIATE; UPDATE documents SET payload=jsonb('{\"n\":2}')")
            .expect("first writer");
        assert!(second
            .execute("UPDATE documents SET payload=jsonb('{\"n\":3}')")
            .is_err());
        first.execute("COMMIT").expect("commit writer");
        assert_eq!(
            query(&db, &second, "SELECT scalar FROM postings WHERE path='$.n'")
                .expect("old snapshot"),
            old
        );
        assert_consistent(&db, &second).expect("reader snapshot consistency");
        second
            .execute("ROLLBACK; UPDATE documents SET payload=jsonb('{\"n\":3}')")
            .expect("retry new transaction");
        assert_consistent(&db, &first).expect("final consistency");
        assert_eq!(
            query(&db, &first, "SELECT scalar FROM postings WHERE path='$.n'")
                .expect("final value"),
            vec![vec![Value::from_i64(3)]]
        );
    }

    #[test]
    fn postings_follow_updates_rollback_savepoints_and_reopen() {
        let dir = tempfile::tempdir().expect("create directory");
        let path = dir.path().join("test.db");
        {
            let (db, conn) = open(&path).expect("open database");
            conn.execute(SCHEMA).expect("create documents");
            conn.execute(POSTINGS).expect("create postings");
            conn.execute("INSERT INTO documents VALUES(1,1,jsonb('{\"n\":1,\"r\":1.0,\"big\":9007199254740993,\"nothing\":null,\"text\":\"1\",\"flag\":true,\"tags\":[1,1,null,false,\"1\"]}'))").expect("insert mixed document");
            assert_consistent(&db, &conn).expect("initial consistency");
            conn.execute("BEGIN; UPDATE documents SET tenant=2,payload=jsonb('{\"n\":2}'); SAVEPOINT edit; DELETE FROM documents; ROLLBACK TO edit; RELEASE edit").expect("savepoint sequence");
            assert_consistent(&db, &conn).expect("savepoint consistency");
            conn.execute("ROLLBACK").expect("rollback transaction");
            assert_consistent(&db, &conn).expect("rollback consistency");
            conn.execute("UPDATE documents SET id=2,tenant=3,payload=jsonb('{\"n\":null,\"tags\":[\"new\"]}')").expect("update identity and content");
            assert_consistent(&db, &conn).expect("update consistency");
            conn.close().expect("close database");
        }
        let (db, conn) = open(&path).expect("reopen database");
        assert_consistent(&db, &conn).expect("reopened consistency");
        conn.execute("DELETE FROM documents")
            .expect("delete document");
        assert_consistent(&db, &conn).expect("delete consistency");
    }

    #[test]
    fn postings_reject_duplicate_keys_without_partial_changes() {
        let dir = tempfile::tempdir().expect("create directory");
        let (db, conn) = open(&dir.path().join("test.db")).expect("open database");
        conn.execute(SCHEMA).expect("create documents");
        conn.execute(POSTINGS).expect("create postings");
        conn.execute("INSERT INTO documents VALUES(1,1,jsonb('{\"n\":1}'))")
            .expect("insert original");
        assert!(conn
            .execute("UPDATE documents SET payload=jsonb('{\"n\":2,\"n\":3}')")
            .is_err());
        assert!(conn
            .execute("INSERT INTO documents VALUES(2,1,jsonb('{\"n\":2,\"\\u006e\":3}'))")
            .is_err());
        assert_consistent(&db, &conn).expect("failed writes preserve postings");
        assert_eq!(
            query(
                &db,
                &conn,
                "SELECT json_extract(payload,'$.n') FROM documents"
            )
            .expect("read original"),
            vec![vec![Value::from_i64(1)]]
        );
    }

    #[test]
    fn postings_preserve_missing_null_types_array_duplicates_and_tenants() {
        let dir = tempfile::tempdir().expect("create directory");
        let (db, conn) = open(&dir.path().join("test.db")).expect("open database");
        conn.execute(SCHEMA).expect("create documents");
        conn.execute(POSTINGS).expect("create postings");
        conn.execute("INSERT INTO documents VALUES(1,1,jsonb('{\"n\":null,\"tags\":[1,1,1.0,true,\"1\"]}')),(2,1,jsonb('{}')),(3,2,jsonb('{\"n\":null,\"tags\":[1]}')), (4,1,jsonb('{\"tags\":1}')), (5,1,jsonb('{\"tags\":{\"k\":1}}'))").expect("insert documents");
        for (mode, path, kind, value, scan, expected) in [
            ("scalar", "$.n", "null", Value::Null, SCALAR_SCAN, 1),
            ("scalar", "$.missing", "null", Value::Null, SCALAR_SCAN, 0),
            ("member", "$.n", "null", Value::Null, MEMBER_SCAN, 0),
            (
                "member",
                "$.tags",
                "number",
                Value::from_i64(1),
                MEMBER_SCAN,
                1,
            ),
            (
                "member",
                "$.tags",
                "boolean",
                Value::from_i64(1),
                MEMBER_SCAN,
                1,
            ),
            (
                "member",
                "$.tags",
                "text",
                Value::build_text("1"),
                MEMBER_SCAN,
                1,
            ),
        ] {
            let parameters = [
                Value::from_i64(1),
                Value::build_text(mode),
                Value::build_text(path),
                Value::build_text(kind),
                value,
            ];
            let mut indexed = conn.prepare(INDEX_QUERY).expect("prepare indexed query");
            let mut reference = conn.prepare(scan).expect("prepare scan");
            bind(&mut indexed, &parameters).expect("bind index");
            bind(&mut reference, &parameters).expect("bind scan");
            let rows = collect(&db, &mut indexed).expect("query index");
            assert_eq!(rows.len(), expected);
            assert_eq!(rows, collect(&db, &mut reference).expect("query reference"));
        }
    }
}
