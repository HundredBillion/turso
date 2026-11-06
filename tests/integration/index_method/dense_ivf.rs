use super::*;

const CREATE: &str = "CREATE INDEX vi ON v USING experimental_dense_ivf(e, tenant) WITH (version=1, metric='cosine', dimensions=2, centroids='[1,0];[0,1];[-1,0];[0,-1]', nprobe=4)";
const QUERY: &str = "SELECT id, vector_distance_cos(e, vector32('[1,0.2]')) AS distance FROM v WHERE tenant=7 ORDER BY distance, rowid LIMIT 10";
const EXACT: &str = "SELECT id, vector_distance_cos(e, vector32('[1,0.2]')) AS distance FROM v NOT INDEXED WHERE tenant=7 ORDER BY distance, rowid LIMIT 10";

#[track_caller]
fn assert_exact(conn: &Arc<turso_core::Connection>) {
    let plan: Vec<(i64, i64, i64, String)> = conn.exec_rows(&format!("EXPLAIN QUERY PLAN {EXACT}"));
    assert!(
        !plan.iter().any(|r| r.3.contains("INDEX METHOD")),
        "oracle must scan: {plan:?}"
    );
    let got: Vec<(i64, f64)> = conn.exec_rows(QUERY);
    let expected: Vec<(i64, f64)> = conn.exec_rows(EXACT);
    assert_eq!(got, expected);
}

#[turso_macros::test]
fn dense_ivf_lifecycle(tmp_db: TempDatabase) {
    check_lifecycle(tmp_db);
}

#[test]
fn dense_ivf_lifecycle_mvcc() {
    check_lifecycle(
        TempDatabase::builder()
            .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
            .with_mvcc(true)
            .build(),
    );
}

fn check_lifecycle(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE v(id INTEGER PRIMARY KEY, e BLOB, tenant INTEGER)")
        .unwrap();
    conn.execute(CREATE).unwrap();
    for i in 1..=100 {
        conn.execute(format!(
            "INSERT INTO v VALUES({i}, vector32('[{},{}]'), {})",
            i - 50,
            i % 9 + 1,
            if i % 3 == 0 { 7 } else { 8 }
        ))
        .unwrap();
    }
    conn.execute("INSERT INTO v VALUES(101, vector32('[1,0]'), 7), (102, vector32('[1,0]'), 7)")
        .unwrap();
    let plan: Vec<(i64, i64, i64, String)> = conn.exec_rows(&format!("EXPLAIN QUERY PLAN {QUERY}"));
    assert!(
        plan.iter().any(|p| p.3.contains("INDEX METHOD")),
        "{plan:?}"
    );
    assert_exact(&conn);
    conn.execute("BEGIN").unwrap();
    conn.execute("UPDATE v SET e=vector32('[0,-1]'), tenant=7 WHERE id=1")
        .unwrap();
    conn.execute("DELETE FROM v WHERE id=102").unwrap();
    assert_exact(&conn);
    conn.execute("SAVEPOINT a").unwrap();
    conn.execute("UPDATE v SET tenant=8 WHERE tenant=7")
        .unwrap();
    assert_exact(&conn);
    conn.execute("ROLLBACK TO a").unwrap();
    assert_exact(&conn);
    conn.execute("ROLLBACK").unwrap();
    assert_exact(&conn);
    assert!(conn
        .execute("UPDATE v SET e=vector32('[0,0]') WHERE id=101")
        .is_err());
    assert!(conn
        .execute("INSERT INTO v VALUES(103, vector32('[1,2,3]'), 7)")
        .is_err());
    assert_exact(&conn);
    for invalid in [
        "x'0000807f0000803f'",
        "x'0000c07f0000803f'",
        "vector32('[0,0]')",
        "vector32('[1]')",
        "NULL",
    ] {
        assert!(conn
            .execute(format!(
                "INSERT INTO v VALUES(104,vector32('[1,0]'),7),(105,{invalid},7)"
            ))
            .is_err());
        let count: Vec<(i64,)> = conn.exec_rows("SELECT count(*) FROM v WHERE id>=104");
        assert_eq!(count, vec![(0,)]);
        assert_exact(&conn);
    }
    conn.execute("UPDATE v SET tenant=7, e=vector32('[0,1]') WHERE id=1")
        .unwrap();
    conn.execute("DELETE FROM v WHERE id=101").unwrap();
    assert_exact(&conn);
    let path = tmp_db.path.clone();
    drop(conn);
    drop(tmp_db);
    let reopened = TempDatabase::new_with_existent(&path);
    let conn = reopened.connect_limbo();
    assert_exact(&conn);
    let empty: Vec<(i64, f64)> = conn.exec_rows(&QUERY.replace("tenant=7", "tenant=99"));
    assert!(empty.is_empty());
    let integrity: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
    assert_eq!(integrity, vec![("ok".to_string(),)]);
}

#[turso_macros::test]
fn dense_ivf_filters_before_limit(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE v(id INTEGER PRIMARY KEY, e BLOB, tenant INTEGER)")
        .unwrap();
    conn.execute(CREATE.replace("nprobe=4", "nprobe=1"))
        .unwrap();
    for i in 1..=100 {
        conn.execute(format!("INSERT INTO v VALUES({i}, vector32('[1,0.2]'),8)"))
            .unwrap();
    }
    conn.execute("INSERT INTO v VALUES(101,vector32('[1,0]'),7),(102,vector32('[1,0]'),7)")
        .unwrap();
    assert_exact(&conn);
    let rows: Vec<(i64, f64)> = conn.exec_rows(QUERY);
    assert_eq!(rows.iter().map(|r| r.0).collect::<Vec<_>>(), vec![101, 102]);
}

#[test]
fn dense_ivf_concurrent_writers() {
    let db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .with_mvcc(true)
        .build();
    let a = db.connect_limbo();
    let b = db.connect_limbo();
    a.execute("CREATE TABLE v(id INTEGER PRIMARY KEY, e BLOB, tenant INTEGER)")
        .unwrap();
    a.execute(CREATE).unwrap();
    a.execute("BEGIN CONCURRENT").unwrap();
    b.execute("BEGIN CONCURRENT").unwrap();
    a.execute("INSERT INTO v VALUES(1,vector32('[1,0]'),7)")
        .unwrap();
    b.execute("INSERT INTO v VALUES(2,vector32('[0,1]'),7)")
        .unwrap();
    assert_exact(&a);
    assert_exact(&b);
    a.execute("COMMIT").unwrap();
    b.execute("COMMIT").unwrap();
    assert_exact(&a);
    let rows: Vec<(i64, f64)> = a.exec_rows(QUERY);
    assert_eq!(rows.len(), 2);
    a.execute("BEGIN CONCURRENT").unwrap();
    b.execute("BEGIN CONCURRENT").unwrap();
    a.execute("DELETE FROM v WHERE id=1").unwrap();
    let update = b.execute("UPDATE v SET e=vector32('[-1,0]') WHERE id=1");
    a.execute("COMMIT").unwrap();
    assert!(update.is_err() || b.execute("COMMIT").is_err());
    let _ = b.execute("ROLLBACK");
    assert_exact(&a);
}

#[test]
fn dense_ivf_write_fault() {
    use crate::queued_io::{QueuedIo, QueuedIoOpKind};
    let io = Arc::new(QueuedIo::new());
    let path = "dense-ivf-write-fault.db";
    let db = turso_core::Database::open_file_with_flags(
        io.clone(),
        path,
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new().with_index_method(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("CREATE TABLE v(id INTEGER PRIMARY KEY, e BLOB, tenant INTEGER)")
        .unwrap();
    conn.execute(CREATE).unwrap();
    conn.execute("INSERT INTO v VALUES(1,vector32('[1,0]'),7)")
        .unwrap();
    io.fault_after(format!("{path}-wal"), QueuedIoOpKind::Pwritev, 0);
    assert!(conn
        .execute("UPDATE v SET e=vector32('[0,1]'),tenant=8 WHERE id=1")
        .is_err());
    io.clear_fault();
    assert_exact(&conn);
    let rows: Vec<(i64, f64)> = conn.exec_rows(QUERY);
    assert_eq!(rows.len(), 1);
}

#[test]
fn dense_ivf_checkpoint_crash() {
    use crate::unreliable_io::UnreliableIo;
    let io = Arc::new(UnreliableIo::new());
    let path = "dense-ivf-checkpoint-crash.db";
    let db = turso_core::Database::open_file_with_flags(
        io.clone(),
        path,
        turso_core::OpenFlags::default(),
        turso_core::DatabaseOpts::new().with_index_method(true),
        None,
        Arc::new(SqliteDialect),
    )
    .unwrap();
    let conn = db.connect(ConnectOpts::new()).unwrap();
    conn.execute("PRAGMA page_size=512").unwrap();
    conn.execute("CREATE TABLE v(id INTEGER PRIMARY KEY, e BLOB, tenant INTEGER)")
        .unwrap();
    conn.execute(CREATE).unwrap();
    conn.execute("BEGIN").unwrap();
    for i in 1..=500 {
        conn.execute(format!(
            "INSERT INTO v VALUES({i},vector32('[1,{}]'),7)",
            i % 7 + 1
        ))
        .unwrap();
    }
    conn.execute("COMMIT").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    io.mark_all_durable();
    conn.execute("PRAGMA synchronous=FULL").unwrap();
    conn.execute("UPDATE v SET e=vector32('[0,1]'),tenant=CASE WHEN id%2=0 THEN 8 ELSE 7 END")
        .unwrap();
    let expected: Vec<(i64, f64)> = conn.exec_rows(EXACT);
    io.arm_crash_on_sync(path);
    conn.execute("PRAGMA wal_checkpoint(PASSIVE)").unwrap();
    let snapshot = io
        .take_crash_snapshot()
        .expect("crash point must be reached");
    assert!(snapshot.writes_persisted > 0 && snapshot.writes_dropped > 0);
    let dir = tempfile::tempdir().unwrap();
    let recovered_path = dir.path().join("recovered.db");
    std::fs::write(&recovered_path, &snapshot.files[path]).unwrap();
    if let Some(wal) = snapshot.files.get(&format!("{path}-wal")) {
        std::fs::write(dir.path().join("recovered.db-wal"), wal).unwrap();
    }
    let recovered = TempDatabase::new_with_existent(&recovered_path);
    let conn = recovered.connect_limbo();
    assert_exact(&conn);
    let got: Vec<(i64, f64)> = conn.exec_rows(QUERY);
    assert_eq!(got, expected);
    let integrity: Vec<(String,)> = conn.exec_rows("PRAGMA integrity_check");
    assert_eq!(integrity, vec![("ok".into(),)]);
}

#[cfg(feature = "test_helper")]
#[test]
#[ignore = "100k synthetic measurement; run alone and retain stdout"]
fn dense_ivf_recall_probe() {
    use std::time::Instant;
    use turso_core::vector::{
        operations::{distance_cos::vector_distance_cos, serialize::vector_serialize},
        vector_types::Vector,
    };
    const D: usize = 32;
    const K: usize = 32;
    let n: usize = std::env::var("DENSE_IVF_ROWS")
        .unwrap_or("100000".into())
        .parse()
        .unwrap();
    let nq: usize = std::env::var("DENSE_IVF_QUERIES")
        .unwrap_or("100".into())
        .parse()
        .unwrap();
    let mut state = 19937_u64;
    let mut random = || {
        state = state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((state >> 32) as u32 as f64 / u32::MAX as f64) as f32 * 2.0 - 1.0
    };
    let centers: Vec<Vec<f32>> = (0..K).map(|_| (0..D).map(|_| random()).collect()).collect();
    let vectors: Vec<Vector<'static>> = (0..n)
        .map(|i| {
            Vector::from_f32(
                (0..D)
                    .map(|d| {
                        let center = if i % 5 == 0 {
                            (centers[i % K][d] + centers[(i + 1) % K][d]) * 0.5
                        } else {
                            centers[i % K][d]
                        };
                        center + 0.7 * random()
                    })
                    .collect(),
            )
        })
        .collect();
    let queries: Vec<Vector<'static>> = (0..nq)
        .map(|i| {
            Vector::from_f32(
                (0..D)
                    .map(|d| (centers[i % K][d] + centers[(i + 1) % K][d]) * 0.5 + 0.7 * random())
                    .collect(),
            )
        })
        .collect();
    let train_start = Instant::now();
    let mut centroids: Vec<Vector<'static>> = vectors
        .iter()
        .take(K)
        .map(|v| Vector::from_f32(v.as_f32_slice().to_vec()))
        .collect();
    for _ in 0..6 {
        let mut sums = vec![vec![0.0_f64; D]; K];
        let mut counts = [0_usize; K];
        for v in vectors.iter().take(2048) {
            let c = nearest(v, &centroids);
            counts[c] += 1;
            for (d, value) in v.as_f32_slice().iter().enumerate() {
                sums[c][d] += *value as f64;
            }
        }
        for c in 0..K {
            if counts[c] > 0 {
                centroids[c] = Vector::from_f32(
                    sums[c]
                        .iter()
                        .map(|v| (*v / counts[c] as f64) as f32)
                        .collect(),
                );
            }
        }
    }
    let train_ms = train_start.elapsed().as_secs_f64() * 1000.0;
    let centroid_text = centroids
        .iter()
        .map(vector_text)
        .collect::<Vec<_>>()
        .join(";");
    let tenant = |i: usize| -> i64 {
        if i % 1000 < 800 {
            0
        } else if i % 1000 < 900 {
            1
        } else if i % 1000 < 999 {
            2
        } else {
            3
        }
    };
    let mut cluster_counts = vec![[0usize; K]; 4];
    for (i, v) in vectors.iter().enumerate() {
        cluster_counts[tenant(i) as usize][nearest(v, &centroids)] += 1;
    }
    let mut db = TempDatabase::builder()
        .with_opts(turso_core::DatabaseOpts::new().with_index_method(true))
        .build();
    let mut conn = db.connect_limbo();
    conn.execute("CREATE TABLE v(id INTEGER PRIMARY KEY,e BLOB,tenant INTEGER)")
        .unwrap();
    let insert_start = Instant::now();
    conn.execute("BEGIN").unwrap();
    let mut insert = conn.prepare("INSERT INTO v VALUES(?1,?2,?3)").unwrap();
    for (i, v) in vectors.iter().enumerate() {
        insert
            .bind_at(1.try_into().unwrap(), Value::from_i64(i as i64))
            .unwrap();
        insert
            .bind_at(
                2.try_into().unwrap(),
                vector_serialize(Vector::from_f32(v.as_f32_slice().to_vec())).unwrap(),
            )
            .unwrap();
        insert
            .bind_at(3.try_into().unwrap(), Value::from_i64(tenant(i)))
            .unwrap();
        insert.run_with_row_callback(|_| Ok(())).unwrap();
        insert.reset().unwrap();
    }
    drop(insert);
    conn.execute("COMMIT").unwrap();
    let insert_ms = insert_start.elapsed().as_secs_f64() * 1000.0;
    conn.execute("CREATE INDEX v_tenant ON v(tenant)").unwrap();
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
    let base_bytes = std::fs::metadata(&db.path).unwrap().len();
    let query_sql = |i: usize, indexed: bool| {
        format!("SELECT id, vector_distance_cos(e,vector32('{}')) AS distance FROM v {} WHERE tenant={} ORDER BY distance,rowid LIMIT 10",vector_text(&queries[i]),if indexed {""} else {"NOT INDEXED"},i%5)
    };
    let mut oracle = Vec::new();
    let mut exact_times = vec![Vec::new(); 5];
    let mut filtered_times = vec![Vec::new(); 5];
    for i in 0..nq {
        let start = Instant::now();
        let rows: Vec<(i64, f64)> = conn.exec_rows(&query_sql(i, false));
        exact_times[i % 5].push(start.elapsed().as_secs_f64() * 1000.0);
        let sql = query_sql(i, false).replace("NOT INDEXED", "INDEXED BY v_tenant");
        let start = Instant::now();
        let indexed_rows: Vec<(i64, f64)> = conn.exec_rows(&sql);
        filtered_times[i % 5].push(start.elapsed().as_secs_f64() * 1000.0);
        assert_eq!(rows, indexed_rows);
        oracle.push(rows);
    }
    println!("DENSE_SETUP rows={n} dimensions={D} clusters={K} queries={nq} seed=19937 train_ms={train_ms:.3} insert_ms={insert_ms:.3} base_bytes={base_bytes} corpus=synthetic_boundary_mixture");
    for t in 0..5 {
        println!("DENSE_EXACT tenant={t} {}", percentiles(&exact_times[t]));
        println!(
            "DENSE_EXACT_TENANT_INDEX tenant={t} {}",
            percentiles(&filtered_times[t])
        );
    }
    for nprobe in [1, 2, 4, 8, 16, 32] {
        let start = Instant::now();
        conn.execute(format!("CREATE INDEX vi ON v USING experimental_dense_ivf(e,tenant) WITH(version=1,metric='cosine',dimensions={D},centroids='{centroid_text}',nprobe={nprobe})")).unwrap();
        let build_ms = start.elapsed().as_secs_f64() * 1000.0;
        conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").unwrap();
        let file_bytes = std::fs::metadata(&db.path).unwrap().len();
        let pages: Vec<(i64,)> = conn.exec_rows("PRAGMA page_count");
        let free: Vec<(i64,)> = conn.exec_rows("PRAGMA freelist_count");
        let page_size: Vec<(i64,)> = conn.exec_rows("PRAGMA page_size");
        let allocated = (pages[0].0 - free[0].0) * page_size[0].0;
        let mut times = vec![Vec::new(); 5];
        let mut recalls = vec![Vec::new(); 5];
        let mut candidates = [0usize; 5];
        for i in 0..nq {
            let sql = query_sql(i, true);
            if i == 0 {
                let plan: Vec<(i64, i64, i64, String)> =
                    conn.exec_rows(&format!("EXPLAIN QUERY PLAN {sql}"));
                assert!(
                    plan.iter().any(|r| r.3.contains("INDEX METHOD")),
                    "{plan:?}"
                );
                let plan: Vec<(i64, i64, i64, String)> =
                    conn.exec_rows(&format!("EXPLAIN QUERY PLAN {}", query_sql(i, false)));
                assert!(
                    !plan.iter().any(|r| r.3.contains("INDEX METHOD")),
                    "oracle: {plan:?}"
                );
            }
            turso_core::index_method::experimental_dense_ivf::take_candidate_visits();
            let start = Instant::now();
            let got: Vec<(i64, f64)> = conn.exec_rows(&sql);
            let visits = turso_core::index_method::experimental_dense_ivf::take_candidate_visits();
            times[i % 5].push(start.elapsed().as_secs_f64() * 1000.0);
            if nprobe == K {
                assert_eq!(got, oracle[i], "all clusters query {i}");
            }
            let hits = got
                .iter()
                .filter(|g| oracle[i].iter().any(|e| e.0 == g.0))
                .count();
            recalls[i % 5].push(if oracle[i].is_empty() {
                1.0
            } else {
                hits as f64 / oracle[i].len() as f64
            });
            let mut ranked: Vec<_> = centroids
                .iter()
                .enumerate()
                .map(|(c, v)| (c, vector_distance_cos(&queries[i], v).unwrap()))
                .collect();
            ranked.sort_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)));
            let expected_visits = if i % 5 < 4 {
                ranked
                    .iter()
                    .take(nprobe)
                    .map(|(c, _)| cluster_counts[i % 5][*c])
                    .sum::<usize>()
            } else {
                0
            };
            assert_eq!(visits as usize, expected_visits);
            candidates[i % 5] += visits as usize;
        }
        let update_start = Instant::now();
        conn.execute("BEGIN").unwrap();
        conn.execute("UPDATE v SET e=e WHERE id<1000").unwrap();
        conn.execute("COMMIT").unwrap();
        let update_ms = update_start.elapsed().as_secs_f64() * 1000.0;
        let update_wal_bytes = std::fs::metadata(format!("{}-wal", db.path.display()))
            .map(|m| m.len())
            .unwrap_or(0);
        let path = db.path.clone();
        drop(conn);
        drop(db);
        let restart_start = Instant::now();
        db = TempDatabase::new_with_existent_with_opts(
            &path,
            turso_core::DatabaseOpts::new().with_index_method(true),
        );
        conn = db.connect_limbo();
        let _: Vec<(i64, f64)> = conn.exec_rows(&query_sql(0, true));
        let restart_ms = restart_start.elapsed().as_secs_f64() * 1000.0;
        println!("DENSE_STORAGE nprobe={nprobe} build_ms={build_ms:.3} file_bytes={file_bytes} allocated_bytes={allocated} incremental_allocated_bytes={} update_1000_ms={update_ms:.3} update_wal_bytes={update_wal_bytes} reopen_first_query_ms={restart_ms:.3}",allocated-base_bytes as i64);
        for t in 0..5 {
            println!("DENSE_RECALL nprobe={nprobe} tenant={t} recall_at_10={:.6} candidate_visits_mean={:.1} {}",recalls[t].iter().sum::<f64>()/recalls[t].len() as f64,candidates[t] as f64/times[t].len() as f64,percentiles(&times[t]));
        }
        conn.execute("DROP INDEX vi").unwrap();
    }
}

fn vector_text(v: &turso_core::vector::vector_types::Vector<'_>) -> String {
    format!(
        "[{}]",
        v.as_f32_slice()
            .iter()
            .map(|v| v.to_string())
            .collect::<Vec<_>>()
            .join(",")
    )
}
fn nearest(
    v: &turso_core::vector::vector_types::Vector<'_>,
    centroids: &[turso_core::vector::vector_types::Vector<'_>],
) -> usize {
    use turso_core::vector::operations::distance_cos::vector_distance_cos;
    centroids
        .iter()
        .enumerate()
        .map(|(i, c)| (i, vector_distance_cos(v, c).unwrap()))
        .min_by(|a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)))
        .unwrap()
        .0
}
fn percentiles(times: &[f64]) -> String {
    let mut times = times.to_vec();
    times.sort_by(f64::total_cmp);
    let p = |q: f64| times[((times.len() as f64 * q).ceil() as usize).saturating_sub(1)];
    format!(
        "p50_ms={:.3} p95_ms={:.3} p99_ms={:.3}",
        p(0.5),
        p(0.95),
        p(0.99)
    )
}

#[turso_macros::test]
fn dense_ivf_rejects_invalid_configuration_and_query_shapes(tmp_db: TempDatabase) {
    let conn = tmp_db.connect_limbo();
    conn.execute("CREATE TABLE v(id INTEGER PRIMARY KEY,e BLOB,tenant INTEGER)")
        .unwrap();
    for sql in [
        CREATE.replace("version=1", "version=2"),
        CREATE.replace("nprobe=4", "nprobe=5"),
        CREATE.replace("dimensions=2", "dimensions=3"),
        CREATE.replace("'[1,0];[0,1];[-1,0];[0,-1]'", "'[0,0]'"),
        CREATE.replace("'cosine'", "'l2'"),
    ] {
        assert!(conn.execute(sql).is_err());
    }
    conn.execute(CREATE).unwrap();
    for sql in [
        QUERY.replace("tenant=7", "tenant>=7"),
        QUERY.replace("tenant=7", "tenant=7 AND id>3"),
        QUERY.replace("ORDER BY distance, rowid", "ORDER BY distance DESC, rowid"),
        format!("{QUERY} OFFSET 2"),
    ] {
        let plan: Vec<(i64, i64, i64, String)> =
            conn.exec_rows(&format!("EXPLAIN QUERY PLAN {sql}"));
        assert!(
            !plan.iter().any(|r| r.3.contains("INDEX METHOD")),
            "{sql}: {plan:?}"
        );
    }
    conn.execute("DROP TABLE v").unwrap();
    let rows: Vec<(String,)> = conn.exec_rows("SELECT name FROM sqlite_schema");
    assert!(rows.is_empty(), "{rows:?}");
}
