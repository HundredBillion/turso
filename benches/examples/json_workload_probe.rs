use std::{hint::black_box, sync::Arc, time::Instant};
use turso_core::{ConnectOpts, Database, PlatformIO, Statement, StepResult, Value};
use turso_sqlite::SqliteDialect;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let rows: usize = std::env::var("PROBE_ROWS")
        .unwrap_or_else(|_| "1000".into())
        .parse()?;
    let samples: usize = std::env::var("PROBE_SAMPLES")
        .unwrap_or_else(|_| "1000".into())
        .parse()?;
    assert!((100..=10000).contains(&rows) && (1..=100000).contains(&samples));
    eprintln!("rows={rows} samples={samples} clients=1 warmup=100 seed=17 durability=FULL deployment=embedded");
    println!(
        "document_bytes,representation,query,samples,p50_us,p95_us,p99_us,inverse_mean_statement_seconds"
    );
    for size in [1024, 10240, 102400] {
        for representation in ["text", "jsonb", "typed"] {
            probe(size, representation, rows, samples)?;
        }
    }
    Ok(())
}

fn probe(
    size: usize,
    representation: &str,
    rows: usize,
    samples: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("probe.db");
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new()?);
    let db = Database::open_file(
        io,
        path.to_str().expect("temporary path is UTF-8"),
        Arc::new(SqliteDialect),
    )?;
    let conn = db.connect(ConnectOpts::new())?;
    for sql in [
        "PRAGMA journal_mode=WAL",
        "PRAGMA synchronous=FULL",
        "PRAGMA cache_size=-65536",
        "CREATE TABLE products(id INTEGER PRIMARY KEY, tenant INTEGER, category INTEGER, price INTEGER, attributes)",
        "BEGIN",
    ] {
        drive(&db, &mut conn.prepare(sql)?)?;
    }
    let expression = if representation == "jsonb" {
        "jsonb(?5)"
    } else {
        "?5"
    };
    let mut insert = conn.prepare(format!(
        "INSERT INTO products VALUES(?1,?2,?3,?4,{expression})"
    ))?;
    for id in 0..rows {
        let prefix = format!(
            "{{\"category\":{},\"price\":{},\"padding\":\"",
            id % 7,
            id % 1000
        );
        let suffix = "\",\"last\":42}";
        let document = format!(
            "{prefix}{}{suffix}",
            "x".repeat(size - prefix.len() - suffix.len())
        );
        for (offset, value) in [id, id % 10, id % 7, id % 1000].into_iter().enumerate() {
            insert.bind_at((offset + 1).try_into()?, Value::from_i64(value as i64))?;
        }
        insert.bind_at(5usize.try_into()?, Value::build_text(document))?;
        drive(&db, &mut insert)?;
    }
    drive(&db, &mut conn.prepare("COMMIT")?)?;
    for pragma in ["journal_mode", "synchronous", "cache_size"] {
        let mut statement = conn.prepare(format!("PRAGMA {pragma}"))?;
        loop {
            match statement.step()? {
                StepResult::Row => {
                    eprintln!(
                        "{size}/{representation} {pragma}: {:?}",
                        statement
                            .row()
                            .expect("pragma row")
                            .get_values()
                            .collect::<Vec<_>>()
                    )
                }
                StepResult::Done => break,
                StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => db.io.step()?,
                other => return Err(format!("unexpected pragma result: {other:?}").into()),
            }
        }
    }
    let category = if representation == "typed" {
        "category"
    } else {
        "json_extract(attributes,'$.category')"
    };
    let price = if representation == "typed" {
        "price"
    } else {
        "json_extract(attributes,'$.price')"
    };
    let lookup = format!("SELECT {price} FROM products WHERE id=?1");
    measure(
        &db,
        &conn,
        size,
        representation,
        "point",
        &lookup,
        rows,
        samples,
        false,
    )?;
    let facet = format!(
        "SELECT id FROM products WHERE tenant=?1 AND {category}=3 ORDER BY {price},id LIMIT 20"
    );
    measure(
        &db,
        &conn,
        size,
        representation,
        "facet_scan",
        &facet,
        rows,
        samples,
        true,
    )?;
    drive(
        &db,
        &mut conn.prepare(format!(
            "CREATE INDEX product_filter ON products(tenant,{category},{price},id)"
        ))?,
    )?;
    measure(
        &db,
        &conn,
        size,
        representation,
        "facet_index",
        &facet,
        rows,
        samples,
        true,
    )?;
    let update = match representation {
        "typed" => "UPDATE products SET price=price+1 WHERE id=?1",
        "jsonb" => "UPDATE products SET attributes=jsonb_set(attributes,'$.price',json_extract(attributes,'$.price')+1) WHERE id=?1",
        _ => "UPDATE products SET attributes=json_set(attributes,'$.price',json_extract(attributes,'$.price')+1) WHERE id=?1",
    };
    measure(
        &db,
        &conn,
        size,
        representation,
        "durable_update",
        update,
        rows,
        samples,
        false,
    )?;
    let mut sum = conn.prepare(format!("SELECT sum({price}) FROM products"))?;
    loop {
        match sum.step()? {
            StepResult::Row => assert_eq!(
                sum.row().expect("sum row").get_value(0).as_int(),
                Some(((0..rows).map(|id| id % 1000).sum::<usize>() + samples + 100) as i64)
            ),
            StepResult::Done => break,
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => db.io.step()?,
            other => return Err(format!("unexpected sum result: {other:?}").into()),
        }
    }
    eprintln!(
        "{size}/{representation} database_bytes={} wal_bytes={}",
        std::fs::metadata(&path)?.len(),
        std::fs::metadata(path.with_extension("db-wal"))
            .map(|m| m.len())
            .unwrap_or(0)
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn measure(
    db: &Database,
    conn: &Arc<turso_core::Connection>,
    size: usize,
    representation: &str,
    label: &str,
    sql: &str,
    rows: usize,
    samples: usize,
    facet: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut plan = conn.prepare(format!("EXPLAIN QUERY PLAN {sql}"))?;
    loop {
        match plan.step()? {
            StepResult::Row => eprintln!(
                "{size}/{representation}/{label} plan: {:?}",
                plan.row()
                    .expect("query plan row")
                    .get_values()
                    .collect::<Vec<_>>()
            ),
            StepResult::Done => break,
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => db.io.step()?,
            other => return Err(format!("unexpected plan result: {other:?}").into()),
        }
    }
    let mut statement = conn.prepare(sql)?;
    let mut latencies = Vec::with_capacity(samples);
    let mut state = 17u64;
    for sample in 0..samples + 100 {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let parameter = (state >> 32) as usize % if facet { 10 } else { rows };
        let start = Instant::now();
        statement.bind_at(1usize.try_into()?, Value::from_i64(parameter as i64))?;
        let actual = drive(db, &mut statement)?;
        let elapsed = start.elapsed().as_nanos();
        let expected = if label == "point" {
            1
        } else if facet {
            (0..rows)
                .filter(|id| id % 10 == parameter && id % 7 == 3)
                .count()
                .min(20)
        } else {
            0
        };
        assert_eq!(actual, expected, "{representation}/{label}");
        if sample >= 100 {
            latencies.push(elapsed);
        }
    }
    let total: u128 = latencies.iter().sum();
    latencies.sort_unstable();
    let percentile = |p: usize| latencies[(samples * p).div_ceil(100) - 1] as f64 / 1000.0;
    println!(
        "{size},{representation},{label},{samples},{:.3},{:.3},{:.3},{:.1}",
        percentile(50),
        percentile(95),
        percentile(99),
        samples as f64 * 1e9 / total as f64
    );
    Ok(())
}

fn drive(db: &Database, statement: &mut Statement) -> turso_core::Result<usize> {
    let mut count = 0;
    loop {
        match statement.step()? {
            StepResult::Row => {
                black_box(statement.row());
                count += 1;
            }
            StepResult::Done => break,
            StepResult::IO | StepResult::Yield | StepResult::Sleep { .. } => db.io.step()?,
            other => panic!("unexpected statement result: {other:?}"),
        }
    }
    statement.reset()?;
    Ok(count)
}
