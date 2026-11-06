use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    path::Path,
    sync::{mpsc, Arc},
    time::{Duration, Instant},
};
use turso_core::{ConnectOpts, Connection, Database, PlatformIO, Statement, Value};
use turso_sqlite::SqliteDialect;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const MAX_RESPONSE: usize = 16 * 1024 * 1024;

#[derive(Clone, Copy, Debug)]
struct Request {
    kind: usize,
    tenant: usize,
    argument: usize,
    field: usize,
}
impl Request {
    fn wire(self) -> String {
        format!(
            "{} {} {} {}",
            self.kind, self.tenant, self.argument, self.field
        )
    }
    fn parse(line: &str) -> Result<Self> {
        let values = line
            .split_whitespace()
            .map(str::parse::<usize>)
            .collect::<std::result::Result<Vec<_>, _>>()?;
        if values.len() != 4 || values[0] > 2 || values[1] >= 10 || values[3] >= 16 {
            return Err("invalid request".into());
        }
        Ok(Self {
            kind: values[0],
            tenant: values[1],
            argument: values[2],
            field: values[3],
        })
    }
}

fn document(id: usize) -> String {
    let fields = (0..16)
        .map(|f| format!("\"f{f}\":{}", (id + f) % 17))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{{\"version\":1,\"custom\":{{{fields}}},\"padding\":\"{}\"}}",
        "x".repeat(768)
    )
}
fn oracle(request: Request, rows: usize) -> Vec<u8> {
    let mut result = String::new();
    for id in 0..rows {
        if id % 10 != request.tenant {
            continue;
        }
        let matches = match request.kind {
            0 => id == request.argument,
            1 => id % 17 == request.argument,
            _ => (id + request.field) % 17 == request.argument,
        };
        if matches {
            if request.kind == 0 {
                result.push_str(&format!("{id}|{}|1|{}\n", id % 10, document(id)));
            } else {
                result.push_str(&format!("{id}|{}|1\n", id % 10));
            }
        }
    }
    result.into_bytes()
}
fn connect(path: &Path) -> Result<(Arc<Database>, Arc<Connection>)> {
    #[allow(clippy::arc_with_non_send_sync)]
    let io = Arc::new(PlatformIO::new()?);
    let db = Database::open_file(
        io,
        path.to_str().ok_or("path is not UTF-8")?,
        Arc::new(SqliteDialect),
    )?;
    let conn = db.connect(ConnectOpts::new())?;
    conn.execute("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA cache_size=-65536")?;
    Ok((db, conn))
}
fn initialize(path: &Path, rows: usize) -> Result<()> {
    if path.exists() {
        return Err("refusing to overwrite existing database".into());
    }
    let (_db, conn) = connect(path)?;
    conn.execute("CREATE TABLE documents(id INTEGER PRIMARY KEY,tenant INTEGER,version INTEGER,category INTEGER,payload BLOB); CREATE INDEX tenant_category ON documents(tenant,category); BEGIN")?;
    let mut statement = conn.prepare("INSERT INTO documents VALUES(?1,?2,1,?3,jsonb(?4))")?;
    for id in 0..rows {
        bind(
            &mut statement,
            &[
                Value::from_i64(id as i64),
                Value::from_i64((id % 10) as i64),
                Value::from_i64((id % 17) as i64),
                Value::build_text(document(id)),
            ],
        )?;
        statement.run_ignore_rows()?;
        statement.reset()?;
    }
    conn.execute("COMMIT; PRAGMA wal_checkpoint(TRUNCATE)")?;
    conn.close()?;
    Ok(())
}
fn bind(statement: &mut Statement, values: &[Value]) -> Result<()> {
    for (i, value) in values.iter().enumerate() {
        statement.bind_at((i + 1).try_into()?, value.clone())?;
    }
    Ok(())
}
struct Local {
    _db: Arc<Database>,
    _conn: Arc<Connection>,
    statements: Vec<Statement>,
}
impl Local {
    fn new(path: &Path) -> Result<Self> {
        let (db, conn) = connect(path)?;
        let mut statements=vec![conn.prepare("SELECT id,tenant,version,json(payload) FROM documents WHERE tenant=?1 AND id=?2")?,conn.prepare("SELECT id,tenant,version FROM documents WHERE tenant=?1 AND category=?2 ORDER BY id")?];
        for field in 0..16 {
            statements.push(conn.prepare(format!("SELECT id,tenant,version FROM documents WHERE tenant=?1 AND json_extract(payload,'$.custom.f{field}')=?2 ORDER BY id"))?);
        }
        Ok(Self {
            _db: db,
            _conn: conn,
            statements,
        })
    }
    fn request(&mut self, request: Request) -> Result<Vec<u8>> {
        let index = if request.kind < 2 {
            request.kind
        } else {
            2 + request.field
        };
        let statement = &mut self.statements[index];
        bind(
            statement,
            &[
                Value::from_i64(request.tenant as i64),
                Value::from_i64(request.argument as i64),
            ],
        )?;
        let mut response = String::new();
        statement.run_with_row_callback(|row| {
            let values = row
                .get_values()
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            response.push_str(&values.join("|"));
            response.push('\n');
            Ok(())
        })?;
        statement.reset()?;
        Ok(response.into_bytes())
    }
}
fn read_frame(reader: &mut BufReader<TcpStream>) -> Result<Vec<u8>> {
    let mut size = String::new();
    reader.read_line(&mut size)?;
    let count: usize = size.trim().parse()?;
    if count > MAX_RESPONSE {
        return Err("oversized response".into());
    }
    let mut bytes = vec![0; count];
    reader.read_exact(&mut bytes)?;
    Ok(bytes)
}
fn serve(path: &Path, address: &str) -> Result<()> {
    let listener = TcpListener::bind(address)?;
    if !listener.local_addr()?.ip().is_loopback() {
        return Err("test service requires a loopback address".into());
    }
    println!("{}", listener.local_addr()?);
    std::io::stdout().flush()?;
    for stream in listener.incoming() {
        let stream = stream?;
        stream.set_nodelay(true)?;
        let path = path.to_owned();
        std::thread::spawn(move || {
            let result = (|| -> Result<()> {
                let mut local = Local::new(&path)?;
                let mut reader = BufReader::new(stream);
                reader.get_mut().write_all(b"READY\n")?;
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line)? == 0 {
                        break;
                    }
                    let response = local.request(Request::parse(&line)?)?;
                    writeln!(reader.get_mut(), "{}", response.len())?;
                    reader.get_mut().write_all(&response)?;
                }
                Ok(())
            })();
            if let Err(error) = result {
                eprintln!("service connection failed: {error}");
            }
        });
    }
    Ok(())
}
struct Redis {
    socket: BufReader<TcpStream>,
}
impl Redis {
    fn new(address: &str) -> Result<Self> {
        let stream = TcpStream::connect(address)?;
        stream.set_nodelay(true)?;
        stream.set_read_timeout(Some(Duration::from_secs(30)))?;
        Ok(Self {
            socket: BufReader::new(stream),
        })
    }
    fn command(&mut self, parts: &[&[u8]]) -> Result<Option<Vec<u8>>> {
        let mut request = format!("*{}\r\n", parts.len()).into_bytes();
        for part in parts {
            request.extend_from_slice(format!("${}\r\n", part.len()).as_bytes());
            request.extend_from_slice(part);
            request.extend_from_slice(b"\r\n");
        }
        self.socket.get_mut().write_all(&request)?;
        let mut header = String::new();
        self.socket.read_line(&mut header)?;
        if header == "$-1\r\n" {
            return Ok(None);
        }
        if let Some(value) = header.strip_prefix('+') {
            return Ok(Some(value.trim().as_bytes().to_vec()));
        }
        if !header.starts_with('$') {
            return Err(format!("unexpected Redis response: {header}").into());
        }
        let size: usize = header[1..].trim().parse()?;
        if size > MAX_RESPONSE {
            return Err("oversized Redis value".into());
        }
        let mut value = vec![0; size];
        self.socket.read_exact(&mut value)?;
        let mut ending = [0; 2];
        self.socket.read_exact(&mut ending)?;
        if ending != *b"\r\n" {
            return Err("invalid Redis response ending".into());
        }
        Ok(Some(value))
    }
    fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>> {
        self.command(&[b"GET", key.as_bytes()])
    }
    fn set(&mut self, key: &str, value: &[u8]) -> Result<()> {
        if self.command(&[b"SET", key.as_bytes(), value])? != Some(b"OK".to_vec()) {
            return Err("SET failed".into());
        }
        Ok(())
    }
}
enum DatabasePath {
    Embedded(Local),
    Shared(BufReader<TcpStream>),
}
impl DatabasePath {
    fn new(path: &Path, address: &str) -> Result<Self> {
        if address == "embedded" {
            Ok(Self::Embedded(Local::new(path)?))
        } else {
            let stream = TcpStream::connect(address)?;
            stream.set_nodelay(true)?;
            stream.set_read_timeout(Some(Duration::from_secs(30)))?;
            let mut socket = BufReader::new(stream);
            let mut ready = String::new();
            socket.read_line(&mut ready)?;
            if ready != "READY\n" {
                return Err("database service did not complete preparation".into());
            }
            Ok(Self::Shared(socket))
        }
    }
    fn request(&mut self, request: Request) -> Result<Vec<u8>> {
        match self {
            Self::Embedded(local) => local.request(request),
            Self::Shared(socket) => {
                writeln!(socket.get_mut(), "{}", request.wire())?;
                read_frame(socket)
            }
        }
    }
}
struct Application {
    database: DatabasePath,
    redis: Option<Redis>,
    prefix: String,
}
impl Application {
    fn request(&mut self, request: Request) -> Result<(Vec<u8>, bool)> {
        let key = format!("{}:{}", self.prefix, request.wire());
        if let Some(redis) = &mut self.redis {
            if let Some(value) = redis.get(&key)? {
                return Ok((value, true));
            }
        }
        let value = self.database.request(request)?;
        if let Some(redis) = &mut self.redis {
            redis.set(&key, &value)?;
        }
        Ok((value, false))
    }
}
fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.into())
}
fn scheduled_request(sequence: usize, rows: usize, hot: bool) -> Request {
    let index = if hot && sequence % 10 != 0 {
        sequence % 10
    } else {
        sequence
    };
    let kind = index % 3;
    let tenant = (index / 3) % 10;
    let argument = if kind == 0 {
        (index * 37) % (rows + 10)
    } else {
        (index / 30) % 18
    };
    Request {
        kind,
        tenant,
        argument,
        field: if kind == 2 { (index / 540) % 16 } else { 0 },
    }
}
fn run(path: &Path, rows: usize) -> Result<()> {
    let workers: usize = env("APP_WORKERS", "1").parse()?;
    let requests: usize = env("APP_REQUESTS", "1000").parse()?;
    let rate: u64 = env("APP_RATE", "0").parse()?;
    if !(1..=32).contains(&workers) || !(1..=100000).contains(&requests) || rate > 1000000 {
        return Err("invalid workload size".into());
    }
    let address = env("APP_DATABASE", "embedded");
    let redis = env("APP_REDIS", "");
    let prefix = env("APP_PREFIX", "turso-cache-probe");
    let warm = env("APP_WARM", "false") == "true";
    let hot = env("APP_HOT", "false") == "true";
    let (ready_tx, ready_rx) = mpsc::channel();
    let mut starters = Vec::new();
    let mut threads = Vec::new();
    for worker in 0..workers {
        let path = path.to_owned();
        let address = address.clone();
        let redis = redis.clone();
        let prefix = prefix.clone();
        let ready = ready_tx.clone();
        let (start_tx, start_rx) = mpsc::channel();
        starters.push(start_tx);
        threads.push(std::thread::spawn(
            move || -> Result<Vec<(u128, u128, bool, usize)>> {
                let prepared = (|| -> Result<_> {
                    let mut app = Application {
                        database: DatabasePath::new(&path, &address)?,
                        redis: if redis.is_empty() {
                            None
                        } else {
                            Some(Redis::new(&redis)?)
                        },
                        prefix,
                    };
                    let workload = (worker..requests)
                        .step_by(workers)
                        .map(|seq| {
                            let request = scheduled_request(seq, rows, hot);
                            (seq, request, oracle(request, rows))
                        })
                        .collect::<Vec<_>>();
                    if warm {
                        for (_, request, expected) in &workload {
                            if &app.request(*request)?.0 != expected {
                                return Err("warmup oracle mismatch".into());
                            }
                        }
                    }
                    Ok((app, workload))
                })();
                let (mut app, workload) = match prepared {
                    Ok(value) => {
                        ready.send(Ok(()))?;
                        value
                    }
                    Err(error) => {
                        ready.send(Err(error.to_string()))?;
                        return Err(error);
                    }
                };
                let origin: Instant = start_rx.recv()?;
                let mut results = Vec::with_capacity(workload.len());
                for (sequence, request, expected) in workload {
                    let due = if rate == 0 {
                        Instant::now()
                    } else {
                        origin + Duration::from_nanos(sequence as u64 * 1000000000 / rate)
                    };
                    if let Some(delay) = due.checked_duration_since(Instant::now()) {
                        std::thread::sleep(delay);
                    }
                    let actual = Instant::now();
                    let (response, hit) = app.request(request)?;
                    let finished = Instant::now();
                    if response != expected {
                        return Err(format!("oracle mismatch for {}", request.wire()).into());
                    }
                    results.push((
                        finished.duration_since(due).as_nanos(),
                        actual.saturating_duration_since(due).as_nanos(),
                        hit,
                        request.kind,
                    ));
                }
                Ok(results)
            },
        ));
    }
    drop(ready_tx);
    for _ in 0..workers {
        ready_rx
            .recv()?
            .map_err(|error| format!("worker preparation failed: {error}"))?;
    }
    let start = Instant::now();
    for starter in starters {
        starter.send(start)?;
    }
    let mut results = Vec::new();
    for thread in threads {
        results.extend(thread.join().map_err(|_| "worker panicked")??);
    }
    let elapsed = start.elapsed().as_secs_f64();
    if let Ok(path) = std::env::var("APP_TRACE") {
        let mut file = std::fs::File::create(path)?;
        writeln!(file, "kind,arrival_latency_ns,late_start_ns,cache_hit")?;
        for (latency, late, hit, kind) in &results {
            writeln!(file, "{kind},{latency},{late},{hit}")?;
        }
    }
    println!("path,cache,warm,hot,workers,rate,requests,elapsed_s,throughput,p50_us,p95_us,p99_us,late_p99_us,hits,misses");
    let mut latency = results.iter().map(|r| r.0).collect::<Vec<_>>();
    latency.sort_unstable();
    let mut late = results.iter().map(|r| r.1).collect::<Vec<_>>();
    late.sort_unstable();
    let p = |values: &[u128], percent: usize| {
        values[(values.len() * percent).div_ceil(100) - 1] as f64 / 1000.0
    };
    for (label, hit) in [("hit", true), ("miss", false)] {
        let mut subset = results
            .iter()
            .filter(|r| r.2 == hit)
            .map(|r| r.0)
            .collect::<Vec<_>>();
        subset.sort_unstable();
        if !subset.is_empty() {
            eprintln!(
                "{label} count={} p50_us={:.3} p95_us={:.3} p99_us={:.3}",
                subset.len(),
                p(&subset, 50),
                p(&subset, 95),
                p(&subset, 99)
            );
        }
    }
    for kind in 0..3 {
        let mut service = results
            .iter()
            .filter(|r| r.3 == kind)
            .map(|r| r.0 - r.1)
            .collect::<Vec<_>>();
        service.sort_unstable();
        if !service.is_empty() {
            eprintln!("kind={kind} service_count={} service_p50_us={:.3} service_p95_us={:.3} service_p99_us={:.3}",service.len(),p(&service,50),p(&service,95),p(&service,99));
        }
    }
    let hits = results.iter().filter(|r| r.2).count();
    println!("{address},{},{warm},{hot},{workers},{rate},{},{elapsed:.6},{:.3},{:.3},{:.3},{:.3},{:.3},{hits},{}",!redis.is_empty(),results.len(),results.len()as f64/elapsed,p(&latency,50),p(&latency,95),p(&latency,99),p(&late,99),results.len()-hits);
    Ok(())
}
fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() < 3 {
        return Err("usage: cache_application_probe init|serve|run DATABASE [rows|address]".into());
    }
    let path = Path::new(&args[2]);
    match args[1].as_str() {
        "serve" => serve(
            path,
            args.get(3).map(String::as_str).unwrap_or("127.0.0.1:0"),
        ),
        "init" | "run" => {
            let rows: usize = args.get(3).map(String::as_str).unwrap_or("1000").parse()?;
            if !(100..=10000).contains(&rows) {
                return Err("rows out of bounds".into());
            }
            if args[1] == "init" {
                initialize(path, rows)
            } else {
                run(path, rows)
            }
        }
        _ => Err("unknown mode".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn shared_connection_waits_for_server_preparation() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let (accepted_tx, accepted_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            accepted_tx.send(()).unwrap();
            release_rx.recv().unwrap();
            socket.write_all(b"READY\n").unwrap();
        });
        let client = std::thread::spawn(move || {
            let connection = DatabasePath::new(Path::new("unused"), &address);
            done_tx.send(connection.is_ok()).unwrap();
        });
        accepted_rx.recv().unwrap();
        let premature = done_rx.recv_timeout(Duration::from_millis(100));
        release_tx.send(()).unwrap();
        server.join().unwrap();
        client.join().unwrap();
        assert!(matches!(premature, Err(mpsc::RecvTimeoutError::Timeout)));
        assert!(done_rx.recv().unwrap());
    }
    #[test]
    fn local_requests_match_independent_oracle() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("app.db");
        initialize(&path, 100).unwrap();
        let mut local = Local::new(&path).unwrap();
        for seq in 0..10000 {
            let request = scheduled_request(seq, 100, false);
            assert_eq!(local.request(request).unwrap(), oracle(request, 100));
        }
    }
    #[test]
    fn request_parser_rejects_invalid_shapes() {
        for value in ["", "0 1", "3 0 1 0", "0 10 1 0", "0 1 1 16"] {
            assert!(Request::parse(value).is_err());
        }
    }
}
