use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    path::Path,
    sync::{mpsc, Arc, RwLock},
    time::{Duration, Instant},
};
use turso_core::{Connection, Value};

#[cfg(test)]
#[allow(dead_code)]
#[path = "../../tests/integration/unreliable_io.rs"]
mod unreliable_io;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
const INITIAL_INVENTORY: i64 = 1_000_000;

#[allow(dead_code)]
mod immutable {
    include!("cache_application_probe.rs");

    pub(super) struct Engine(Local);
    impl Engine {
        pub(super) fn new(path: &Path) -> super::Result<Self> {
            Ok(Self(Local::new(path)?))
        }
        pub(super) fn read(&mut self, request: super::ReadRequest) -> super::Result<Vec<u8>> {
            self.0.request(Request {
                kind: request.kind,
                tenant: request.tenant,
                argument: request.argument,
                field: request.field,
            })
        }
        pub(super) fn connection(&self) -> Arc<Connection> {
            self.0._conn.clone()
        }
    }
    pub(super) struct Cache(Redis);
    impl Cache {
        pub(super) fn new(address: &str) -> super::Result<Self> {
            Ok(Self(Redis::new(address)?))
        }
        pub(super) fn get(&mut self, key: &str) -> super::Result<Option<Vec<u8>>> {
            self.0.get(key)
        }
        pub(super) fn set(&mut self, key: &str, value: &[u8]) -> super::Result<()> {
            self.0.set(key, value)
        }
    }
    pub(super) fn scheduled(sequence: usize, rows: usize, hot: bool) -> super::ReadRequest {
        let request = scheduled_request(sequence, rows, hot);
        super::ReadRequest {
            kind: request.kind,
            tenant: request.tenant,
            argument: request.argument,
            field: request.field,
        }
    }
    pub(super) fn seed(path: &Path, rows: usize) -> super::Result<()> {
        initialize(path, rows)
    }
    pub(super) fn frame(socket: &mut BufReader<TcpStream>) -> super::Result<Vec<u8>> {
        read_frame(socket)
    }
}

#[derive(Clone, Copy, Debug)]
struct ReadRequest {
    kind: usize,
    tenant: usize,
    argument: usize,
    field: usize,
}
impl ReadRequest {
    fn wire(self) -> String {
        format!(
            "R {} {} {} {}",
            self.kind, self.tenant, self.argument, self.field
        )
    }
}
#[derive(Clone, Debug)]
struct Model {
    versions: Vec<usize>,
    inventory: Vec<i64>,
    orders: Vec<(usize, usize)>,
}
impl Model {
    fn new(rows: usize) -> Self {
        Self {
            versions: vec![1; rows],
            inventory: vec![INITIAL_INVENTORY; rows],
            orders: Vec::new(),
        }
    }
    fn apply(&mut self, id: usize, generation: usize) {
        self.versions[id] = generation;
        self.inventory[id] -= 1;
        self.orders.push((generation, id));
    }
    fn expected(&self, request: ReadRequest) -> Vec<u8> {
        let mut response = String::new();
        for (id, version) in self.versions.iter().copied().enumerate() {
            if id % 10 != request.tenant {
                continue;
            }
            let matches = match request.kind {
                0 => id == request.argument,
                1 => category(id, version) == request.argument,
                2 => field_value(id, request.field, version) == request.argument,
                _ => false,
            };
            if matches {
                if request.kind == 0 {
                    response.push_str(&format!(
                        "{id}|{}|{version}|{}\n",
                        id % 10,
                        document(id, version)
                    ));
                } else {
                    response.push_str(&format!("{id}|{}|{version}\n", id % 10));
                }
            }
        }
        response.into_bytes()
    }
}
struct State {
    generation: usize,
    model: Arc<Model>,
    failed: bool,
}
impl State {
    fn new(rows: usize) -> Self {
        Self {
            generation: 1,
            model: Arc::new(Model::new(rows)),
            failed: false,
        }
    }
}

fn category(id: usize, version: usize) -> usize {
    (id + version - 1) % 17
}
fn field_value(id: usize, field: usize, version: usize) -> usize {
    (id + field + if field == 3 { version - 1 } else { 0 }) % 17
}
fn document(id: usize, version: usize) -> String {
    let fields = (0..16)
        .map(|field| format!("\"f{field}\":{}", field_value(id, field, version)))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{{\"version\":{version},\"custom\":{{{fields}}},\"padding\":\"{}\"}}",
        "x".repeat(768)
    )
}
fn initialize(path: &Path, rows: usize) -> Result<()> {
    immutable::seed(path, rows)?;
    let engine = immutable::Engine::new(path)?;
    let conn = engine.connection();
    conn.execute(format!("ALTER TABLE documents ADD COLUMN inventory INTEGER NOT NULL DEFAULT {INITIAL_INVENTORY}; CREATE TABLE orders(txn INTEGER PRIMARY KEY,document_id INTEGER NOT NULL,tenant INTEGER NOT NULL,quantity INTEGER NOT NULL,version INTEGER NOT NULL)"))?;
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")?;
    Ok(())
}
fn rows(conn: &Arc<Connection>, sql: &str) -> Result<Vec<Vec<Value>>> {
    let mut statement = conn.prepare(sql)?;
    let mut result = Vec::new();
    statement.run_with_row_callback(|row| {
        result.push(row.get_values().cloned().collect());
        Ok(())
    })?;
    Ok(result)
}
fn update(conn: &Arc<Connection>, id: usize, tenant: usize, generation: usize) -> Result<()> {
    update_before_commit(conn, id, tenant, generation, || Ok(()))
}
fn update_before_commit(
    conn: &Arc<Connection>,
    id: usize,
    tenant: usize,
    generation: usize,
    before_commit: impl FnOnce() -> Result<()>,
) -> Result<()> {
    if id % 10 != tenant || generation < 2 {
        return Err("invalid update identity or generation".into());
    }
    conn.execute("BEGIN IMMEDIATE")?;
    let transaction = (|| -> Result<()> {
        let mut statement=conn.prepare("UPDATE documents SET version=?1,category=?2,payload=jsonb(?3),inventory=inventory-1 WHERE id=?4 AND tenant=?5 AND inventory>0")?;
        for (index, value) in [
            Value::from_i64(generation as i64),
            Value::from_i64(category(id, generation) as i64),
            Value::build_text(document(id, generation)),
            Value::from_i64(id as i64),
            Value::from_i64(tenant as i64),
        ]
        .into_iter()
        .enumerate()
        {
            statement.bind_at((index + 1).try_into()?, value)?;
        }
        statement.run_ignore_rows()?;
        if rows(conn, "SELECT changes()")? != vec![vec![Value::from_i64(1)]] {
            return Err("inventory update must affect exactly one row".into());
        }
        let mut statement = conn.prepare("INSERT INTO orders VALUES(?1,?2,?3,1,?1)")?;
        for (index, value) in [generation, id, tenant].into_iter().enumerate() {
            statement.bind_at((index + 1).try_into()?, Value::from_i64(value as i64))?;
        }
        statement.run_ignore_rows()?;
        before_commit()?;
        conn.execute("COMMIT")?;
        Ok(())
    })();
    if transaction.is_err() {
        let _ = conn.execute("ROLLBACK");
    }
    transaction
}
fn verify(conn: &Arc<Connection>, model: &Model) -> Result<()> {
    let actual = rows(
        conn,
        "SELECT id,version,inventory FROM documents ORDER BY id",
    )?;
    let expected = model
        .versions
        .iter()
        .enumerate()
        .map(|(id, version)| {
            vec![
                Value::from_i64(id as i64),
                Value::from_i64(*version as i64),
                Value::from_i64(model.inventory[id]),
            ]
        })
        .collect::<Vec<_>>();
    if actual != expected {
        return Err("document versions/inventory differ from independent model".into());
    }
    for (id, row) in rows(
        conn,
        "SELECT id,tenant,category,json(payload) FROM documents ORDER BY id",
    )?
    .iter()
    .enumerate()
    {
        let version = model.versions[id];
        if row[0] != Value::from_i64(id as i64)
            || row[1] != Value::from_i64((id % 10) as i64)
            || row[2] != Value::from_i64(category(id, version) as i64)
            || row[3].to_string() != document(id, version)
        {
            return Err("document content differs from independent model".into());
        }
    }
    let expected = model
        .orders
        .iter()
        .map(|(generation, id)| {
            vec![
                Value::from_i64(*generation as i64),
                Value::from_i64(*id as i64),
                Value::from_i64((id % 10) as i64),
                Value::from_i64(1),
                Value::from_i64(*generation as i64),
            ]
        })
        .collect::<Vec<_>>();
    if rows(
        conn,
        "SELECT txn,document_id,tenant,quantity,version FROM orders ORDER BY txn",
    )? != expected
    {
        return Err("orders differ from independent model".into());
    }
    if !rows(conn,&format!("SELECT d.id FROM documents d LEFT JOIN orders o ON d.id=o.document_id GROUP BY d.id,d.inventory HAVING {INITIAL_INVENTORY}-d.inventory<>coalesce(sum(o.quantity),0)"))?.is_empty(){return Err("inventory/order invariant violated".into());}
    if rows(conn, "PRAGMA integrity_check")? != vec![vec![Value::build_text("ok")]] {
        return Err("integrity check failed".into());
    }
    Ok(())
}

enum Backend {
    Embedded(immutable::Engine),
    Shared(BufReader<TcpStream>),
}
impl Backend {
    fn new(path: &Path, address: &str) -> Result<Self> {
        if address == "embedded" {
            return Ok(Self::Embedded(immutable::Engine::new(path)?));
        }
        let socket = TcpStream::connect(address)?;
        socket.set_nodelay(true)?;
        socket.set_read_timeout(Some(Duration::from_secs(30)))?;
        let mut socket = BufReader::new(socket);
        let mut ready = String::new();
        socket.read_line(&mut ready)?;
        if ready != "READY\n" {
            return Err("mixed service preparation failed".into());
        }
        Ok(Self::Shared(socket))
    }
    fn send(socket: &mut BufReader<TcpStream>, command: &str) -> Result<Vec<u8>> {
        writeln!(socket.get_mut(), "{command}")?;
        let response = immutable::frame(socket)?;
        if response.starts_with(b"ERROR ") {
            return Err(String::from_utf8(response)?.into());
        }
        Ok(response)
    }
    fn read(&mut self, request: ReadRequest) -> Result<Vec<u8>> {
        match self {
            Self::Embedded(engine) => engine.read(request),
            Self::Shared(socket) => Self::send(socket, &request.wire()),
        }
    }
    fn update(&mut self, id: usize, tenant: usize, generation: usize) -> Result<()> {
        match self {
            Self::Embedded(engine) => update(&engine.connection(), id, tenant, generation),
            Self::Shared(socket) => {
                let response = Self::send(socket, &format!("W {tenant} {id} {generation}"))?;
                if response != format!("COMMITTED {generation}").as_bytes() {
                    return Err("uncertain write response".into());
                }
                Ok(())
            }
        }
    }
    fn checkpoint(&mut self) -> Result<Vec<u8>> {
        match self {
            Self::Embedded(engine) => checkpoint(&engine.connection()),
            Self::Shared(socket) => Self::send(socket, "C"),
        }
    }
}
fn checkpoint(conn: &Arc<Connection>) -> Result<Vec<u8>> {
    let result = rows(conn, "PRAGMA wal_checkpoint(PASSIVE)")?;
    if result.len() != 1 || result[0].len() != 3 {
        return Err("invalid checkpoint result".into());
    }
    Ok(result[0]
        .iter()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(":")
        .into_bytes())
}
fn serve(path: &Path, address: &str) -> Result<()> {
    let listener = TcpListener::bind(address)?;
    if !listener.local_addr()?.ip().is_loopback() {
        return Err("test service requires loopback".into());
    }
    println!("{}", listener.local_addr()?);
    std::io::stdout().flush()?;
    for stream in listener.incoming() {
        let stream = stream?;
        stream.set_nodelay(true)?;
        let path = path.to_owned();
        std::thread::spawn(move || {
            let result = (|| -> Result<()> {
                let mut engine = immutable::Engine::new(&path)?;
                let mut socket = BufReader::new(stream);
                socket.get_mut().write_all(b"READY\n")?;
                loop {
                    let mut line = String::new();
                    if socket.read_line(&mut line)? == 0 {
                        break;
                    }
                    let response = service_request(&mut engine, &line)
                        .unwrap_or_else(|error| format!("ERROR {error}").into_bytes());
                    writeln!(socket.get_mut(), "{}", response.len())?;
                    socket.get_mut().write_all(&response)?;
                }
                Ok(())
            })();
            if let Err(error) = result {
                eprintln!("mixed service connection failed: {error}");
            }
        });
    }
    Ok(())
}
fn service_request(engine: &mut immutable::Engine, line: &str) -> Result<Vec<u8>> {
    let mut parts = line.split_whitespace();
    let operation = parts.next().ok_or("empty command")?;
    let values = parts
        .map(str::parse::<usize>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    match (operation, values.as_slice()) {
        ("R", [kind, tenant, argument, field]) if *kind < 3 && *tenant < 10 && *field < 16 => {
            engine.read(ReadRequest {
                kind: *kind,
                tenant: *tenant,
                argument: *argument,
                field: *field,
            })
        }
        ("W", [tenant, id, generation]) if *tenant < 10 && *id < 10000 && *generation < 1000000 => {
            update(&engine.connection(), *id, *tenant, *generation)?;
            Ok(format!("COMMITTED {generation}").into_bytes())
        }
        ("C", []) => checkpoint(&engine.connection()),
        _ => Err("invalid mixed command".into()),
    }
}
struct Application {
    backend: Backend,
    cache: Option<immutable::Cache>,
    namespace: String,
    state: Arc<RwLock<State>>,
    gate_wait: Duration,
    model_update: Duration,
}
impl Application {
    fn read(&mut self, request: ReadRequest) -> Result<(Vec<u8>, Arc<Model>, bool)> {
        let waiting = Instant::now();
        let guard = self.state.read().map_err(|_| "application gate poisoned")?;
        self.gate_wait = waiting.elapsed();
        if guard.failed {
            return Err("application closed after uncertain write".into());
        }
        let snapshot = guard.model.clone();
        let key = format!("{}:{}:{}", self.namespace, guard.generation, request.wire());
        if let Some(cache) = &mut self.cache {
            if let Some(value) = cache.get(&key)? {
                return Ok((value, snapshot, true));
            }
        }
        let value = self.backend.read(request)?;
        if let Some(cache) = &mut self.cache {
            cache.set(&key, &value)?;
        }
        Ok((value, snapshot, false))
    }
    fn update(&mut self, id: usize) -> Result<usize> {
        let waiting = Instant::now();
        let mut guard = self
            .state
            .write()
            .map_err(|_| "application gate poisoned")?;
        self.gate_wait = waiting.elapsed();
        if guard.failed {
            return Err("application closed after uncertain write".into());
        }
        let generation = guard.generation + 1;
        if let Err(error) = self.backend.update(id, id % 10, generation) {
            guard.failed = true;
            return Err(error);
        }
        let model_started = Instant::now();
        Arc::make_mut(&mut guard.model).apply(id, generation);
        self.model_update = model_started.elapsed();
        guard.generation = generation;
        Ok(generation)
    }
}
fn namespace() -> Result<String> {
    let mut bytes = [0u8; 16];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}
struct Sample {
    sequence: usize,
    kind: usize,
    latency: u128,
    late: u128,
    gate: u128,
    service: u128,
    model_update: u128,
    hit: bool,
}
fn run(path: &Path, rows: usize) -> Result<()> {
    let env = |name: &str, default: &str| std::env::var(name).unwrap_or_else(|_| default.into());
    let workers: usize = env("APP_WORKERS", "1").parse()?;
    let requests: usize = env("APP_REQUESTS", "1000").parse()?;
    let rate: u64 = env("APP_RATE", "0").parse()?;
    let write_every: usize = env("APP_WRITE_EVERY", "20").parse()?;
    let checkpoint_every: usize = env("APP_CHECKPOINT_EVERY", "200").parse()?;
    let address = env("APP_DATABASE", "embedded");
    let redis = env("APP_REDIS", "");
    let warm = env("APP_WARM", "false") == "true";
    let hot = env("APP_HOT", "true") == "true";
    if !(100..=10000).contains(&rows)
        || !(1..=32).contains(&workers)
        || !(1..=100000).contains(&requests)
        || rate > 1000000
        || write_every < 2
        || checkpoint_every == 0
    {
        return Err("invalid mixed workload size".into());
    }
    let state = Arc::new(RwLock::new(State::new(rows)));
    verify(
        &immutable::Engine::new(path)?.connection(),
        &state.read().map_err(|_| "poisoned gate")?.model,
    )?;
    let namespace = namespace()?;
    let mut checkpointer = Backend::new(path, &address)?;
    let (completed_tx, completed_rx) = mpsc::channel();
    let checkpoint_thread = std::thread::spawn(move || -> Result<Vec<(usize, u128, String)>> {
        let mut count = 0;
        let mut checkpoints = Vec::new();
        while completed_rx.recv().is_ok() {
            count += 1;
            if count % checkpoint_every == 0 {
                let started = Instant::now();
                let result = checkpointer.checkpoint()?;
                checkpoints.push((
                    count,
                    started.elapsed().as_nanos(),
                    String::from_utf8(result)?,
                ));
            }
        }
        Ok(checkpoints)
    });
    let (ready_tx, ready_rx) = mpsc::channel();
    let mut starters = Vec::new();
    let mut threads = Vec::new();
    for worker in 0..workers {
        let path = path.to_owned();
        let address = address.clone();
        let redis = redis.clone();
        let namespace = namespace.clone();
        let state = state.clone();
        let ready = ready_tx.clone();
        let completed = completed_tx.clone();
        let (start_tx, start_rx) = mpsc::channel();
        starters.push(start_tx);
        threads.push(std::thread::spawn(move || -> Result<Vec<Sample>> {
            let prepared = (|| -> Result<Application> {
                let mut app = Application {
                    backend: Backend::new(&path, &address)?,
                    cache: if redis.is_empty() {
                        None
                    } else {
                        Some(immutable::Cache::new(&redis)?)
                    },
                    namespace,
                    state,
                    gate_wait: Duration::ZERO,
                    model_update: Duration::ZERO,
                };
                if warm {
                    for sequence in (worker..requests)
                        .step_by(workers)
                        .filter(|s| s % write_every != 0)
                    {
                        let request = immutable::scheduled(sequence, rows, hot);
                        let (actual, model, _) = app.read(request)?;
                        if actual != model.expected(request) {
                            return Err("warmup oracle mismatch".into());
                        }
                    }
                }
                Ok(app)
            })();
            let mut app = match prepared {
                Ok(app) => {
                    ready.send(Ok(()))?;
                    app
                }
                Err(error) => {
                    ready.send(Err(error.to_string()))?;
                    return Err(error);
                }
            };
            let origin: Instant = start_rx.recv()?;
            let mut samples = Vec::new();
            for sequence in (worker..requests).step_by(workers) {
                let due = if rate == 0 {
                    Instant::now()
                } else {
                    origin + Duration::from_nanos(sequence as u64 * 1_000_000_000 / rate)
                };
                if let Some(delay) = due.checked_duration_since(Instant::now()) {
                    std::thread::sleep(delay);
                }
                let started = Instant::now();
                let request = immutable::scheduled(sequence, rows, hot);
                let is_write = sequence % write_every == 0;
                let outcome = if is_write {
                    app.update((sequence * 37) % rows)?;
                    None
                } else {
                    Some(app.read(request)?)
                };
                let finished = Instant::now();
                let hit = outcome.as_ref().is_some_and(|result| result.2);
                if let Some((actual, model, _)) = outcome {
                    if actual != model.expected(request) {
                        return Err(format!("mixed oracle mismatch sequence {sequence}").into());
                    }
                }
                samples.push(Sample {
                    sequence,
                    kind: if is_write { 3 } else { request.kind },
                    latency: finished.duration_since(due).as_nanos(),
                    late: started.saturating_duration_since(due).as_nanos(),
                    gate: app.gate_wait.as_nanos(),
                    service: finished.duration_since(started).as_nanos(),
                    hit,
                    model_update: if is_write {
                        app.model_update.as_nanos()
                    } else {
                        0
                    },
                });
                completed.send(())?;
            }
            Ok(samples)
        }));
    }
    drop(ready_tx);
    drop(completed_tx);
    for _ in 0..workers {
        ready_rx
            .recv()?
            .map_err(|error| format!("mixed preparation: {error}"))?;
    }
    let start = Instant::now();
    for starter in starters {
        starter.send(start)?;
    }
    let mut samples = Vec::new();
    for thread in threads {
        samples.extend(thread.join().map_err(|_| "mixed worker panicked")??);
    }
    let request_elapsed = start.elapsed().as_secs_f64();
    let checkpoints = checkpoint_thread
        .join()
        .map_err(|_| "checkpoint worker panicked")??;
    let drained_elapsed = start.elapsed().as_secs_f64();
    let final_model = state.read().map_err(|_| "poisoned gate")?.model.clone();
    verify(&immutable::Engine::new(path)?.connection(), &final_model)?;
    let writes = samples.iter().filter(|sample| sample.kind == 3).count();
    if samples.len() != requests
        || writes != requests.div_ceil(write_every)
        || final_model.orders.len() != writes
    {
        return Err("mixed request/commit accounting mismatch".into());
    }
    samples.sort_by_key(|sample| sample.sequence);
    if let Ok(path) = std::env::var("APP_TRACE") {
        let mut file = std::fs::File::create(path)?;
        writeln!(
            file,
            "sequence,kind,arrival_latency_ns,late_start_ns,gate_wait_ns,service_ns,model_update_ns,cache_hit"
        )?;
        for sample in &samples {
            writeln!(
                file,
                "{},{},{},{},{},{},{},{}",
                sample.sequence,
                sample.kind,
                sample.latency,
                sample.late,
                sample.gate,
                sample.service,
                sample.model_update,
                sample.hit
            )?;
        }
    }
    for (count, elapsed, result) in &checkpoints {
        eprintln!("checkpoint completed_requests={count} elapsed_ns={elapsed} busy:log:checkpointed={result}");
    }
    let percentile = |values: &mut Vec<u128>, percent: usize| -> f64 {
        if values.is_empty() {
            return 0.0;
        }
        values.sort_unstable();
        values[(values.len() * percent).div_ceil(100) - 1] as f64 / 1000.0
    };
    for kind in 0..4 {
        let mut values = samples
            .iter()
            .filter(|sample| sample.kind == kind)
            .map(|sample| sample.latency)
            .collect::<Vec<_>>();
        eprintln!(
            "kind={kind} count={} p50_us={:.3} p95_us={:.3} p99_us={:.3}",
            values.len(),
            percentile(&mut values, 50),
            percentile(&mut values, 95),
            percentile(&mut values, 99)
        );
    }
    let mut model_cost = samples
        .iter()
        .filter(|sample| sample.kind == 3)
        .map(|sample| sample.model_update)
        .collect::<Vec<_>>();
    eprintln!(
        "included_model_publication total_ns={} p50_us={:.3} p99_us={:.3}",
        model_cost.iter().sum::<u128>(),
        percentile(&mut model_cost, 50),
        percentile(&mut model_cost, 99)
    );
    let hits = samples.iter().filter(|sample| sample.hit).count();
    let mut latency = samples
        .iter()
        .map(|sample| sample.latency)
        .collect::<Vec<_>>();
    let mut late = samples.iter().map(|sample| sample.late).collect::<Vec<_>>();
    let mut gate = samples.iter().map(|sample| sample.gate).collect::<Vec<_>>();
    println!("path,cache,warm,hot,workers,rate,requests,write_every,writes,elapsed_s,drained_s,throughput,p50_us,p95_us,p99_us,late_p99_us,gate_p99_us,hits,misses,checkpoints");
    println!("{address},{},{warm},{hot},{workers},{rate},{requests},{write_every},{writes},{request_elapsed:.6},{drained_elapsed:.6},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{hits},{},{}",!redis.is_empty(),requests as f64/request_elapsed,percentile(&mut latency,50),percentile(&mut latency,95),percentile(&mut latency,99),percentile(&mut late,99),percentile(&mut gate,99),requests-writes-hits,checkpoints.len());
    Ok(())
}

fn restart_requests() -> [ReadRequest; 3] {
    [
        ReadRequest {
            kind: 0,
            tenant: 3,
            argument: 3,
            field: 0,
        },
        ReadRequest {
            kind: 1,
            tenant: 3,
            argument: 3,
            field: 0,
        },
        ReadRequest {
            kind: 2,
            tenant: 3,
            argument: 6,
            field: 3,
        },
    ]
}
fn verify_cache_restart(
    path: &Path,
    address: &str,
    old_prefix: &str,
    model: Model,
    generation: usize,
) -> Result<String> {
    if old_prefix.len() != 32 || !old_prefix.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("invalid previous namespace".into());
    }
    let mut old = immutable::Cache::new(address)?;
    let initial = Model::new(100);
    for request in restart_requests() {
        if old.get(&format!("{old_prefix}:{generation}:{}", request.wire()))?
            != Some(initial.expected(request))
        {
            return Err("prior process cache evidence missing".into());
        }
    }
    let prefix = namespace()?;
    if prefix == old_prefix {
        return Err("namespace reused after process restart".into());
    }
    let mut app = Application {
        backend: Backend::new(path, "embedded")?,
        cache: Some(immutable::Cache::new(address)?),
        namespace: prefix.clone(),
        state: Arc::new(RwLock::new(State {
            generation,
            model: Arc::new(model),
            failed: false,
        })),
        gate_wait: Duration::ZERO,
        model_update: Duration::ZERO,
    };
    for request in restart_requests() {
        let (actual, model, hit) = app.read(request)?;
        if hit || actual != model.expected(request) {
            return Err("process restart cache isolation failed".into());
        }
    }
    Ok(prefix)
}
fn cache_restart_check(path: &Path, address: &str) -> Result<()> {
    let state = Arc::new(RwLock::new(State::new(100)));
    let mut app = Application {
        backend: Backend::new(path, "embedded")?,
        cache: Some(immutable::Cache::new(address)?),
        namespace: namespace()?,
        state,
        gate_wait: Duration::ZERO,
        model_update: Duration::ZERO,
    };
    verify(
        &immutable::Engine::new(path)?.connection(),
        &Model::new(100),
    )?;
    let requests = [
        ReadRequest {
            kind: 0,
            tenant: 3,
            argument: 3,
            field: 0,
        },
        ReadRequest {
            kind: 1,
            tenant: 3,
            argument: 3,
            field: 0,
        },
        ReadRequest {
            kind: 2,
            tenant: 3,
            argument: 6,
            field: 3,
        },
    ];
    for request in requests {
        let (actual, model, _) = app.read(request)?;
        if actual != model.expected(request) {
            return Err("initial cache oracle mismatch".into());
        }
    }
    app.update(3)?;
    for request in requests {
        let (actual, model, hit) = app.read(request)?;
        if hit || actual != model.expected(request) {
            return Err("generation invalidation failed".into());
        }
    }
    let old_namespace = app.namespace.clone();
    drop(app);
    let mut model = Model::new(100);
    model.apply(3, 2);
    verify(&immutable::Engine::new(path)?.connection(), &model)?;
    let mut stale = immutable::Cache::new(address)?;
    for request in requests {
        stale.set(
            &format!("{old_namespace}:2:{}", request.wire()),
            b"stale prior process response",
        )?;
    }
    let restarted_namespace = namespace()?;
    if restarted_namespace == old_namespace {
        return Err("namespace reused".into());
    }
    let mut restarted = Application {
        backend: Backend::new(path, "embedded")?,
        cache: Some(immutable::Cache::new(address)?),
        namespace: restarted_namespace,
        state: Arc::new(RwLock::new(State {
            generation: 2,
            model: Arc::new(model),
            failed: false,
        })),
        gate_wait: Duration::ZERO,
        model_update: Duration::ZERO,
    };
    for request in requests {
        let (actual, model, hit) = restarted.read(request)?;
        if hit || actual != model.expected(request) {
            return Err("restart namespace isolation failed".into());
        }
        if stale.get(&format!("{old_namespace}:2:{}", request.wire()))?
            != Some(b"stale prior process response".to_vec())
        {
            return Err("old cache evidence disappeared".into());
        }
    }
    println!("{{\"ok\":true,\"generation_invalidation\":true,\"fresh_namespace\":true}}");
    Ok(())
}
fn event_barrier(event: &str) -> Result<()> {
    println!("{{\"event\":\"{event}\"}}");
    std::io::stdout().flush()?;
    continue_command()
}
fn continue_command() -> Result<()> {
    let mut command = String::new();
    std::io::stdin().read_line(&mut command)?;
    if command != "CONTINUE\n" {
        return Err("crash worker expected CONTINUE".into());
    }
    Ok(())
}

fn crash_worker(path: &Path, id: usize, generation: usize, redis: Option<&str>) -> Result<()> {
    if id != 3 || generation != 2 {
        return Err("crash fixture requires id 3 generation 2".into());
    }
    let engine = immutable::Engine::new(path)?;
    let mut state = State::new(100);
    verify(&engine.connection(), &state.model)?;
    if let Some(address) = redis {
        let prefix = namespace()?;
        let mut cache = immutable::Cache::new(address)?;
        for request in restart_requests() {
            for generation in [1, 2] {
                cache.set(
                    &format!("{prefix}:{generation}:{}", request.wire()),
                    &state.model.expected(request),
                )?;
            }
        }
        println!("{{\"event\":\"ready\",\"namespace\":\"{prefix}\"}}");
        std::io::stdout().flush()?;
        continue_command()?;
    } else {
        event_barrier("ready")?;
    }
    update_before_commit(&engine.connection(), id, id % 10, generation, || {
        event_barrier("before_commit")
    })?;
    event_barrier("committed")?;
    Arc::make_mut(&mut state.model).apply(id, generation);
    state.generation = generation;
    event_barrier("published")?;
    println!("{{\"event\":\"ack\",\"generation\":{generation}}}");
    std::io::stdout().flush()?;
    let mut command = String::new();
    std::io::stdin().read_line(&mut command)?;
    if command != "CONTINUE\n" {
        return Err("crash worker expected final CONTINUE".into());
    }
    Ok(())
}
fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    if args.len() < 3 {
        return Err("usage: cache_mixed_write_probe init|serve|run DATABASE [rows|address]".into());
    }
    match args[1].as_str() {
        "cache-check" if args.len() == 4 => cache_restart_check(Path::new(&args[2]), &args[3]),
        "crash-worker" if args.len() == 5 || args.len() == 6 => crash_worker(
            Path::new(&args[2]),
            args[3].parse()?,
            args[4].parse()?,
            args.get(5).map(String::as_str),
        ),
        "check-recovery" if args.len() == 5 || args.len() == 7 => {
            let count: usize = args[3].parse()?;
            let generation: usize = args[4].parse()?;
            if count != 100 || !(1..=2).contains(&generation) {
                return Err("invalid recovery fixture".into());
            }
            let mut model = Model::new(count);
            if generation == 2 {
                model.apply(3, 2);
            }
            verify(
                &immutable::Engine::new(Path::new(&args[2]))?.connection(),
                &model,
            )?;
            if args.len() == 7 {
                let prefix = verify_cache_restart(
                    Path::new(&args[2]),
                    &args[5],
                    &args[6],
                    model,
                    generation,
                )?;
                println!("{{\"ok\":true,\"generation\":{generation},\"cache_restart\":true,\"namespace\":\"{prefix}\"}}");
            } else {
                println!("{{\"ok\":true,\"generation\":{generation}}}");
            }
            Ok(())
        }
        "init" => {
            let rows: usize = args.get(3).map(String::as_str).unwrap_or("1000").parse()?;
            if !(100..=10000).contains(&rows) {
                return Err("invalid rows".into());
            }
            initialize(Path::new(&args[2]), rows)
        }
        "run" => run(
            Path::new(&args[2]),
            args.get(3).map(String::as_str).unwrap_or("1000").parse()?,
        ),
        "serve" => serve(
            Path::new(&args[2]),
            args.get(3).map(String::as_str).unwrap_or("127.0.0.1:0"),
        ),
        _ => Err("unknown mode".into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn synced_commit_recovers_atomic_inventory_and_orders_after_crash() {
        use turso_core::{ConnectOpts, Database, DatabaseOpts, OpenFlags};
        use turso_sqlite::SqliteDialect;
        for during_checkpoint in [false, true] {
            let io = Arc::new(unreliable_io::UnreliableIo::new());
            let name = "mixed-recovery.db";
            let db = Database::open_file_with_flags(
                io.clone(),
                name,
                OpenFlags::default(),
                DatabaseOpts::new(),
                None,
                Arc::new(SqliteDialect),
            )
            .unwrap();
            let conn = db.connect(ConnectOpts::new()).unwrap();
            conn.execute("PRAGMA synchronous=FULL; CREATE TABLE documents(id INTEGER PRIMARY KEY,tenant INTEGER,version INTEGER,category INTEGER,payload BLOB,inventory INTEGER); CREATE INDEX tenant_category ON documents(tenant,category); CREATE TABLE orders(txn INTEGER PRIMARY KEY,document_id INTEGER,tenant INTEGER,quantity INTEGER,version INTEGER); BEGIN").unwrap();
            for id in 0..100 {
                conn.execute(format!(
                    "INSERT INTO documents VALUES({id},{},1,{},jsonb('{}'),{INITIAL_INVENTORY})",
                    id % 10,
                    category(id, 1),
                    document(id, 1)
                ))
                .unwrap();
            }
            conn.execute("COMMIT; PRAGMA wal_checkpoint(TRUNCATE)")
                .unwrap();
            io.mark_all_durable();
            let mut expected = Model::new(100);
            for (generation, id) in [(2, 3), (3, 13)] {
                update(&conn, id, id % 10, generation).unwrap();
                expected.apply(id, generation);
            }
            let files = if during_checkpoint {
                io.arm_crash_on_sync(name);
                let _ = conn.execute("PRAGMA wal_checkpoint(PASSIVE)");
                let snapshot = io
                    .take_crash_snapshot()
                    .expect("checkpoint must reach armed sync");
                assert!(snapshot.writes_persisted > 0 && snapshot.writes_dropped > 0);
                snapshot.files
            } else {
                conn.execute("BEGIN; UPDATE documents SET inventory=inventory-1 WHERE id=3")
                    .unwrap();
                io.durable_files()
            };
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("recovered.db");
            std::fs::write(&path, files.get(name).unwrap()).unwrap();
            if let Some(wal) = files.get(&format!("{name}-wal")) {
                std::fs::write(path.with_extension("db-wal"), wal).unwrap();
            }
            verify(
                &immutable::Engine::new(&path).unwrap().connection(),
                &expected,
            )
            .unwrap();
        }
    }
    #[test]
    fn inventory_and_order_commit_match_independent_model() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mixed.db");
        initialize(&path, 100).unwrap();
        let state = Arc::new(RwLock::new(State::new(100)));
        let mut app = Application {
            backend: Backend::new(&path, "embedded").unwrap(),
            cache: None,
            namespace: namespace().unwrap(),
            state: state.clone(),
            gate_wait: Duration::ZERO,
            model_update: Duration::ZERO,
        };
        for id in [3, 13, 3] {
            app.update(id).unwrap();
        }
        for kind in 0..3 {
            for argument in 0..18 {
                let request = ReadRequest {
                    kind,
                    tenant: 3,
                    argument,
                    field: 3,
                };
                let (actual, model, _) = app.read(request).unwrap();
                assert_eq!(actual, model.expected(request));
            }
        }
        let engine = immutable::Engine::new(&path).unwrap();
        verify(&engine.connection(), &state.read().unwrap().model).unwrap();
    }
    #[test]
    fn committed_write_with_lost_acknowledgement_closes_application() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mixed.db");
        initialize(&path, 100).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server_path = path.clone();
        let server = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            let mut socket = BufReader::new(stream);
            let mut engine = immutable::Engine::new(&server_path).unwrap();
            socket.get_mut().write_all(b"READY\n").unwrap();
            let mut command = String::new();
            socket.read_line(&mut command).unwrap();
            assert_eq!(command, "W 3 3 2\n");
            assert_eq!(
                service_request(&mut engine, &command).unwrap(),
                b"COMMITTED 2"
            );
            socket.get_mut().write_all(b"11\nCOMMITTED 0").unwrap();
            command.clear();
            assert_eq!(socket.read_line(&mut command).unwrap(), 0);
        });
        let state = Arc::new(RwLock::new(State::new(100)));
        let mut app = Application {
            backend: Backend::new(&path, &address).unwrap(),
            cache: None,
            namespace: namespace().unwrap(),
            state: state.clone(),
            gate_wait: Duration::ZERO,
            model_update: Duration::ZERO,
        };
        assert_eq!(
            app.update(3).unwrap_err().to_string(),
            "uncertain write response"
        );
        assert_eq!(state.read().unwrap().generation, 1);
        let error = app
            .read(ReadRequest {
                kind: 0,
                tenant: 3,
                argument: 3,
                field: 0,
            })
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "application closed after uncertain write"
        );
        drop(app);
        server.join().unwrap();
        let mut recovered = Model::new(100);
        recovered.apply(3, 2);
        verify(
            &immutable::Engine::new(&path).unwrap().connection(),
            &recovered,
        )
        .unwrap();
    }
    #[test]
    fn failed_write_closes_application_and_preserves_multirow_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mixed.db");
        initialize(&path, 100).unwrap();
        let state = Arc::new(RwLock::new(State::new(100)));
        let mut app = Application {
            backend: Backend::new(&path, "embedded").unwrap(),
            cache: None,
            namespace: namespace().unwrap(),
            state: state.clone(),
            gate_wait: Duration::ZERO,
            model_update: Duration::ZERO,
        };
        assert!(app.update(999).is_err());
        assert!(state.read().unwrap().failed);
        assert!(app
            .read(ReadRequest {
                kind: 0,
                tenant: 0,
                argument: 0,
                field: 0
            })
            .is_err());
        let engine = immutable::Engine::new(&path).unwrap();
        verify(&engine.connection(), &Model::new(100)).unwrap();
    }
}
