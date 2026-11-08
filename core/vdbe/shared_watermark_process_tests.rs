use super::*;
use std::io::{BufRead, Write};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::{Duration, Instant};

#[test]
#[ignore = "requires native Linux ext-family filesystem"]
fn shared_watermark_process_native_reads_live_rollback_and_dead_owner_claims() {
    let directory = tempfile::tempdir().unwrap();
    let (path, limit) = crate::mvcc::database::shared_runtime_fixture(directory.path());
    let open = |readonly| {
        let io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
        Database::open(
            io,
            path.to_str().unwrap(),
            OpenOptions::new(Arc::new(EngineDialect))
                .flags(if readonly {
                    crate::OpenFlags::ReadOnly
                } else {
                    crate::OpenFlags::None
                })
                .with_shared_mvcc_for_test(limit),
        )
        .unwrap()
        .connect(ConnectOpts::new())
        .unwrap()
    };
    let allocator = open(false);
    allocator
        .execute("CREATE SEQUENCE process_sequence")
        .unwrap();
    let reader = open(true);
    let mut child = WatermarkChild::spawn(&path, limit);
    assert_eq!(child.receive(), "READY");
    child.send("claim");
    assert_eq!(child.receive(), "CLAIM 1");
    assert_eq!(
        integers(&allocator, "SELECT nextval('process_sequence')"),
        [2]
    );
    let before = watermark_durable_files(directory.path());
    assert_eq!(
        integers(
            &reader,
            "SELECT sequence_watermark_experimental('process_sequence')"
        ),
        [1]
    );
    assert_eq!(watermark_durable_files(directory.path()), before);
    child.send("rollback");
    assert_eq!(child.receive(), "ROLLED_BACK");
    assert_eq!(
        integers(
            &reader,
            "SELECT sequence_watermark_experimental('process_sequence')"
        ),
        [3]
    );
    child.send("claim");
    assert_eq!(child.receive(), "CLAIM 3");
    assert_eq!(
        integers(&allocator, "SELECT nextval('process_sequence')"),
        [4]
    );
    assert_eq!(
        integers(
            &reader,
            "SELECT sequence_watermark_experimental('process_sequence')"
        ),
        [3]
    );
    child.child.kill().unwrap();
    let status = child.wait();
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(status.signal(), Some(9));
    let before = watermark_durable_files(directory.path());
    assert_eq!(
        integers(
            &reader,
            "SELECT sequence_watermark_experimental('process_sequence')"
        ),
        [5]
    );
    assert_eq!(
        watermark_durable_files(directory.path()),
        before,
        "read-only liveness sampling journaled dead-owner cleanup"
    );
    assert_eq!(
        integers(&reader, "SELECT count(*) FROM preserved WHERE id=100"),
        [0]
    );
}

#[test]
#[ignore = "subprocess helper; requires a parent-provided shared fixture"]
fn shared_watermark_process_child() {
    let path = std::env::var("TURSO_WATERMARK_CHILD_PATH").expect("parent fixture path");
    let limit = std::env::var("TURSO_WATERMARK_CHILD_LIMIT")
        .unwrap()
        .parse()
        .unwrap();
    let io: Arc<dyn crate::IO> = Arc::new(crate::io::SyscallIO::new().unwrap());
    let database = Database::open(
        io,
        &path,
        OpenOptions::new(Arc::new(EngineDialect)).with_shared_mvcc_for_test(limit),
    )
    .unwrap();
    let connection = database.connect(ConnectOpts::new()).unwrap();
    let reply = |message: &str| {
        println!("WATERMARK_CHILD {message}");
        std::io::stdout().flush().unwrap();
    };
    reply("READY");
    for line in std::io::stdin().lock().lines() {
        match line.unwrap().as_str() {
            "claim" => {
                connection.execute("BEGIN CONCURRENT").unwrap();
                connection
                    .execute("INSERT INTO preserved(id,value) VALUES(100,'child outer')")
                    .unwrap();
                let value = integers(&connection, "SELECT nextval('process_sequence')")[0];
                reply(&format!("CLAIM {value}"));
            }
            "rollback" => {
                connection.execute("ROLLBACK").unwrap();
                reply("ROLLED_BACK");
            }
            other => panic!("unknown parent command: {other}"),
        }
    }
}

struct WatermarkChild {
    child: Child,
    output: Receiver<std::io::Result<String>>,
}
impl WatermarkChild {
    fn spawn(path: &std::path::Path, limit: u64) -> Self {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "vdbe::shared_id_tests::shared_watermark_process_tests::shared_watermark_process_child", "--ignored", "--nocapture", "--test-threads=1"])
            .env("TURSO_WATERMARK_CHILD_PATH", path)
            .env("TURSO_WATERMARK_CHILD_LIMIT", limit.to_string())
            .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::inherit()).spawn().unwrap();
        let stdout = child.stdout.take().unwrap();
        let (send, output) = mpsc::channel();
        std::thread::spawn(move || {
            for line in std::io::BufReader::new(stdout).lines() {
                if send.send(line).is_err() {
                    break;
                }
            }
        });
        Self { child, output }
    }
    fn send(&mut self, command: &str) {
        let stdin = self.child.stdin.as_mut().unwrap();
        writeln!(stdin, "{command}").unwrap();
        stdin.flush().unwrap();
    }
    fn receive(&self) -> String {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            let line = self
                .output
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .expect("child acknowledgement before deadline")
                .unwrap();
            if let Some((_, message)) = line.split_once("WATERMARK_CHILD ") {
                return message.to_owned();
            }
        }
    }
    fn wait(&mut self) -> std::process::ExitStatus {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(status) = self.child.try_wait().unwrap() {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "child did not exit before deadline"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}
impl Drop for WatermarkChild {
    fn drop(&mut self) {
        if matches!(self.child.try_wait(), Ok(None)) {
            let _ = self.child.kill();
            let deadline = Instant::now() + Duration::from_secs(5);
            while matches!(self.child.try_wait(), Ok(None)) && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(10));
            }
        }
    }
}
