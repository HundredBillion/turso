use super::*;
use crate::{ConnectOpts, Database, EngineDialect, OpenOptions};
use std::os::unix::process::ExitStatusExt;
use std::path::Path;
use std::process::{Child, Command, Output, Stdio};
use std::time::{Duration, Instant};

struct OwnedChild(Child);
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}
fn wait_output(mut child: Child) -> Output {
    let deadline = Instant::now() + Duration::from_secs(15);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("lock subprocess timed out");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    child.wait_with_output().unwrap()
}
fn python(script: &str, path: &Path) -> Output {
    wait_output(
        Command::new("python3")
            .args(["-c", script])
            .arg(path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    )
}
fn fixture(path: &Path) {
    let output = python("import sqlite3,sys; c=sqlite3.connect(sys.argv[1]); c.execute('pragma journal_mode=wal'); c.execute('create table t(id primary key, value)'); c.execute(\"insert into t values(1,'durable')\"); c.commit(); c.execute('pragma wal_checkpoint(truncate)'); c.close(); print('SQLITE_FIXTURE')", path);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        "SQLITE_FIXTURE"
    );
}
fn sqlite_blocked(path: &Path) {
    let output = python("import sqlite3,sys\ntry:\n c=sqlite3.connect(sys.argv[1],timeout=0,isolation_level=None); c.execute('begin immediate')\nexcept sqlite3.OperationalError as e:\n assert 'locked' in str(e),str(e); print('SQLITE_BUSY')\nelse:\n raise AssertionError('SQLite acquired competing lock')", path);
    assert!(
        output.status.success(),
        "{} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        "SQLITE_BUSY"
    );
}
fn backend(name: &str) -> Arc<dyn IO> {
    assert!(std::env::var(common::ENV_DISABLE_FILE_LOCK).is_err());
    match name {
        "syscall" => Arc::new(UnixIO::new().unwrap()),
        #[cfg(feature = "io_uring")]
        "uring" => Arc::new(
            crate::io::UringIO::new().expect("io_uring backend must actually be available"),
        ),
        _ => panic!("unsupported lock-test backend"),
    }
}
fn wait_ready(child: &mut OwnedChild, ready: &Path) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while std::fs::read_to_string(ready).ok().as_deref() != Some("LOCKED") {
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "lock child exited before ownership acknowledgement"
        );
        assert!(
            Instant::now() < deadline,
            "lock child never acknowledged ownership"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}
fn kill_owner(child: &mut OwnedChild) {
    child.0.kill().unwrap();
    assert_eq!(child.0.wait().unwrap().signal(), Some(9));
}
#[test]
#[ignore = "native ordinary lock owner subprocess helper"]
fn linux_lock_holder() {
    let path = std::env::var("TURSO_ORDINARY_LOCK_PATH").unwrap();
    let ready = std::env::var("TURSO_ORDINARY_LOCK_READY").unwrap();
    let io = backend(&std::env::var("TURSO_ORDINARY_LOCK_BACKEND").unwrap());
    let _owner = io.open_file(&path, OpenFlags::None, false).unwrap();
    let pending = Path::new(&ready).with_extension("pending");
    std::fs::write(&pending, "LOCKED").unwrap();
    std::fs::File::open(&pending).unwrap().sync_all().unwrap();
    std::fs::rename(pending, ready).unwrap();
    loop {
        std::thread::park();
    }
}
fn alias_cache_and_sigkill(name: &str) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ordinary.db");
    fixture(&path);
    let io = backend(name);
    let alias = io
        .open_file(path.to_str().unwrap(), OpenFlags::NoLock, false)
        .unwrap();
    let native_alias = std::fs::File::open(&path).unwrap();
    let owner = io
        .open_file(path.to_str().unwrap(), OpenFlags::None, false)
        .unwrap();
    assert!(matches!(
        io.open_file(path.to_str().unwrap(), OpenFlags::None, false),
        Err(LimboError::LockingError(_))
    ));
    let readonly = io
        .open_file(path.to_str().unwrap(), OpenFlags::ReadOnly, false)
        .unwrap();
    let unlocked = io
        .open_file(path.to_str().unwrap(), OpenFlags::NoLock, false)
        .unwrap();
    drop(alias);
    drop(native_alias);
    drop(readonly);
    drop(unlocked);
    sqlite_blocked(&path);
    drop(owner);
    let db = Database::open(
        io.clone(),
        path.to_str().unwrap(),
        OpenOptions::new(Arc::new(EngineDialect)),
    )
    .unwrap();
    let alias_path = directory.path().join("same-inode.db");
    std::fs::hard_link(&path, &alias_path).unwrap();
    let cached_writer = Database::open(
        io.clone(),
        alias_path.to_str().unwrap(),
        OpenOptions::new(Arc::new(EngineDialect)),
    )
    .unwrap();
    let reader = Database::open(
        io.clone(),
        path.to_str().unwrap(),
        OpenOptions::new(Arc::new(EngineDialect)).flags(OpenFlags::ReadOnly),
    )
    .unwrap();
    let writer_connection = db.connect(ConnectOpts::new()).unwrap();
    let reader_connection = reader
        .connect(ConnectOpts::new().with_readonly(true))
        .unwrap();
    assert!(matches!(
        reader_connection.execute("UPDATE t SET value='forbidden'"),
        Err(LimboError::ReadOnly)
    ));
    writer_connection
        .execute("UPDATE t SET value='written'")
        .unwrap();
    drop(reader_connection);
    drop(reader);
    sqlite_blocked(&path);
    drop(writer_connection);
    drop(db);
    sqlite_blocked(&path);
    drop(cached_writer);
    let ready = directory.path().join("owner-ready");
    let mut child = OwnedChild(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "io::unix::ordinary_lock_tests::linux_lock_holder",
                "--nocapture",
            ])
            .env("TURSO_ORDINARY_LOCK_PATH", &path)
            .env("TURSO_ORDINARY_LOCK_READY", &ready)
            .env("TURSO_ORDINARY_LOCK_BACKEND", name)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    wait_ready(&mut child, &ready);
    assert!(matches!(
        io.open_file(path.to_str().unwrap(), OpenFlags::None, false),
        Err(LimboError::LockingError(_))
    ));
    kill_owner(&mut child);
    let _released = io
        .open_file(path.to_str().unwrap(), OpenFlags::None, false)
        .unwrap();
}
#[test]
#[ignore = "requires native Linux64 and Python SQLite"]
fn linux_ordinary_lock_native_syscall_alias_cache_and_sigkill() {
    alias_cache_and_sigkill("syscall");
}
#[cfg(feature = "io_uring")]
#[test]
#[ignore = "requires native Linux64, enabled io_uring, and Python SQLite"]
fn linux_ordinary_lock_native_uring_alias_cache_and_sigkill() {
    alias_cache_and_sigkill("uring");
    respects_sqlite_posix_owner("uring");
}
#[test]
#[ignore = "requires native Linux64 and Python SQLite"]
fn linux_ordinary_lock_native_respects_sqlite_posix_owner() {
    respects_sqlite_posix_owner("syscall");
}
fn respects_sqlite_posix_owner(name: &str) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("sqlite-owned.db");
    fixture(&path);
    let ready = directory.path().join("sqlite-ready");
    let script = "import sqlite3,sys,pathlib,time,os\nc=sqlite3.connect(sys.argv[1],timeout=0,isolation_level=None); c.execute('begin immediate'); c.execute(\"update t set value='uncommitted'\"); p=pathlib.Path(sys.argv[2]); q=p.with_suffix('.pending'); q.write_text('LOCKED'); os.rename(q,p); time.sleep(3600)";
    let mut child = OwnedChild(
        Command::new("python3")
            .args(["-c", script])
            .arg(&path)
            .arg(&ready)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    wait_ready(&mut child, &ready);
    let io = backend(name);
    assert!(matches!(
        io.open_file(path.to_str().unwrap(), OpenFlags::None, false),
        Err(LimboError::LockingError(_))
    ));
    kill_owner(&mut child);
    let _released = io
        .open_file(path.to_str().unwrap(), OpenFlags::None, false)
        .unwrap();
}

#[test]
#[ignore = "native Linux64 unsupported-lock subprocess helper"]
fn linux_unsupported_lock_child() {
    let path = std::env::var("TURSO_ORDINARY_LOCK_PATH").unwrap();
    // Deny only OFD lock syscalls in this disposable child test thread. This
    // exercises the actual open-error and destructor paths without an old kernel.
    let mut filters = [
        libc::sock_filter {
            code: (libc::BPF_LD | libc::BPF_W | libc::BPF_ABS) as u16,
            jt: 0,
            jf: 0,
            k: 0,
        },
        libc::sock_filter {
            code: (libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K) as u16,
            jt: 0,
            jf: 3,
            k: libc::SYS_fcntl as u32,
        },
        libc::sock_filter {
            code: (libc::BPF_LD | libc::BPF_W | libc::BPF_ABS) as u16,
            jt: 0,
            jf: 0,
            k: if cfg!(target_endian = "little") {
                24
            } else {
                28
            },
        },
        libc::sock_filter {
            code: (libc::BPF_JMP | libc::BPF_JEQ | libc::BPF_K) as u16,
            jt: 0,
            jf: 1,
            k: libc::F_OFD_SETLK as u32,
        },
        libc::sock_filter {
            code: (libc::BPF_RET | libc::BPF_K) as u16,
            jt: 0,
            jf: 0,
            k: libc::SECCOMP_RET_ERRNO | libc::EOPNOTSUPP as u32,
        },
        libc::sock_filter {
            code: (libc::BPF_RET | libc::BPF_K) as u16,
            jt: 0,
            jf: 0,
            k: libc::SECCOMP_RET_ALLOW,
        },
    ];
    let program = libc::sock_fprog {
        len: filters.len() as u16,
        filter: filters.as_mut_ptr(),
    };
    assert_eq!(
        unsafe { libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) },
        0
    );
    assert_eq!(
        unsafe { libc::prctl(libc::PR_SET_SECCOMP, libc::SECCOMP_MODE_FILTER, &program) },
        0
    );
    let io = backend("syscall");
    let error = match io.open_file(&path, OpenFlags::None, false) {
        Err(error) => error,
        Ok(_) => panic!("denied OFD lock must fail open"),
    };
    assert!(matches!(error, LimboError::LockingError(_)), "{error}");
    for flags in [OpenFlags::NoLock, OpenFlags::ReadOnly] {
        drop(io.open_file(&path, flags, false).unwrap());
    }
    println!("UNSUPPORTED_OFD_RETURNED_WITHOUT_DROP_PANIC");
}

#[test]
#[ignore = "requires native Linux64 seccomp syscall error injection"]
fn linux_ordinary_lock_native_unsupported_lock_drops_safely() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("unsupported.db");
    fixture(&path);
    let output = wait_output(
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--ignored",
                "--exact",
                "io::unix::ordinary_lock_tests::linux_unsupported_lock_child",
                "--nocapture",
            ])
            .env("TURSO_ORDINARY_LOCK_PATH", path)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{stdout}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains("UNSUPPORTED_OFD_RETURNED_WITHOUT_DROP_PANIC"),
        "{stdout}"
    );
    assert!(
        stdout
            .lines()
            .any(|line| line.starts_with("test result: ok. 1 passed; 0 failed;")),
        "{stdout}"
    );
}

#[test]
#[ignore = "requires native Linux64 and Python SQLite"]
fn linux_ordinary_lock_native_descriptor_clone_keeps_ownership() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("duplicate.db");
    fixture(&path);
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let retained = file.try_clone().unwrap();
    let owner = UnixFile {
        file,
        path: path.to_str().unwrap().to_string(),
    };
    owner.lock_file(true).unwrap();
    drop(owner);
    sqlite_blocked(&path);
    drop(retained);
    let _released = backend("syscall")
        .open_file(path.to_str().unwrap(), OpenFlags::None, false)
        .unwrap();
}
