#[cfg(all(target_os = "linux", target_pointer_width = "64"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1);
    let path = arguments.next().ok_or("expected marked database path")?;
    let limit = arguments.next().ok_or("expected history limit")?.parse()?;
    if arguments.next().is_some() {
        return Err("unexpected extra argument".into());
    }
    turso_core::Database::test_shared_open_pair(&path, limit)?;
    turso_core::Database::test_shared_open_pair(&path, limit)?;
    println!("production-library shared open probe passed");
    Ok(())
}

#[cfg(not(all(target_os = "linux", target_pointer_width = "64")))]
fn main() {
    eprintln!("shared open probe requires 64-bit Linux");
    std::process::exit(78);
}
