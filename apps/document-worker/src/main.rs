use openlegal_application::document::*;
use std::io::{Read, Write};

fn read_input() -> Result<DocumentInput, DocumentError> {
    let mut stdin = std::io::stdin().lock();
    let mut length = [0; 4];
    stdin
        .read_exact(&mut length)
        .map_err(|_| DocumentError::InvalidInput)?;
    let length = u32::from_be_bytes(length) as usize;
    if length == 0 || length > MAX_DOCUMENT_HEADER_BYTES {
        return Err(DocumentError::ResourceLimit);
    }
    let mut header = vec![0; length];
    stdin
        .read_exact(&mut header)
        .map_err(|_| DocumentError::InvalidInput)?;
    let header: DocumentHeader =
        serde_json::from_slice(&header).map_err(|_| DocumentError::InvalidInput)?;
    if header.bytes_len == 0 || header.bytes_len > MAX_DOCUMENT_BYTES {
        return Err(DocumentError::ResourceLimit);
    }
    let mut raw = vec![0; header.bytes_len];
    stdin
        .read_exact(&mut raw)
        .map_err(|_| DocumentError::InvalidInput)?;
    let mut extra = [0; 1];
    if stdin
        .read(&mut extra)
        .map_err(|_| DocumentError::InvalidInput)?
        != 0
    {
        return Err(DocumentError::InvalidInput);
    }
    Ok(DocumentInput {
        format: header.format,
        source_sha256: header.source_sha256,
        ocr: header.ocr,
        raw,
    })
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Native dependencies enable Tokio's signal/network features transitively.
    // No I/O driver is needed for in-memory extraction: avoid creating even an
    // anonymous signal socketpair, which the sandbox deliberately denies.
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()?
        .block_on(run())
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    // An exec invocation is the only path that consumes content. PID 1 contains
    // no parser job and never writes document input/output to container logs.
    match std::env::args().nth(1).as_deref() {
        Some("--probe-exhaust-memory") => {
            let mut bytes = vec![0u8; 5 * 1024 * 1024 * 1024usize];
            for chunk in bytes.chunks_mut(4096) {
                chunk[0] = 1;
            }
            std::hint::black_box(bytes);
            return Err("memory limit was not enforced".into());
        }
        Some("--probe-exhaust-pids") => {
            let mut created = 0;
            for _ in 0..128 {
                match std::thread::Builder::new()
                    .stack_size(64 * 1024)
                    .spawn(|| std::thread::sleep(std::time::Duration::from_secs(60)))
                {
                    Ok(_) => created += 1,
                    Err(_) if created > 0 => {
                        serde_json::to_writer(
                            std::io::stdout().lock(),
                            &serde_json::json!({"denied": true, "created": created}),
                        )?;
                        return Ok(());
                    }
                    Err(_) => {
                        return Err("thread creation failed before the PID limit probe".into());
                    }
                }
            }
            return Err("PID limit was not enforced".into());
        }
        Some("--probe") => {
            probe()?;
            return Ok(());
        }
        Some("--idle") => {
            tokio::time::sleep(std::time::Duration::from_secs(300)).await;
            return Ok(());
        }
        Some("--process") => {}
        _ => return Err("expected a fixed worker mode".into()),
    }
    // Third-party panic/error messages may contain source text. The process
    // fails through its exit status; raw panic diagnostics are never emitted.
    std::panic::set_hook(Box::new(|_| {}));
    let result = match read_input() {
        Ok(input) => openlegal_document_worker::process(input).await,
        Err(error) => Err(error),
    };
    let response = match result {
        Ok(output) => DocumentResponse::Success(Box::new(output)),
        Err(error) => DocumentResponse::Error(error),
    };
    let mut bytes = serde_json::to_vec(&response)?;
    if bytes.len() > MAX_DOCUMENT_OUTPUT_BYTES {
        bytes = serde_json::to_vec(&DocumentResponse::Error(DocumentError::ResourceLimit))?;
    }
    let mut stdout = std::io::stdout().lock();
    stdout.write_all(&(bytes.len() as u32).to_be_bytes())?;
    stdout.write_all(&bytes)?;
    stdout.flush()?;
    Ok(())
}

fn probe() -> Result<(), Box<dyn std::error::Error>> {
    let status = std::fs::read_to_string("/proc/self/status")?;
    let value = |name: &str| {
        status
            .lines()
            .find_map(|line| line.strip_prefix(name))
            .map(str::trim)
            .unwrap_or("")
    };
    let address = "198.18.0.1:9".parse()?;
    let network_denied =
        std::net::TcpStream::connect_timeout(&address, std::time::Duration::from_millis(250))
            .is_err_and(|error| matches!(error.raw_os_error(), Some(1 | 13)));
    // The image owns this directory as UID 65532. DAC denial or a missing
    // path is not evidence of a read-only root: require Linux EROFS.
    let root_readonly = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open("/opt/probe/rootfs-write-test")
        .is_err_and(|error| error.raw_os_error() == Some(30));
    if !root_readonly {
        let _ = std::fs::remove_file("/opt/probe/rootfs-write-test");
    }
    let uid_map = std::fs::read_to_string("/proc/self/uid_map")?;
    let namespace_isolated = uid_map
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .is_some_and(|v| v != "0");
    let report = serde_json::json!({
        "nonroot": !value("Uid:").starts_with("0\t"),
        "no_new_privileges": value("NoNewPrivs:") == "1",
        "capabilities_dropped": value("CapEff:") == "0000000000000000",
        "seccomp_filter": value("Seccomp:") == "2",
        "apparmor": std::fs::read_to_string("/proc/self/attr/current").unwrap_or_default().trim(),
        "user_namespace": namespace_isolated, "network_denied": network_denied,
        "root_readonly": root_readonly,
        "no_token": !std::path::Path::new("/var/run/secrets/kubernetes.io/serviceaccount/token").exists(),
        "pids_max": std::fs::read_to_string("/sys/fs/cgroup/pids.max").unwrap_or_default().trim(),
        "memory_max": std::fs::read_to_string("/sys/fs/cgroup/memory.max").unwrap_or_default().trim(),
        "cpu_max": std::fs::read_to_string("/sys/fs/cgroup/cpu.max").unwrap_or_default().trim()
    });
    serde_json::to_writer(std::io::stdout().lock(), &report)?;
    Ok(())
}
