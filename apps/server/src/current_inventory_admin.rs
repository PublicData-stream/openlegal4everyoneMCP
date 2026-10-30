//! Explicit offline corpus administration; opens no serving, ingestion or blobs.
use openlegal_adapters::corpus::current_inventory_admin::{
    CurrentInventoryAdminStore, CurrentInventoryManifest,
};
use openlegal_domain::legal::ObjectId;
use openlegal_server::{
    ServerError,
    config::{CacheConfig, Config},
};
use serde::{Serialize, de::DeserializeOwned};
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
};
use tokio::io::AsyncReadExt;

const MAX_INPUT: usize = 256 * 1024;
const MAX_OUTPUT: usize = 256 * 1024;
const USAGE: &str = "usage: openlegal-server --current-inventory-admin inspect OBJECT.json CONFIG.toml | apply MANIFEST.json CONFIG.toml | readback OPERATION_ID CONFIG.toml";
#[derive(Debug, PartialEq, Eq)]
enum Command {
    Inspect(PathBuf),
    Apply(PathBuf),
    Readback(String),
}
fn parse(args: Vec<OsString>) -> Result<(Command, PathBuf), ServerError> {
    let [action, input, config] = args.as_slice() else {
        return Err(USAGE.into());
    };
    let command = if action == "inspect" {
        Command::Inspect(input.into())
    } else if action == "apply" {
        Command::Apply(input.into())
    } else if action == "readback" {
        Command::Readback(
            input
                .to_str()
                .ok_or("invalid current inventory operation ID")?
                .to_owned(),
        )
    } else {
        return Err(USAGE.into());
    };
    Ok((command, config.into()))
}
async fn read_bounded(path: &Path) -> Result<Vec<u8>, ServerError> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|_| "cannot open current inventory administrator input")?;
    let mut bytes = Vec::new();
    file.take((MAX_INPUT + 1) as u64)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| "cannot read current inventory administrator input")?;
    if bytes.len() > MAX_INPUT {
        return Err("current inventory administrator input exceeds 256 KiB".into());
    }
    Ok(bytes)
}
async fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T, ServerError> {
    serde_json::from_slice(&read_bounded(path).await?)
        .map_err(|_| "invalid current inventory administrator JSON input".into())
}
fn encode(value: &impl Serialize) -> Result<Vec<u8>, ServerError> {
    let mut bytes = serde_json::to_vec(value)
        .map_err(|_| "cannot serialize current inventory administrator result")?;
    if bytes.len() > MAX_OUTPUT {
        return Err("current inventory administrator result exceeds 256 KiB".into());
    }
    bytes.push(b'\n');
    Ok(bytes)
}
fn print_json(value: &impl Serialize) -> Result<(), ServerError> {
    use std::io::Write;
    std::io::stdout()
        .lock()
        .write_all(&encode(value)?)
        .map_err(|_| "cannot write current inventory administrator result".into())
}
#[tokio::main]
pub async fn run(args: Vec<OsString>) -> Result<(), ServerError> {
    let (command, path) = parse(args)?;
    // Invalid operator input fails before a database connection is constructed.
    let object = if let Command::Inspect(path) = &command {
        let object: ObjectId = read_json(path).await?;
        object.validate()?;
        Some(object)
    } else {
        None
    };
    let manifest = if let Command::Apply(path) = &command {
        let manifest: CurrentInventoryManifest = read_json(path).await?;
        manifest.validate()?;
        Some(manifest)
    } else {
        None
    };
    if let Command::Readback(id) = &command
        && !uuid_shape(id)
    {
        return Err("invalid current inventory operation ID".into());
    }
    let config: Config = toml::from_str(
        std::str::from_utf8(&read_bounded(&path).await?)
            .map_err(|_| "current inventory administrator config must be UTF-8")?,
    )
    .map_err(|_| "invalid current inventory administrator configuration")?;
    let Some(CacheConfig::Persistent { postgres, .. }) = &config.cache else {
        return Err(
            "current inventory administration requires persistent PostgreSQL storage".into(),
        );
    };
    let store =
        CurrentInventoryAdminStore::open(&postgres.connection_url(false)?, postgres.options()?)
            .await?;
    let result = async {
        match command {
            Command::Inspect(_) => print_json(
                &store
                    .inspect(object.as_ref().ok_or("missing current inventory object")?)
                    .await?,
            ),
            Command::Apply(_) => print_json(
                &store
                    .apply(
                        manifest
                            .as_ref()
                            .ok_or("missing current inventory manifest")?,
                    )
                    .await?,
            ),
            Command::Readback(id) => print_json(&store.readback(&id).await?),
        }
    }
    .await;
    store.close().await;
    result
}
fn uuid_shape(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_digit() || (b'a'..=b'f').contains(&b)
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn args(values: &[&str]) -> Vec<OsString> {
        values.iter().map(OsString::from).collect()
    }
    #[test]
    fn only_explicit_closed_commands_are_accepted() {
        assert_eq!(
            parse(args(&["inspect", "object.json", "config.toml"]))
                .unwrap()
                .0,
            Command::Inspect("object.json".into())
        );
        assert_eq!(
            parse(args(&["apply", "manifest.json", "config.toml"]))
                .unwrap()
                .0,
            Command::Apply("manifest.json".into())
        );
        for values in [
            &["inspect", "config.toml"][..],
            &["apply", "x", "--force", "config"][..],
            &["resume", "x", "config"][..],
        ] {
            assert!(parse(args(values)).is_err());
        }
        assert!(uuid_shape("12345678-1234-1234-1234-123456789abc"));
        assert!(!uuid_shape("12345678-1234-1234-1234-123456789ABC"));
    }
    #[tokio::test]
    async fn malformed_and_oversized_inputs_do_not_echo_contents() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("input.json");
        tokio::fs::write(&path, b"credential-bearing-private-value")
            .await
            .unwrap();
        let error = read_json::<ObjectId>(&path).await.unwrap_err().to_string();
        assert!(!error.contains("credential-bearing-private-value"));
        tokio::fs::write(&path, vec![b'x'; MAX_INPUT + 1])
            .await
            .unwrap();
        assert!(read_bounded(&path).await.is_err());
    }
    #[test]
    fn output_is_checked_before_any_write() {
        assert!(encode(&"x".repeat(MAX_OUTPUT)).is_err());
        assert_eq!(
            encode(&serde_json::json!({"status":"ok"})).unwrap(),
            b"{\"status\":\"ok\"}\n"
        );
    }
}
