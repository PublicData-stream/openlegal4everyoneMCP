//! Offline administrator CLI. Never constructs serving or collection services.
use openlegal_adapters::postgres::provider_admin::ProviderAdminStore;
use openlegal_domain::provider_admin::{ProviderRecoveryPlan, ProviderRecoveryRequest};
use openlegal_server::{
    ServerError,
    config::{CacheConfig, Config},
};
use serde::{Serialize, de::DeserializeOwned};
use std::{ffi::OsString, path::PathBuf};
use tokio::io::AsyncReadExt;

const MAX_INPUT: usize = 256 * 1024;
const USAGE: &str = "usage: openlegal-server --provider-admin inspect CONFIG.toml | plan SPEC.json [--resume] CONFIG.toml | apply PLAN.json CONFIG.toml | readback OPERATION_ID CONFIG.toml";
#[derive(Debug, PartialEq, Eq)]
enum AdminCommand {
    Inspect,
    Plan(PathBuf, bool),
    Apply(PathBuf),
    Readback(String),
}
fn parse(mut args: Vec<OsString>) -> Result<(AdminCommand, PathBuf), ServerError> {
    if args.len() < 2 {
        return Err(USAGE.into());
    }
    let config = PathBuf::from(args.pop().ok_or(USAGE)?);
    let command = match args.as_slice() {
        [action] if action == "inspect" => AdminCommand::Inspect,
        [action, file] if action == "plan" => AdminCommand::Plan(file.into(), false),
        [action, file, flag] if action == "plan" && flag == "--resume" => {
            AdminCommand::Plan(file.into(), true)
        }
        [action, file] if action == "apply" => AdminCommand::Apply(file.into()),
        [action, id] if action == "readback" => {
            AdminCommand::Readback(id.to_str().ok_or("operation ID must be UTF-8")?.to_owned())
        }
        _ => return Err(USAGE.into()),
    };
    Ok((command, config))
}
async fn read_bounded(path: &std::path::Path) -> Result<Vec<u8>, ServerError> {
    let file = tokio::fs::File::open(path)
        .await
        .map_err(|_| "cannot open administrator input file")?;
    let mut bytes = Vec::new();
    file.take((MAX_INPUT + 1) as u64)
        .read_to_end(&mut bytes)
        .await
        .map_err(|_| "cannot read administrator input file")?;
    if bytes.len() > MAX_INPUT {
        return Err("administrator input exceeds 256 KiB".into());
    }
    Ok(bytes)
}
async fn read_json<T: DeserializeOwned>(path: &std::path::Path) -> Result<T, ServerError> {
    serde_json::from_slice(&read_bounded(path).await?)
        .map_err(|_| "invalid administrator JSON input".into())
}
fn print_json(value: &impl Serialize) -> Result<(), ServerError> {
    use std::io::Write;
    let stdout = std::io::stdout();
    let mut output = stdout.lock();
    serde_json::to_writer(&mut output, value)
        .map_err(|_| "cannot serialize administrator result")?;
    output
        .write_all(b"\n")
        .map_err(|_| "cannot write administrator result")?;
    Ok(())
}
#[tokio::main]
pub async fn run(args: Vec<OsString>) -> Result<(), ServerError> {
    let (command, config_path) = parse(args)?;
    let config_bytes = read_bounded(&config_path).await?;
    let config: Config = toml::from_str(
        std::str::from_utf8(&config_bytes).map_err(|_| "administrator config must be UTF-8")?,
    )
    .map_err(|_| "invalid administrator configuration")?;
    let Some(CacheConfig::Persistent { postgres, .. }) = &config.cache else {
        return Err("provider administration requires persistent PostgreSQL storage".into());
    };
    let options = postgres.options()?;
    let store =
        ProviderAdminStore::open(&postgres.provider_admin_connection_url()?, options).await?;
    let result: Result<(), ServerError> = async {
        match command {
            AdminCommand::Inspect => print_json(&store.inspect().await?),
            AdminCommand::Plan(path, resume) => {
                let mut request: ProviderRecoveryRequest = read_json(&path).await?;
                if request.resume && !resume {
                    return Err("resumption requires the explicit --resume plan option".into());
                }
                request.resume = resume;
                let plan = store.plan(store.inspect_selected(&request.waits).await?, request)?;
                print_json(&plan)
            }
            AdminCommand::Apply(path) => {
                let plan: ProviderRecoveryPlan = read_json(&path).await?;
                print_json(&store.apply(&plan).await?)
            }
            AdminCommand::Readback(id) => print_json(&store.readback(&id).await?),
        }
    }
    .await;
    store.close().await;
    result
}
#[cfg(test)]
mod tests {
    use super::*;
    fn args(values: &[&str]) -> Vec<OsString> {
        values.iter().map(OsString::from).collect()
    }
    #[test]
    fn resumption_is_an_explicit_plan_intent() {
        assert_eq!(
            parse(args(&["plan", "spec.json", "config.toml"]))
                .unwrap()
                .0,
            AdminCommand::Plan("spec.json".into(), false)
        );
        assert_eq!(
            parse(args(&["plan", "spec.json", "--resume", "config.toml"]))
                .unwrap()
                .0,
            AdminCommand::Plan("spec.json".into(), true)
        );
        assert!(parse(args(&["apply", "plan.json", "--resume", "config.toml"])).is_err());
        assert!(parse(args(&["inspect", "--force", "config.toml"])).is_err());
    }
    #[tokio::test]
    async fn inputs_are_bounded_and_errors_do_not_echo_contents() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("input.json");
        tokio::fs::write(&path, vec![b'x'; MAX_INPUT + 1])
            .await
            .unwrap();
        assert!(
            read_bounded(&path)
                .await
                .unwrap_err()
                .to_string()
                .contains("256 KiB")
        );
        tokio::fs::write(&path, b"private-invalid-value")
            .await
            .unwrap();
        let error = read_json::<ProviderRecoveryRequest>(&path)
            .await
            .unwrap_err();
        assert!(!error.to_string().contains("private-invalid-value"));
    }
}

#[cfg(test)]
mod credential_config_tests {
    use openlegal_server::config::PostgresConfig;
    #[test]
    fn administrator_secret_name_is_separate_and_optional_for_existing_configs() {
        let existing: PostgresConfig =
            toml::from_str("url_env='RUNTIME_DB'\nmigration_url_env='MIGRATION_DB'\n").unwrap();
        assert_eq!(
            existing.provider_admin_url_env,
            "OPENLEGAL_PROVIDER_ADMIN_DATABASE_URL"
        );
        assert!(existing.options().is_ok());
        for name in ["RUNTIME_DB", "MIGRATION_DB", "bad env"] {
            let invalid: PostgresConfig = toml::from_str(&format!("url_env='RUNTIME_DB'\nmigration_url_env='MIGRATION_DB'\nprovider_admin_url_env='{name}'\n")).unwrap();
            assert!(invalid.options().is_err());
        }
    }
}
