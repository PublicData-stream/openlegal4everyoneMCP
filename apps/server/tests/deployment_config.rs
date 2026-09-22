//! Exercise the production template with the real Rust configuration contract.
//! The image gate supplies the rendered ConfigMap; ordinary tests use its source.
use openlegal_server::config::{AccessPolicy, CacheConfig, Config};

fn template() -> String {
    match std::env::var_os("OPENLEGAL_RENDERED_CONFIG") {
        Some(path) => std::fs::read_to_string(path).expect("rendered deployment configuration"),
        None => include_str!("../../../deploy/kubernetes/config/server.toml").into(),
    }
}

fn validate(raw: &str) -> Result<(), openlegal_server::ServerError> {
    let config: Config = toml::from_str(raw)?;
    config.limits.validate()?;
    config.validate_storage()?;
    if let Some(diff) = &config.text_diff {
        diff.validate(&config.limits)?;
    }
    for access in [
        AccessPolicy {
            allowed_hosts: config.http.allowed_hosts.clone(),
            allowed_origins: config.http.allowed_origins.clone(),
        },
        AccessPolicy {
            allowed_hosts: config.webtransport.allowed_hosts.clone(),
            allowed_origins: config.webtransport.allowed_origins.clone(),
        },
    ] {
        access.validate()?;
    }
    Ok(())
}

#[test]
fn retained_template_uses_existing_configuration_contract_without_secrets() {
    let raw = template();
    validate(&raw).unwrap();
    let config: Config = toml::from_str(&raw).unwrap();
    let corpus = config.database.unwrap();
    assert!(corpus.ingestion.is_none());
    assert!(corpus.mecab_dictionary_path.is_absolute());
    let Some(CacheConfig::Persistent { postgres, .. }) = config.cache else {
        panic!("retained deployment requires persistent cache");
    };
    // Does not resolve either environment variable or open any storage/network.
    postgres.options().unwrap();
    assert_eq!(postgres.url_env, "OPENLEGAL_DATABASE_URL");
    assert_eq!(
        postgres.migration_url_env,
        "OPENLEGAL_MIGRATION_DATABASE_URL"
    );
}

#[test]
fn retained_configuration_rejects_missing_dependencies() {
    let baseline: toml::Value = toml::from_str(&template()).unwrap();
    for section in ["cache", "text_diff"] {
        let mut value = baseline.clone();
        value.as_table_mut().unwrap().remove(section);
        assert!(
            validate(&toml::to_string(&value).unwrap()).is_err(),
            "{section}"
        );
    }
    let mut value = baseline;
    value["database"]
        .as_table_mut()
        .unwrap()
        .remove("mecab_dictionary_path");
    assert!(validate(&toml::to_string(&value).unwrap()).is_err());
}

#[test]
fn retained_configuration_rejects_equal_and_nested_storage_roots() {
    let baseline: toml::Value = toml::from_str(&template()).unwrap();
    for path in [
        "/var/lib/openlegal/cache-blobs/data",
        "/var/lib/openlegal/cache-blobs",
    ] {
        let mut value = baseline.clone();
        value["database"]["blob_path"] = path.into();
        assert!(
            validate(&toml::to_string(&value).unwrap()).is_err(),
            "{path}"
        );
    }
    for field in ["blob_path", "index_path"] {
        let mut value = baseline.clone();
        value["database"][field] = "/var/lib/openlegal/cache-blobs/data/nested".into();
        assert!(
            validate(&toml::to_string(&value).unwrap()).is_err(),
            "{field}"
        );
    }
    let mut value = baseline;
    value["database"]["index_path"] = value["database"]["blob_path"].clone();
    assert!(validate(&toml::to_string(&value).unwrap()).is_err());
}

#[test]
fn retained_configuration_rejects_shared_credential_names_and_tls_downgrade_with_ca() {
    let baseline: toml::Value = toml::from_str(&template()).unwrap();
    let mut value = baseline.clone();
    value["cache"]["postgres"]["migration_url_env"] = "OPENLEGAL_DATABASE_URL".into();
    assert!(validate(&toml::to_string(&value).unwrap()).is_err());
    let mut value = baseline;
    value["cache"]["postgres"]["tls_mode"] = "plaintext".into();
    assert!(validate(&toml::to_string(&value).unwrap()).is_err());
}

#[test]
fn ingestion_template_preserves_retained_config_and_explicit_controller_contract() {
    let raw = include_str!("../../../deploy/kubernetes/ingestion/server.toml");
    validate(raw).unwrap();
    let mut value: toml::Value = toml::from_str(raw).unwrap();
    value["database"]
        .as_table_mut()
        .unwrap()
        .remove("ingestion");
    let retained: toml::Value = toml::from_str(include_str!(
        "../../../deploy/kubernetes/config/server.toml"
    ))
    .unwrap();
    assert_eq!(value, retained);
    let config: Config = toml::from_str(raw).unwrap();
    let ingestion = config.database.unwrap().ingestion.unwrap();
    assert!(ingestion.enabled);
    assert!(!ingestion.retain_history_bodies);
    assert_eq!(
        ingestion.credential_env,
        "OPENLEGAL_LAW_PROVIDER_CREDENTIAL"
    );
    // Construction validates paths, context, namespace and immutable image
    // without reading credentials, launching kubectl or making upstream calls.
    openlegal_adapters::document_jobs::KubernetesDocumentProcessor::new(
        ingestion.kubectl,
        ingestion.kubeconfig,
        ingestion.context,
        ingestion.namespace,
        ingestion.worker_image,
    )
    .unwrap();
}
