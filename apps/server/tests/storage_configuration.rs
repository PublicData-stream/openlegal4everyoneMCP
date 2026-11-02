use openlegal_server::config::{
    BlobConfig, CacheConfig, Config, CorpusStorageFailurePolicy, DatabaseConfig, PostgresConfig,
    ReplicationMode,
};

#[test]
fn legacy_configuration_keeps_local_replication_and_corpus_shutdown_defaults() {
    let postgres: PostgresConfig = toml::from_str("").unwrap();
    assert_eq!(postgres.replication_mode, ReplicationMode::Local);
    assert!(postgres.replication_epoch.is_none());
    postgres.options().unwrap();
    let corpus: DatabaseConfig = toml::from_str(
        "blob_path='/tmp/corpus'\nindex_path='/tmp/index'\nmecab_dictionary_path='/tmp/dictionary'\nwidget_html='/tmp/widget.html'",
    ).unwrap();
    assert_eq!(
        corpus.storage_failure_policy,
        CorpusStorageFailurePolicy::Shutdown
    );
    assert_eq!(corpus.blob_operation_timeout_secs, 5);
    assert!(corpus.blob_replica_path.is_none());
    corpus.validate().unwrap();
}

#[test]
fn required_replication_rejects_missing_nil_or_inapplicable_epoch() {
    for text in [
        "replication_mode='required'",
        "replication_mode='required'\nreplication_epoch='00000000-0000-0000-0000-000000000000'",
        "replication_mode='local'\nreplication_epoch='a1f71db8-bd59-4d33-a3bb-fb77154b571e'",
        "replication_mode='required'\nreplication_epoch='a1f71db8-bd59-4d33-a3bb-fb77154b571e'\ntls_mode='plaintext'",
    ] {
        let postgres: PostgresConfig = toml::from_str(text).unwrap();
        assert!(postgres.options().is_err());
    }
    let postgres: PostgresConfig = toml::from_str(
        "replication_mode='required'\nreplication_epoch='a1f71db8-bd59-4d33-a3bb-fb77154b571e'",
    )
    .unwrap();
    postgres.options().unwrap();
}

#[test]
fn strict_cache_requires_mirroring_and_mirror_roots_cannot_alias_by_nesting() {
    for blob in [
        "kind='filesystem'\npath='/tmp/primary'",
        "kind='filesystem_mirror'\npath='/tmp/primary'\nreplica_path='/tmp/primary/replica'",
    ] {
        let cache: CacheConfig = toml::from_str(&format!(
            "mode='persistent'\n[postgres]\nreplication_mode='required'\nreplication_epoch='a1f71db8-bd59-4d33-a3bb-fb77154b571e'\n[blob]\n{blob}"
        )).unwrap();
        assert!(cache.validate().is_err());
    }
}

#[test]
fn strict_corpus_requires_both_pairs_and_rejects_cross_store_root_overlap() {
    let mut config: Config = toml::from_str(include_str!(
        "../../../deploy/kubernetes/config/server.toml"
    ))
    .unwrap();
    let Some(CacheConfig::Persistent { postgres, blob, .. }) = config.cache.as_mut() else {
        panic!("fixture requires persistent storage");
    };
    postgres.replication_mode = ReplicationMode::Required;
    postgres.replication_epoch =
        Some(uuid::Uuid::parse_str("a1f71db8-bd59-4d33-a3bb-fb77154b571e").unwrap());
    *blob = BlobConfig::FilesystemMirror {
        path: "/tmp/cache-primary".into(),
        replica_path: "/tmp/cache-replica".into(),
    };
    let corpus = config.database.as_mut().unwrap();
    corpus.blob_path = "/tmp/corpus-primary".into();
    corpus.index_path = "/tmp/corpus-index".into();
    assert!(config.validate_storage().is_err());
    config.database.as_mut().unwrap().blob_replica_path = Some("/tmp/cache-replica/corpus".into());
    assert!(config.validate_storage().is_err());
    config.database.as_mut().unwrap().blob_replica_path = Some("/tmp/corpus-replica".into());
    config.validate_storage().unwrap();
    config.database.as_mut().unwrap().blob_replica_path = Some(
        config
            .database
            .as_ref()
            .unwrap()
            .mecab_dictionary_path
            .join("replica"),
    );
    assert!(config.validate_storage().is_err());
}
