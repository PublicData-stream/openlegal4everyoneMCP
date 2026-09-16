//! Disposable Kubernetes document processing. `kubectl` is an explicitly
//! configured, trusted cluster adapter; document bytes travel only on exec pipes.
use futures::{FutureExt, future::BoxFuture};
use openlegal_application::document::{
    DocumentError, DocumentHeader, DocumentInput, DocumentOutput, DocumentProcessor,
    DocumentResponse, MAX_DOCUMENT_BYTES, MAX_DOCUMENT_HEADER_BYTES, MAX_DOCUMENT_OUTPUT_BYTES,
    validate_output,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    path::PathBuf,
    process::Stdio,
    sync::{Arc, LazyLock},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::Semaphore,
};
use tokio_util::sync::CancellationToken;

static DOCUMENT_SLOTS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(2)));

#[derive(Clone)]
pub struct KubernetesDocumentProcessor {
    kubectl: PathBuf,
    kubeconfig: PathBuf,
    context: String,
    namespace: String,
    image: String,
}

impl KubernetesDocumentProcessor {
    /// No ambient kubeconfig, context, namespace, executable search, or image tag.
    pub fn new(
        kubectl: PathBuf,
        kubeconfig: PathBuf,
        context: String,
        namespace: String,
        image: String,
    ) -> Result<Self, DocumentError> {
        let safe_name = |v: &str| {
            !v.is_empty()
                && v.len() <= 253
                && v.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.:/@".contains(&b))
                && !v.starts_with('-')
        };
        let digest = image.rsplit_once("@sha256:");
        if !kubectl.is_absolute()
            || !kubeconfig.is_absolute()
            || !safe_name(&context)
            || namespace.is_empty()
            || namespace.len() > 63
            || !namespace
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
            || namespace.starts_with('-')
            || namespace.ends_with('-')
            || !safe_name(&image)
            || !digest.is_some_and(|(_, hash)| {
                hash.len() == 64 && hash.bytes().all(|b| b.is_ascii_hexdigit())
            })
        {
            return Err(DocumentError::InvalidInput);
        }
        Ok(Self {
            kubectl,
            kubeconfig,
            context,
            namespace,
            image,
        })
    }

    fn command(&self) -> Command {
        let mut command = Command::new(&self.kubectl);
        command
            .args(["--kubeconfig"])
            .arg(&self.kubeconfig)
            .args([
                "--context",
                &self.context,
                "--namespace",
                &self.namespace,
                "--request-timeout=30s",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        command
    }

    async fn control(
        &self,
        arguments: &[&str],
        input: Option<Vec<u8>>,
    ) -> Result<(), DocumentError> {
        let mut command = self.command();
        command.args(arguments).stdout(Stdio::null());
        if input.is_some() {
            command.stdin(Stdio::piped());
        }
        let mut child = command
            .spawn()
            .map_err(|_| DocumentError::SandboxUnavailable)?;
        if let Some(input) = input {
            let mut stdin = child
                .stdin
                .take()
                .ok_or(DocumentError::SandboxUnavailable)?;
            stdin
                .write_all(&input)
                .await
                .map_err(|_| DocumentError::SandboxUnavailable)?;
            stdin
                .shutdown()
                .await
                .map_err(|_| DocumentError::SandboxUnavailable)?;
        }
        if !child
            .wait()
            .await
            .map_err(|_| DocumentError::SandboxUnavailable)?
            .success()
        {
            return Err(DocumentError::SandboxUnavailable);
        }
        Ok(())
    }

    async fn execute(
        &self,
        pod: &str,
        input: &DocumentInput,
    ) -> Result<DocumentOutput, DocumentError> {
        // The process semaphore cannot survive a controller restart. Refuse to
        // create work unless the namespace's persistent live-Pod cap exists.
        let mut quota = self
            .command()
            .args(["get", "resourcequota", "document-budget", "-o", "json"])
            .spawn()
            .map_err(|_| DocumentError::SandboxUnavailable)?;
        let mut bytes = Vec::new();
        quota
            .stdout
            .take()
            .ok_or(DocumentError::SandboxUnavailable)?
            .take(65_537)
            .read_to_end(&mut bytes)
            .await
            .map_err(|_| DocumentError::SandboxUnavailable)?;
        if bytes.len() > 65_536
            || !quota
                .wait()
                .await
                .map_err(|_| DocumentError::SandboxUnavailable)?
                .success()
        {
            return Err(DocumentError::SandboxUnavailable);
        }
        let quota: Value =
            serde_json::from_slice(&bytes).map_err(|_| DocumentError::SandboxUnavailable)?;
        if quota.pointer("/spec/hard/pods").and_then(Value::as_str) != Some("2")
            || quota
                .pointer("/spec/scopes")
                .is_some_and(|scopes| scopes.as_array().is_none_or(|items| !items.is_empty()))
            || quota
                .pointer("/spec/scopeSelector")
                .is_some_and(|selector| !selector.is_null())
        {
            return Err(DocumentError::SandboxUnavailable);
        }
        self.control(
            &["create", "-f", "-"],
            Some(
                serde_json::to_vec(&pod_manifest(pod, &self.namespace, &self.image))
                    .map_err(|_| DocumentError::SandboxUnavailable)?,
            ),
        )
        .await?;
        self.control(
            &[
                "wait",
                "--for=condition=Ready",
                &format!("pod/{pod}"),
                "--timeout=60s",
            ],
            None,
        )
        .await?;
        let header = serde_json::to_vec(&DocumentHeader {
            format: input.format,
            source_sha256: input.source_sha256.clone(),
            ocr: input.ocr,
            bytes_len: input.raw.len(),
        })
        .map_err(|_| DocumentError::InvalidInput)?;
        if header.len() > MAX_DOCUMENT_HEADER_BYTES {
            return Err(DocumentError::InvalidInput);
        }
        let mut child = self
            .command()
            .args([
                "exec",
                "-i",
                pod,
                "--container=worker",
                "--",
                "/usr/local/bin/openlegal-document-worker",
                "--process",
            ])
            .stdin(Stdio::piped())
            .spawn()
            .map_err(|_| DocumentError::SandboxUnavailable)?;
        let mut stdin = child
            .stdin
            .take()
            .ok_or(DocumentError::SandboxUnavailable)?;
        let mut stdout = child
            .stdout
            .take()
            .ok_or(DocumentError::SandboxUnavailable)?;
        let write = async move {
            stdin
                .write_all(&(header.len() as u32).to_be_bytes())
                .await?;
            stdin.write_all(&header).await?;
            stdin.write_all(&input.raw).await?;
            stdin.shutdown().await?;
            // Closing the owned pipe is necessary: ChildStdin::shutdown alone
            // does not guarantee EOF while the handle remains alive.
            drop(stdin);
            Ok::<(), std::io::Error>(())
        };
        let read = async {
            let length = stdout
                .read_u32()
                .await
                .map_err(|_| DocumentError::ProcessingFailed)? as usize;
            if length > MAX_DOCUMENT_OUTPUT_BYTES {
                return Err(DocumentError::ResourceLimit);
            }
            let mut bytes = vec![0; length];
            stdout
                .read_exact(&mut bytes)
                .await
                .map_err(|_| DocumentError::ProcessingFailed)?;
            let mut extra = [0; 1];
            if stdout
                .read(&mut extra)
                .await
                .map_err(|_| DocumentError::ProcessingFailed)?
                != 0
            {
                return Err(DocumentError::InvalidDocument);
            }
            serde_json::from_slice::<DocumentResponse>(&bytes)
                .map_err(|_| DocumentError::InvalidDocument)
        };
        let (write, read) = tokio::join!(write, read);
        write.map_err(|_| DocumentError::ProcessingFailed)?;
        if !child
            .wait()
            .await
            .map_err(|_| DocumentError::SandboxUnavailable)?
            .success()
        {
            return Err(DocumentError::ProcessingFailed);
        }
        let output = match read? {
            DocumentResponse::Success(output) => *output,
            DocumentResponse::Error(error) => return Err(error),
        };
        validate_output(&output, input)?;
        Ok(output)
    }

    async fn run(
        self,
        input: DocumentInput,
        cancellation: CancellationToken,
    ) -> Result<DocumentOutput, DocumentError> {
        let _permit = DOCUMENT_SLOTS
            .clone()
            .try_acquire_owned()
            .map_err(|_| DocumentError::ResourceLimit)?;
        if cancellation.is_cancelled() {
            return Err(DocumentError::Cancelled);
        }
        if input.raw.is_empty()
            || input.raw.len() > MAX_DOCUMENT_BYTES
            || input.source_sha256
                != Sha256::digest(&input.raw)
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<String>()
        {
            return Err(DocumentError::InvalidInput);
        }
        let mut random = [0u8; 16];
        getrandom::fill(&mut random).map_err(|_| DocumentError::SandboxUnavailable)?;
        let suffix: String = random.iter().map(|b| format!("{b:02x}")).collect();
        let pod = format!("document-{suffix}");
        let result = tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(DocumentError::Cancelled),
            result = tokio::time::timeout(Duration::from_secs(300), self.execute(&pod, &input)) => result.unwrap_or(Err(DocumentError::TimedOut)),
        };
        // Always attempt deletion, including uncertain create outcomes. A namespace
        // quota caps live Pods if the API server becomes unavailable during cleanup.
        let cleanup = tokio::time::timeout(
            Duration::from_secs(35),
            self.control(
                &[
                    "delete",
                    "pod",
                    &pod,
                    "--ignore-not-found",
                    "--wait=true",
                    "--timeout=30s",
                ],
                None,
            ),
        )
        .await;
        if !matches!(cleanup, Ok(Ok(()))) {
            // An uncertain deletion continues consuming admission until restart;
            // do not start a replacement while the old Pod may still be alive.
            _permit.forget();
            return Err(DocumentError::SandboxUnavailable);
        }
        result
    }
}

impl DocumentProcessor for KubernetesDocumentProcessor {
    fn process(
        &self,
        input: DocumentInput,
        cancellation: CancellationToken,
    ) -> BoxFuture<'static, Result<DocumentOutput, DocumentError>> {
        let this = self.clone();
        async move {
            // Detached ownership is intentional: dropping a caller must not drop
            // Pod cleanup. The task remains bounded by its deadline and quota.
            tokio::spawn(this.run(input, cancellation))
                .await
                .map_err(|_| DocumentError::SandboxUnavailable)?
        }
        .boxed()
    }
}

fn pod_manifest(name: &str, namespace: &str, image: &str) -> Value {
    json!({
        "apiVersion": "v1", "kind": "Pod",
        "metadata": {"name": name, "namespace": namespace, "labels": {"app.kubernetes.io/name": "openlegal-document-worker"}},
        "spec": {
            "runtimeClassName": "openlegal-document", "hostUsers": false,
            "automountServiceAccountToken": false, "restartPolicy": "Never",
            "activeDeadlineSeconds": 300, "terminationGracePeriodSeconds": 1,
            "enableServiceLinks": false, "dnsPolicy": "None",
            "dnsConfig": {"nameservers": ["127.0.0.1"]},
            "nodeSelector": {"openlegal.document-sandbox/ready": "true"},
            "securityContext": {"runAsNonRoot": true, "runAsUser": 65532, "runAsGroup": 65532, "fsGroup": 65532,
                "seccompProfile": {"type": "Localhost", "localhostProfile": "openlegal-document.json"}},
            "containers": [{"name": "worker", "image": image, "imagePullPolicy": "IfNotPresent",
                "command": ["/usr/local/bin/openlegal-document-worker", "--idle"],
                "env": [{"name": "TMPDIR", "value": "/scratch"}, {"name": "TESSDATA_PREFIX", "value": "/opt/tessdata"},
                    {"name": "OMP_THREAD_LIMIT", "value": "2"}, {"name": "RAYON_NUM_THREADS", "value": "2"}],
                "securityContext": {"allowPrivilegeEscalation": false, "readOnlyRootFilesystem": true,
                    "capabilities": {"drop": ["ALL"]},
                    "appArmorProfile": {"type": "Localhost", "localhostProfile": "openlegal-document"}},
                "resources": {"requests": {"cpu": "2", "memory": "4Gi", "ephemeral-storage": "2Gi"},
                    "limits": {"cpu": "2", "memory": "4Gi", "ephemeral-storage": "2Gi"}},
                "volumeMounts": [{"name": "scratch", "mountPath": "/scratch"}]}],
            "volumes": [{"name": "scratch", "emptyDir": {"sizeLimit": "2Gi"}}]
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    // Controller admission is intentionally process-wide, including these tests.
    static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
    #[test]
    fn rejects_ambient_contexts_and_mutable_images() {
        assert!(
            KubernetesDocumentProcessor::new(
                "kubectl".into(),
                "/config".into(),
                "test".into(),
                "workers".into(),
                "worker:latest".into()
            )
            .is_err()
        );
        assert!(
            KubernetesDocumentProcessor::new(
                "/bin/kubectl".into(),
                "/config".into(),
                "test".into(),
                "workers".into(),
                format!("worker@sha256:{}", "a".repeat(64))
            )
            .is_ok()
        );
    }

    #[cfg(unix)]
    fn fixture_controller(directory: &std::path::Path, mode: &str) -> KubernetesDocumentProcessor {
        use std::os::unix::fs::PermissionsExt;
        let executable = directory.join("kubectl");
        let script = r#"#!/usr/bin/python3
import sys,json,pathlib,struct,time
root=pathlib.Path(@ROOT@)
args=sys.argv[1:]
if 'get' in args:
    spec={'hard':{'pods':'2'}}
    if @MODE@ == 'scoped': spec['scopes']=['BestEffort']
    if @MODE@ == 'selector': spec['scopeSelector']={'matchExpressions':[]}
    if @MODE@ == 'unbounded': spec['hard']['pods']='3'
    print(json.dumps({'spec':spec}))
elif 'create' in args:
    (root/'created').write_text('yes')
    pod=json.load(sys.stdin)
    assert pod['spec']['hostUsers'] is False
    assert pod['spec']['containers'][0]['securityContext']['readOnlyRootFilesystem']
elif 'exec' in args:
    data=sys.stdin.buffer.read()
    n=struct.unpack('>I',data[:4])[0]
    header=json.loads(data[4:4+n])
    (root/'running').write_text('yes')
    if @MODE@ == 'wait': time.sleep(30)
    out={'source_sha256':header['source_sha256'] if @MODE@ == 'valid' else 'wrong',
         'processor_version':'fixture-v1','format':'xml','tree':{'kind':'element','name':'root','attributes':[],'children':[]},
         'text':'fixture','pages':[],'ocr_pages':[],'diagnostics':[]}
    encoded=json.dumps({'status':'success','value':out}).encode()
    sys.stdout.buffer.write(struct.pack('>I',len(encoded))+encoded)
elif 'delete' in args:
    (root/'deleted').write_text('yes')
"#
            .replace("@ROOT@", &serde_json::to_string(directory.to_str().unwrap()).unwrap())
            .replace("@MODE@", &serde_json::to_string(mode).unwrap());
        std::fs::write(&executable, script).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        KubernetesDocumentProcessor::new(
            executable,
            directory.join("kubeconfig"),
            "fixture".into(),
            "documents".into(),
            format!("worker@sha256:{}", "a".repeat(64)),
        )
        .unwrap()
    }

    fn fixture_input() -> DocumentInput {
        let raw = b"<root/>".to_vec();
        DocumentInput {
            source_sha256: Sha256::digest(&raw)
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect(),
            raw,
            format: openlegal_application::document::DocumentFormat::Xml,
            ocr: false,
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_quotas_that_do_not_bound_every_worker_pod() {
        let _guard = SERIAL.lock().await;
        for mode in ["scoped", "selector", "unbounded"] {
            let directory = tempfile::tempdir().unwrap();
            let processor = fixture_controller(directory.path(), mode);
            assert!(matches!(
                processor
                    .process(fixture_input(), CancellationToken::new())
                    .await,
                Err(DocumentError::SandboxUnavailable)
            ));
            assert!(!directory.path().join("created").exists());
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn invalid_worker_identity_is_rejected_and_pod_deleted() {
        let _guard = SERIAL.lock().await;
        let directory = tempfile::tempdir().unwrap();
        let processor = fixture_controller(directory.path(), "wrong");
        assert!(matches!(
            processor
                .process(fixture_input(), CancellationToken::new())
                .await,
            Err(DocumentError::InvalidDocument)
        ));
        assert!(directory.path().join("deleted").is_file());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancellation_still_deletes_the_pod() {
        let _guard = SERIAL.lock().await;
        let directory = tempfile::tempdir().unwrap();
        let processor = fixture_controller(directory.path(), "wait");
        let cancellation = CancellationToken::new();
        let future = processor.process(fixture_input(), cancellation.clone());
        let task = tokio::spawn(future);
        tokio::time::timeout(Duration::from_secs(5), async {
            while !directory.path().join("running").is_file() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        cancellation.cancel();
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(5), task)
                .await
                .unwrap()
                .unwrap(),
            Err(DocumentError::Cancelled)
        ));
        assert!(directory.path().join("deleted").is_file());
    }
}
