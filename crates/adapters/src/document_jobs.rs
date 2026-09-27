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
use std::{path::PathBuf, process::Stdio, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::Semaphore,
};
use tokio_util::sync::CancellationToken;

/// Resource quantities for one disposable Pod and the maximum number of Pods.
/// The original spelling is retained for the Pod manifest; admission compares
/// exact, Kubernetes-rounded milli-units rather than textual spellings.
#[derive(Clone, Debug)]
pub struct DocumentWorkerLimits {
    pool_limit: u32,
    cpu: String,
    memory: String,
    scratch: String,
    cpu_milli: i128,
    memory_milli: i128,
    scratch_milli: i128,
}

impl Default for DocumentWorkerLimits {
    fn default() -> Self {
        Self::new(2, "2", "4Gi", "2Gi").expect("valid built-in document worker limits")
    }
}

impl DocumentWorkerLimits {
    pub fn new(
        pool_limit: u32,
        cpu: &str,
        memory: &str,
        scratch: &str,
    ) -> Result<Self, DocumentError> {
        if pool_limit == 0 || (pool_limit as usize) > Semaphore::MAX_PERMITS {
            return Err(DocumentError::InvalidInput);
        }
        let cpu_milli = parse_quantity_milli(cpu).ok_or(DocumentError::InvalidInput)?;
        let memory_milli = parse_quantity_milli(memory).ok_or(DocumentError::InvalidInput)?;
        let scratch_milli = parse_quantity_milli(scratch).ok_or(DocumentError::InvalidInput)?;
        for amount in [cpu_milli, memory_milli, scratch_milli] {
            let total = amount
                .checked_mul(i128::from(pool_limit))
                .ok_or(DocumentError::InvalidInput)?;
            if total > i128::from(i64::MAX) * 1000 {
                return Err(DocumentError::InvalidInput);
            }
        }
        Ok(Self {
            pool_limit,
            cpu: cpu.into(),
            memory: memory.into(),
            scratch: scratch.into(),
            cpu_milli,
            memory_milli,
            scratch_milli,
        })
    }

    pub fn pool_limit(&self) -> u32 {
        self.pool_limit
    }
    pub fn cpu(&self) -> &str {
        &self.cpu
    }
    pub fn memory(&self) -> &str {
        &self.memory
    }
    pub fn scratch(&self) -> &str {
        &self.scratch
    }

    fn threads(&self) -> i128 {
        ((self.cpu_milli + 999) / 1000).clamp(1, 64)
    }

    fn matches_quota(&self, quota: &Value) -> bool {
        let Some(spec) = quota.pointer("/spec").and_then(Value::as_object) else {
            return false;
        };
        let Some(hard) = spec.get("hard").and_then(Value::as_object) else {
            return false;
        };
        if spec.len() != 1 || hard.len() != 7 {
            return false;
        }
        let expected = [
            ("requests.cpu", self.cpu_milli),
            ("limits.cpu", self.cpu_milli),
            ("requests.memory", self.memory_milli),
            ("limits.memory", self.memory_milli),
            ("requests.ephemeral-storage", self.scratch_milli),
            ("limits.ephemeral-storage", self.scratch_milli),
        ];
        hard.get("pods")
            .and_then(Value::as_str)
            .and_then(parse_quantity_milli)
            == Some(i128::from(self.pool_limit) * 1000)
            && expected.iter().all(|(key, per_pod)| {
                hard.get(*key)
                    .and_then(Value::as_str)
                    .and_then(parse_quantity_milli)
                    == per_pod.checked_mul(i128::from(self.pool_limit))
            })
    }
}

// Multiply decimal digits by a bounded binary-SI factor without first forcing
// the unscaled coefficient into a machine integer.
fn multiply_decimal(digits: &str, factor: u128) -> Option<String> {
    let mut carry = 0u128;
    let mut output = Vec::with_capacity(digits.len() + 20);
    for digit in digits.bytes().rev() {
        let value = u128::from(digit - b'0')
            .checked_mul(factor)?
            .checked_add(carry)?;
        output.push(b'0' + (value % 10) as u8);
        carry = value / 10;
    }
    while carry != 0 {
        output.push(b'0' + (carry % 10) as u8);
        carry /= 10;
    }
    output.reverse();
    String::from_utf8(output).ok()
}

/// Kubernetes Quantity grammar and its documented round-up to milli-units.
/// This same normalization is used for configured values and API quota values.
fn parse_quantity_milli(raw: &str) -> Option<i128> {
    if raw.is_empty() || raw.len() > 128 {
        return None;
    }
    let raw = raw.strip_prefix('+').unwrap_or(raw);
    let split = raw
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(raw.len());
    let (number, suffix) = raw.split_at(split);
    let (integer, fraction) = match number.split_once('.') {
        Some((integer, fraction)) if !integer.is_empty() || !fraction.is_empty() => {
            (integer, fraction)
        }
        None if !number.is_empty() => (number, ""),
        _ => return None,
    };
    if !integer.bytes().all(|b| b.is_ascii_digit()) || !fraction.bytes().all(|b| b.is_ascii_digit())
    {
        return None;
    }
    let digits = format!("{integer}{fraction}");
    if !digits.bytes().any(|b| b != b'0') {
        return None;
    }
    let (scale, binary) = match suffix {
        "n" => (-9, 0),
        "u" => (-6, 0),
        "m" => (-3, 0),
        "" => (0, 0),
        "k" => (3, 0),
        "M" => (6, 0),
        "G" => (9, 0),
        "T" => (12, 0),
        "P" => (15, 0),
        "E" => (18, 0),
        "Ki" => (0, 1),
        "Mi" => (0, 2),
        "Gi" => (0, 3),
        "Ti" => (0, 4),
        "Pi" => (0, 5),
        "Ei" => (0, 6),
        _ if suffix.starts_with('e') || suffix.starts_with('E') => {
            let exponent = &suffix[1..];
            if exponent.is_empty() || exponent == "+" || exponent == "-" {
                return None;
            }
            let power: i32 = exponent.parse().ok()?;
            (power, 0)
        }
        _ => return None,
    };
    let digits = if binary == 0 {
        digits
    } else {
        multiply_decimal(&digits, 1024u128.checked_pow(binary)?)?
    };
    let shift = i64::from(scale) + 3 - i64::try_from(fraction.len()).ok()?;
    let normalized = if shift >= 0 {
        let zeros = usize::try_from(shift).ok()?;
        if zeros > 40 {
            return None;
        }
        format!("{digits}{}", "0".repeat(zeros))
    } else {
        let divisor_digits = usize::try_from(-shift).ok()?;
        let cut = digits.len().saturating_sub(divisor_digits);
        let quotient = &digits[..cut];
        let remainder = &digits[cut..];
        let quotient = quotient.trim_start_matches('0');
        let whole = if quotient.is_empty() {
            0
        } else {
            quotient.parse::<i128>().ok()?
        };
        return whole
            .checked_add(i128::from(remainder.bytes().any(|b| b != b'0')))
            .filter(|v| *v > 0 && *v <= i128::from(i64::MAX) * 1000);
    };
    normalized
        .trim_start_matches('0')
        .parse::<i128>()
        .ok()
        .filter(|v| *v > 0 && *v <= i128::from(i64::MAX) * 1000)
}

#[derive(Clone)]
pub struct KubernetesDocumentProcessor {
    kubectl: PathBuf,
    kubeconfig: PathBuf,
    context: String,
    namespace: String,
    image: String,
    limits: DocumentWorkerLimits,
    slots: Arc<Semaphore>,
}

impl KubernetesDocumentProcessor {
    /// No ambient kubeconfig, context, namespace, executable search, or image tag.
    pub fn new(
        kubectl: PathBuf,
        kubeconfig: PathBuf,
        context: String,
        namespace: String,
        image: String,
        limits: DocumentWorkerLimits,
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
            slots: Arc::new(Semaphore::new(limits.pool_limit as usize)),
            limits,
        })
    }

    fn command(&self) -> Command {
        let mut command = Command::new(&self.kubectl);
        command
            // The trusted adapter needs only explicit file-backed credentials;
            // never pass server secrets, proxies or ambient kubectl settings.
            .env_clear()
            .env("PATH", "/usr/local/bin:/usr/bin:/bin")
            .env("HOME", "/tmp")
            .env("TMPDIR", "/tmp")
            .args(["--kubeconfig"])
            .arg(&self.kubeconfig)
            .args([
                "--context",
                &self.context,
                "--namespace",
                &self.namespace,
                "--request-timeout=30s",
                "--cache-dir=/tmp/openlegal-kubectl-cache",
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
        if !self.limits.matches_quota(&quota) {
            return Err(DocumentError::SandboxUnavailable);
        }
        self.control(
            &["create", "-f", "-"],
            Some(
                serde_json::to_vec(&pod_manifest(
                    pod,
                    &self.namespace,
                    &self.image,
                    &self.limits,
                ))
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
        let _permit = self
            .slots
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

fn pod_manifest(name: &str, namespace: &str, image: &str, limits: &DocumentWorkerLimits) -> Value {
    let threads = limits.threads().to_string();
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
                    {"name": "OMP_THREAD_LIMIT", "value": threads}, {"name": "RAYON_NUM_THREADS", "value": threads}],
                "securityContext": {"allowPrivilegeEscalation": false, "readOnlyRootFilesystem": true,
                    "capabilities": {"drop": ["ALL"]},
                    "appArmorProfile": {"type": "Localhost", "localhostProfile": "openlegal-document"}},
                "resources": {"requests": {"cpu": limits.cpu, "memory": limits.memory, "ephemeral-storage": limits.scratch},
                    "limits": {"cpu": limits.cpu, "memory": limits.memory, "ephemeral-storage": limits.scratch}},
                "volumeMounts": [{"name": "scratch", "mountPath": "/scratch"}]}],
            "volumes": [{"name": "scratch", "emptyDir": {"sizeLimit": limits.scratch}}]
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
                "worker:latest".into(),
                DocumentWorkerLimits::default(),
            )
            .is_err()
        );
        assert!(
            KubernetesDocumentProcessor::new(
                "/bin/kubectl".into(),
                "/config".into(),
                "test".into(),
                "workers".into(),
                format!("worker@sha256:{}", "a".repeat(64)),
                DocumentWorkerLimits::default(),
            )
            .is_ok()
        );
    }

    #[cfg(unix)]
    fn fixture_controller(directory: &std::path::Path, mode: &str) -> KubernetesDocumentProcessor {
        fixture_controller_with_limits(directory, mode, DocumentWorkerLimits::default())
    }

    #[cfg(unix)]
    fn fixture_controller_with_limits(
        directory: &std::path::Path,
        mode: &str,
        limits: DocumentWorkerLimits,
    ) -> KubernetesDocumentProcessor {
        use std::os::unix::fs::PermissionsExt;
        let executable = directory.join("kubectl");
        let script = r#"#!/usr/bin/python3
import sys,json,pathlib,struct,time,os
root=pathlib.Path(@ROOT@)
args=sys.argv[1:]
assert args[:8] == ['--kubeconfig',str(root/'kubeconfig'),'--context','fixture',
                    '--namespace','documents','--request-timeout=30s',
                    '--cache-dir=/tmp/openlegal-kubectl-cache']
assert os.environ['PATH'] == '/usr/local/bin:/usr/bin:/bin'
assert os.environ['HOME'] == os.environ['TMPDIR'] == '/tmp'
for key in ['OPENLEGAL_LAW_PROVIDER_CREDENTIAL','OPENLEGAL_DATABASE_URL',
            'KUBECONFIG','HTTP_PROXY','HTTPS_PROXY','ALL_PROXY','NO_PROXY',
            'http_proxy','https_proxy','all_proxy','no_proxy','OPENLEGAL_CONTROLLER_ENV_TEST']:
    assert key not in os.environ
with (root/'commands').open('a') as log:
    log.write(json.dumps(args[8:])+'\n')
if 'get' in args:
    spec={'hard':{'pods':@PODS@,'requests.cpu':@CPU@,'limits.cpu':@CPU@,
                  'requests.memory':@MEMORY@,'limits.memory':@MEMORY@,
                  'requests.ephemeral-storage':@SCRATCH@,'limits.ephemeral-storage':@SCRATCH@}}
    if @MODE@ == 'scoped': spec['scopes']=['BestEffort']
    if @MODE@ == 'selector': spec['scopeSelector']={'matchExpressions':[]}
    if @MODE@ == 'unbounded': spec['hard']['pods']='3'
    if @MODE@ == 'missing-resource': del spec['hard']['requests.cpu']
    if @MODE@ == 'wrong-resource': spec['hard']['limits.memory']='1Gi'
    if @MODE@ == 'extra-resource': spec['hard']['configmaps']='1'
    print(json.dumps({'spec':spec}))
elif 'create' in args:
    (root/'created').write_text('yes')
    pod=json.load(sys.stdin)
    assert pod['spec']['hostUsers'] is False
    assert pod['spec']['automountServiceAccountToken'] is False
    assert pod['spec']['volumes'] == [{'name':'scratch','emptyDir':{'sizeLimit':@POD_SCRATCH@}}]
    assert pod['spec']['containers'][0]['volumeMounts'] == [{'name':'scratch','mountPath':'/scratch'}]
    assert pod['spec']['containers'][0]['env'] == [
        {'name':'TMPDIR','value':'/scratch'}, {'name':'TESSDATA_PREFIX','value':'/opt/tessdata'},
        {'name':'OMP_THREAD_LIMIT','value':@THREADS@}, {'name':'RAYON_NUM_THREADS','value':@THREADS@}]
    for scope in ['requests','limits']:
        assert pod['spec']['containers'][0]['resources'][scope] == {
            'cpu':@POD_CPU@,'memory':@POD_MEMORY@,'ephemeral-storage':@POD_SCRATCH@}
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
    if @MODE@ == 'delete-fails': sys.exit(1)
"#
            .replace("@ROOT@", &serde_json::to_string(directory.to_str().unwrap()).unwrap())
            .replace("@MODE@", &serde_json::to_string(mode).unwrap())
            .replace("@PODS@", &serde_json::to_string(&limits.pool_limit.to_string()).unwrap())
            .replace("@CPU@", &serde_json::to_string(&format!("{}m", limits.cpu_milli * i128::from(limits.pool_limit))).unwrap())
            .replace("@MEMORY@", &serde_json::to_string(&format!("{}m", limits.memory_milli * i128::from(limits.pool_limit))).unwrap())
            .replace("@SCRATCH@", &serde_json::to_string(&format!("{}m", limits.scratch_milli * i128::from(limits.pool_limit))).unwrap())
            .replace("@POD_CPU@", &serde_json::to_string(&limits.cpu).unwrap())
            .replace("@POD_MEMORY@", &serde_json::to_string(&limits.memory).unwrap())
            .replace("@POD_SCRATCH@", &serde_json::to_string(&limits.scratch).unwrap())
            .replace("@THREADS@", &serde_json::to_string(&limits.threads().to_string()).unwrap());
        std::fs::write(&executable, script).unwrap();
        std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
        KubernetesDocumentProcessor::new(
            executable,
            directory.join("kubeconfig"),
            "fixture".into(),
            "documents".into(),
            format!("worker@sha256:{}", "a".repeat(64)),
            limits,
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

    #[test]
    fn parses_kubernetes_quantities_and_rejects_unrepresentable_limits() {
        for (raw, expected) in [
            ("1", 1000),
            ("1000m", 1000),
            ("0.1m", 1),
            ("1e-9", 1),
            ("1.25", 1250),
            ("+1.25E+3", 1_250_000),
            ("1Ki", 1_024_000),
            ("1.5Mi", 1_572_864_000),
            ("1G", 1_000_000_000_000),
            ("1k", 1_000_000),
            ("1u", 1),
            ("1n", 1),
            (".5", 500),
            ("1.", 1000),
            ("1E", 1_000_000_000_000_000_000_000),
        ] {
            assert_eq!(parse_quantity_milli(raw), Some(expected), "{raw}");
        }
        assert_eq!(parse_quantity_milli("1e-9999"), Some(1));
        for raw in [
            "",
            "0",
            "-1",
            "1x",
            "1K",
            "1e",
            "1e+",
            ".",
            "1.2.3",
            "NaN",
            "1 Ei",
            "9223372036854775808",
            "1e9999",
        ] {
            assert_eq!(parse_quantity_milli(raw), None, "{raw}");
        }
        assert_eq!(parse_quantity_milli(&"0".repeat(128)), None);
        assert_eq!(parse_quantity_milli(&"0".repeat(129)), None);
        assert!(DocumentWorkerLimits::new(0, "1", "1Gi", "1Gi").is_err());
        assert!(DocumentWorkerLimits::new(2, "9223372036854775807", "1", "1").is_err());
    }

    #[test]
    fn quota_matches_normalized_aggregate_values_only() {
        let limits = DocumentWorkerLimits::new(3, "1250m", "1.5Gi", "512Mi").unwrap();
        let mut quota = json!({"spec":{"hard":{
            "pods":"3", "requests.cpu":"3.75", "limits.cpu":"3750m",
            "requests.memory":"4.5Gi", "limits.memory":"4608Mi",
            "requests.ephemeral-storage":"1.5Gi", "limits.ephemeral-storage":"1536Mi"
        }}});
        assert!(limits.matches_quota(&quota));
        quota["spec"]["hard"]["limits.memory"] = json!("4Gi");
        assert!(!limits.matches_quota(&quota));
        quota["spec"]["hard"]
            .as_object_mut()
            .unwrap()
            .remove("limits.memory");
        assert!(!limits.matches_quota(&quota));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn nondefault_limits_reach_pod_and_quota_gate() {
        let _guard = SERIAL.lock().await;
        let directory = tempfile::tempdir().unwrap();
        let limits = DocumentWorkerLimits::new(3, "1250m", "1.5Gi", "512Mi").unwrap();
        let processor = fixture_controller_with_limits(directory.path(), "valid", limits);
        assert_eq!(
            processor
                .process(fixture_input(), CancellationToken::new())
                .await
                .unwrap()
                .text,
            "fixture"
        );
        assert!(directory.path().join("created").is_file());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cloned_controllers_share_pool_and_failed_delete_consumes_slot() {
        let _guard = SERIAL.lock().await;
        let directory = tempfile::tempdir().unwrap();
        let limits = DocumentWorkerLimits::new(1, "500m", "1Gi", "1Gi").unwrap();
        let processor = fixture_controller_with_limits(directory.path(), "delete-fails", limits);
        assert!(matches!(
            processor
                .process(fixture_input(), CancellationToken::new())
                .await,
            Err(DocumentError::SandboxUnavailable)
        ));
        assert!(directory.path().join("deleted").is_file());
        assert!(matches!(
            processor
                .clone()
                .process(fixture_input(), CancellationToken::new())
                .await,
            Err(DocumentError::ResourceLimit)
        ));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cloned_controllers_reject_work_while_pool_is_full() {
        let _guard = SERIAL.lock().await;
        let directory = tempfile::tempdir().unwrap();
        let limits = DocumentWorkerLimits::new(1, "500m", "1Gi", "1Gi").unwrap();
        let processor = fixture_controller_with_limits(directory.path(), "wait", limits);
        let cancellation = CancellationToken::new();
        let task = tokio::spawn(processor.process(fixture_input(), cancellation.clone()));
        tokio::time::timeout(Duration::from_secs(5), async {
            while !directory.path().join("running").is_file() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(matches!(
            processor
                .clone()
                .process(fixture_input(), CancellationToken::new())
                .await,
            Err(DocumentError::ResourceLimit)
        ));
        cancellation.cancel();
        assert!(matches!(task.await.unwrap(), Err(DocumentError::Cancelled)));
        assert!(directory.path().join("deleted").is_file());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn controller_subprocess_environment_is_isolated() {
        const CHILD: &str = "OPENLEGAL_CONTROLLER_ENV_TEST";
        const INJECTED: &[&str] = &[
            "OPENLEGAL_LAW_PROVIDER_CREDENTIAL",
            "OPENLEGAL_DATABASE_URL",
            "KUBECONFIG",
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "NO_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
            "no_proxy",
        ];
        if std::env::var_os(CHILD).is_none() {
            // Inject into a separate test process; never mutate the concurrent
            // test runner's global environment (which would require unsafe Rust).
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "document_jobs::tests::controller_subprocess_environment_is_isolated",
                    "--nocapture",
                ])
                .env(CHILD, "child")
                .env("HOME", "/ambient-home-must-not-be-used")
                .envs(
                    INJECTED
                        .iter()
                        .map(|name| (*name, "synthetic-private-value")),
                )
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "child test failed: {}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            return;
        }
        for name in INJECTED {
            assert_eq!(std::env::var(name).unwrap(), "synthetic-private-value");
        }
        let _guard = SERIAL.lock().await;
        let directory = tempfile::tempdir().unwrap();
        let processor = fixture_controller(directory.path(), "valid");
        let result = processor
            .process(fixture_input(), CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(result.text, "fixture");
        let commands = std::fs::read_to_string(directory.path().join("commands")).unwrap();
        let commands: Vec<Vec<String>> = commands
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert_eq!(
            commands
                .iter()
                .map(|args| args[0].as_str())
                .collect::<Vec<_>>(),
            ["get", "create", "wait", "exec", "delete"]
        );
        assert!(directory.path().join("deleted").is_file());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn rejects_quotas_that_do_not_bound_every_worker_pod() {
        let _guard = SERIAL.lock().await;
        for mode in [
            "scoped",
            "selector",
            "unbounded",
            "missing-resource",
            "wrong-resource",
            "extra-resource",
        ] {
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
