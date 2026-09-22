#!/usr/bin/env python3
"""Prepare private disposable workload manifests; never connects to a cluster.

Invoke with the hash-locked deployment-tools Python (PyYAML) and kubectl.
All images must already be admitted and loaded into the owned kind node.
Apply files individually in numbered order, waiting for each Job before the next.
The final serving Deployment starts at zero replicas: explicitly scale to one
only after dictionary admission, migration, grants and synthetic seed succeed.
"""
import argparse
import copy
import ipaddress
import os
from pathlib import Path
import re
import secrets
import subprocess
import sys

import yaml

REPO = Path(__file__).resolve().parents[2]
SERVING = "openlegal-serving"
POSTGRES = "openlegal-accept-postgres"
MONITOR = "openlegal-accept-monitor"
CLIENT = "openlegal-accept-client"
PG_HOST = f"postgres.{POSTGRES}.svc.cluster.local"
POSTGRES_IMAGE = "postgres@sha256:ae6c78831cbc35fa3a4aaf4d763ddacf6183d6004774cc2dc28b3920410d1d1a"


def run(command):
    """Never expose subprocess output, which can contain generated credentials."""
    try:
        result = subprocess.run(command, cwd=REPO, stdin=subprocess.DEVNULL,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                timeout=30, check=True)
    except (OSError, subprocess.SubprocessError) as error:
        raise ValueError("fixture preparation subprocess failed") from error
    if len(result.stdout) > 2 * 1024 * 1024:
        raise ValueError("fixture preparation output exceeded limit")
    return result.stdout.decode("utf-8")


def validate(args):
    if not re.fullmatch(r"openlegal-accept-[0-9a-f]{16}-control-plane", args.node):
        raise ValueError("expected owned disposable acceptance node")
    if not re.fullmatch(r"[0-9a-f]{40}", args.source_revision):
        raise ValueError("expected explicit source revision")
    for value in (args.server_image, args.seed_image):
        if not re.fullmatch(r"[a-z0-9][a-z0-9./:_-]*@sha256:[0-9a-f]{64}", value):
            raise ValueError("expected immutable admitted image reference")
    if args.postgres_image != POSTGRES_IMAGE:
        raise ValueError("expected existing pinned PostgreSQL 18 image")
    for value in (args.edge_ip, args.node_ip):
        address = ipaddress.ip_address(value)
        if address.version != 4 or address.is_unspecified or address.is_multicast or address.is_loopback:
            raise ValueError("expected explicit unicast fixture IPv4 address")
    if not args.kubectl.is_absolute() or not args.kubectl.is_file():
        raise ValueError("expected explicit provisioned kubectl binary")
    if not args.output.is_absolute() or args.output.exists() or args.output.is_symlink():
        raise ValueError("output must be a fresh absolute private directory")


def object_(kind, name, namespace=None, **fields):
    metadata = {"name": name}
    if namespace:
        metadata["namespace"] = namespace
    api = {"Job": "batch/v1", "Deployment": "apps/v1", "NetworkPolicy": "networking.k8s.io/v1"}.get(kind, "v1")
    return {"apiVersion": api, "kind": kind, "metadata": metadata, **fields}


def secret(name, data, namespace=SERVING):
    return object_("Secret", name, namespace, type="Opaque", stringData=data)


def dump(root, name, docs):
    with (root / name).open("x") as output:
        yaml.safe_dump_all(docs, output, sort_keys=False)


def load(path):
    return yaml.safe_load((REPO / path).read_text())


def namespace(name):
    obj = load("deploy/kubernetes/serving/namespace.yaml")
    obj["metadata"]["name"] = name
    return obj


def certificate(root, name, sans, serial):
    run(["openssl", "req", "-new", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256",
         "-nodes", "-subj", f"/CN=disposable-{name}", "-keyout", str(root / f"{name}.key"),
         "-out", str(root / f"{name}.csr")])
    (root / f"{name}.ext").write_text(f"subjectAltName={sans}\nbasicConstraints=critical,CA:FALSE\n"
                                         "keyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth\n")
    run(["openssl", "x509", "-req", "-days", "2", "-in", str(root / f"{name}.csr"),
         "-CA", str(root / "ca.crt"), "-CAkey", str(root / "ca.key"), "-set_serial", str(serial),
         "-extfile", str(root / f"{name}.ext"), "-out", str(root / f"{name}.crt")])


def security():
    return {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
            "capabilities": {"drop": ["ALL"]}}


def pod_security(uid=10004):
    return {"runAsNonRoot": True, "runAsUser": uid, "runAsGroup": uid, "fsGroup": uid,
            "fsGroupChangePolicy": "OnRootMismatch", "seccompProfile": {"type": "RuntimeDefault"}}


def mount(name, path, readonly=True):
    return {"name": name, "mountPath": path, "readOnly": readonly}


def secret_volume(name, secret_name):
    return {"name": name, "secret": {"secretName": secret_name, "defaultMode": 0o440}}


def env_secret(name, secret_name, key=None):
    return {"name": name, "valueFrom": {"secretKeyRef": {"name": secret_name, "key": key or name}}}


def pg_job(name, sql_secret, image, node):
    container = {"name": "sql", "image": image, "imagePullPolicy": "Never", "securityContext": security(),
                 "command": ["psql"], "args": ["-X", "-v", "ON_ERROR_STOP=1", "-f", "/sql/run.sql"],
                 "env": [{"name": k, "value": v} for k, v in {"PGHOST": PG_HOST, "PGUSER": "postgres",
                         "PGDATABASE": "postgres" if name.endswith("bootstrap") else "openlegal",
                         "PGSSLMODE": "verify-full", "PGSSLROOTCERT": "/ca/ca.crt", "PGCONNECT_TIMEOUT": "5"}.items()]
                         + [env_secret("PGPASSWORD", "postgres-auth", "POSTGRES_PASSWORD")],
                 "volumeMounts": [mount("ca", "/ca"), mount("sql", "/sql")],
                 "resources": {"requests": {"cpu": "25m", "memory": "32Mi"}, "limits": {"cpu": "1", "memory": "128Mi"}}}
    return object_("Job", name, POSTGRES, spec={"backoffLimit": 0, "activeDeadlineSeconds": 90,
        "template": {"metadata": {"labels": {"app.kubernetes.io/name": "postgres-admin"}},
        "spec": {"restartPolicy": "Never", "automountServiceAccountToken": False, "enableServiceLinks": False,
                 "nodeSelector": {"kubernetes.io/hostname": node}, "securityContext": pod_security(),
                 "containers": [container], "volumes": [secret_volume("ca", "postgres-ca"), secret_volume("sql", sql_secret)]}}})


def prepare(args):
    validate(args)
    old_umask = os.umask(0o077)
    try:
        args.output.mkdir(mode=0o700)
        prepare_private(args)
    finally:
        os.umask(old_umask)


def prepare_private(args):
    root = args.output
    tls = root / "tls"
    tls.mkdir(mode=0o700)
    run(["openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
         "-days", "2", "-subj", "/CN=disposable-serving-ca", "-addext", "basicConstraints=critical,CA:TRUE",
         "-addext", "keyUsage=critical,keyCertSign,cRLSign", "-keyout", str(tls / "ca.key"), "-out", str(tls / "ca.crt")])
    certificate(tls, "backend", f"IP:{args.node_ip}", 1)
    certificate(tls, "edge", f"IP:{args.edge_ip},DNS:openlegal4everyone.stream", 2)
    certificate(tls, "postgres", f"DNS:{PG_HOST},DNS:postgres", 3)
    certificate(tls, "wrong-backend", "DNS:wrong.invalid", 4)
    run(["openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
         "-days", "2", "-subj", "/CN=unrelated-disposable-ca", "-addext", "basicConstraints=critical,CA:TRUE",
         "-keyout", str(tls / "unrelated-ca.key"), "-out", str(tls / "unrelated-ca.crt")])
    passwords = {role: secrets.token_hex(24) for role in ("postgres", "migration", "runtime")}
    serving = list(yaml.safe_load_all(run([str(args.kubectl), "kustomize", "deploy/kubernetes/serving"])))
    migration = list(yaml.safe_load_all(run([str(args.kubectl), "kustomize", "deploy/kubernetes/admin/migrate"])))
    config, = [obj for obj in serving if obj["kind"] == "ConfigMap"]
    config_text = config["data"]["server.toml"].replace("REPLACE_WITH_RELEASE_REVISION", args.source_revision)
    config_text = config_text.replace("REPLACE_WITH_OXIBELT_BACKEND_AUTHORITY", f"{args.node_ip}:30080")
    config_text = config_text.replace("REPLACE_WITH_OXIBELT_WEBTRANSPORT_AUTHORITY", f"{args.node_ip}:30433")
    if "REPLACE_WITH_" in config_text:
        raise ValueError("canonical config contains unresolved fixture placeholders")
    config["data"]["server.toml"] = config_text
    server, = [obj for obj in serving if obj["kind"] == "Deployment"]
    server["spec"]["replicas"] = 0
    server_pod = server["spec"]["template"]["spec"]
    server_pod["nodeSelector"]["kubernetes.io/hostname"] = args.node
    server_pod["containers"][0]["image"] = args.server_image
    server_pod["containers"][0]["imagePullPolicy"] = "Never"
    migrate, = [obj for obj in migration if obj["kind"] == "Job"]
    migrate["spec"]["suspend"] = False
    migrate_pod = migrate["spec"]["template"]["spec"]
    migrate_pod["nodeSelector"]["kubernetes.io/hostname"] = args.node
    migrate_pod["containers"][0]["image"] = args.server_image
    migrate_pod["containers"][0]["imagePullPolicy"] = "Never"
    seed_pod = copy.deepcopy(server_pod)
    seed_pod["restartPolicy"] = "Never"
    seed = seed_pod["containers"][0]
    seed["name"] = "seed"
    seed["image"] = args.seed_image
    seed["args"] = ["/var/lib/openlegal/cache-blobs/data", "/var/lib/openlegal/corpus-blobs/data", "/run/secrets/postgres-ca/ca.crt"]
    for field in ("ports", "startupProbe", "livenessProbe", "readinessProbe"):
        seed.pop(field, None)
    seed["resources"] = {"requests": {"cpu": "100m", "memory": "128Mi"}, "limits": {"cpu": "1", "memory": "512Mi"}}
    seed_mounts = {"cache-blobs", "corpus-blobs", "postgres-ca"}
    seed["volumeMounts"] = [m for m in seed["volumeMounts"] if m["name"] in seed_mounts]
    seed_pod["volumes"] = [v for v in seed_pod["volumes"] if v["name"] in seed_mounts]
    seed_job = object_("Job", "openlegal-seed", SERVING, spec={"backoffLimit": 0, "activeDeadlineSeconds": 180,
        "template": {"metadata": {"labels": {"app.kubernetes.io/name": "openlegal-admin"}}, "spec": seed_pod}})
    dump(root, "00-namespaces.yaml", [namespace(name) for name in (SERVING, POSTGRES, MONITOR, CLIENT)])
    credentials = [config, secret("openlegal-postgres-ca", {"ca.crt": (tls / "ca.crt").read_text()}),
                   secret("openlegal-backend-tls", {"tls.crt": (tls / "backend.crt").read_text(), "tls.key": (tls / "backend.key").read_text()})]
    for role, variable in (("runtime", "OPENLEGAL_DATABASE_URL"), ("migration", "OPENLEGAL_MIGRATION_DATABASE_URL")):
        credentials.append(secret(f"openlegal-{role}-db", {variable: f"postgresql://{role}:{passwords[role]}@{PG_HOST}:5432/openlegal"}))
    dump(root, "10-serving-inputs.yaml", credentials)
    prepare_postgres(root, args, passwords, tls)
    policies = [obj for obj in serving if obj["kind"] == "NetworkPolicy"]
    for profile in ("edge", "postgres-in-cluster", "dns-cluster", "monitoring"):
        policy = load(f"deploy/kubernetes/network/{profile}/policy.yaml")
        policy["metadata"]["namespace"] = SERVING
        if profile == "edge":
            policy["spec"]["ingress"][0]["from"][0]["ipBlock"]["cidr"] = f"{args.edge_ip}/32"
        elif profile == "postgres-in-cluster":
            peer = policy["spec"]["egress"][0]["to"][0]
            peer["namespaceSelector"]["matchLabels"]["kubernetes.io/metadata.name"] = POSTGRES
            peer["podSelector"]["matchLabels"]["app.kubernetes.io/name"] = "postgres"
        elif profile == "monitoring":
            peer = policy["spec"]["ingress"][0]["from"][0]
            peer["namespaceSelector"]["matchLabels"]["kubernetes.io/metadata.name"] = MONITOR
            peer["podSelector"]["matchLabels"]["app.kubernetes.io/name"] = "accept-monitor"
        policies.append(policy)
    pg_default = load("deploy/kubernetes/network/base/policy.yaml")
    pg_default["metadata"]["namespace"] = POSTGRES
    policies.append(pg_default)
    pg_ingress = object_("NetworkPolicy", "postgres-clients", POSTGRES, spec={
        "podSelector": {"matchLabels": {"app.kubernetes.io/name": "postgres"}}, "policyTypes": ["Ingress"],
        "ingress": [{"from": [
            {"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": SERVING}},
             "podSelector": {"matchExpressions": [{"key": "app.kubernetes.io/name", "operator": "In",
                                                     "values": ["openlegal-server", "openlegal-admin"]}]}},
            {"podSelector": {"matchLabels": {"app.kubernetes.io/name": "postgres-admin"}}}],
            "ports": [{"protocol": "TCP", "port": 5432}]}]})
    pg_egress = object_("NetworkPolicy", "postgres-admin-egress", POSTGRES, spec={
        "podSelector": {"matchLabels": {"app.kubernetes.io/name": "postgres-admin"}}, "policyTypes": ["Egress"],
        "egress": [{"to": [{"podSelector": {"matchLabels": {"app.kubernetes.io/name": "postgres"}}}],
                    "ports": [{"protocol": "TCP", "port": 5432}]},
                   {"to": [{"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "kube-system"}},
                             "podSelector": {"matchLabels": {"k8s-app": "kube-dns"}}}],
                    "ports": [{"protocol": "UDP", "port": 53}, {"protocol": "TCP", "port": 53}]}]})
    policies.extend([pg_ingress, pg_egress])
    dump(root, "15-network.yaml", policies)
    dump(root, "30-bootstrap.yaml", [pg_job("postgres-bootstrap", "postgres-bootstrap-sql", args.postgres_image, args.node)])
    dump(root, "40-migrate.yaml", [migrate])
    dump(root, "50-grants.yaml", [pg_job("postgres-grants", "postgres-grants-sql", args.postgres_image, args.node)])
    dump(root, "60-seed.yaml", [seed_job])
    dump(root, "70-serving.yaml", [obj for obj in serving if obj["kind"] in ("Deployment", "Service")])
    edge_config = (REPO / "deploy/oxibelt/kubernetes-upstream.example.toml").read_text()
    edge_config = edge_config.replace("replace-with-private-node.invalid", args.node_ip)
    edge_config = edge_config.replace('hosts = ["openlegal4everyone.stream"]', f'hosts = ["{args.edge_ip}", "openlegal4everyone.stream"]')
    (root / "oxibelt.toml").write_text(edge_config)
    for source, destination in (("edge.crt", "edge.pem"), ("edge.key", "edge-key.pem"), ("ca.crt", "backend-ca.pem")):
        (root / destination).write_bytes((tls / source).read_bytes())


def prepare_postgres(root, args, passwords, tls):
    auth = secret("postgres-auth", {"POSTGRES_PASSWORD": passwords["postgres"]}, POSTGRES)
    pg_tls = secret("postgres-tls", {"server.crt": (tls / "postgres.crt").read_text(), "server.key": (tls / "postgres.key").read_text()}, POSTGRES)
    ca = secret("postgres-ca", {"ca.crt": (tls / "ca.crt").read_text()}, POSTGRES)
    bootstrap = secret("postgres-bootstrap-sql", {"run.sql":
        f"CREATE ROLE migration LOGIN PASSWORD '{passwords['migration']}';\n"
        f"CREATE ROLE runtime LOGIN PASSWORD '{passwords['runtime']}';\nCREATE DATABASE openlegal OWNER migration;\n"}, POSTGRES)
    grants = secret("postgres-grants-sql", {"run.sql": "REVOKE CREATE ON SCHEMA public FROM PUBLIC;\n"
        "GRANT USAGE ON SCHEMA openlegal TO runtime;\nGRANT SELECT ON public._sqlx_migrations TO runtime;\n"
        "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA openlegal TO runtime;\n"}, POSTGRES)
    hba = object_("ConfigMap", "postgres-hba", POSTGRES, data={"pg_hba.conf":
        "local all all trust\nhostssl all all all scram-sha-256\nhostnossl all all all reject\n"})
    init = {"name": "tls-permissions", "image": args.postgres_image, "imagePullPolicy": "Never",
            "securityContext": security(), "command": ["sh", "-ec", "cp /tls-input/* /tls/; chmod 600 /tls/*"],
            "volumeMounts": [mount("tls-input", "/tls-input"), mount("tls", "/tls", False)],
            "resources": {"requests": {"cpu": "25m", "memory": "16Mi"}, "limits": {"cpu": "1", "memory": "64Mi"}}}
    container = {"name": "postgres", "image": args.postgres_image, "imagePullPolicy": "Never", "securityContext": security(),
        "args": ["postgres", "-c", "ssl=on", "-c", "ssl_cert_file=/tls/server.crt", "-c", "ssl_key_file=/tls/server.key",
                 "-c", "hba_file=/hba/pg_hba.conf", "-c", "log_statement=none", "-c", "log_min_error_statement=panic"],
        "env": [env_secret("POSTGRES_PASSWORD", "postgres-auth")], "ports": [{"containerPort": 5432}],
        "volumeMounts": [mount("data", "/var/lib/postgresql", False), mount("socket", "/var/run/postgresql", False),
                         mount("tmp", "/tmp", False), mount("tls", "/tls"), mount("hba", "/hba")],
        "resources": {"requests": {"cpu": "100m", "memory": "128Mi"}, "limits": {"cpu": "1", "memory": "512Mi"}},
        "readinessProbe": {"exec": {"command": ["pg_isready", "-h", "127.0.0.1", "-U", "postgres"]},
                           "initialDelaySeconds": 3, "periodSeconds": 2, "timeoutSeconds": 2}}
    labels = {"app.kubernetes.io/name": "postgres"}
    workload = object_("Deployment", "postgres", POSTGRES, spec={"replicas": 1, "strategy": {"type": "Recreate"},
        "selector": {"matchLabels": labels}, "template": {"metadata": {"labels": labels}, "spec": {
            "automountServiceAccountToken": False, "enableServiceLinks": False, "securityContext": pod_security(999),
            "nodeSelector": {"kubernetes.io/hostname": args.node}, "initContainers": [init], "containers": [container],
            "volumes": [{"name": name, "emptyDir": {"sizeLimit": "1Gi" if name == "data" else "16Mi"}} for name in ("data", "socket", "tmp", "tls")]
                + [secret_volume("tls-input", "postgres-tls"), {"name": "hba", "configMap": {"name": "postgres-hba"}}]}}})
    service = object_("Service", "postgres", POSTGRES, spec={"selector": labels, "ports": [{"port": 5432, "targetPort": 5432}]})
    dump(root, "20-postgres.yaml", [auth, pg_tls, ca, bootstrap, grants, hba, workload, service])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("output", "kubectl"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    for name in ("node", "server-image", "seed-image", "postgres-image", "edge-ip", "node-ip", "source-revision"):
        parser.add_argument(f"--{name}", required=True)
    try:
        prepare(parser.parse_args())
    except (ValueError, OSError, yaml.YAMLError):
        print("Disposable workload preparation failed; inspect private run inputs.", file=sys.stderr)
        return 1
    print("Disposable manifests prepared; no cluster operations performed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
