#!/usr/bin/env python3
"""Disposable PG18/TLS physical replication fixture. Never uses production paths.

All Docker resources are fixed by a private /tmp manifest before creation. The
only SQL privileges provisioned belong to the two disposable fixture clusters.
No provider request, host port, image pull or production configuration is used.
"""
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import time

IMAGE = "postgres@sha256:ae6c78831cbc35fa3a4aaf4d763ddacf6183d6004774cc2dc28b3920410d1d1a"
POSTGRES_OWNER = "999:999"  # Verified UID/GID in this pinned Debian PG18 image.
ERROR_ROOT = None


def run(args, *, input=None, timeout=60, required=True):
    result = subprocess.run(args, input=input, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, timeout=timeout, check=False)
    if required and result.returncode:
        if ERROR_ROOT is not None:
            try:
                fd = os.open(ERROR_ROOT / "failure-stderr.bin", os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
                with os.fdopen(fd, "wb") as stream:
                    stream.write(result.stderr[:65536])
            except FileExistsError:
                pass
        raise RuntimeError("replication fixture command failed: " + args[0])
    return result


def private_json(path, data):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "w") as stream:
        json.dump(data, stream, sort_keys=True)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def sql(state, statement):
    result = run(["docker", "exec", "-i", state["primary"], "psql", "-X", "-At",
                  "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "postgres"],
                 input=statement.encode(), timeout=20)
    return result.stdout.decode().strip()


def address(state, name):
    template = '{{with index .NetworkSettings.Networks "' + state["network"] + '"}}{{.IPAddress}}{{end}}'
    value = run(["docker", "inspect", "--format", template, name]).stdout.decode().strip()
    import ipaddress
    ipaddress.IPv4Address(value)
    return value


def certificate(base, name, subject, *, ips=(), client=False):
    certs = base / "certs"
    run(["openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", subject,
         "-keyout", str(certs / (name + ".key")), "-out", str(certs / (name + ".csr"))])
    extensions = certs / (name + ".ext")
    extensions.write_text("extendedKeyUsage=" + ("clientAuth" if client else "serverAuth")
                          + ("\nsubjectAltName=" + ",".join("IP:" + ip for ip in ips) if ips else "") + "\n")
    run(["openssl", "x509", "-req", "-days", "1", "-in", str(certs / (name + ".csr")),
         "-CA", str(certs / "ca.crt"), "-CAkey", str(certs / "ca.key"),
         "-set_serial", str(secrets.randbits(120) + 1), "-extfile", str(extensions),
         "-out", str(certs / (name + ".crt"))])
    os.chmod(certs / (name + ".key"), 0o600)


def permissions(state, *, restore=False):
    owner = str(state["uid"]) + ":" + str(state["gid"]) if restore else POSTGRES_OWNER
    # The mount is the single UUID-owned disposable /tmp root. No arbitrary path
    # is accepted from the command line or a production configuration.
    run(["docker", "run", "--rm", "--pull=never", "--network=none", "--user", "0:0",
         "--name", state["helper"], "-v", state["base"] + ":/fixture", "--entrypoint", "sh",
         IMAGE, "-c", 'chown -R "$1" /fixture/certs /fixture/primary /fixture/standby; '
         'chmod 700 /fixture/certs /fixture/primary /fixture/standby; '
         + ('' if restore else 'touch /fixture/certs/READY; chown 999:999 /fixture/certs/READY'), "fixture", owner],
        timeout=30)


def prepare(base):
    suffix = base.name.removeprefix("openlegal-replication-test-")
    if not suffix or len(suffix) > 80 or not all(c.isalnum() or c in "_-" for c in suffix):
        raise RuntimeError("invalid fixture namespace")
    prefix = "openlegal-repl-" + suffix.lower()
    state = {"base": str(base), "uid": os.geteuid(), "gid": os.getegid(),
             "network": prefix + "-net", "primary": prefix + "-primary",
             "standby": prefix + "-standby", "helper": prefix + "-helper", "runner": None}
    private_json(base / "state.json", state)
    for child in ("certs", "primary", "standby"):
        (base / child).mkdir(mode=0o700)
    run(["docker", "image", "inspect", IMAGE])
    run(["docker", "network", "create", "--internal", state["network"]])
    if Path("/.dockerenv").exists():
        runner = Path("/etc/hostname").read_text().strip()
        if not runner or not all(c.isalnum() or c in "_-" for c in runner):
            raise RuntimeError("unsupported runner container identity")
        run(["docker", "inspect", "--type", "container", runner])
        run(["docker", "network", "connect", state["network"], runner])
        state["runner"] = runner
        private_json(base / "runner.json", {"runner": runner})
    certs = base / "certs"
    run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
         "-subj", "/CN=openlegal-replication-fixture-ca", "-keyout", str(certs / "ca.key"),
         "-out", str(certs / "ca.crt")])
    certificate(base, "replica", "/CN=openlegal_fixture_replica", client=True)
    (certs / "pg_hba.conf").write_text(
        "local all all trust\n"
        "hostssl replication openlegal_fixture_replica all cert\n"
        "hostssl all postgres all scram-sha-256\n"
        "hostssl all openlegal_fixture_runtime all scram-sha-256\n"
        "host all all all reject\n")
    password = secrets.token_hex(24)
    runtime_password = secrets.token_hex(24)
    wait = ('for n in $(seq 1 600); do if test -f /certs/READY; then '
            'exec docker-entrypoint.sh postgres -c ssl=on -c ssl_cert_file=/certs/server.crt '
            '-c ssl_key_file=/certs/server.key -c ssl_ca_file=/certs/ca.crt '
            '-c hba_file=/certs/pg_hba.conf -c log_statement=none '
            '-c log_min_error_statement=panic -c wal_keep_size=64MB; fi; sleep 0.1; done; exit 1')
    run(["docker", "run", "-d", "--pull=never", "--name", state["primary"],
         "--network", state["network"], "--memory", "512m", "--cpus", "2",
         "-e", "POSTGRES_PASSWORD=" + password, "-v", str(base / "primary") + ":/var/lib/postgresql",
         "-v", str(certs) + ":/certs:ro", "--entrypoint", "sh", IMAGE, "-c", wait])
    # Reserve both ACTUAL addresses before issuing the VerifyFull server cert.
    # The standby does not start PostgreSQL until basebackup exits successfully.
    standby_wait = ('for n in $(seq 1 1800); do '
                    'if test -f /var/lib/postgresql/.fixture-basebackup-ready; then '
                    'exec docker-entrypoint.sh postgres -c ssl=on -c ssl_cert_file=/certs/server.crt '
                    '-c ssl_key_file=/certs/server.key -c ssl_ca_file=/certs/ca.crt '
                    '-c hba_file=/certs/pg_hba.conf -c log_statement=none '
                    '-c log_min_error_statement=panic; fi; sleep 0.1; done; exit 1')
    run(["docker", "run", "-d", "--pull=never", "--name", state["standby"],
         "--network", state["network"], "--memory", "512m", "--cpus", "2",
         "-v", str(base / "standby") + ":/var/lib/postgresql", "-v", str(certs) + ":/certs:ro",
         "--entrypoint", "sh", IMAGE, "-c", standby_wait])
    primary_ip = address(state, state["primary"])
    standby_ip = address(state, state["standby"])
    if primary_ip == standby_ip:
        raise RuntimeError("fixture peer address alias")
    certificate(base, "server", "/CN=openlegal-fixture-primary", ips=(primary_ip, standby_ip))
    # Host Sqlx only needs the CA, copied outside the postgres-private cert tree.
    (base / "ca.crt").write_bytes((certs / "ca.crt").read_bytes())
    os.chmod(base / "ca.crt", 0o600)
    (certs / "ca.key").unlink()
    permissions(state)
    for _ in range(120):
        ready = run(["docker", "exec", state["primary"], "pg_isready", "-h", "127.0.0.1", "-U", "postgres"],
                    required=False, timeout=5)
        if ready.returncode == 0:
            break
        time.sleep(0.25)
    else:
        raise RuntimeError("primary readiness deadline")
    sql(state, "CREATE ROLE openlegal_fixture_replica LOGIN REPLICATION;\n"
        "CREATE ROLE openlegal_fixture_runtime LOGIN PASSWORD '" + runtime_password + "';\n")
    conninfo = ("host=" + primary_ip + " port=5432 user=openlegal_fixture_replica "
                "application_name=openlegal_ssd sslmode=verify-full sslrootcert=/certs/ca.crt "
                "sslcert=/certs/replica.crt sslkey=/certs/replica.key")
    run(["docker", "run", "--rm", "--pull=never", "--name", state["helper"],
         "--network", state["network"], "--user", POSTGRES_OWNER, "-v",
         str(base / "standby") + ":/var/lib/postgresql", "-v", str(certs) + ":/certs:ro",
         "--entrypoint", "sh", IMAGE, "-c",
         'pg_basebackup "$@" && touch /var/lib/postgresql/.fixture-basebackup-ready', "fixture",
         "-D", "/var/lib/postgresql/18/docker",
         "-d", conninfo, "-R", "-X", "stream", "-c", "fast", "--create-slot",
         "--slot=openlegal_fixture"], timeout=90)
    for _ in range(120):
        if sql(state, "SELECT count(*) FROM pg_stat_replication WHERE application_name='openlegal_ssd' AND state='streaming';") == "1":
            break
        time.sleep(0.25)
    else:
        raise RuntimeError("standby streaming deadline")
    sql(state, "ALTER SYSTEM SET synchronous_standby_names='FIRST 1 (openlegal_ssd)';\nSELECT pg_reload_conf();")
    for _ in range(120):
        if sql(state, "SELECT count(*) FROM pg_stat_replication WHERE application_name='openlegal_ssd' AND sync_state='sync';") == "1":
            break
        time.sleep(0.25)
    else:
        raise RuntimeError("standby synchronous deadline")
    private_json(base / "connection.json", {
        "admin_url": "postgresql://postgres:" + password + "@" + primary_ip + "/postgres",
        "runtime_url": "postgresql://openlegal_fixture_runtime:" + runtime_password + "@" + primary_ip + "/postgres",
        "standby_admin_url": "postgresql://postgres:" + password + "@" + standby_ip + "/postgres",
        "standby_runtime_url": "postgresql://openlegal_fixture_runtime:" + runtime_password + "@" + standby_ip + "/postgres",
        "primary": state["primary"], "standby": state["standby"], "ca_file": str(base / "ca.crt")})


def qualify(base, state):
    sql(state, """
CREATE ROLE openlegal_fixture_observer NOLOGIN;
GRANT pg_read_all_stats TO openlegal_fixture_observer;
GRANT EXECUTE ON FUNCTION pg_catalog.pg_control_system(),pg_catalog.pg_control_checkpoint() TO openlegal_fixture_observer;
GRANT USAGE,CREATE ON SCHEMA openlegal,openlegal_replication TO openlegal_fixture_observer;
GRANT SELECT ON openlegal_replication.control TO openlegal_fixture_observer;
ALTER FUNCTION openlegal.replication_required() OWNER TO openlegal_fixture_observer;
ALTER FUNCTION openlegal_replication.capture_prefix() OWNER TO openlegal_fixture_observer;
ALTER FUNCTION openlegal_replication.observe_flush() OWNER TO openlegal_fixture_observer;
REVOKE CREATE ON SCHEMA openlegal,openlegal_replication FROM openlegal_fixture_observer;
GRANT USAGE ON SCHEMA openlegal,openlegal_admin,openlegal_replication TO openlegal_fixture_runtime;
GRANT SELECT ON public._sqlx_migrations TO openlegal_fixture_runtime;
GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA openlegal TO openlegal_fixture_runtime;
GRANT EXECUTE ON FUNCTION openlegal_replication.capture_prefix(),openlegal_replication.observe_flush(),
 openlegal_admin.canary_consume(uuid) TO openlegal_fixture_runtime;
INSERT INTO openlegal_replication.control
 SELECT true,uuidv7(),p.system_identifier::text,c.timeline_id,'openlegal_ssd',r.usename,r.client_addr,
 ssl.client_dn,ssl.issuer_dn,ssl.client_serial,
 '1111111111111111111111111111111111111111111111111111111111111111'
 FROM pg_control_system() p CROSS JOIN pg_control_checkpoint() c CROSS JOIN pg_stat_replication r
 JOIN pg_stat_ssl ssl USING(pid) WHERE r.application_name='openlegal_ssd' AND r.sync_state='sync';
""")
    epoch = sql(state, "SELECT managed_epoch FROM openlegal_replication.control WHERE singleton;")
    if not epoch or len(epoch) != 36:
        raise RuntimeError("replication control missing")
    private_json(base / "qualified.json", {"epoch": epoch})


def stop_primary(base, state):
    # Read the exact owned CID before the effect and stop/read back that CID.
    cid = run(["docker", "inspect", "--type", "container", "--format", "{{.Id}}", state["primary"]]).stdout.decode().strip()
    if len(cid) != 64 or any(c not in "0123456789abcdef" for c in cid):
        raise RuntimeError("owned primary container identity")
    run(["docker", "stop", "--time", "5", cid], timeout=20)
    waited = run(["docker", "wait", cid], timeout=10).stdout.decode().strip()
    actual = json.loads(run(["docker", "inspect", "--type", "container", "--format", "{{json .State}}", cid]).stdout)
    if (actual["Running"] or actual["Restarting"] or actual["Dead"]
            or actual["Pid"] != 0 or actual["Status"] != "exited"
            or waited != str(actual["ExitCode"]) or actual["ExitCode"] != 0):
        raise RuntimeError("owned primary stop not acknowledged")
    private_json(base / "primary-stopped.json", {
        "schema_version": 1, "kind": "openlegal_fixture_primary_stopped", "container_id": cid,
        "status": actual["Status"], "running": actual["Running"], "pid": actual["Pid"],
        "exit_code": actual["ExitCode"]})


def cleanup(base, state):
    for name in (state["standby"], state["primary"], state["helper"]):
        run(["docker", "unpause", name], required=False, timeout=10)
        run(["docker", "rm", "-f", "-v", name], required=False, timeout=20)
    runner_file = base / "runner.json"
    if runner_file.exists():
        runner = json.loads(runner_file.read_text())["runner"]
        run(["docker", "network", "disconnect", state["network"], runner], required=False, timeout=15)
    run(["docker", "network", "rm", state["network"]], required=False, timeout=15)
    permissions(state, restore=True)
    # Do not call successful cleanup from rm ACK alone. The fixed endpoint must
    # still answer, and every exact owned container/network must be absent.
    run(["docker", "info", "--format", "{{.ID}}"], timeout=15)
    for name in (state["standby"],state["primary"],state["helper"]):
        result = run(["docker","inspect","--type","container","--format","{{.Id}}",name],required=False,timeout=10)
        if result.returncode == 0 or b"No such" not in result.stderr:
            raise RuntimeError("owned fixture container closure unknown")
    result = run(["docker","network","inspect",state["network"]],required=False,timeout=10)
    if result.returncode == 0 or b"not found" not in result.stderr.lower():
        raise RuntimeError("owned fixture network closure unknown")


def main():
    global ERROR_ROOT
    if len(sys.argv) != 3 or sys.argv[1] not in ("prepare", "qualify", "stop_primary", "cleanup"):
        raise RuntimeError("fixed fixture command required")
    base = Path(sys.argv[2])
    if base.parent != Path("/tmp") or not base.name.startswith("openlegal-replication-test-") or base.is_symlink():
        raise RuntimeError("fixture must be a private direct /tmp child")
    st = base.stat()
    if st.st_uid != os.geteuid() or st.st_mode & 0o077 or not base.is_dir():
        raise RuntimeError("fixture owner/mode rejected")
    ERROR_ROOT = base
    if sys.argv[1] == "prepare":
        prepare(base)
    else:
        state = json.loads((base / "state.json").read_text())
        if state["base"] != str(base) or state["uid"] != os.geteuid():
            raise RuntimeError("fixture identity rejected")
        {"qualify": qualify, "stop_primary": stop_primary, "cleanup": cleanup}[sys.argv[1]](base, state)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # No subprocess output, URLs, SQL text or test credentials in diagnostics.
        detail = str(error) if type(error) is RuntimeError else type(error).__name__
        print("replication fixture failed: " + detail, file=sys.stderr)
        sys.exit(1)
