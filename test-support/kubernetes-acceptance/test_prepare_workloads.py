"""Offline safety and rendered contract tests for private acceptance inputs."""
import argparse
import importlib.util
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("prepare_workloads", Path(__file__).with_name("prepare-workloads.py"))
prepare = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(prepare)


class WorkloadTests(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.root = Path(self.scratch.name)
        self.binary = self.root / "kubectl"
        self.binary.touch()
        self.args = argparse.Namespace(output=self.root / "out", kubectl=self.binary,
            node="openlegal-accept-0123456789abcdef-control-plane", server_image="fixture/server@sha256:" + "1" * 64,
            seed_image="fixture/seed@sha256:" + "2" * 64, postgres_image=prepare.POSTGRES_IMAGE,
            edge_ip="172.28.0.3", node_ip="172.28.0.2", source_revision="a" * 40)

    def test_rejects_unowned_node_mutable_images_and_existing_output(self):
        for field, value in (("node", "production-node"), ("source_revision", "main"), ("server_image", "fixture:latest"),
                             ("seed_image", "fixture:latest"), ("postgres_image", "postgres:18"),
                             ("edge_ip", "0.0.0.0"), ("node_ip", "127.0.0.1"),
                             ("output", self.root), ("kubectl", Path("kubectl"))):
            with self.subTest(field=field):
                args = argparse.Namespace(**vars(self.args))
                setattr(args, field, value)
                with self.assertRaises(ValueError):
                    prepare.validate(args)
        self.assertFalse(self.args.output.exists())

    def fake_run(self, command):
        if command[0] == "openssl":
            for flag in ("-keyout", "-out"):
                if flag in command:
                    Path(command[command.index(flag) + 1]).write_text("synthetic-test-only-pem\n")
            return ""
        base = prepare.REPO / command[-1]
        config = prepare.object_("ConfigMap", "openlegal-server-config-test", prepare.SERVING,
            data={"server.toml": (prepare.REPO / "deploy/kubernetes/config/server.toml").read_text()})
        names = ["namespace.yaml", "deployment.yaml", "service.yaml"] if base.name == "serving" else ["job.yaml"]
        docs = [prepare.yaml.safe_load((base / name).read_text()) for name in names] + [config]
        for obj in docs:
            if obj["kind"] != "Namespace":
                obj["metadata"]["namespace"] = prepare.SERVING
        return prepare.yaml.safe_dump_all(docs)

    def test_outputs_private_staged_jobs_and_preserves_serving_resources(self):
        with patch.object(prepare, "run", side_effect=self.fake_run):
            prepare.prepare(self.args)
        self.assertEqual(self.args.output.stat().st_mode & 0o777, 0o700)
        for path in self.args.output.rglob("*"):
            if path.is_file():
                self.assertEqual(path.stat().st_mode & 0o777, 0o600)
        def docs(name):
            return list(prepare.yaml.safe_load_all((self.args.output / name).read_text()))
        server, = [o for o in docs("70-serving.yaml") if o["kind"] == "Deployment"]
        container, = server["spec"]["template"]["spec"]["containers"]
        self.assertEqual(server["spec"]["replicas"], 0)
        self.assertEqual(server["spec"]["strategy"]["type"], "Recreate")
        self.assertEqual(container["resources"]["limits"]["memory"], "4Gi")
        self.assertEqual([e["name"] for e in container["env"]], ["OPENLEGAL_DATABASE_URL"])
        self.assertTrue(next(m for m in container["volumeMounts"] if m["name"] == "mecab-ko-dictionary")["readOnly"])
        migrate, = docs("40-migrate.yaml")
        self.assertFalse(migrate["spec"]["suspend"])
        self.assertEqual([e["name"] for e in migrate["spec"]["template"]["spec"]["containers"][0]["env"]],
                         ["OPENLEGAL_MIGRATION_DATABASE_URL"])
        seed, = docs("60-seed.yaml")
        self.assertEqual({v["name"] for v in seed["spec"]["template"]["spec"]["volumes"]},
                         {"postgres-ca", "cache-blobs", "corpus-blobs"})
        bootstrap, = docs("30-bootstrap.yaml")
        self.assertEqual(bootstrap["spec"]["backoffLimit"], 0)
        self.assertEqual(bootstrap["metadata"]["namespace"], prepare.POSTGRES)
        self.assertNotIn("PASSWORD", (self.args.output / "oxibelt.toml").read_text())
        self.assertIn("DNS:wrong.invalid", (self.args.output / "tls/wrong-backend.ext").read_text())
        self.assertNotIn("REPLACE_WITH_", (self.args.output / "10-serving-inputs.yaml").read_text())
        self.assertIn("172.28.0.2:30433", (self.args.output / "oxibelt.toml").read_text())
        network = docs("15-network.yaml")
        pg_deny, = [o for o in network if o["metadata"]["namespace"] == prepare.POSTGRES and o["metadata"]["name"] == "openlegal-default-deny"]
        self.assertEqual(pg_deny["spec"]["egress"], [])
        self.assertEqual(pg_deny["spec"]["ingress"], [])

    def test_subprocess_failure_does_not_include_diagnostics(self):
        import subprocess
        with patch.object(prepare.subprocess, "run", side_effect=subprocess.CalledProcessError(1, ["secret"], stderr=b"credential")):
            with self.assertRaisesRegex(ValueError, "^fixture preparation subprocess failed$"):
                prepare.run(["secret"])


if __name__ == "__main__":
    unittest.main()
