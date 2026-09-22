"""Offline admission checks: mutations must fail before cluster creation."""
import hashlib
import importlib.util
from pathlib import Path
import unittest


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, Path(__file__).with_name(filename))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


assets = load("assets", "assets.py")
storage = load("storage", "prepare-storage.py")
networks = load("networks", "networks.py")


class AdmissionTests(unittest.TestCase):
    def fixture(self):
        raw = b'            # - name: CALICO_IPV4POOL_CIDR\n            #   value: "192.168.0.0/16"\n' + b"containers:\n  - image: quay.io/calico/node:v3.32.2\n    imagePullPolicy: IfNotPresent\n"
        lock = {
            "calico_sha256": hashlib.sha256(raw).hexdigest(),
            "calico_images": {"quay.io/calico/node:v3.32.2": "quay.io/calico/node:v3.32.2@sha256:" + "a" * 64},
        }
        return raw, lock

    def test_verified_manifest_gets_immutable_image_and_no_pull(self):
        raw, lock = self.fixture()
        result = assets.pin_manifest(raw, lock)
        self.assertIn("@sha256:" + "a" * 64, result)
        self.assertIn("imagePullPolicy: Never", result)
        self.assertIn('value: "10.244.0.0/16"', result)

    def test_source_mutation_fails(self):
        raw, lock = self.fixture()
        with self.assertRaisesRegex(ValueError, "checksum"):
            assets.pin_manifest(raw + b"#changed", lock)

    def test_unadmitted_image_fails_even_with_updated_checksum(self):
        raw, lock = self.fixture()
        raw = raw.replace(b"calico/node", b"unexpected/node")
        lock["calico_sha256"] = hashlib.sha256(raw).hexdigest()
        with self.assertRaisesRegex(ValueError, "Unadmitted"):
            assets.pin_manifest(raw, lock)

    def test_mutable_pin_fails(self):
        raw, lock = self.fixture()
        lock["calico_images"]["quay.io/calico/node:v3.32.2"] = "quay.io/calico/node:latest"
        with self.assertRaisesRegex(ValueError, "Invalid"):
            assets.pin_manifest(raw, lock)

    def test_missing_expected_image_fails(self):
        raw, lock = self.fixture()
        lock["calico_images"]["missing/image:v1"] = "missing/image:v1@sha256:" + "b" * 64
        with self.assertRaisesRegex(ValueError, "inventory"):
            assets.pin_manifest(raw, lock)

    def test_route_conflict_preflight(self):
        networks.check([{"dst": "default"}, {"dst": "192.168.122.0/24"}], [])
        for destination in ("10.244.0.0/24", "10.96.1.1", "10.0.0.0/8"):
            with self.assertRaisesRegex(ValueError, "overlaps"):
                networks.check([{"dst": destination}], [])
        with self.assertRaisesRegex(ValueError, "overlaps"):
            networks.check([], [{"IPAM": {"Config": [{"Subnet": "10.244.0.0/16"}]}}])

    def test_storage_uses_owned_node_and_separate_paths(self):
        node = "openlegal-accept-0123456789abcdef-control-plane"
        rendered = storage.render(node)
        self.assertNotIn("REPLACE_WITH_", rendered)
        self.assertEqual(rendered.count("values: [" + node + "]"), 4)
        for directory in ("cache-blobs", "corpus-blobs", "corpus-index", "mecab-ko-dictionary"):
            self.assertEqual(rendered.count("path: /var/local/openlegal/" + directory), 1)

    def test_storage_rejects_unowned_or_injected_node(self):
        for node in ("production", "openlegal-accept-0123456789abcdef-control-plane\nother: field"):
            with self.assertRaises(ValueError):
                storage.render(node)


if __name__ == "__main__":
    unittest.main()
