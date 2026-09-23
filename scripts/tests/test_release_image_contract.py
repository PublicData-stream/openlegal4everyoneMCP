"""Offline checks for the GHCR release event and source contract."""

from pathlib import Path
import subprocess
import sys
import tempfile
import tomllib
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from release_image_contract import validate  # noqa: E402
from sync_release_version import (  # noqa: E402
    DENY, FILES, LOCKS, PLACEHOLDER, load_documents, render, sync,
    validate_documents,
)


class ReleaseImageContractTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        repository = Path(__file__).resolve().parents[2]
        for path in FILES:
            destination = self.root / path
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_text((repository / path).read_text())
        self.git("init", "-b", "main")
        self.git("config", "user.name", "Image Contract Test")
        self.git("config", "user.email", "test@example.invalid")
        self.git("config", "commit.gpgsign", "false")
        self.git("config", "tag.gpgsign", "false")
        self.git("add", ".")
        self.git("commit", "-m", "test source")
        self.revision = self.git("rev-parse", "HEAD")
        self.git("update-ref", "refs/remotes/origin/main", self.revision)

    def git(self, *args):
        return subprocess.run(
            ["git", "-C", str(self.root), *args], capture_output=True,
            text=True, check=True,
        ).stdout.strip()

    def event(self, tag, *, event="release", prerelease="false"):
        return {
            "GITHUB_REPOSITORY": "PublicData-stream/openlegal4everyoneMCP",
            "GITHUB_REF": f"refs/tags/{tag}",
            "GITHUB_SHA": self.revision,
            "GITHUB_EVENT_NAME": event,
            "RELEASE_TAG": tag if event == "release" else "",
            "RELEASE_PRERELEASE": prerelease if event == "release" else "",
            "RELEASE_DRAFT": "false" if event == "release" else "",
        }

    def annotate(self, tag):
        self.git("tag", "-a", tag, "-m", tag)

    def test_stable_beta_and_commit_bound_build(self):
        for tag, event, prerelease, kind in (
            ("1.0.0", "release", "false", "stable"),
            ("1.0.0-beta.2", "release", "true", "beta"),
            (f"1.0.0-build.{self.revision[:8]}", "push", "", "build"),
        ):
            with self.subTest(tag=tag):
                self.annotate(tag)
                output = validate(self.root, self.event(tag, event=event, prerelease=prerelease))
                self.assertEqual(output["kind"], kind)
                self.assertEqual(output["version"], tag)
                self.assertEqual(output["revision"], self.revision)
                self.assertIn(self.revision, output["source_url"])

    def test_rejects_wrong_event_or_release_classification(self):
        self.annotate("1.0.0-beta.2")
        with self.assertRaisesRegex(ValueError, "published release"):
            validate(self.root, self.event("1.0.0-beta.2", prerelease="false"))
        with self.assertRaisesRegex(ValueError, "published release"):
            validate(self.root, self.event("1.0.0-beta.2", event="push"))

    def test_rejects_lightweight_or_moved_tag(self):
        self.git("tag", "1.0.0")
        with self.assertRaisesRegex(ValueError, "annotated"):
            validate(self.root, self.event("1.0.0"))
        self.git("tag", "-d", "1.0.0")
        self.annotate("1.0.0")
        event = self.event("1.0.0")
        event["GITHUB_SHA"] = "0" * 40
        with self.assertRaisesRegex(ValueError, "do not match"):
            validate(self.root, event)

    def test_rejects_off_main_and_version_mismatch(self):
        (self.root / "next").write_text("different commit")
        self.git("add", ".")
        self.git("commit", "-m", "off main")
        self.revision = self.git("rev-parse", "HEAD")
        self.annotate("1.0.0")
        with self.assertRaisesRegex(ValueError, "not on main"):
            validate(self.root, self.event("1.0.0"))
        self.git("update-ref", "refs/remotes/origin/main", self.revision)
        worker = self.root / "apps/document-worker/Cargo.toml"
        worker.write_text(worker.read_text().replace('version = "0.0.0"', 'version = "0.2.0"', 1))
        with self.assertRaisesRegex(ValueError, "package version differs"):
            validate(self.root, self.event("1.0.0"))

    def test_rejects_spoofed_build_suffix_and_bad_repository(self):
        self.annotate("1.0.0-build.deadbeef")
        with self.assertRaisesRegex(ValueError, "commit prefix"):
            validate(self.root, self.event("1.0.0-build.deadbeef", event="push"))
        event = self.event("1.0.0-build.deadbeef", event="push")
        event["GITHUB_REPOSITORY"] = "someone/fork"
        with self.assertRaisesRegex(ValueError, "canonical repository"):
            validate(self.root, event)

    def test_stamps_full_tag_without_changing_third_party_versions(self):
        baseline = load_documents(self.root)
        validate_documents(baseline, PLACEHOLDER)
        for version in ("1.0.0", "1.0.0-beta.2", f"1.0.0-build.{self.revision[:8]}"):
            with self.subTest(version=version):
                stamped = render(baseline, version)
                validate_documents(stamped, version)
                self.assertEqual(stamped["crates/domain/Cargo.toml"], baseline["crates/domain/Cargo.toml"])
                self.assertEqual(sum(stamped[path] != baseline[path] for path in FILES), 10)
                for path in LOCKS:
                    third_party = lambda content: [
                        entry for entry in tomllib.loads(content)["package"]
                        if not entry["name"].startswith("openlegal-")
                    ]
                    self.assertEqual(third_party(stamped[path]), third_party(baseline[path]))
                for path in DENY:
                    third_party = lambda content: [
                        entry for entry in tomllib.loads(content)["licenses"]["exceptions"]
                        if not entry["crate"].startswith("openlegal-")
                    ]
                    self.assertEqual(third_party(stamped[path]), third_party(baseline[path]))
        sync(self.root, "1.0.0-beta.2")
        self.assertEqual(load_documents(self.root), render(baseline, "1.0.0-beta.2"))

    def test_rejects_inconsistent_baseline_before_writing(self):
        for path, old in (
            ("apps/server/Cargo.toml", 'version = "0.0.0"'),
            ("Cargo.lock", 'name = "openlegal-server"\nversion = "0.0.0"'),
            ("deny.toml", 'crate = "openlegal-server@0.0.0"'),
        ):
            with self.subTest(path=path):
                original = load_documents(self.root)
                target = self.root / path
                target.write_text(original[path].replace(old, old.replace("0.0.0", "0.2.0"), 1))
                changed = load_documents(self.root)
                with self.assertRaises(ValueError):
                    sync(self.root, "1.0.0-beta.2")
                self.assertEqual(load_documents(self.root), changed)
                target.write_text(original[path])

    def test_rejects_placeholder_and_bad_tag(self):
        baseline = load_documents(self.root)
        for version in ("0.0.0", "1.0.0-beta.0", "1.0.0-build.NOTHEX", "1.0.0/other"):
            with self.subTest(version=version), self.assertRaisesRegex(ValueError, "unsupported format"):
                render(baseline, version)


if __name__ == "__main__":
    unittest.main()
