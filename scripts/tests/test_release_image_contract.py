"""Offline checks for the GHCR release event and source contract."""

from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from release_image_contract import validate  # noqa: E402


class ReleaseImageContractTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        (self.root / "apps/document-worker").mkdir(parents=True)
        (self.root / "Cargo.toml").write_text('[workspace.package]\nversion = "0.1.0"\n')
        (self.root / "apps/document-worker/Cargo.toml").write_text('[package]\nversion = "0.1.0"\n')
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
            ("0.1.0", "release", "false", "stable"),
            ("0.1.0-beta.1", "release", "true", "beta"),
            (f"0.1.0-build.{self.revision[:8]}", "push", "", "build"),
        ):
            with self.subTest(tag=tag):
                self.annotate(tag)
                output = validate(self.root, self.event(tag, event=event, prerelease=prerelease))
                self.assertEqual(output["kind"], kind)
                self.assertEqual(output["revision"], self.revision)
                self.assertIn(self.revision, output["source_url"])

    def test_rejects_wrong_event_or_release_classification(self):
        self.annotate("0.1.0-beta.1")
        with self.assertRaisesRegex(ValueError, "published release"):
            validate(self.root, self.event("0.1.0-beta.1", prerelease="false"))
        with self.assertRaisesRegex(ValueError, "published release"):
            validate(self.root, self.event("0.1.0-beta.1", event="push"))

    def test_rejects_lightweight_or_moved_tag(self):
        self.git("tag", "0.1.0")
        with self.assertRaisesRegex(ValueError, "annotated"):
            validate(self.root, self.event("0.1.0"))
        self.git("tag", "-d", "0.1.0")
        self.annotate("0.1.0")
        event = self.event("0.1.0")
        event["GITHUB_SHA"] = "0" * 40
        with self.assertRaisesRegex(ValueError, "do not match"):
            validate(self.root, event)

    def test_rejects_off_main_and_version_mismatch(self):
        (self.root / "next").write_text("different commit")
        self.git("add", ".")
        self.git("commit", "-m", "off main")
        self.revision = self.git("rev-parse", "HEAD")
        self.annotate("0.1.0")
        with self.assertRaisesRegex(ValueError, "not on main"):
            validate(self.root, self.event("0.1.0"))
        self.git("update-ref", "refs/remotes/origin/main", self.revision)
        (self.root / "apps/document-worker/Cargo.toml").write_text('[package]\nversion = "0.2.0"\n')
        with self.assertRaisesRegex(ValueError, "Rust package versions"):
            validate(self.root, self.event("0.1.0"))

    def test_rejects_spoofed_build_suffix_and_bad_repository(self):
        self.annotate("0.1.0-build.deadbeef")
        with self.assertRaisesRegex(ValueError, "commit prefix"):
            validate(self.root, self.event("0.1.0-build.deadbeef", event="push"))
        event = self.event("0.1.0-build.deadbeef", event="push")
        event["GITHUB_REPOSITORY"] = "someone/fork"
        with self.assertRaisesRegex(ValueError, "canonical repository"):
            validate(self.root, event)


if __name__ == "__main__":
    unittest.main()
