"""Offline partition and exit-status checks; no operator fixtures execute."""

import io
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import run_storage_stability_tests as runner  # noqa: E402


def case(module, name="test_fixture", method=None):
    cls = type("Fixture", (unittest.TestCase,), {
        "__module__": module, name: method or (lambda self: None),
    })
    return cls(name)


def complete_suite(*extra):
    return unittest.TestSuite([
        unittest.TestSuite([case(module)])
        for module in ("test_core", "test_maintenance_intake", "test_maintenance_completion",
                       "test_pair_intake", "test_pair_completion", "test_certificate_intake")
    ] + list(extra))


class StorageOperatorRunnerTests(unittest.TestCase):
    def test_exact_disjoint_partition_and_new_modules_default_to_core(self):
        suite = complete_suite(case("test_future_module"))
        original = list(runner.test_cases(suite))
        groups = runner.partition_tests(suite)
        selected = [test for tests in groups.values() for test in tests]
        self.assertEqual(len(selected), len(original))
        self.assertEqual({test.id() for test in selected}, {test.id() for test in original})
        for name, tests in groups.items():
            self.assertEqual(tests, [test for test in original if test in tests])
            if name != "core":
                self.assertEqual({type(test).__module__ for test in tests}, runner.SHARD_MODULES[name])
        self.assertEqual([type(test).__module__ for test in groups["core"]],
                         ["test_core", "test_future_module"])

    def test_duplicate_ids_missing_required_modules_and_empty_core_refuse(self):
        with self.assertRaisesRegex(ValueError, "duplicate"):
            runner.partition_tests(complete_suite(case("test_pair_intake")))
        with self.assertRaisesRegex(ValueError, "required test modules missing"):
            runner.partition_tests(unittest.TestSuite([case("test_core")]))
        cases = [test for test in runner.test_cases(complete_suite())
                 if type(test).__module__ != "test_core"]
        with self.assertRaisesRegex(ValueError, "empty test shards"):
            runner.partition_tests(unittest.TestSuite(cases))

    def test_selected_module_class_fixtures_and_order_are_preserved(self):
        events = []
        cls = type("Grouped", (unittest.TestCase,), {
            "__module__": "test_core",
            "setUpClass": classmethod(lambda cls: events.append("class-start")),
            "tearDownClass": classmethod(lambda cls: events.append("class-end")),
            "test_first": lambda self: events.append("first"),
            "test_second": lambda self: events.append("second"),
        })
        suite = complete_suite(unittest.TestSuite([cls("test_second"), cls("test_first")]))
        output = io.StringIO()
        self.assertEqual(runner.run_shard(suite, "core", stream=output), 0)
        self.assertEqual(events, ["class-start", "second", "first", "class-end"])
        self.assertIn("test_second", output.getvalue())
        self.assertIn("Slowest test durations", output.getvalue())

    def test_failure_error_and_skip_cannot_pass_gate(self):
        for kind in ("failure", "error", "skip"):
            def method(self, kind=kind):
                if kind == "failure": self.fail("synthetic assertion")
                if kind == "error": raise RuntimeError("synthetic exception")
                self.skipTest("synthetic missing prerequisite")
            with self.subTest(kind=kind):
                output = io.StringIO()
                suite = complete_suite(case("test_core", "test_rejected", method))
                self.assertEqual(runner.run_shard(suite, "core", stream=output), 1)
                if kind == "skip": self.assertIn("gate incomplete", output.getvalue())

    def test_active_test_has_flushed_complete_start_line_before_finishing(self):
        class ObservedStream(io.StringIO):
            flushed = ""

            def flush(self):
                self.flushed = self.getvalue()
                super().flush()

        output = ObservedStream()
        observed = []

        def method(test):
            line = f"START {test.id()}\n"
            observed.append(line in output.flushed)
            self.assertIn(line, output.flushed)
            self.assertNotIn("Ran ", output.getvalue())

        suite = complete_suite(case("test_core", "test_active", method))
        self.assertEqual(runner.run_shard(suite, "core", stream=output), 0)
        self.assertEqual(observed, [True])

    def test_discovery_errors_fail_before_running_even_unselected_modules(self):
        loader = unittest.TestLoader()
        loader.discover = lambda *args, **kwargs: complete_suite()
        loader.errors = ["synthetic broken core import"]
        with patch.object(runner.os, "geteuid", return_value=0), \
             patch.object(runner.unittest, "TestLoader", return_value=loader), \
             patch.object(runner, "run_shard") as execute, patch.object(sys, "stderr", io.StringIO()):
            self.assertEqual(runner.main(["--shard", "certificate"]), 1)
        execute.assert_not_called()

    def test_nonroot_fails_before_discovery(self):
        with patch.object(runner.os, "geteuid", return_value=1000), \
             patch.object(runner.unittest, "TestLoader") as loader, \
             patch.object(sys, "stderr", io.StringIO()):
            self.assertEqual(runner.main(["--shard", "core"]), 1)
        loader.assert_not_called()

    def test_main_discovers_once_and_propagates_selected_status(self):
        loader = unittest.TestLoader()
        suite = complete_suite()
        with patch.object(loader, "discover", return_value=suite) as discover, \
             patch.object(runner.os, "geteuid", return_value=0), \
             patch.object(runner.unittest, "TestLoader", return_value=loader), \
             patch.object(runner, "run_shard", return_value=1) as execute:
            self.assertEqual(runner.main(["--shard", "pair"]), 1)
        discover.assert_called_once_with(str(runner.TEST_DIRECTORY), pattern="test_*.py")
        execute.assert_called_once_with(suite, "pair")


if __name__ == "__main__":
    unittest.main()
