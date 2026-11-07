#!/usr/bin/env python3
"""Run one complete, sequential storage-operator test shard without skips."""

import argparse
import os
from pathlib import Path
import sys
import unittest


TEST_DIRECTORY = Path(__file__).resolve().parent / "storage-stability" / "tests"
SHARD_MODULES = {
    "core": frozenset(),
    "maintenance": frozenset({"test_maintenance_intake", "test_maintenance_completion"}),
    "pair": frozenset({"test_pair_intake", "test_pair_completion"}),
    "certificate": frozenset({"test_certificate_intake"}),
}


class ProgressResult(unittest.TextTestResult):
    def startTest(self, test):
        # GitHub's line-oriented logs must expose the active test before it ends.
        self.stream.writeln(f"START {test.id()}")
        self.stream.flush()
        super().startTest(test)


def test_cases(suite):
    """Keep discovery order, including module and class fixture grouping."""
    for item in suite:
        if isinstance(item, unittest.TestSuite):
            yield from test_cases(item)
        else:
            yield item


def partition_tests(suite):
    """Assign every discovered test once; future modules belong to core."""
    groups = {name: [] for name in SHARD_MODULES}
    required = set().union(*SHARD_MODULES.values())
    modules = set()
    identifiers = set()
    for test in test_cases(suite):
        identifier = test.id()
        if identifier in identifiers:
            raise ValueError(f"duplicate discovered test: {identifier}")
        identifiers.add(identifier)
        module = type(test).__module__
        modules.add(module)
        target = next((name for name, names in SHARD_MODULES.items() if module in names), "core")
        groups[target].append(test)
    missing = required - modules
    if missing:
        raise ValueError("required test modules missing: " + ", ".join(sorted(missing)))
    empty = [name for name, tests in groups.items() if not tests]
    if empty:
        raise ValueError("empty test shards: " + ", ".join(empty))
    return groups


def run_shard(suite, shard, *, stream=None):
    stream = sys.stderr if stream is None else stream
    groups = partition_tests(suite)
    print(f"Storage operator shard {shard}: {len(groups[shard])} of "
          f"{sum(map(len, groups.values()))} discovered tests", file=stream, flush=True)
    result = unittest.TextTestRunner(stream=stream, verbosity=2, durations=20,
                                     resultclass=ProgressResult).run(
        unittest.TestSuite(groups[shard]))
    if result.skipped:
        print("Storage operator gate incomplete: skipped tests are not accepted.",
              file=stream, flush=True)
    return 0 if result.wasSuccessful() and not result.skipped else 1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shard", choices=SHARD_MODULES, required=True)
    args = parser.parse_args(argv)
    try:
        if os.geteuid() != 0:
            raise ValueError("root is required for complete ownership and permission coverage")
        loader = unittest.TestLoader()
        suite = loader.discover(str(TEST_DIRECTORY), pattern="test_*.py")
        if loader.errors:
            raise ValueError("test discovery failed:\n" + "\n".join(loader.errors))
        return run_shard(suite, args.shard)
    except (ImportError, OSError, ValueError) as error:
        print(f"Storage operator gate failed: {error}", file=sys.stderr, flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
