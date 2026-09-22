"""Resource admission must fail before creating a cluster or changing its cgroup."""
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location("fixture_memory", Path(__file__).with_name("memory.py"))
memory = importlib.util.module_from_spec(spec)
spec.loader.exec_module(memory)


class MemoryAdmissionTests(unittest.TestCase):
    def test_existing_guest_admits_only_default_profile(self):
        # Actual dev observation: assigned RAM is not the guest's usable RAM.
        observed = "MemTotal:        7280096 kB\nSwapTotal:       4194300 kB\nBalloon:        13631488 kB\n"
        self.assertEqual(memory.node_limit("6", observed), "6g")
        with self.assertRaisesRegex(ValueError, "Guest RAM"):
            memory.node_limit("18", observed)

    def test_expanded_guest_admits_explicit_profile(self):
        self.assertEqual(memory.node_limit("18", "MemTotal: 22020096 kB\n"), "18g")

    def test_expanded_profile_requires_at_least_one_and_half_gib_headroom(self):
        threshold = 19968 * 1024
        self.assertEqual(memory.node_limit("18", f"MemTotal: {threshold} kB\n"), "18g")
        with self.assertRaisesRegex(ValueError, "Guest RAM"):
            memory.node_limit("18", f"MemTotal: {threshold - 1} kB\n")

    def test_guest_reservations_do_not_require_full_nominal_allocation(self):
        # Synthetic projection from observed total + balloon; actual admission
        # still reads only MemTotal, never adds Balloon or assumes its return.
        projected_total_kib = 7280096 + 13631488
        self.assertEqual(memory.node_limit("18", f"MemTotal: {projected_total_kib} kB\n"), "18g")

    def test_insufficient_default_guest_fails(self):
        with self.assertRaisesRegex(ValueError, "Guest RAM"):
            memory.node_limit("6", "MemTotal: 6291456 kB\n")

    def test_invalid_profiles_and_inventories_fail(self):
        for profile in ("", "7", "21", "18g", "-1", "6\n18"):
            with self.subTest(profile=profile), self.assertRaisesRegex(ValueError, "must be 6 or 18"):
                memory.node_limit(profile, "MemTotal: 22020096 kB\n")
        for inventory in ("", "MemTotal: unknown kB\n", "MemTotal: 22020096 MB\n",
                          "MemTotal: 22020096 kB\nMemTotal: 22020096 kB\n"):
            with self.subTest(inventory=inventory), self.assertRaisesRegex(ValueError, "Guest RAM"):
                memory.node_limit("6", inventory)


if __name__ == "__main__":
    unittest.main()
