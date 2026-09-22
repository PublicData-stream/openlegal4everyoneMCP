"""Admit explicit fixture memory profiles using RAM visible inside the guest."""
from pathlib import Path
import re
import sys


def node_limit(profile, meminfo):
    # Preserve the existing 7 GiB fixture, allowing normal kernel reservations.
    # The expanded profile reserves at least 1.5 GiB outside the node, without
    # requiring the guest to expose every byte of its nominal VM allocation.
    minimum_kib = {"6": 6656 * 1024, "18": 19968 * 1024}
    if profile not in minimum_kib:
        raise ValueError("ACCEPTANCE_NODE_MEMORY_GIB must be 6 or 18")
    totals = re.findall(r"^MemTotal:\s+([0-9]+)\s+kB\s*$", meminfo, re.MULTILINE)
    if len(totals) != 1 or int(totals[0]) < minimum_kib[profile]:
        raise ValueError(
            f"Guest RAM is insufficient for the {profile} GiB node profile; "
            "allocated, ballooned and swap memory do not count as usable RAM"
        )
    return f"{profile}g"


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("usage: memory.py NODE_MEMORY_GIB")
    try:
        print(node_limit(sys.argv[1], Path("/proc/meminfo").read_text()))
    except (OSError, ValueError) as error:
        raise SystemExit(str(error)) from error
