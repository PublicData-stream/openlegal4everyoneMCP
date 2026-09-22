#!/usr/bin/env python3
"""Reject pod/service CIDRs overlapping existing specific host/Docker routes."""
import ipaddress
import json
import sys

CLUSTER_NETWORKS = tuple(map(ipaddress.ip_network, ("10.244.0.0/16", "10.96.0.0/16")))


def check(routes, networks):
    observed = []
    for route in routes:
        destination = route.get("dst")
        if destination and destination not in ("default", "0.0.0.0/0"):
            observed.append(destination)
    for network in networks:
        for config in (network.get("IPAM", {}).get("Config") or []):
            if config.get("Subnet"):
                observed.append(config["Subnet"])
    for value in observed:
        existing = ipaddress.ip_network(value, strict=False)
        if any(existing.overlaps(cluster) for cluster in CLUSTER_NETWORKS):
            raise ValueError("Existing route/network overlaps the fixture pod or service network")


def main():
    if len(sys.argv) != 3:
        raise SystemExit("usage: networks.py ROUTES_JSON DOCKER_NETWORKS_JSON")
    check(json.loads(sys.argv[1]), json.loads(sys.argv[2]))


if __name__ == "__main__":
    main()
