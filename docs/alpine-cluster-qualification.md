# Alpine cluster qualification status

The Alpine image migration does not qualify the document sandbox. The existing
serving fixture can still exercise the server images, but document and synthetic
controller integration remain blocked by the runtime compatibility issue below.
No document node was labeled ready and no sandbox policy was weakened.

## Runtime compatibility evidence (2026-09-22)

Public GitHub API requests identified gVisor `release-20260914.0` (published
2026-09-16) and resolved its annotated tag to commit
`95eb5d5930b0e7736826cc2cb949ba9d2c4d5d29`. Inspection of that exact release
establishes:

- [`ValidateSpec`, lines 164–166](https://github.com/google/gvisor/blob/95eb5d5930b0e7736826cc2cb949ba9d2c4d5d29/runsc/specutils/specutils.go#L164-L166)
  logs and ignores a requested OCI AppArmor profile.
- [Runtime feature reporting, lines 1004–1005](https://github.com/google/gvisor/blob/95eb5d5930b0e7736826cc2cb949ba9d2c4d5d29/runsc/specutils/specutils.go#L1004-L1005)
  declares AppArmor unsupported.

The committed worker manifest requires the named `openlegal-document` AppArmor
profile. Its acceptance gate checks that the worker reports that profile in
enforce mode. Loading the profile into the host kernel does not make this release
apply it to the worker. These are upstream code facts, not a runtime test or
evidence that another release has been qualified. Installing this advertised
incompatible release merely to retry acceptance would not resolve the conflict.

Reproduce the source lookup with:

```sh
gh api repos/google/gvisor/git/ref/tags/release-20260914.0
gh api repos/google/gvisor/git/tags/96928b6cccd8472a28b20bbffe6f3166c81cf9c5
gh api 'repos/google/gvisor/contents/runsc/specutils/specutils.go?ref=95eb5d5930b0e7736826cc2cb949ba9d2c4d5d29' \
  --jq '.content' | base64 --decode
```

## Resource preflight

The fixture retains its default 6 GiB node ceiling. The explicit
`ACCEPTANCE_NODE_MEMORY_GIB=18` profile requires at least 19.5 GiB of guest-visible
RAM for the approved 21 GiB allocation, reserving at least 1.5 GiB outside the
node. Admission checks `/proc/meminfo` before
creating a network or node; no VM resize or balloon change is performed.

A read-only observation on `dev` reported `MemTotal: 7280096 kB` and
`Balloon: 13631488 kB`. The guest therefore exposed about 6.94 GiB of usable total
RAM at that observation, despite the approved larger allocation. The 18 GiB
profile correctly fails on this input. Recheck current guest RAM before another
run; the observation is not a permanent host property. Swap does not substitute
for admitted RAM, and a node cgroup limit does not bound the aggregate workload.

Adding the observed total and balloon values gives 20,911,584 KiB, about
19.94 GiB. This arithmetic projects potential returned memory; it does not
establish current usable RAM or promise what a later guest will expose.
A strict 20 GiB guard would reject even that projection by 59,936 KiB. The
19.5 GiB guard allows guest reservations while retaining explicit headroom.
Admission always uses the observed `MemTotal` alone and never adds `Balloon`.

## Conditions for continuing

The canonical [document sandbox requirements](document-sandbox.md#cluster-preparation-and-acceptance)
remain unchanged. Before implementing or running dependent controller acceptance:

1. Select a runtime/configuration that demonstrably enforces the complete existing
   contract, or obtain an explicit reviewed change to that contract. Pin and
   checksum-admit its runtime and shim artifacts. A different runtime, disabled
   AppArmor, or an altered probe cannot silently stand in for the current policy.
2. Prove actual user-namespace, AppArmor, seccomp, network and cgroup enforcement
   together on the disposable node. Account for gVisor's virtualized proc/sys
   views; host evidence must identify the correct sandbox processes and cannot
   replace missing guest protection. Use owned resources and preserve cleanup on
   failed qualification. Scoped host AppArmor loading is allowed only if the
   profile is absent, and cleanup must unload only the profile created by that run.
3. Confirm sufficient guest-visible memory, preload digest-pinned images and run
   builds, serving, worker exhaustion probes and integration sequentially. Keep
   the worker's canonical resource limits and the approved host allocation.
4. Run synthetic controller integration only after qualification: use the real
   Kubernetes API, projected rotating credentials, namespace-scoped controller
   Role and production `KubernetesDocumentProcessor`; assert fictional output,
   cancellation and Pod cleanup. Keep provider traffic disabled. This is a
   separate gate from the serving fixture and from live-provider acceptance.

No runtime/shim was installed, host AppArmor policy loaded, cluster workload
created, or live legal-provider call made for this investigation. The resource
profile and its offline tests are implemented; full document-profile provisioning
and controller integration are intentionally pending the runtime decision.
