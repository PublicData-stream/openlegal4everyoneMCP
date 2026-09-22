# kubectl redistribution notices

The optional ingestion runtime includes official Kubernetes kubectl v1.37.0
(Apache-2.0), built with Go 1.26.6. The minimal serving runtime excludes it.

Binary source: `https://dl.k8s.io/release/v1.37.0/bin/linux/{amd64,arm64}/kubectl`.
Architecture-specific SHA-256 pins are in `scripts/deployment-kubectl.sha256`.
The release reports source commit `f54c212e3a2f75d674b717a9b29052b20b60aefc`.

The adjacent LICENSE, vendor and third_party directories preserve the complete
upstream v1.37.0 LICENSES bundle. This is deliberately broader than kubectl's
linked dependency set; inclusion of a notice does not assert that its component
is present in the runtime. OWNERS routing files are omitted. GO-LICENSE preserves
the Go toolchain/runtime license. No license text is abbreviated or replaced.

Build-time inputs, verified before extraction or packaging:

| Input | SHA-256 |
| --- | --- |
| https://codeload.github.com/kubernetes/kubernetes/tar.gz/refs/tags/v1.37.0 | `956ddae3b12acc08a715aea0411a168a36d5989b3e7185deeb6bf46b7dac19cf` |
| https://raw.githubusercontent.com/golang/go/go1.26.6/LICENSE | `911f8f5782931320f5b8d1160a76365b83aea6447ee6c04fa6d5591467db9dad` |

Selection and checksum verification recorded on 2026-09-22. Updates require
renewed binary/source verification, notice review and both architecture gates.
