"""Check synthetic fixture coverage without printing authentication material."""
import json
import sys

with open(sys.argv[1], encoding="utf-8") as source:
    events = [json.loads(line) for line in source]
base = "/api/v1/namespaces/openlegal-documents"
for generation in (1, 2):
    assert any(e["authenticated"] and e["generation"] == generation
               and e["path"] == f"{base}/resourcequotas/document-budget" for e in events)
for method, path in (("GET", "/openapi/v3"), ("GET", "/openapi/v3/api/v1"),
                     ("POST", f"{base}/pods"), ("DELETE", f"{base}/pods/document-image-fixture")):
    assert any(e["authenticated"] and e["method"] == method and e["path"] == path for e in events), (method, path)
assert any(e["forbidden"] for e in events)
assert any(not e["authenticated"] and not e["forbidden"] for e in events)
print("Synthetic TLS API observed quota, validated create, delete, both token generations, 401 and 403.")
