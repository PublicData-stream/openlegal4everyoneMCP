# Disposable serving acceptance operations

These manual checks extend [fixture preparation](README.md), on its owned `dev`
cluster only. Complete preparation and wait for serving readiness first. Run
blocks in order from the repository root in one Bash session, stopping on any
unexpected result. They interrupt serving and change only the owned node's
firewall, fixture trust material and disposable Kubernetes resources. They do not
qualify production firewall/storage, cross-node routing or document isolation.

## Private prerequisites and bounded helpers

Retain the README's `accept_*` variables. The tested native edge harness must
contain `python3`, OxiBelt and `/usr/local/bin/wt_client`; the last is the built
`wt_client` example with `--serving-smoke` support. Set
`accept_client_image` to a digest-pinned, preloaded native probe image containing
Python 3 and that same `/usr/local/bin/wt_client`. Neither image is built or
pulled by these snippets. The edge container is named `$accept_name-edge` and
uses the README's curated `$accept_edge_volume` at `/fixture`. If that edge
has not been launched, start it once after the README's configuration check;
do not recreate or overwrite an existing container with the owned name:

```sh
docker run -d --name "$accept_name-edge" --network "$accept_name" --ip "$accept_edge_ip" \
    --user 65532:65532 --read-only --cap-drop ALL --security-opt no-new-privileges \
    --memory 256m --cpus 1 --pids-limit 64 --ulimit stack=67108864:67108864 \
    --tmpfs /tmp:rw,noexec,nosuid,size=16m \
    --mount "type=volume,source=$accept_edge_volume,target=/fixture,readonly" \
    --entrypoint /usr/local/bin/oxibelt "$accept_edge_image" \
    --config /fixture/config/oxibelt.toml
```

```sh
set -euo pipefail
umask 077
: "${accept_client_image:?Set the admitted native client image digest reference}"
export accept_run accept_tools accept_workloads accept_name accept_edge_ip accept_node_ip
export accept_client_image accept_edge_image accept_edge_volume
export accept_repo="$PWD"
cat > "$accept_run/ops-common.py" <<'PY'
import copy, datetime, json, os, pathlib, re, subprocess, time
import yaml
run = pathlib.Path(os.environ['accept_run'])
repo = pathlib.Path(os.environ['accept_repo'])
name = os.environ['accept_name']; node = name + '-control-plane'
nip = os.environ['accept_node_ip']; edge = os.environ['accept_edge_ip']
tls = pathlib.Path(os.environ['accept_workloads']) / 'tls'
k = [os.environ['accept_tools'] + '/bin/kubectl', '--kubeconfig', str(run/'kubeconfig'),
     '--context', 'kind-'+name, '--request-timeout=20s']
def command(args, timeout=45, data=None):
    return subprocess.run(args, input=data, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
def checked(args, timeout=45, data=None):
    r = command(args, timeout, data)
    if r.returncode:
        (run/'operations-private.stderr').write_bytes(r.stderr)
        (run/'operations-private.stdout').write_bytes(r.stdout)
        raise RuntimeError('operation failed; inspect private diagnostic')
    return r.stdout

def kc(args, timeout=45, data=None): return checked(k+args, timeout, data)
def get(kind, ns, obj): return json.loads(kc(['-n',ns,'get',kind,obj,'-o','json']))
def server():
    pods = json.loads(kc(['-n','openlegal-serving','get','pods','-l',
        'app.kubernetes.io/name=openlegal-server','-o','json']))['items']
    assert len(pods) == 1
    return pods[0]
def apply(objects, filename):
    path = run/filename; path.write_text(yaml.safe_dump_all(objects))
    kc(['apply','-f',str(path)])
def probe(code, *args, ns='openlegal-accept-monitor', pod='monitor', timeout=45):
    return command(k+['-n',ns,'exec',pod,'--','python3','-c',code,*args],timeout)
def retained():
    code = '''import json,ssl,sys,time,types
smoke=types.ModuleType('smoke')
exec(SOURCE,smoke.__dict__)
e=json.load(sys.stdin)
c=smoke.HttpClient(smoke.endpoint(URL,'/mcp',private=True),
 'https://openlegal4everyone.stream',ssl.create_default_context(),time.monotonic()+45)
for rev in smoke.REVISIONS:
 if rev==smoke.REVISIONS[0]:
  c.rpc(rev,'initialize',{'protocolVersion':rev,'capabilities':{},'clientInfo':{'name':'retained-acceptance','version':'1'}})
  c.rpc(rev,'notifications/initialized',notification=True)
 else: c.rpc(rev,'server/discover')
 d=c.call(rev,'database.get',{'object':e['object']})
 assert d['metadata']['capture_id']==e['head_capture_id'] and d['text']==e['body']
 h=c.call(rev,'database.history',{'object':e['object'],'kind':'captures'})
 assert {x['capture_id'] for x in h['entries']}==set(e['captures'])
 q=c.call(rev,'database.query',{'query':e['query']})
 assert q['index_lag']==0 and len(q['hits'])==1 and q['hits'][0]['capture_id']==e['head_capture_id']
print('retained capture/history/search: passed')
'''
    code='SOURCE='+repr((repo/'scripts/serving_smoke.py').read_text())+'\nURL='+repr('http://'+nip+':30080/mcp')+'\n'+code
    checked(['docker','exec','-i',name+'-edge','python3','-B','-c',code],60,(run/'expected.json').read_bytes())
    print('retained capture/history/search: passed')
def public_smoke():
    sip=server()['status']['podIP']
    r=command(k+['-n','openlegal-accept-monitor','exec','monitor','--','python3','-B','/inputs/smoke.py',
     '--http-url','https://openlegal4everyone.stream:8443/mcp',
     '--webtransport-url','https://openlegal4everyone.stream:8443/mcp-wt/v1',
     '--origin','https://openlegal4everyone.stream','--ca-file','/inputs/ca.crt','--wt-client','/usr/local/bin/wt_client',
     '--live-url','http://'+sip+':9090/live','--ready-url','http://'+sip+':9090/ready'],230)
    stem='smoke-'+str(time.time_ns())
    (run/(stem+'.stdout')).write_bytes(r.stdout); (run/(stem+'.stderr')).write_bytes(r.stderr)
    assert r.returncode==0
    print('public smoke and current private monitor health: passed')
PY
```

The helper captures subprocess diagnostics privately and prints fixed results.
Do not publish its private outputs, generated workloads or unsanitized Job logs.
Capture the fictional seed's expected values once; use these same values for all
later comparisons, without regenerating the seed:

```sh
"${accept_kubectl[@]}" -n openlegal-serving logs job/openlegal-seed > "$accept_run/expected.json"
"$accept_tools/bin/python" - <<'PY'
import os, pathlib
exec((pathlib.Path(os.environ['accept_run'])/'ops-common.py').read_text())
e=json.loads((run/'expected.json').read_text())
assert {'object','head_capture_id','body','captures','query'} <= e.keys()
retained()
pg=json.loads(kc(['-n','openlegal-accept-postgres','get','pods','-l',
 'app.kubernetes.io/name=postgres','-o','json']))['items']; assert len(pg)==1
base=['-n','openlegal-accept-postgres','exec',pg[0]['metadata']['name'],'--',
 'psql','-X','-U','postgres','-d','openlegal','-v','ON_ERROR_STOP=1']
r=command(k+base+['-v','VERBOSITY=sqlstate','-c',
 'SET ROLE runtime; CREATE TABLE public.openlegal_acceptance_ddl_probe(id integer)'])
(run/'ddl-private.stderr').write_bytes(r.stderr)
assert r.returncode != 0 and b'42501' in r.stderr
assert kc(base+['-Atc',"SELECT to_regclass('public.openlegal_acceptance_ddl_probe') IS NULL"]).strip()==b't'
print('runtime DDL denied with SQLSTATE 42501; table absent: passed')
PY
```

This exercises the runtime role's effective SQL permissions through a local
administrative socket with `SET ROLE`; it does not substitute for the serving
runtime credential's verified-TLS connection established during startup.

## Probe Pods and network evidence

Create bounded probes with only the public fixture CA and smoke source. The
identical monitor label in the denied namespace deliberately tests the combined
namespace-and-Pod policy selector. Stop if these owned names already exist.

```sh
"$accept_tools/bin/python" - <<'PY'
import os, pathlib
exec((pathlib.Path(os.environ['accept_run'])/'ops-common.py').read_text())
objects=[]
for ns,pod in [('openlegal-accept-monitor','monitor'),('openlegal-accept-client','denied')]:
 assert not kc(['-n',ns,'get','pod',pod,'--ignore-not-found','-o','name']).strip()
 objects.append({'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':'smoke-inputs','namespace':ns},
  'data':{'smoke.py':(repo/'scripts/serving_smoke.py').read_text(),'ca.crt':(tls/'ca.crt').read_text()}})
 objects.append({'apiVersion':'v1','kind':'Pod','metadata':{'name':pod,'namespace':ns,
  'labels':{'app.kubernetes.io/name':'accept-monitor'}},'spec':{
  'automountServiceAccountToken':False,'enableServiceLinks':False,'activeDeadlineSeconds':3600,'restartPolicy':'Never',
  'hostAliases':[{'ip':edge,'hostnames':['openlegal4everyone.stream']}],
  'securityContext':{'runAsNonRoot':True,'runAsUser':65532,'runAsGroup':65532,'seccompProfile':{'type':'RuntimeDefault'}},
  'containers':[{'name':'client','image':os.environ['accept_client_image'],'imagePullPolicy':'Never',
   'command':['sleep','3600'],'resources':{'requests':{'cpu':'25m','memory':'32Mi'},'limits':{'cpu':'1','memory':'192Mi'}},
   'securityContext':{'readOnlyRootFilesystem':True,'allowPrivilegeEscalation':False,'capabilities':{'drop':['ALL']}},
   'volumeMounts':[{'name':'inputs','mountPath':'/inputs','readOnly':True}]}],
  'volumes':[{'name':'inputs','configMap':{'name':'smoke-inputs','defaultMode':0o444}}]}})
apply(objects,'probe-pods.yaml')
for ns,pod in [('openlegal-accept-monitor','monitor'),('openlegal-accept-client','denied')]:
 kc(['-n',ns,'wait','pod/'+pod,'--for=condition=Ready','--timeout=90s'],100)
public_smoke()
PY
```

The next block installs `OLA_PHASE9` only inside the owned node's network
namespace. It preserves CNI rules and rejects an existing chain. Record the
counter deltas while this isolated fixture has no other test traffic; a timeout
alone never passes a denial check. The raw chain precedes NodePort destination
translation and allows only the explicit edge source for TCP 30080/UDP 30433.

```sh
"$accept_tools/bin/python" - <<'PY'
import os, pathlib
exec((pathlib.Path(os.environ['accept_run'])/'ops-common.py').read_text())
def ipt(*args): return checked(['docker','exec',node,'iptables',*args])
def drops(marker):
 table='raw' if marker=='-A OLA_PHASE9 ' else 'filter'
 rules=checked(['docker','exec',node,'iptables-save','-t',table,'-c']).decode()
 return sum(int(re.match(r'\[(\d+):',line)[1]) for line in rules.splitlines() if marker in line and '-j DROP' in line)
http="""import http.client,sys
from urllib.parse import urlsplit
u=urlsplit(sys.argv[1]); c=http.client.HTTPConnection(u.hostname,u.port,timeout=3)
try:
 c.request('GET',u.path); assert c.getresponse().status==200
except (OSError,TimeoutError): sys.exit(3)
"""
sip=server()['status']['podIP']; health='http://'+sip+':9090/ready'
assert probe(http,health).returncode==0
before=drops('-A cali-tw-')
assert probe(http,health,ns='openlegal-accept-client',pod='denied').returncode==3
delta=drops('-A cali-tw-')-before; assert delta>0
print('CNI denied health DROP counter delta:',delta)
assert probe(http,health).returncode==0
ipt('-t','raw','-N','OLA_PHASE9')
ipt('-t','raw','-A','OLA_PHASE9','-s',edge+'/32','-j','RETURN')
ipt('-t','raw','-A','OLA_PHASE9','-j','DROP')
for protocol,port in [('tcp','30080'),('udp','30433')]:
 ipt('-t','raw','-I','PREROUTING','1','-d',nip+'/32','-p',protocol,'--dport',port,'-j','OLA_PHASE9')
retained() # Positive TCP NodePort request from the permitted edge.
wt=['/usr/local/bin/wt_client','https://'+nip+':30433/mcp-wt/v1','/fixture/cert/backend-ca.pem',
 '2026-07-28','https://openlegal4everyone.stream','--serving-smoke']
r=checked(['docker','exec',name+'-edge',*wt],100)
assert all(c['status']=='passed' for c in json.loads(r)['checks'])
for protocol in ('tcp','udp'):
 before=drops('-A OLA_PHASE9 ')
 if protocol=='tcp':
  r=probe(http,'http://'+nip+':30080/mcp',ns='openlegal-accept-client',pod='denied'); assert r.returncode==3
 else:
  wt[2]='/inputs/ca.crt'
  r=command(k+['-n','openlegal-accept-client','exec','denied','--',*wt],100); assert r.returncode!=0
 delta=drops('-A OLA_PHASE9 ')-before; assert delta>0
 print(protocol+' NodePort denied; DROP counter delta:',delta)
retained()
print('CNI health isolation and allowed NodePort controls: passed')
PY
```

## Trust rejection and restoration

Run fresh native WebTransport sessions after restarting the edge for each trust
change. Each negative must retain a successful public HTTP control, then report
a WebTransport **connect** failure; an unrelated fixture failure is insufficient.
The wrong-SAN certificate must verify under the correct CA and fail IP identity
verification. Only the backend TLS Secret is replaced; runtime credentials stay
untouched. A `finally` restoration trap handles assertion failures and normal
interrupts. SIGKILL or machine loss requires the explicit recovery block below.

```sh
"$accept_tools/bin/python" - <<'PY'
import os, pathlib
exec((pathlib.Path(os.environ['accept_run'])/'ops-common.py').read_text())
# Save reusable recovery functions privately before altering any trust material.
(run/'ops-trust.py').write_text('''
import signal
signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
def edge_ca(filename):
 checked(['docker','run','--rm','-i','--network','none','--user','0:0',
  '--mount','type=volume,source='+os.environ['accept_edge_volume']+',target=/fixture',
  '--entrypoint','sh',os.environ['accept_edge_image'],'-ec',
  'cat > /fixture/cert/backend-ca.pem; chown 65532:65532 /fixture/cert/backend-ca.pem; chmod 400 /fixture/cert/backend-ca.pem'],
  data=(tls/filename).read_bytes())
def leaf(which):
 obj={'apiVersion':'v1','kind':'Secret','metadata':{'name':'openlegal-backend-tls','namespace':'openlegal-serving'},
  'type':'Opaque','stringData':{'tls.crt':(tls/(which+'.crt')).read_text(),'tls.key':(tls/(which+'.key')).read_text()}}
 apply([obj],'tls-replacement.yaml')
 kc(['-n','openlegal-serving','rollout','restart','deployment/openlegal-server'])
 kc(['-n','openlegal-serving','rollout','status','deployment/openlegal-server','--timeout=360s'],370)
def edge_restart():
 checked(['docker','restart',name+'-edge'],35)
 code="import ssl,urllib.request; c=ssl.create_default_context(cafile='/inputs/ca.crt'); r=urllib.request.urlopen(urllib.request.Request('https://openlegal4everyone.stream:8443/mcp',data=b'{\\\"jsonrpc\\\":\\\"2.0\\\",\\\"id\\\":1,\\\"method\\\":\\\"initialize\\\",\\\"params\\\":{\\\"protocolVersion\\\":\\\"2025-11-25\\\",\\\"capabilities\\\":{},\\\"clientInfo\\\":{\\\"name\\\":\\\"trust-control\\\",\\\"version\\\":\\\"1\\\"}}}',headers={'Content-Type':'application/json','Accept':'application/json, text/event-stream','Origin':'https://openlegal4everyone.stream'}),context=c,timeout=3); assert r.status==200"
 for attempt in range(20):
  if probe(code,timeout=8).returncode==0: return
  time.sleep(.25)
 raise RuntimeError('HTTP listener control failed')
def public_wt(success):
 r=command(k+['-n','openlegal-accept-monitor','exec','monitor','--','/usr/local/bin/wt_client',
  'https://openlegal4everyone.stream:8443/mcp-wt/v1','/inputs/ca.crt','2026-07-28',
  'https://openlegal4everyone.stream','--serving-smoke'],100)
 report=json.loads(r.stdout)
 if success: assert r.returncode==0 and all(c['status']=='passed' for c in report['checks'])
 else: assert r.returncode!=0 and any(c['id']=='connect' and c['status']=='failed' for c in report['checks'])
def restore():
 edge_ca('ca.crt'); leaf('backend'); edge_restart(); public_wt(True)
''')
exec((run/'ops-trust.py').read_text())
checked(['openssl','verify','-CAfile',str(tls/'ca.crt'),str(tls/'wrong-backend.crt')])
assert command(['openssl','verify','-CAfile',str(tls/'ca.crt'),'-verify_ip',nip,str(tls/'wrong-backend.crt')]).returncode!=0
assert command(['openssl','verify','-CAfile',str(tls/'unrelated-ca.crt'),str(tls/'backend.crt')]).returncode!=0
edge_restart(); public_wt(True)
try:
 edge_ca('unrelated-ca.crt'); edge_restart(); public_wt(False)
 print('unrelated backend CA rejected with HTTP control: passed')
finally:
 edge_ca('ca.crt'); edge_restart(); public_wt(True)
try:
 leaf('wrong-backend'); edge_restart(); public_wt(False)
 print('trusted wrong-SAN backend rejected with HTTP control: passed')
finally:
 restore()
retained()
PY
```

Only listener startup controls above retry within a fixed bound; smoke RPCs and
negative WebTransport calls do not retry. To restore after an interrupted shell:

```sh
"$accept_tools/bin/python" - <<'PY'
import os, pathlib
exec((pathlib.Path(os.environ['accept_run'])/'ops-common.py').read_text())
exec((run/'ops-trust.py').read_text())
restore(); retained()
PY
```

## Clean termination, Recreate and competing lease

Begin only with an already-ready Pod. Capture its container ID before scaling
down: Kubernetes may delete the Pod object before its final status can be read.
Use the owned node's CRI timestamps and exit status as termination evidence. A
missing CRI record, signal/OOM exit or elapsed deadline fails this check.

```sh
"$accept_tools/bin/python" - <<'PY'
import os, pathlib
exec((pathlib.Path(os.environ['accept_run'])/'ops-common.py').read_text())
retained(); old=server()
assert any(c['type']=='Ready' and c['status']=='True' for c in old['status']['conditions'])
cid=old['status']['containerStatuses'][0]['containerID'].split('://',1)[1]
def exited(container_id):
 r=command(['docker','exec',node,'crictl','inspect',container_id],5)
 if r.returncode==0:
  status=json.loads(r.stdout)['status']
  if status['state']=='CONTAINER_EXITED': return status
start=time.monotonic()
kc(['-n','openlegal-serving','scale','deployment/openlegal-server','--replicas=0'])
terminal=None
while time.monotonic()-start<30:
 terminal=exited(cid)
 if terminal: break
 time.sleep(.2)
assert terminal and terminal['exitCode']==0 and terminal['reason']=='Completed'
assert time.monotonic()-start<30
kc(['-n','openlegal-serving','wait','pod/'+old['metadata']['name'],'--for=delete','--timeout=30s'])
kc(['-n','openlegal-serving','scale','deployment/openlegal-server','--replicas=1'])
kc(['-n','openlegal-serving','rollout','status','deployment/openlegal-server','--timeout=360s'],370)
assert server()['metadata']['uid']!=old['metadata']['uid']
retained(); print('ready-server clean SIGTERM and retained restart: passed')
old=server(); cid=old['status']['containerStatuses'][0]['containerID'].split('://',1)[1]
assert get('deployment','openlegal-serving','openlegal-server')['spec']['strategy']['type']=='Recreate'
kc(['-n','openlegal-serving','rollout','restart','deployment/openlegal-server'])
end=time.monotonic()+360; terminal=None; new=None; max_running=0
while time.monotonic()<end:
 pods=json.loads(kc(['-n','openlegal-serving','get','pods','-l',
  'app.kubernetes.io/name=openlegal-server','-o','json']))['items']
 max_running=max(max_running,sum(p.get('status',{}).get('phase')=='Running' for p in pods))
 terminal=terminal or exited(cid)
 for pod in pods:
  if pod['metadata']['uid']!=old['metadata']['uid'] and any(c['type']=='Ready' and c['status']=='True'
    for c in pod.get('status',{}).get('conditions',[])): new=pod
 if new and terminal: break
 time.sleep(.2)
assert new and terminal and terminal['exitCode']==0 and terminal['reason']=='Completed' and max_running==1
new_cid=new['status']['containerStatuses'][0]['containerID'].split('://',1)[1]
started=json.loads(checked(['docker','exec',node,'crictl','inspect',new_cid]))['status']['startedAt']
assert datetime.datetime.fromisoformat(started)>=datetime.datetime.fromisoformat(terminal['finishedAt'])
retained(); print('observed old exit before Recreate replacement start: passed')
PY
```

The polling observation is supported by old/new CRI timestamps; replica count or
Pod phase alone is not fencing. The next check uses the same runtime database
but a distinct empty index PV/PVC. Its Job uses the admin label and is not
selected by the serving Service. Keep the sole serving Pod running throughout.

```sh
"$accept_tools/bin/python" - <<'PY'
import os, pathlib
exec((pathlib.Path(os.environ['accept_run'])/'ops-common.py').read_text())
original=server()['metadata']['uid']; retained()
for kind,obj in [('pv','openlegal-competing-index'),('pvc','openlegal-competing-index'),('job','openlegal-competing')]:
 assert not kc(['-n','openlegal-serving','get',kind,obj,'--ignore-not-found','-o','name']).strip()
checked(['docker','exec',node,'sh','-ec',
 'test -d /var/local/openlegal/competing-index/data; test -z "$(find /var/local/openlegal/competing-index/data -mindepth 1 -print -quit)"'])
pv=copy.deepcopy(get('pv','openlegal-serving','openlegal-corpus-index'))
pv['metadata']={'name':'openlegal-competing-index'}; pv.pop('status',None)
pv['spec']['claimRef']={'namespace':'openlegal-serving','name':'openlegal-competing-index'}
pv['spec']['local']['path']='/var/local/openlegal/competing-index'
pvc={'apiVersion':'v1','kind':'PersistentVolumeClaim','metadata':{'name':'openlegal-competing-index','namespace':'openlegal-serving'},
 'spec':{'storageClassName':'openlegal-local','volumeName':'openlegal-competing-index','accessModes':['ReadWriteOnce'],
 'resources':{'requests':{'storage':'4Gi'}}}}
spec=copy.deepcopy(get('deployment','openlegal-serving','openlegal-server')['spec']['template']['spec'])
spec['restartPolicy']='Never'; container=spec['containers'][0]
for field in ('startupProbe','readinessProbe','livenessProbe','ports'): container.pop(field,None)
for volume in spec['volumes']:
 if volume['name']=='corpus-index': volume['persistentVolumeClaim']['claimName']='openlegal-competing-index'
job={'apiVersion':'batch/v1','kind':'Job','metadata':{'name':'openlegal-competing','namespace':'openlegal-serving'},
 'spec':{'backoffLimit':0,'activeDeadlineSeconds':45,'template':{
 'metadata':{'labels':{'app.kubernetes.io/name':'openlegal-admin'}},'spec':spec}}}
apply([pv,pvc,job],'competing.yaml')
end=time.monotonic()+60
while time.monotonic()<end:
 j=get('job','openlegal-serving','openlegal-competing')
 if j.get('status',{}).get('failed',0): break
 time.sleep(.5)
assert j.get('status',{}).get('failed')==1 and not j.get('status',{}).get('succeeded')
assert not any(c.get('reason')=='DeadlineExceeded' for c in j.get('status',{}).get('conditions',[]))
pods=json.loads(kc(['-n','openlegal-serving','get','pods','-l','job-name=openlegal-competing','-o','json']))['items']
assert len(pods)==1
termination=pods[0]['status']['containerStatuses'][0]['state']['terminated']
assert termination['exitCode']==1 and termination['reason']!='OOMKilled'
logs=kc(['-n','openlegal-serving','logs','job/openlegal-competing'])
(run/'competing-private.log').write_bytes(logs)
assert logs.strip()==b'Error: Conflict'
assert server()['metadata']['uid']==original
retained(); print('separate-index runtime lease Conflict, sole server unchanged: passed')
PY
```

If the terminal diagnostic differs from the fixed `Error: Conflict` line, inspect
it privately and resolve the difference; do not treat any nonzero exit or timeout
as a successful lease rejection. The index remains on its separate disposable
path. Do not repurpose it for serving or delete the live index.

## Recovery, cleanup and evidence limits

Restore trust and require fresh public HTTP/WebTransport plus retained-data
positive controls before ending acceptance. Run this final smoke once against the
current Pod IP; it reuses the existing probes and never recreates their names.
Restart the edge explicitly and perform its bounded listener control first.
Each full smoke invocation retains separately named private stdout/stderr, even
on failure. Investigate a failure and record it before any explicit rerun.

```sh
"$accept_tools/bin/python" - <<'PY'
import os, pathlib
exec((pathlib.Path(os.environ['accept_run'])/'ops-common.py').read_text())
exec((run/'ops-trust.py').read_text())
restore(); public_smoke(); retained()
PY
```

Remove the chain using its exact rules only, then clean the run-owned edge/client
containers and edge volume before invoking README cluster deletion. Run firewall removal only if the installation
block completed; investigate partial setup instead of suppressing its failures.

```sh
for accept_protocol_port in tcp:30080 udp:30433; do
    docker exec "$accept_name-control-plane" iptables -t raw -D PREROUTING \
        -d "$accept_node_ip/32" -p "${accept_protocol_port%:*}" \
        --dport "${accept_protocol_port#*:}" -j OLA_PHASE9
done
docker exec "$accept_name-control-plane" iptables -t raw -F OLA_PHASE9
docker exec "$accept_name-control-plane" iptables -t raw -X OLA_PHASE9
```

Record sanitized assertions and relevant counters/timestamps in the acceptance
evidence, with revision, artifact identities and the single-node topology.
The scenarios above were transcribed from the executed disposable acceptance
scripts; the complete appendix, including its curated-volume adaptation, was
syntax-checked after that cluster was deleted and was not itself replayed.
Do not equate that transcription check with another successful cluster run.
