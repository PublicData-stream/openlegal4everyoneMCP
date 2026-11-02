"""Exact graph registration tests only; no backend invocation or authority."""
import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import patch
BASE=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(BASE))
import interface
import inventory
import checkpoint
import canary_watch

EXPECTED={'interface.py','inventory.py','journal.py','copy_tree.py','retention.py','checkpoint.py',
          'linux_backend.py','restore_backend.py','manual_backend.py','provider_backend.py',
          'bootstrap_backend.py','command_process.py','canary_native.py','preservation_backend.py','preservation_capture.py'}


class SourceGraphTests(unittest.TestCase):
    def test_all_three_loaders_require_exact_fifteen_not_standalone_authority(self):
        self.assertEqual(set(interface.SOURCE_FILES),EXPECTED)
        self.assertEqual(set(checkpoint.FILES),EXPECTED)
        self.assertEqual(set(canary_watch.FILES),EXPECTED)
        self.assertEqual(len(EXPECTED),15)
        self.assertNotIn('canary_supervisor.py',EXPECTED)
        self.assertNotIn('canary_watch.py',EXPECTED)

    def test_missing_extra_or_renamed_graph_refuses_before_any_source_import(self):
        graph={name:'a'*64 for name in EXPECTED}
        for change in ('missing','extra','renamed'):
            bad=dict(graph)
            if change=='missing':bad.pop('command_process.py')
            elif change=='extra':bad['canary_supervisor.py']='b'*64
            else:bad['other_native.py']=bad.pop('canary_native.py')
            with self.subTest(change=change),patch.object(checkpoint,'bootstrap_file',side_effect=AssertionError('must not read source')):
                with self.assertRaisesRegex(RuntimeError,'bootstrap_graph'):checkpoint.modules(inventory.canonical({'source_graph':bad}))
            with self.subTest(watcher=change),patch.object(canary_watch,'private_bytes',side_effect=AssertionError('must not read source')):
                with self.assertRaises(canary_watch.Refused):canary_watch._modules({'source_graph':bad},BASE)

    def test_all_source_reads_precede_inert_imports_and_new_dependencies_resolve(self):
        import builtins
        events=[];sources={}
        for name in EXPECTED:
            code="import builtins\nassert sum(x.startswith('read:') for x in builtins._synthetic_graph_events)==15\n"
            code+="builtins._synthetic_graph_events.append('exec:"+name+"')\nmarker="+repr(name)+"\n"
            if name=='bootstrap_backend.py':code+="import manual_backend\nassert manual_backend.marker=='manual_backend.py'\n"
            if name=='command_process.py':code+="import interface\nassert interface.marker=='interface.py'\n"
            if name=='canary_native.py':code+="import inventory\nassert inventory.marker=='inventory.py'\n"
            if name=='preservation_capture.py':code+="import preservation_backend\nassert preservation_backend.marker=='preservation_backend.py'\n"
            sources[name]=code.encode()
        graph={name:inventory.digest(raw)for name,raw in sources.items()}
        def read(path,pin,limit=0):
            raw=sources[path.name];self.assertEqual(inventory.digest(raw),pin);events.append('read:'+path.name);return raw
        with patch.dict(sys.modules),patch.object(builtins,'_synthetic_graph_events',events,create=True),patch.object(checkpoint,'bootstrap_file',side_effect=read):
            for name in EXPECTED:
                if name!='checkpoint.py':sys.modules.pop(name[:-3],None)
            modules=checkpoint.modules(inventory.canonical({'source_graph':graph}),base=BASE)
            self.assertEqual(modules['bootstrap_backend'].marker,'bootstrap_backend.py')
            self.assertEqual(modules['command_process'].marker,'command_process.py')
            self.assertEqual(modules['canary_native'].marker,'canary_native.py')
        self.assertEqual(sum(x.startswith('read:')for x in events),15)
        self.assertEqual(sum(x.startswith('exec:')for x in events),14)


if __name__=='__main__':unittest.main()
