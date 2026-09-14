import json
from pathlib import Path
import tempfile
import unittest
from codex_source import read_thread, send

class CodexTests(unittest.TestCase):
    def test_only_completed_visible_items_and_stable_identity(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);(root/'sessions').mkdir();path=root/'sessions/session.jsonl'
            row={'id':'thread','rollout_path':str(path),'history_mode':'paginated'}
            records=[{'type':'task_started'},
                {'type':'item_completed','item':{'type':'UserMessage','id':'user','client_id':'transaction','content':[{'type':'text','text':'Hello'}]}},
                {'type':'item_completed','item':{'type':'Reasoning','id':'hidden','content':[{'type':'text','text':'secret'}]}},
                {'type':'item_completed','item':{'type':'AgentMessage','id':'progress','phase':'commentary','content':[{'type':'Text','text':'Working'}]}},
                {'type':'item_completed','item':{'type':'AgentMessage','id':'answer','phase':'final_answer','content':[{'type':'Text','text':'Done'}]}},
                {'type':'task_complete'}]
            path.write_text('\n'.join(json.dumps({'timestamp':'2026-09-14T17:00:00Z','type':'event_msg','payload':p}) for p in records)+'\n{"partial":')
            messages,running=read_thread(root,row)
            self.assertEqual([m['id'] for m in messages],['user','progress','answer']);self.assertFalse(running)
            self.assertEqual(messages[0]['client_id'],'transaction')
            self.assertEqual(read_thread(root,row),(messages,running))
    def test_outside_store_rejected(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);path=root/'outside.jsonl';path.write_text('')
            with self.assertRaises(ValueError):read_thread(root,{'rollout_path':str(path)})

if __name__=='__main__':unittest.main()
