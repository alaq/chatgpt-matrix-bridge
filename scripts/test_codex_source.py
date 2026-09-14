import json
from pathlib import Path
import tempfile
import unittest
import sqlite3
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from unittest.mock import patch
from codex_source import read_thread, send, probe, DesktopIPC

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



class SendTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        (self.root / 'sessions').mkdir()
        self.rollout = self.root / 'sessions/thread.jsonl'
        self.rollout.write_text('')
        self.journal = self.root / 'journal'
        self.cid = '11111111-1111-4111-8111-111111111111'
        self.req = {'version': 1, 'accountKey': 'a'*64, 'conversationId': 'codex:'+self.cid,
                    'transactionId': 'b'*64, 'text': 'Keep the current task settings.\nContinue here.'}
        with closing(sqlite3.connect(self.root / 'state_5.sqlite')) as db:
            db.execute('CREATE TABLE threads (id TEXT, rollout_path TEXT, history_mode TEXT, archived INTEGER, cwd TEXT)')
            db.execute('INSERT INTO threads VALUES (?,?,?,?,?)',
                       (self.cid, str(self.rollout), 'paginated', 0, '/original/workspace'))
            db.commit()
        self.calls = []
        self.lose_reply = False
        self.no_owner = False
        self.append_receipt = True
        outer = self
        class Owner:
            def __init__(self, root):
                outer.assertEqual(root, outer.root)
            def close(self): pass
            def call(self, method, params, version=1, target=None):
                if method == 'thread-owner-discovery':
                    outer.assertEqual(params, {'hostId': 'local', 'conversationId': outer.cid})
                    if outer.no_owner: raise ValueError('no-client-found')
                    return {'handledByClientId': 'original-owner'}
                outer.assertEqual(target, 'original-owner')
                outer.calls.append((method, params, version))
                if method == 'thread-follower-start-turn':
                    request = params['turnStart']['request']
                else:
                    request = params
                if outer.append_receipt:
                    outer.record({'type':'item_completed','item':{
                        'type':'UserMessage','id':'33333333-3333-4333-8333-333333333333',
                        'client_id':request['clientUserMessageId'], 'content':request['input']}})
                if outer.lose_reply: raise ConnectionError('lost after dispatch')
                return {'result': {'result': {'turn': {'id': 'same-task-turn'}}}}
        self.owner = Owner
        self.patcher = patch('codex_source.DesktopIPC', Owner)
        self.patcher.start()
        self.addCleanup(self.patcher.stop)

    def record(self, payload, when=None):
        with self.rollout.open('a') as f:
            f.write(json.dumps({'timestamp':when or datetime.now(timezone.utc).isoformat(),
                                'type':'event_msg','payload':payload})+'\n')

    def test_idle_continues_original_owner_without_settings_overrides(self):
        result = send(self.root, self.journal, self.req)
        self.assertEqual(result['status'], 'accepted')
        method, params, version = self.calls[0]
        self.assertEqual((method,version), ('thread-follower-start-turn',2))
        self.assertEqual(params['conversationId'],self.cid)
        self.assertEqual(set(params['turnStart']['request']), {'threadId','input','clientUserMessageId'})
        self.assertEqual(params['turnStart']['context'], {'inheritThreadSettings':True})
        self.assertEqual(params['turnStart']['request']['input'][0]['text'],self.req['text'])
        self.assertEqual(send(self.root,self.journal,self.req),result)
        self.assertEqual(len(self.calls),1)

    def test_active_turn_is_steered_even_after_long_silence(self):
        self.record({'type':'task_started','turn_id':'active-turn'},'2026-09-01T00:00:00Z')
        result = send(self.root,self.journal,self.req)
        self.assertEqual(result['status'],'accepted')
        method, params, version = self.calls[0]
        self.assertEqual((method,version),('thread-follower-steer-turn',1))
        self.assertEqual(params['conversationId'],self.cid)
        self.assertEqual(params['restoreMessage']['cwd'],'/original/workspace')
        for override in ['model','serviceTier','effort','permissions','sandboxPolicy','approvalPolicy']:
            self.assertNotIn(override,params)
        self.assertEqual(len(self.calls),1)

    def test_lost_receipt_reconciles_exact_client_id_without_resend(self):
        self.lose_reply = True
        with self.assertRaises(ConnectionError): send(self.root,self.journal,self.req)
        result = send(self.root,self.journal,self.req)
        self.assertEqual(result['status'],'accepted')
        self.assertEqual(result['userMessageId'],'33333333-3333-4333-8333-333333333333')
        self.assertEqual(len(self.calls),1)

    def test_unconfirmed_steer_never_falls_back_to_start(self):
        self.record({'type':'task_started','turn_id':'active-turn'})
        self.lose_reply = True
        self.append_receipt = False
        with self.assertRaises(ConnectionError):send(self.root,self.journal,self.req)
        self.record({'type':'task_complete','turn_id':'active-turn'})
        self.assertEqual(send(self.root,self.journal,self.req)['status'],'uncertain')
        self.assertEqual([c[0] for c in self.calls],['thread-follower-steer-turn'])

    def test_identical_text_with_wrong_client_id_does_not_resolve(self):
        self.lose_reply = True
        self.append_receipt = False
        with self.assertRaises(ConnectionError):send(self.root,self.journal,self.req)
        self.record({'type':'item_completed','item':{'type':'UserMessage',
            'id':'44444444-4444-4444-8444-444444444444','client_id':'unrelated',
            'content':[{'type':'text','text':self.req['text']}]}})
        self.assertEqual(send(self.root,self.journal,self.req)['status'],'uncertain')
        self.assertEqual(len(self.calls),1)

    def test_missing_owner_is_not_submitted_and_probe_has_no_side_effect(self):
        self.no_owner = True
        result=send(self.root,self.journal,self.req)
        self.assertEqual(result, {'version':1,'status':'not_sent','error':'codex_owner_unavailable'})
        self.assertEqual(list(self.journal.glob('*.json')),[])
        self.assertFalse(probe(self.root,self.cid)['available'])
        self.assertEqual(self.calls,[])
        self.assertEqual(self.rollout.read_text(),'')

    def test_transaction_payload_change_is_rejected(self):
        send(self.root,self.journal,self.req)
        with self.assertRaises(ValueError):send(self.root,self.journal,{**self.req,'text':'different'})
        self.assertEqual(len(self.calls),1)

    def test_concurrent_duplicate_calls_submit_once(self):
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(lambda _:send(self.root,self.journal,self.req),range(2)))
        self.assertEqual(results[0],results[1])
        self.assertEqual(len(self.calls),1)

class IPCResponseTests(unittest.TestCase):
    def test_different_owner_response_is_rejected(self):
        ipc=object.__new__(DesktopIPC)
        ipc.client_id='bridge'
        class Sock:
            def settimeout(self, value):pass
        ipc.sock=Sock()
        frames=[]
        import struct
        def write(request):
            data=json.dumps({'type':'response','requestId':request['requestId'],
                'resultType':'success','method':request['method'],
                'handledByClientId':'other-owner','result':{}}).encode()
            frames.extend([struct.pack('<I',len(data)),data])
        ipc.write=write
        ipc.read=lambda count:frames.pop(0)
        with self.assertRaisesRegex(ValueError,'different task owner'):
            ipc.call('thread-follower-start-turn',{},version=2,target='original-owner')

if __name__=='__main__':unittest.main()
