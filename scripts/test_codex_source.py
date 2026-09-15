import json
import os
from pathlib import Path
import tempfile
import unittest
import sqlite3
import subprocess
from contextlib import closing
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from unittest.mock import patch
from codex_source import feed, read_thread, send, probe, DesktopIPC, DesktopRequestError, UnavailableRollout, OversizedRollout, open_original_task, rollout_delivery_fingerprint

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

    def test_feed_keeps_other_tasks_when_one_rollout_exceeds_safety_bound(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);(root/'sessions').mkdir()
            valid=root/'sessions/valid.jsonl';valid.write_text('')
            oversized=root/'sessions/oversized.jsonl'
            with oversized.open('wb') as stream:
                stream.truncate(128 * 1024 * 1024 + 1)
            with closing(sqlite3.connect(root/'state_1.sqlite')) as db:
                db.execute('CREATE TABLE threads (id TEXT, rollout_path TEXT, history_mode TEXT, archived INTEGER, source TEXT, name TEXT, title TEXT, created_at REAL, updated_at REAL)')
                db.executemany('INSERT INTO threads VALUES (?,?,?,?,?,?,?,?,?)', [
                    ('valid',str(valid),'paginated',0,'cli','Valid',None,1,3),
                    ('oversized',str(oversized),'paginated',0,'cli','Oversized',None,1,2),
                ])
                db.commit()
            result=feed(root,0)
            self.assertEqual([chat['id'] for chat in result],['codex:oversized','codex:valid'])
            self.assertEqual(result[0]['messages'],[])

    def test_feed_reads_only_the_ten_most_recent_tasks(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);(root/'sessions').mkdir()
            rows=[]
            for index in range(12):
                path=root/'sessions'/f'{index}.jsonl';path.write_text('')
                rows.append((str(index),str(path),'paginated',0,'cli',f'Task {index}',None,1,index))
            with closing(sqlite3.connect(root/'state_1.sqlite')) as db:
                db.execute('CREATE TABLE threads (id TEXT, rollout_path TEXT, history_mode TEXT, archived INTEGER, source TEXT, name TEXT, title TEXT, created_at REAL, updated_at REAL)')
                db.executemany('INSERT INTO threads VALUES (?,?,?,?,?,?,?,?,?)', rows)
                db.commit()
            result=feed(root,0,10)
            self.assertEqual([chat['id'] for chat in result], [f'codex:{index}' for index in range(2,12)])
            allowed=feed(root,0,10,['codex:0'])
            self.assertEqual([chat['id'] for chat in allowed], ['codex:0'])

    def test_known_fingerprint_skips_rollout_open(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);(root/'sessions').mkdir()
            path=root/'sessions/thread.jsonl';path.write_text('{malformed')
            row=('thread',str(path),'paginated',0,'cli','Task',None,1,2)
            with closing(sqlite3.connect(root/'state_1.sqlite')) as db:
                db.execute('CREATE TABLE threads (id TEXT, rollout_path TEXT, history_mode TEXT, archived INTEGER, source TEXT, name TEXT, title TEXT, created_at REAL, updated_at REAL)')
                db.execute('INSERT INTO threads VALUES (?,?,?,?,?,?,?,?,?)',row);db.commit()
                db.row_factory=sqlite3.Row
                record=db.execute('SELECT * FROM threads').fetchone()
                fingerprint=rollout_delivery_fingerprint(root,record)
            result=feed(root,0,known_fingerprints={'codex:thread':fingerprint})
            self.assertEqual(result,[{'id':'codex:thread','kind':'codex','title':'Task',
                'url':'codex://threads/thread','created_at':1,'updated_at':2,'messages':[],
                'running':False,'running_known':False,
                'delivery_fingerprint':fingerprint,'unchanged':True}])

    def test_read_thread_rejects_growth_beyond_safety_bound_after_open(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);(root/'sessions').mkdir();path=root/'sessions/growing.jsonl'
            with path.open('wb') as stream:
                stream.truncate(128 * 1024 * 1024)
            real_fstat=os.fstat
            real_fdopen=os.fdopen
            grown=False
            bytes_returned=0
            def grow_after_fstat(fd):
                nonlocal grown
                info=real_fstat(fd)
                if not grown:
                    grown=True
                    with path.open('ab') as stream:
                        stream.write(b'x')
                return info
            class TrackingStream:
                def __init__(self, fd, mode): self.stream=real_fdopen(fd,mode)
                def __enter__(self): return self
                def __exit__(self,*args): return self.stream.__exit__(*args)
                def fileno(self): return self.stream.fileno()
                def readline(self, size=-1):
                    nonlocal bytes_returned
                    data=self.stream.readline(size);bytes_returned+=len(data);return data
            with patch('codex_source.os.fstat',side_effect=grow_after_fstat), \
                 patch('codex_source.os.fdopen',side_effect=TrackingStream):
                with self.assertRaises(ValueError):
                    read_thread(root,{'rollout_path':str(path),'history_mode':'paginated'})
            self.assertLessEqual(bytes_returned,128 * 1024 * 1024)

    def test_nonowner_rollout_is_not_suppressed_as_oversized(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);(root/'sessions').mkdir();path=root/'sessions/nonowner.jsonl';path.write_text('')
            with closing(sqlite3.connect(root/'state_1.sqlite')) as db:
                db.execute('CREATE TABLE threads (id TEXT, rollout_path TEXT, history_mode TEXT, archived INTEGER, source TEXT, name TEXT, title TEXT, created_at REAL, updated_at REAL)')
                db.execute('INSERT INTO threads VALUES (?,?,?,?,?,?,?,?,?)',
                           ('nonowner',str(path),'paginated',0,'cli','Nonowner',None,1,2))
                db.commit()
            info=path.stat()
            connection=sqlite3.connect(root/'state_1.sqlite');connection.row_factory=sqlite3.Row
            class TrackingCatalog:
                closed=False
                def execute(self,*args): return connection.execute(*args)
                def close(self): self.closed=True;connection.close()
            tracked=TrackingCatalog()
            try:
                with patch('codex_source.catalog',return_value=tracked), \
                     patch('codex_source.os.fstat',return_value=type('Info',(),{'st_uid':os.getuid()+1,'st_size':info.st_size})()):
                    with self.assertRaises(UnavailableRollout):
                        feed(root,0)
                self.assertTrue(tracked.closed)
            finally:
                if not tracked.closed: connection.close()



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
        self.inactive_rejection = False
        self.activation_ids = []
        self.restore_owner = True
        self.lookup_error = None
        self.on_activate = None
        self.connections = []
        outer = self
        class Owner:
            def __init__(self, root, timeout=20):
                outer.assertEqual(root, outer.root)
                self.closed = False
                outer.connections.append(self)
            def close(self): self.closed = True
            def call(self, method, params, version=1, target=None, timeout=20):
                if method == 'thread-owner-discovery':
                    outer.assertEqual(params, {'hostId': 'local', 'conversationId': outer.cid})
                    if outer.lookup_error:
                        error, outer.lookup_error = outer.lookup_error, None
                        raise error
                    if outer.no_owner: raise DesktopRequestError('no-client-found')
                    return {'handledByClientId': 'original-owner'}
                outer.assertEqual(target, 'original-owner')
                outer.calls.append((method, params, version))
                if outer.inactive_rejection:
                    raise DesktopRequestError('Cannot steer conversation ' + outer.cid + ' because its active turn already ended')
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
        def activate(conversation_id):
            self.activation_ids.append(conversation_id)
            if self.on_activate: self.on_activate()
            if self.restore_owner: self.no_owner = False
        activation = patch('codex_source.open_original_task', side_effect=activate)
        self.activation = activation.start()
        self.addCleanup(activation.stop)

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
        self.assertEqual(self.activation_ids, [])

    def test_oversized_rollout_rejects_reply_before_submission(self):
        with self.rollout.open('wb') as stream:
            stream.truncate(128 * 1024 * 1024 + 1)
        result = send(self.root, self.journal, self.req)
        self.assertEqual(result, {'version': 1, 'status': 'not_sent',
                                  'error': 'codex_rollout_unavailable'})
        self.assertEqual(self.calls, [])
        self.assertEqual(self.activation_ids, [])
        self.assertFalse(self.journal.exists())

    def test_send_closes_catalog_when_initial_query_fails(self):
        class BrokenCatalog:
            closed=False
            def execute(self,*_args): raise sqlite3.OperationalError('malformed catalog')
            def close(self): self.closed=True
        catalog=BrokenCatalog()
        with patch('codex_source.catalog',return_value=catalog):
            with self.assertRaises(sqlite3.OperationalError):
                send(self.root,self.journal,self.req)
        self.assertTrue(catalog.closed)
        self.assertFalse(self.journal.exists())

    def test_rollout_growth_after_preflight_rejects_before_owner_lookup(self):
        with patch('codex_source.read_thread', side_effect=[([],False),OversizedRollout('oversized rollout')]):
            result = send(self.root, self.journal, self.req)
        self.assertEqual(result, {'version': 1, 'status': 'not_sent',
                                  'error': 'codex_rollout_unavailable'})
        self.assertEqual(self.calls, [])
        self.assertEqual(self.activation_ids, [])
        self.assertEqual(list(self.journal.glob('*.json')), [])

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

    def test_explicit_active_turn_rejection_allows_original_message_retry(self):
        self.record({'type':'task_started','turn_id':'active-turn'})
        self.inactive_rejection = True
        self.assertEqual(send(self.root,self.journal,self.req),
                         {'version':1,'status':'not_sent','error':'codex_turn_ended'})
        self.assertEqual(len(self.calls),1)
        self.record({'type':'task_complete','turn_id':'active-turn'})
        self.inactive_rejection = False
        self.assertEqual(send(self.root,self.journal,self.req)['status'],'accepted')
        self.assertEqual([c[0] for c in self.calls],
                         ['thread-follower-steer-turn','thread-follower-start-turn'])
        self.assertEqual(self.calls[0][1]['clientUserMessageId'],
                         self.calls[1][1]['turnStart']['request']['clientUserMessageId'])

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
        self.restore_owner = False
        self.assertFalse(probe(self.root,self.cid)['available'])
        self.assertEqual(self.activation_ids, [])
        with patch('codex_source.OWNER_RECOVERY_TIMEOUT', 0.01):
            result=send(self.root,self.journal,self.req)
        self.assertEqual(result, {'version':1,'status':'not_sent','error':'codex_owner_unavailable'})
        self.assertEqual(list(self.journal.glob('*.json')),[])
        self.assertEqual(self.activation_ids, [self.cid])
        self.assertTrue(all(c.closed for c in self.connections))
        self.assertEqual(self.calls,[])
        self.assertEqual(self.rollout.read_text(),'')

    def test_missing_owner_opens_original_task_and_submits_same_transaction_once(self):
        self.no_owner = True
        result = send(self.root, self.journal, self.req)
        self.assertEqual(result['status'], 'accepted')
        self.assertEqual(self.activation_ids, [self.cid])
        self.assertEqual(len(self.connections), 2)
        self.assertTrue(all(c.closed for c in self.connections))
        method, params, version = self.calls[0]
        self.assertEqual((method, version), ('thread-follower-start-turn', 2))
        self.assertEqual(params['turnStart']['context'], {'inheritThreadSettings': True})
        request = params['turnStart']['request']
        self.assertEqual(request['clientUserMessageId'], 'bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb')
        self.assertEqual(request['threadId'], self.cid)
        self.assertEqual(request['input'][0]['text'], self.req['text'])
        self.no_owner = True
        self.assertEqual(send(self.root, self.journal, self.req), result)
        self.assertEqual(len(self.calls), 1)
        self.assertEqual(self.activation_ids, [self.cid])

    def test_recovery_refreshes_active_state_before_choosing_steer(self):
        self.no_owner = True
        self.on_activate = lambda: self.record({'type': 'task_started', 'turn_id': 'existing-turn'})
        self.assertEqual(send(self.root, self.journal, self.req)['status'], 'accepted')
        self.assertEqual([c[0] for c in self.calls], ['thread-follower-steer-turn'])

    def test_task_archived_during_recovery_is_not_submitted(self):
        self.no_owner = True
        def archive():
            with closing(sqlite3.connect(self.root / 'state_5.sqlite')) as db:
                db.execute('UPDATE threads SET archived=1')
                db.commit()
        self.on_activate = archive
        self.assertEqual(send(self.root, self.journal, self.req)['status'], 'not_sent')
        self.assertEqual(self.calls, [])
        self.assertEqual(list(self.journal.glob('*.json')), [])
        self.assertTrue(all(c.closed for c in self.connections))

    def test_lookup_timeout_reconnects_before_submission(self):
        self.lookup_error = TimeoutError('lookup timed out')
        self.assertEqual(send(self.root, self.journal, self.req)['status'], 'accepted')
        self.assertEqual(self.activation_ids, [self.cid])
        self.assertEqual(len(self.connections), 2)
        self.assertEqual(len(self.calls), 1)

    def test_bad_lookup_response_does_not_activate(self):
        self.lookup_error = ValueError('desktop response method/owner mismatch')
        self.assertEqual(send(self.root, self.journal, self.req)['status'], 'not_sent')
        self.assertEqual(self.activation_ids, [])
        self.assertEqual(self.calls, [])

    def test_missing_initial_socket_does_not_launch_another_app(self):
        with patch('codex_source.DesktopIPC', side_effect=FileNotFoundError()):
            self.assertEqual(send(self.root, self.journal, self.req)['status'], 'not_sent')
        self.assertEqual(self.activation_ids, [])
        self.assertEqual(self.calls, [])

    def test_activation_failure_is_not_sent_and_original_transaction_can_retry(self):
        self.no_owner = True
        self.activation.side_effect = subprocess.TimeoutExpired('open', 5)
        self.assertEqual(send(self.root, self.journal, self.req)['status'], 'not_sent')
        self.assertEqual(list(self.journal.glob('*.json')), [])
        self.assertEqual(self.calls, [])
        self.no_owner = False
        self.assertEqual(send(self.root, self.journal, self.req)['status'], 'accepted')
        self.assertEqual(len(self.calls), 1)

    def test_reconnect_command_opens_without_submitting_and_rejects_archived_task(self):
        self.no_owner = True
        self.assertTrue(probe(self.root, self.cid, reconnect=True)['available'])
        self.assertEqual(self.activation_ids, [self.cid])
        self.assertEqual(self.calls, [])
        self.assertEqual(self.rollout.read_text(), '')
        with closing(sqlite3.connect(self.root / 'state_5.sqlite')) as db:
            db.execute('UPDATE threads SET archived=1')
            db.commit()
        self.assertFalse(probe(self.root, self.cid, reconnect=True)['available'])
        self.assertEqual(self.activation_ids, [self.cid])

    def test_recovered_owner_with_uncertain_send_is_never_reactivated_or_resent(self):
        self.no_owner = True
        self.lose_reply = True
        self.append_receipt = False
        with self.assertRaises(ConnectionError): send(self.root, self.journal, self.req)
        self.no_owner = True
        self.assertEqual(send(self.root, self.journal, self.req)['status'], 'uncertain')
        self.assertEqual(self.activation_ids, [self.cid])
        self.assertEqual(len(self.calls), 1)

    def test_transaction_payload_change_is_rejected(self):
        send(self.root,self.journal,self.req)
        with self.assertRaises(ValueError):send(self.root,self.journal,{**self.req,'text':'different'})
        self.assertEqual(len(self.calls),1)

    def test_concurrent_duplicate_calls_submit_once(self):
        self.no_owner = True
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(lambda _:send(self.root,self.journal,self.req),range(2)))
        self.assertEqual(results[0],results[1])
        self.assertEqual(len(self.calls),1)
        self.assertEqual(self.activation_ids, [self.cid])

class ActivationTests(unittest.TestCase):
    def test_only_valid_original_uuid_is_opened_in_the_codex_bundle_without_prompt(self):
        cid = '11111111-1111-4111-8111-111111111111'
        with patch('codex_source.sys.platform', 'darwin'), patch('codex_source.subprocess.run') as run:
            open_original_task(cid)
            run.assert_called_once_with(['/usr/bin/open', '-g', '-b', 'com.openai.codex',
                                         'codex://threads/' + cid], check=True,
                                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
            run.reset_mock()
            for invalid in ['new?prompt=hello', cid + '?prompt=hello', '../' + cid]:
                with self.assertRaises(ValueError): open_original_task(invalid)
            run.assert_not_called()
        with patch('codex_source.sys.platform', 'linux'), patch('codex_source.subprocess.run') as run:
            with self.assertRaises(ValueError): open_original_task(cid)
            run.assert_not_called()

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
