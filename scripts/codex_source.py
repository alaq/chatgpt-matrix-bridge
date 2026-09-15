#!/usr/bin/env python3
"""Visible local Work/Codex history and owner-routed replies. No tools or reasoning.

Reads the local thread catalog and immutable rollout records. Replies use the
running desktop app's owner-routing IPC so existing model/permission settings stay
with the original task. The app must be running; approvals remain in its UI.
"""
import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import socket
import sqlite3
import stat
import struct
import subprocess
import sys
import time
import uuid
from datetime import datetime


def digest(data):
    return hashlib.sha256(json.dumps(data, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def timestamp(raw):
    return datetime.fromisoformat(raw.replace('Z', '+00:00')).timestamp()


def catalog(root):
    paths = sorted(root.glob('state_*.sqlite'), key=lambda p: int(p.stem.split('_')[1]))
    if not paths:
        raise ValueError('local task catalog unavailable')
    db = sqlite3.connect(paths[-1].as_uri() + '?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    return db


def read_thread(root, row, running_timeout=300):
    root = root.resolve()
    path = Path(row['rollout_path']).resolve()
    if not path.is_relative_to(root / 'sessions') and not path.is_relative_to(root / 'archived_sessions'):
        raise ValueError('rollout outside task store')
    if path.stat().st_uid != os.getuid() or path.stat().st_size > 128 * 1024 * 1024:
        raise ValueError('unavailable rollout')
    messages, seen, active, last_seen = [], set(), False, 0
    with path.open() as stream:
        for line in stream:
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue  # an append may be in progress
            if isinstance(record.get('timestamp'),str):last_seen=max(last_seen,timestamp(record['timestamp']))
            payload = record.get('payload', {})
            if record.get('type') == 'event_msg':
                typ = payload.get('type')
                if typ == 'task_started':
                    active = True
                elif typ in ('task_complete', 'turn_aborted'):
                    active = False
                item = payload.get('item', {}) if typ == 'item_completed' else {}
                kind = item.get('type')
                if kind not in ('UserMessage', 'AgentMessage'):
                    continue
                if kind == 'AgentMessage' and item.get('phase') not in (None, 'final_answer', 'final', 'commentary'):
                    continue
                role = 'user' if kind == 'UserMessage' else 'assistant'
                text = '\n'.join(block['text'] for block in item.get('content', []) if isinstance(block, dict) and block.get('type') in ('text', 'Text') and isinstance(block.get('text'), str))
                mid = item.get('id')
                client_id = item.get('client_id')
            elif record.get('type') == 'response_item' and payload.get('type') == 'message' and row['history_mode'] != 'paginated':
                role = payload.get('role')
                if role not in ('user', 'assistant') or role == 'assistant' and payload.get('channel') not in ('final', 'commentary'):
                    continue
                text = '\n'.join(v['text'] for v in payload.get('content', []) if isinstance(v, dict) and isinstance(v.get('text'), str))
                if role == 'user' and text.startswith(('# AGENTS.md instructions', '<environment_context>', '<permissions instructions>')):
                    continue
                mid = payload.get('id') or digest([row['id'], record.get('ordinal', record['timestamp']), role])
                client_id = None
            else:
                continue
            if not mid or mid in seen or not text.strip():
                continue
            seen.add(mid)
            messages.append({'id': mid, 'client_id': client_id, 'role': role, 'text': text,
                'created_at': timestamp(record['timestamp']), 'attachment_count': 0})
    return messages, active and (running_timeout is None or time.time()-last_seen < running_timeout)


class DesktopRequestError(ValueError):
    def __init__(self, error):
        self.error = error
        super().__init__('desktop request unavailable: ' + str(error))


class DesktopIPC:
    def __init__(self, root, timeout=20):
        path = root / 'ipc/ipc.sock'
        info = path.lstat()
        if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError('unsafe desktop socket')
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(timeout)
        self.client_id = None
        try:
            self.sock.connect(str(path))
            response = self.call('initialize', {'clientType': 'chatgpt-matrix-bridge'}, 0, timeout=timeout)
            self.client_id = response['result']['clientId']
        except Exception:
            self.sock.close()
            raise

    def write(self, data):
        raw = json.dumps(data).encode()
        self.sock.sendall(struct.pack('<I', len(raw)) + raw)

    def read(self, count):
        parts = bytearray()
        while len(parts) < count:
            chunk = self.sock.recv(count - len(parts))
            if not chunk:
                raise ConnectionError('desktop connection closed')
            parts.extend(chunk)
        return bytes(parts)

    def call(self, method, params, version=1, target=None, timeout=20):
        request_id = str(uuid.uuid4())
        request = {'type': 'request', 'requestId': request_id, 'sourceClientId': self.client_id,
            'method': method, 'params': params, 'version': version, 'timeoutMs': min(15000, max(1, int(timeout * 1000)))}
        if target:
            request['targetClientId'] = target
        self.write(request)
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            self.sock.settimeout(max(0.01, deadline - time.monotonic()))
            size = struct.unpack('<I', self.read(4))[0]
            if size <= 0 or size > 64 * 1024 * 1024:
                raise ValueError('invalid desktop frame')
            response = json.loads(self.read(size))
            if response.get('type') == 'client-discovery-request':
                self.write({'type': 'client-discovery-response', 'requestId': response['requestId'], 'response': {'canHandle': False}})
            if response.get('type') == 'response' and response.get('requestId') == request_id:
                if response.get('resultType') != 'success':
                    raise DesktopRequestError(response.get('error'))
                if response.get('method') != method or not response.get('handledByClientId'):
                    raise ValueError('desktop response method/owner mismatch')
                if target and response['handledByClientId'] != target:
                    raise ValueError('desktop response from a different task owner')
                return response
        raise TimeoutError('desktop request timed out')

    def close(self):
        self.sock.close()


OWNER_LOOKUP_TIMEOUT = 3
OWNER_RECOVERY_TIMEOUT = 12


def open_original_task(conversation_id):
    """Open only the existing UUID, without a prompt or a replacement executor."""
    if str(uuid.UUID(conversation_id)) != conversation_id or sys.platform != 'darwin':
        raise ValueError('desktop task activation unavailable')
    # Explicit bundle identity avoids the browser/default URL handler. -g asks
    # macOS to leave the foreground app alone; Codex may change its selected task.
    subprocess.run(['/usr/bin/open', '-g', '-b', 'com.openai.codex',
                    'codex://threads/' + conversation_id],
                   check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)


def owner_unavailable(error):
    return isinstance(error, OSError) or (
        isinstance(error, DesktopRequestError) and error.error == 'no-client-found')


def connect_owner(root, conversation_id):
    """Find an owner, or open this task once and await its owner within a bound.

    The initial trusted socket must already connect: this does not launch an app
    in a different store or substitute a CLI when the desktop is unavailable.
    No turn is submitted here. Caller owns the returned connection.
    """
    params = {'hostId': 'local', 'conversationId': conversation_id}
    ipc = DesktopIPC(root, timeout=OWNER_LOOKUP_TIMEOUT)
    try:
        owner = ipc.call('thread-owner-discovery', params, timeout=OWNER_LOOKUP_TIMEOUT)['handledByClientId']
        return ipc, owner
    except Exception as error:
        ipc.close()
        if not owner_unavailable(error):
            raise
    open_original_task(conversation_id)
    deadline = time.monotonic() + OWNER_RECOVERY_TIMEOUT
    while time.monotonic() < deadline:
        ipc = None
        try:
            ipc = DesktopIPC(root, timeout=min(OWNER_LOOKUP_TIMEOUT, deadline - time.monotonic()))
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError('desktop task owner recovery timed out')
            owner = ipc.call('thread-owner-discovery', params,
                             timeout=min(OWNER_LOOKUP_TIMEOUT, remaining))['handledByClientId']
            return ipc, owner
        except Exception as error:
            if ipc is not None:
                ipc.close()
            if not owner_unavailable(error):
                raise
        time.sleep(max(0, min(0.25, deadline - time.monotonic())))
    raise DesktopRequestError('no-client-found')


def feed(root, since):
    db = catalog(root)
    rows = db.execute("SELECT * FROM threads WHERE updated_at>=? AND archived=0 AND source IN ('cli','vscode','exec','appServer') ORDER BY updated_at", (since,)).fetchall()
    conversations = []
    for row in rows:
        messages, active = read_thread(root, row)
        public = [{k: v for k, v in m.items() if k != 'client_id'} for m in messages]
        conversations.append({'id': 'codex:' + row['id'], 'kind': 'codex', 'title': row['name'] or row['title'] or 'Untitled task',
            'revision': digest([public, active]), 'url': 'codex://threads/' + row['id'], 'created_at': row['created_at'],
            'updated_at': row['updated_at'], 'messages': public, 'running': active})
    db.close()
    return conversations


def send(root, journal, request):
    cid = request.get('conversationId', '')
    if not cid.startswith('codex:') or str(uuid.UUID(cid[6:])) != cid[6:] or request.get('attachments'):
        raise ValueError('unsupported local task request')
    if not isinstance(request.get('text'), str) or not request['text'].strip() or len(request['text'].encode()) > 12000:
        raise ValueError('invalid task text')
    transaction = request.get('transactionId', '')
    if len(transaction) != 64 or any(c not in '0123456789abcdef' for c in transaction):
        raise ValueError('invalid transaction')
    journal.mkdir(parents=True, mode=0o700, exist_ok=True)
    info=journal.lstat()
    if journal.is_symlink() or info.st_uid!=os.getuid() or info.st_mode & 0o077:
        raise ValueError('unsafe local send journal')
    # Serialize all transactions for this task, including another bridge process.
    # A process dying after source acceptance must leave its record to reconcile.
    lock = journal / (digest([str(root), cid]) + '.lock')
    fd = os.open(lock, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise ValueError('unsafe local send lock')
        fcntl.flock(fd, fcntl.LOCK_EX)
        return send_locked(root, journal, request)
    finally:
        os.close(fd)


def send_locked(root, journal, request):
    cid, transaction = request['conversationId'], request['transactionId']
    file = journal / (transaction + '.json')
    if file.is_symlink() or file.exists() and (file.stat().st_uid!=os.getuid() or file.stat().st_mode & 0o077 or file.stat().st_size>8192):
        raise ValueError('unsafe local send record')
    identity = {'conversation_id': cid, 'transaction_id': transaction, 'text_hash': digest(request['text']), 'account_key':request['accountKey'], 'store':digest(str(root))}
    record = json.loads(file.read_text()) if file.exists() else None
    if record and any(record[k] != v for k, v in identity.items()):
        raise ValueError('transaction conflict')
    db = catalog(root)
    row = db.execute('SELECT * FROM threads WHERE id=? AND archived=0', (cid[6:],)).fetchone()
    db.close()
    if row is None:
        raise ValueError('local task is unavailable')
    messages, active = read_thread(root, row)
    if record and record.get('status') != 'not_sent':
        if record.get('status') == 'accepted':
            return {'version': 1, 'status': 'accepted', 'userMessageId': record['message_id']}
        matches = [m for m in messages if m.get('client_id') == record['client_id'] and m['role'] == 'user' and digest(m['text']) == identity['text_hash']]
        if len(matches) != 1:
            return {'version': 1, 'status': 'uncertain', 'error': 'codex_send_uncertain'}
        record.update(status='accepted', message_id=matches[0]['id'])
        atomic_record(file, record)
        return {'version': 1, 'status': 'accepted', 'userMessageId': record['message_id']}
    try:
        ipc, owner = connect_owner(root, cid[6:])
    except (OSError, ValueError, KeyError, subprocess.SubprocessError):
        return {'version': 1, 'status': 'not_sent', 'error': 'codex_owner_unavailable'}
    try:
        # Activation can take several seconds. Respect an archive/handoff and
        # use the current rollout/workspace before any submission is journaled.
        db = catalog(root)
        try:
            row = db.execute('SELECT * FROM threads WHERE id=? AND archived=0', (cid[6:],)).fetchone()
        finally:
            db.close()
        if row is None:
            return {'version': 1, 'status': 'not_sent', 'error': 'codex_owner_unavailable'}
        client_id = str(uuid.UUID(transaction[:32]))
        # Refresh immediately before choosing start vs. steer. The desktop owner
        # remains responsible for an active-turn race; never fall back to a new
        # start after an uncertain steering request.
        _messages, active = read_thread(root, row, running_timeout=None)
        method, params, version = turn_request(cid[6:], request['text'], client_id, row['cwd'], active)
        record = {**identity, 'client_id': client_id, 'status': 'submitting', 'method': method}
        atomic_record(file, record)
        try:
            ipc.call(method, params, version=version, target=owner)
        except DesktopRequestError as error:
            # Only explicit pre-submission rejections permit another attempt.
            # Timeouts, disconnects and arbitrary handler errors stay uncertain.
            inactive = 'Cannot steer conversation ' + cid[6:] + ' because its active turn already ended'
            if error.error not in ('no-client-found', 'request-version-mismatch', 'no-handler-for-request', inactive):
                raise
            code = 'codex_turn_ended' if error.error == inactive else 'codex_owner_unavailable'
            record.update(status='not_sent')
            atomic_record(file, record)
            return {'version': 1, 'status': 'not_sent', 'error': code}
    finally:
        ipc.close()
    for _ in range(20):
        messages, _active = read_thread(root, row)
        matches = [m for m in messages if m.get('client_id') == client_id and m['role'] == 'user' and digest(m['text']) == identity['text_hash']]
        if len(matches) == 1:
            record.update(status='accepted', message_id=matches[0]['id']); atomic_record(file, record)
            return {'version': 1, 'status': 'accepted', 'userMessageId': record['message_id']}
        time.sleep(0.5)
    return {'version': 1, 'status': 'uncertain', 'error': 'codex_send_uncertain'}


def turn_request(thread_id, text, client_id, cwd, active):
    """Use the original desktop owner, with no model or permission overrides."""
    inputs = [{'type': 'text', 'text': text, 'text_elements': []}]
    if not active:
        return 'thread-follower-start-turn', {'conversationId': thread_id, 'turnStart': {
            'request': {'threadId': thread_id, 'input': inputs, 'clientUserMessageId': client_id},
            'context': {'inheritThreadSettings': True}}}, 2
    if not isinstance(cwd, str) or not Path(cwd).is_absolute():
        raise ValueError('local task workspace unavailable')
    return 'thread-follower-steer-turn', {'conversationId': thread_id,
        'input': inputs, 'clientUserMessageId': client_id, 'attachments': [],
        'restoreMessage': {'id': client_id, 'text': text, 'cwd': cwd,
            'createdAt': int(time.time() * 1000), 'context': {'prompt': text,
                'addedFiles': [], 'fileAttachments': [], 'imageAttachments': [],
                'ideContext': None, 'workspaceRoots': [cwd]}}}, 1


def probe(root, conversation_id, reconnect=False):
    """Read-only by default. Explicit reconnect may open, but never submit."""
    if str(uuid.UUID(conversation_id)) != conversation_id:
        raise ValueError('invalid task identity')
    db = catalog(root)
    try:
        row = db.execute('SELECT * FROM threads WHERE id=? AND archived=0', (conversation_id,)).fetchone()
    finally:
        db.close()
    if row is None:
        return {'available': False, 'reason': 'codex_task_unavailable'}
    _, active = read_thread(root, row)
    try:
        if reconnect:
            ipc, owner = connect_owner(root, conversation_id)
            ipc.close()
            ready = bool(owner)
        else:
            ipc = DesktopIPC(root)
            try:
                response = ipc.call('thread-owner-discovery', {'hostId': 'local', 'conversationId': conversation_id})
                ready = bool(response['handledByClientId'])
            finally:
                ipc.close()
    except (OSError, ValueError, KeyError, subprocess.SubprocessError):
        ready = False
    return {'available': ready, 'reason': None if ready else 'codex_owner_unavailable',
        'running': active, 'conversationId': conversation_id}


def atomic_record(file, record):
    tmp = file.with_suffix('.' + str(uuid.uuid4()) + '.tmp')
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        with os.fdopen(fd, 'w') as out:
            json.dump(record, out); out.flush(); os.fsync(out.fileno())
        tmp.replace(file)
        fd = os.open(str(file.parent), os.O_RDONLY)
        try: os.fsync(fd)
        finally: os.close(fd)
    finally:
        tmp.unlink(missing_ok=True)


if __name__ == '__main__':
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--codex-home', required=True)
    parser.add_argument('--since', type=float, default=0)
    parser.add_argument('--journal')
    parser.add_argument('--conversation-id')
    parser.add_argument('operation', choices=['feed', 'send', 'probe', 'reconnect'])
    args = parser.parse_args()
    try:
        root = Path(args.codex_home).expanduser().resolve()
        if args.operation == 'feed':
            result = feed(root, args.since)
        elif args.operation in ('probe', 'reconnect'):
            result = probe(root, args.conversation_id, reconnect=args.operation == 'reconnect')
        else:
            result = send(root, Path(args.journal), json.loads(sys.stdin.buffer.read(80 * 1024)))
        print(json.dumps(result))
    except Exception:
        if args.operation == 'send': print('{"version":1,"status":"uncertain","error":"codex_source_unavailable"}')
        else: sys.stderr.write('local task source unavailable\n'); sys.exit(1)
