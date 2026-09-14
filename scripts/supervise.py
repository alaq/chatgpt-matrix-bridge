#!/usr/bin/env python3
"""Own just this bridge's processes; launchd restarts this supervisor after login.

Configuration and status are private, never command-line prompts or credentials.
Children restart independently with capped backoff. SIGTERM stops both children.
"""
import argparse
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import stat
from datetime import datetime, timezone


def atomic_json(path, data):
    tmp = path.with_suffix('.tmp')
    with tmp.open('w') as out:
        json.dump(data, out)
        out.flush()
        os.fsync(out.fileno())
    tmp.replace(path)


def run(config_path):
    os.umask(0o077)
    config_path = Path(config_path).resolve()
    info=config_path.stat()
    if info.st_uid!=os.getuid() or info.st_mode & 0o077:raise ValueError('supervisor config must be private')
    cfg = json.loads(config_path.read_text())
    root = config_path.parent
    lock = (root / 'supervisor.lock').open('a')
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    children, stopping = {}, False

    def stop(_sig, _frame):
        nonlocal stopping
        stopping = True

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, stop)
    for name in ('browser', 'bridge'):
        spec = cfg[name]
        if not spec['argv'] or not Path(spec['argv'][0]).is_absolute() or not Path(spec['cwd']).is_absolute():
            raise ValueError('service commands and working directories must be absolute')
        children[name] = dict(process=None, failures=0, next_start=0, started=0, spec=spec)
    try:
        while not stopping:
            now = time.monotonic()
            for name, state in children.items():
                process = state['process']
                if process is not None and process.poll() is not None:
                    state['failures'] = 0 if now - state['started'] >= 120 else state['failures'] + 1
                    state['next_start'] = now + min(120, 2 ** min(state['failures'], 7))
                    state['process'] = None
                if state['process'] is None and now >= state['next_start']:
                    spec = state['spec']
                    log_path = root / (name + '-supervised.log')
                    if log_path.exists() and log_path.stat().st_size > 20 * 1024 * 1024:
                        log_path.replace(root / (name + '-supervised.previous.log'))
                    with log_path.open('ab', buffering=0) as out:
                        state['process'] = subprocess.Popen(spec['argv'], cwd=spec['cwd'],
                            env={**os.environ, **cfg.get('environment', {}), **spec.get('environment', {})},
                            stdin=subprocess.DEVNULL, stdout=out, stderr=out, start_new_session=True)
                    state['started'] = now
                    if spec.get('metadata_path'):
                        metadata_path=Path(spec['metadata_path'])
                        if not metadata_path.is_absolute():raise ValueError('process metadata path must be absolute')
                        stamp=datetime.now(timezone.utc).isoformat()
                        atomic_json(metadata_path,{**spec.get('metadata',{}),'pid':state['process'].pid,'started_at':stamp,'startedAt':stamp,'supervised':True})
            atomic_json(root / 'supervisor-status.json', {
                'version': 1, 'pid': os.getpid(), 'checked_at': time.time(),
                'children': {name: {'pid': state['process'].pid if state['process'] else None,
                    'running': state['process'] is not None and state['process'].poll() is None,
                    'consecutive_failures': state['failures']} for name, state in children.items()}})
            time.sleep(1)
    finally:
        for state in children.values():
            proc = state['process']
            if proc is not None and proc.poll() is None:
                os.killpg(proc.pid, signal.SIGTERM)
        deadline = time.monotonic() + 20
        for state in children.values():
            proc = state['process']
            if proc is not None:
                try:
                    proc.wait(timeout=max(0.1, deadline - time.monotonic()))
                except subprocess.TimeoutExpired:
                    os.killpg(proc.pid, signal.SIGKILL)
                    proc.wait()
        atomic_json(root / 'supervisor-status.json', {'version': 1, 'pid': os.getpid(), 'stopped': True, 'checked_at': time.time()})


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config', required=True)
    run(parser.parse_args().config)
