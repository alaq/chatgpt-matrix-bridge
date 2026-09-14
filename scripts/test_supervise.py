import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

class SupervisorTests(unittest.TestCase):
    def test_independent_restart_and_clean_shutdown(self):
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            config={name:{'argv':[sys.executable,'-c','import time;time.sleep(60)'],'cwd':d} for name in ('bridge','browser')}
            path=root/'services.json';path.write_text(json.dumps(config));path.chmod(0o600)
            proc=subprocess.Popen([sys.executable,str(Path(__file__).with_name('supervise.py')),'--config',str(path)],stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
            def state_until(predicate):
                deadline=time.monotonic()+15
                while time.monotonic()<deadline:
                    try:
                        data=json.loads((root/'supervisor-status.json').read_text())
                        if predicate(data):return data
                    except (FileNotFoundError,json.JSONDecodeError):pass
                    time.sleep(.1)
                self.fail('supervisor state timed out')
            try:
                start=state_until(lambda s:all(v['running'] for v in s['children'].values()))
                old=start['children']['bridge']['pid'];browser=start['children']['browser']['pid']
                os.kill(old,signal.SIGKILL)
                new=state_until(lambda s:s['children']['bridge']['running'] and s['children']['bridge']['pid']!=old)
                self.assertEqual(new['children']['browser']['pid'],browser)
                proc.terminate();self.assertEqual(proc.wait(timeout=10),0)
                self.assertTrue(json.loads((root/'supervisor-status.json').read_text())['stopped'])
                for pid in (browser,new['children']['bridge']['pid']):
                    with self.assertRaises(ProcessLookupError):os.kill(pid,0)
            finally:
                if proc.poll() is None:proc.terminate();proc.wait(timeout=25)
                proc.stderr.close()

if __name__=='__main__':unittest.main()
