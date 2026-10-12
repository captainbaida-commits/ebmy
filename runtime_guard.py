"""Small, credential-free progress monitor; never calls application locks or APIs."""
import json
import os
import threading
import time


MAIN_STALL_SECONDS = 300


def stalled_worker(snapshot):
    """A failed proxy is normal work. Only missing control-flow progress is unhealthy."""
    for name, state in snapshot.get('workers', {}).items():
        if state['registered_age'] < 15:
            continue
        if not state['alive']:
            return 'worker stopped: ' + name
        limit = state['timeout']
        if name == 'main-ebay-worker' and snapshot.get('paused'):
            continue
        if limit and state['progress_age'] > limit:
            return 'worker stalled: %s (%s)' % (name, state['stage'])
    return None


class RuntimeGuard:
    def __init__(self, clock=time.monotonic):
        self.clock = clock
        self.lock = threading.Lock()
        self.workers = {}
        self.paused = os.getenv('SCANNER_RECOVERY_PAUSED') == '1'
        self.mode = 'starting'
        self.started = False
        self.wakeup = threading.Event()

    def register(self, thread, timeout=0):
        now = self.clock()
        with self.lock:
            self.workers[thread.name] = [thread, now, now, 'starting', timeout]

    def progress(self, name, stage):
        # Shared fetching functions can also run in auction workers. Their activity
        # must never conceal a stuck main worker.
        with self.lock:
            state = self.workers.get(name)
            if state and state[0] is threading.current_thread():
                state[2], state[3] = self.clock(), stage[:64]

    def set_paused(self, paused):
        with self.lock:
            self.paused = bool(paused)
            if not paused and 'main-ebay-worker' in self.workers:
                self.workers['main-ebay-worker'][2] = self.clock()
        self.wakeup.set()

    def snapshot(self):
        now = self.clock()
        with self.lock:
            rows = [(name, list(state)) for name, state in self.workers.items()]
            paused = self.paused
        return {
            'pid': os.getpid(), 'nonce': os.getenv('SCANNER_GUARD_NONCE', ''),
            'updated': now, 'paused': paused, 'mode': self.mode,
            'workers': {
                name: {'alive': state[0].is_alive(),
                       'registered_age': max(0, now - state[1]),
                       'progress_age': max(0, now - state[2]),
                       'stage': state[3], 'timeout': state[4]}
                for name, state in rows
            },
        }

    def start(self):
        if self.started:
            return
        self.started = True
        threading.Thread(target=self._publish, name='runtime-heartbeat', daemon=True).start()

    def _publish(self):
        path = os.getenv('SCANNER_GUARD_FILE')
        if not path:
            return
        while True:
            self.wakeup.clear()
            # No logging here: the watchdog must survive a blocked log sink.
            try:
                data = self.snapshot()
                with open(path + '.tmp', 'w', encoding='utf-8') as handle:
                    json.dump(data, handle, separators=(',', ':'))
                os.replace(path + '.tmp', path)
            except OSError:
                pass  # The independent parent detects an absent/stale heartbeat.
            self.wakeup.wait(5)


_parent_rss_cache = (0.0, 0.0)


def supervisor_rss_mb():
    """Include the small supervisor in the existing 512 MB memory safeguards."""
    global _parent_rss_cache
    now = time.monotonic()
    if now - _parent_rss_cache[0] < 30:
        return _parent_rss_cache[1]
    rss = 0.0
    pid = os.getenv('SCANNER_SUPERVISOR_PID', '')
    if pid.isdigit():
        try:
            with open('/proc/' + pid + '/status', encoding='utf-8') as handle:
                for line in handle:
                    if line.startswith('VmRSS:'):
                        rss = float(line.split()[1]) / 1024.0
                        break
        except (OSError, ValueError):
            pass
    _parent_rss_cache = (now, rss)
    return rss


runtime_guard = RuntimeGuard()
