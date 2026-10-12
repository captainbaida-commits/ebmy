"""Independent process watchdog. Start with: python -u runtime_supervisor.py.

The parent uses no network, database, application locks or synchronous logging.
It always reaps the old child before starting another; the DB leader lock remains
the authority across separate Render instances.
"""
import json
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import uuid

from runtime_guard import stalled_worker


def read_snapshot(path, pid, nonce):
    try:
        with open(path, encoding='utf-8') as handle:
            text = handle.read(16385)
        if len(text) > 16384:
            return None
        data = json.loads(text)
        if data.get('pid') == pid and data.get('nonce') == nonce:
            return data
    except (OSError, ValueError, TypeError):
        pass
    return None


def restart_reason(snapshot, now, born, heartbeat_timeout=120, startup_grace=180):
    if snapshot is None:
        return 'heartbeat missing' if now - born > startup_grace else None
    try:
        if now - float(snapshot['updated']) > heartbeat_timeout:
            return 'heartbeat stopped'
        return stalled_worker(snapshot)
    except (KeyError, TypeError, ValueError):
        return 'heartbeat invalid' if now - born > startup_grace else None


def stop_child(child, grace=8):
    if child.poll() is None:
        child.terminate()
        try:
            child.wait(timeout=grace)
        except subprocess.TimeoutExpired:
            child.kill()
    child.wait()


def run(command=None, *, heartbeat_timeout=120, startup_grace=180, poll=5,
        stop_grace=8, retry_delay=3, repeated_delay=60):
    command = command or [sys.executable, '-u', os.path.join(os.path.dirname(__file__), 'app.py')]
    stopping = threading.Event()
    def on_signal(signum, frame):
        stopping.set()
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, on_signal)
    paused = os.getenv('SCANNER_RECOVERY_PAUSED') == '1'
    restarts = []
    previous_reason = ''
    child = None
    with tempfile.TemporaryDirectory(prefix='ebay-runtime-') as folder:
        path = os.path.join(folder, 'heartbeat.json')
        try:
            while not stopping.is_set():
                nonce = uuid.uuid4().hex
                env = dict(os.environ, SCANNER_GUARD_FILE=path,
                           SCANNER_GUARD_NONCE=nonce,
                           SCANNER_SUPERVISOR_PID=str(os.getpid()),
                           SCANNER_RECOVERY_PAUSED='1' if paused else '0',
                           SCANNER_RECOVERY_REASON=previous_reason[:160])
                child = subprocess.Popen(command, env=env)
                born = time.monotonic()
                strikes = 0
                reason = None
                while not stopping.wait(poll):
                    snapshot = read_snapshot(path, child.pid, nonce)
                    if snapshot is not None:
                        paused = bool(snapshot.get('paused', paused))
                    if child.poll() is not None:
                        reason = 'child exited: %s' % child.returncode
                        break
                    reason = restart_reason(snapshot, time.monotonic(), born,
                                            heartbeat_timeout, startup_grace)
                    strikes = strikes + 1 if reason else 0
                    if strikes >= 2:
                        break
                stop_child(child, stop_grace)
                child = None
                if stopping.is_set():
                    break
                previous_reason = reason or 'child exited'
                now = time.monotonic()
                restarts = [stamp for stamp in restarts if now - stamp < 600]
                restarts.append(now)
                # Bound a repeated startup failure, without Render's long crash backoff.
                stopping.wait(repeated_delay if len(restarts) >= 3 else retry_delay)
        finally:
            if child is not None:
                stop_child(child, stop_grace)
    return 0


if __name__ == '__main__':
    sys.exit(run())
