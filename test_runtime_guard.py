"""Fault injection uses local fake children only, never production services."""
import json
import logging
import os
from pathlib import Path
import subprocess
import sys
import threading
import time

import pytest

from async_logging import BoundedLogHandler
from runtime_guard import RuntimeGuard, stalled_worker
from runtime_supervisor import read_snapshot, restart_reason, stop_child


class Worker:
    name = 'main-ebay-worker'
    def is_alive(self):
        return True


def snapshot(age=0, paused=False, alive=True):
    return {'updated': 1000, 'paused': paused, 'workers': {
        'main-ebay-worker': {'registered_age': 1000, 'alive': alive,
                             'progress_age': age, 'stage': 'fetch search page', 'timeout': 300}}}


def test_guard_detects_stalled_main_even_with_fresh_web_and_reporter():
    assert restart_reason(snapshot(age=301), 1001, 0).startswith('worker stalled:')
    assert restart_reason(snapshot(alive=False), 1001, 0).startswith('worker stopped:')


def test_proxy_outage_with_progress_is_not_a_process_failure():
    for minute in range(1, 61):
        state = snapshot(age=2)
        state['updated'] = minute * 60
        assert restart_reason(state, minute * 60 + 1, 0) is None


def test_intentional_pause_and_normal_standby_are_healthy():
    assert stalled_worker(snapshot(age=86400, paused=True)) is None
    assert stalled_worker(snapshot(age=86400, paused=True, alive=False))
    assert restart_reason({'updated': 1000, 'workers': {}, 'mode': 'starting'}, 1001, 0) is None
    state = snapshot(age=2)
    state['workers']['leader-supervisor'] = state['workers'].pop('main-ebay-worker')
    assert restart_reason(state, 1001, 0) is None


def test_missing_and_stopped_heartbeat_have_finite_grace():
    assert restart_reason(None, 179, 0) is None
    assert restart_reason(None, 181, 0) == 'heartbeat missing'
    assert restart_reason(snapshot(), 1121, 0) == 'heartbeat stopped'


def test_unrelated_thread_cannot_mask_main_stall(monkeypatch):
    clock = [1.0]
    guard = RuntimeGuard(clock=lambda: clock[0])
    worker = Worker()
    guard.register(worker, timeout=300)
    clock[0] = 400
    guard.progress(worker.name, 'background auction fetch')
    assert stalled_worker(guard.snapshot())
    monkeypatch.setattr(threading, 'current_thread', lambda: worker)
    guard.progress(worker.name, 'next failed proxy completed')
    assert stalled_worker(guard.snapshot()) is None


def test_resume_gets_a_new_progress_deadline(monkeypatch):
    clock = [0.0]
    guard = RuntimeGuard(clock=lambda: clock[0])
    guard.register(Worker(), timeout=300)
    guard.set_paused(True)
    clock[0] = 3600
    assert stalled_worker(guard.snapshot()) is None
    guard.set_paused(False)
    assert stalled_worker(guard.snapshot()) is None


def test_previous_process_snapshot_is_rejected(tmp_path):
    path = tmp_path / 'heartbeat.json'
    path.write_text(json.dumps({'pid': 123, 'nonce': 'previous', 'updated': 1}), encoding='utf-8')
    assert read_snapshot(path, 456, 'current') is None
    assert read_snapshot(path, 123, 'current') is None
    assert read_snapshot(path, 123, 'previous')['pid'] == 123
    path.write_text('x' * 17000, encoding='utf-8')
    assert read_snapshot(path, 123, 'previous') is None


def test_blocked_log_sink_cannot_block_workers_and_queue_is_bounded():
    entered, release = threading.Event(), threading.Event()
    class BlockedStream:
        def write(self, value):
            entered.set()
            release.wait(5)
        def flush(self):
            pass
    handler = BoundedLogHandler(BlockedStream(), capacity=2, max_chars=80)
    logger = logging.Logger('isolated-test', level=logging.INFO)
    logger.addHandler(handler)
    try:
        logger.info('first')
        assert entered.wait(1)
        complete = threading.Event()
        def work():
            for _ in range(100):
                logger.warning('A' * 1000)
            complete.set()
        thread = threading.Thread(target=work, daemon=True)
        thread.start()
        assert complete.wait(1), 'log collector blocked a producer'
        thread.join(timeout=1)
        assert handler.pending.qsize() == 2 and handler.dropped >= 98
        assert all(isinstance(row, str) and len(row) <= 92 for row in handler.pending.queue)
    finally:
        release.set()


def wait_for_file(path, timeout=6):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if path.exists() and path.stat().st_size:
            return
        time.sleep(.025)
    raise AssertionError('fake child did not start')


@pytest.mark.parametrize('failure,expected_paused,expected_reason', [
    ('heartbeat', '1', 'heartbeat stopped'),
    ('main', '0', 'worker stalled: main-ebay-worker (starting)'),
])
def test_real_supervisor_reaps_frozen_child_and_preserves_pause(tmp_path, failure, expected_paused, expected_reason):
    source = str(Path(__file__).resolve().parent)
    # Windows venv python.exe is a redirector with a different PID from its
    # interpreter. Fake stdlib-only children use the real interpreter, as on Linux.
    executable = getattr(sys, '_base_executable', sys.executable)
    starts = tmp_path / 'starts.jsonl'
    fake = tmp_path / 'fake.py'
    fake.write_text('''
import json, os, sys, time, threading
sys.path.insert(0, %r)
from runtime_guard import RuntimeGuard
with open(%r, 'a') as fh:
    fh.write(json.dumps({'pid':os.getpid(), 'paused':os.getenv('SCANNER_RECOVERY_PAUSED'),
                         'reason':os.getenv('SCANNER_RECOVERY_REASON')}) + '\\n')
guard = RuntimeGuard()
if %r == 'heartbeat':
    guard.set_paused(True)
else:
    thread = threading.current_thread()
    thread.name = 'main-ebay-worker'
    guard.register(thread, timeout=300)
    guard.workers[thread.name][1] -= 400
    guard.workers[thread.name][2] -= 400
guard.start()
time.sleep(.15)
# Stop publishing entirely: independent parent must replace this process.
while True: time.sleep(1)
''' % (source, str(starts), failure), encoding='utf-8')
    driver = tmp_path / 'driver.py'
    driver.write_text('''
import sys
sys.path.insert(0, %r)
from runtime_supervisor import run
run([%r, '-u', %r], heartbeat_timeout=.4, startup_grace=1,
    poll=.05, stop_grace=.2, retry_delay=.05, repeated_delay=.1)
''' % (source, executable, str(fake)), encoding='utf-8')
    parent = subprocess.Popen([executable, str(driver)], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    child_pids = []
    try:
        wait_for_file(starts)
        deadline = time.monotonic() + 6
        rows = []
        while time.monotonic() < deadline:
            rows = [json.loads(line) for line in starts.read_text().splitlines()]
            if len(rows) >= 2:
                break
            time.sleep(.025)
        assert len(rows) >= 2
        child_pids = [row['pid'] for row in rows]
        assert rows[1]['paused'] == expected_paused
        assert rows[1]['reason'] == expected_reason
        # Windows can query process exit without sending a signal.
        if os.name == 'nt':
            import ctypes
            handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, rows[0]['pid'])
            if handle:
                code = ctypes.c_ulong()
                ctypes.windll.kernel32.GetExitCodeProcess(handle, ctypes.byref(code))
                ctypes.windll.kernel32.CloseHandle(handle)
                assert code.value != 259, 'old scanner still alive when replacement launched'
        else:
            with pytest.raises(ProcessLookupError):
                os.kill(rows[0]['pid'], 0)
    finally:
        # Windows terminate does not deliver SIGTERM; reap this test's child
        # explicitly after terminating the local fake supervisor.
        stop_child(parent, grace=.3)
        if starts.exists():
            child_pids += [json.loads(line)['pid'] for line in starts.read_text().splitlines()]
        for pid in set(child_pids):
            try:
                os.kill(pid, signal_for_kill())
            except (ProcessLookupError, OSError):
                pass


def signal_for_kill():
    import signal
    return signal.SIGTERM if os.name == 'nt' else signal.SIGKILL


@pytest.mark.skipif(os.name == 'nt', reason='POSIX signal semantics used on Render')
def test_stop_child_kills_a_child_ignoring_sigterm():
    child = subprocess.Popen([sys.executable, '-u', '-c',
                              'import signal,time; signal.signal(signal.SIGTERM, signal.SIG_IGN); print("ready",flush=True); time.sleep(60)'],
                             stdout=subprocess.PIPE, text=True)
    try:
        assert child.stdout.readline().strip() == 'ready'
        stop_child(child, grace=.1)
        assert child.poll() is not None
    finally:
        if child.poll() is None:
            child.kill()
            child.wait()
