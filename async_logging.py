"""Bounded log output: a slow hosting log collector cannot stop scanner workers."""
import logging
import queue
import sys
import threading


class BoundedLogHandler(logging.Handler):
    def __init__(self, stream=None, capacity=256, max_chars=4096):
        super().__init__()
        self.stream = stream if stream is not None else sys.stderr
        self.pending = queue.Queue(maxsize=capacity)
        self.max_chars = max_chars
        self.dropped = 0
        self.writer = threading.Thread(target=self._write, name='log-writer', daemon=True)
        self.writer.start()

    def emit(self, record):
        try:
            # Queue strings only, never HTML/Session objects, exception frames or args.
            line = self.format(record)
            if len(line) > self.max_chars:
                line = line[:self.max_chars] + ' [truncated]'
            self.pending.put_nowait(line)
        except Exception:
            # logging.handleError writes to the same possibly blocked stderr.
            self.dropped += 1

    def _write(self):
        while True:
            line = self.pending.get()
            try:
                self.stream.write(line + '\n')
                self.stream.flush()
            except Exception:
                self.dropped += 1
            finally:
                self.pending.task_done()


def configure_logging():
    root = logging.getLogger()
    handler = BoundedLogHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    # Production installs once from __main__; importing app in offline tests is inert.
    for old in root.handlers[:]:
        root.removeHandler(old)
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    return handler
