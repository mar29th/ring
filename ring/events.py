import threading

from ring.connection_impl import Again
from ring.pipes import Pipe
from ring.utils import InconsistentStateError
from ring.waker import Waker


class Mail(object):

    def __init__(self, command, *args, **kwargs):
        self.command = command
        self.args = args
        self.kwargs = kwargs


class Mailbox(object):
    """As in libzmq"""

    def __init__(self):
        self._waker = Waker()
        self._pipe = Pipe()
        self._active = False
        self._lock = threading.RLock()

    def send(self, msg):
        with self._lock:
            if not self._pipe.write(msg):
                self._waker.wake()

    def recv(self, timeout=None):
        if self._active:
            try:
                return self._pipe.read()[0]
            except Again:
                self._active = False

        self._waker.wait(timeout)
        self._waker.deplete()

        self._active = True

        try:
            return self._pipe.read()[0]
        except Again:
            # Should not happen, otherwise it's a bug
            raise InconsistentStateError('Pipe is still empty after waiting for waker. BUG.')

    def close(self):
        with self._lock:
            self._waker.close()

    @property
    def waker_fd(self):
        return self._waker.waker_fd
