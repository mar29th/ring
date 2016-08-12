import functools
import unittest

from ring.co import make_coroutine, run_sync
from ring.io_loop import IOLoop


def coroutine_test(func):

    @functools.wraps(func)
    def wrapped(*args, **kwargs):
        fun = make_coroutine(func)
        future = fun(*args, **kwargs)
        self = args[0]
        run_sync(future, self._io_loop)

    return wrapped


class AsyncTestCase(unittest.TestCase):

    def setUp(self):
        self._io_loop = IOLoop.get_thread_instance()

    def tearDown(self):
        self._io_loop.stop()


def blocking_send(so, data):
    sent = 0
    while sent != len(data):
        sent += so.send(data)


def blocking_recv(so, length):
    buf = []
    while length > 0:
        received = so.recv(length)
        length -= len(received)
        buf.append(received)
    return ''.join(buf)
