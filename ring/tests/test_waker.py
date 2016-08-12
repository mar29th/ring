import unittest
from threading import Thread

from ring import Again
from ring.waker import Waker


class TestWaker(unittest.TestCase):

    def setUp(self):
        self._waker = Waker()

    def tearDown(self):
        self._waker.close()

    def test_wait(self):
        def thread():
            self._waker.wake()

        th = Thread(target=thread)
        th.daemon = True
        th.start()

        self._waker.wait(3)
        self._waker.deplete()

    def test_wait_timeout(self):
        try:
            self._waker.wait(1)
            self.fail('Should raise Again')
        except Again:
            pass
