# Copyright 2016 Douban Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


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
