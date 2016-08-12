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


import socket
import threading

import errno

import time

import select

from ring.connection_impl import Again
from ring.constants import ERR_WOULD_BLOCK
from ring.poller import READ, PollImpl, SelectImpl
from ring.utils import errno_from_exception


class Waker(object):

    def __init__(self):
        self._r, self._w = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        assert self._r.fileno() != -1
        assert self._w.fileno() != -1

        self._r.setblocking(0)
        self._w.setblocking(0)
        self._lock = threading.RLock()
        self._closed = False

        if hasattr(select, 'poll'):
            self._poller = PollImpl()
        elif hasattr(select, 'select'):
            self._poller = SelectImpl()
        else:
            raise OSError('System not supported')

    @property
    def waker_fd(self):
        return self._r.fileno()

    def wake(self):
        with self._lock:
            if self._closed:
                return

            while 1:
                try:
                    self._w.send(b'a')
                    break
                except (OSError, IOError, socket.error) as e:
                    if errno_from_exception(e) == errno.EINTR:
                        continue
                    else:
                        raise

    def deplete(self):
        with self._lock:
            if self._closed:
                return

            try:
                self._r.recv(1)
            except (OSError, IOError, socket.error) as e:
                if errno_from_exception(e) in ERR_WOULD_BLOCK or \
                        errno_from_exception(e) == errno.EINTR:
                    raise Again
                else:
                    raise

    def wait(self, timeout):
        start_time = time.time()
        if timeout is None:
            timeout_remaining = None
        else:
            timeout_remaining = timeout

        self._poller.register(self._r.fileno(), READ)

        while 1:
            try:
                events = self._poller.poll(timeout_remaining)
            except Exception as e:
                if errno_from_exception(e) == errno.EINTR:
                    if timeout is not None:
                        timeout_remaining = timeout - (time.time() - start_time)
                        if timeout_remaining < 0:
                            self._poller.unregister(self._r.fileno())
                            raise Again
                    continue
                else:
                    self._poller.unregister(self._r.fileno())
                    raise
            else:
                self._poller.unregister(self._r.fileno())
                if events:
                    return
                else:
                    raise Again

    def close(self):
        with self._lock:
            self._w.close()
            self._r.close()
            self._closed = True
