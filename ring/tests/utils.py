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


import functools
import os
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


def skip_on_ci(fun):
    return unittest.skipIf(
        'CI' in os.environ and os.environ['CI'] == 'true', 'Test skipped on CI')(fun)
