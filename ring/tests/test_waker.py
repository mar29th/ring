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
