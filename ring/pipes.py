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


import collections
import threading

from ring.connection_impl import Again, Done


class Pipe(object):

    # TODO: Change implementation to ring buffer

    def __init__(self, hwm=None):
        self._queue = collections.deque()
        self._lock = threading.RLock()
        self._high_watermark = hwm
        if self._high_watermark is not None:
            self._low_watermark = (self._high_watermark + 1) / 2
        else:
            self._low_watermark = None
        self._watermark = 0
        self._messages_read = 0
        self._readable = False

    def write_available(self):
        with self._lock:
            return not (self._high_watermark is not None and self._watermark > self._high_watermark)

    def write(self, data):
        with self._lock:
            if not isinstance(data, Done) and not self.write_available():
                raise Again

            was_readable = self._readable
            self._queue.append(data)
            self._watermark += 1
            self._readable = True
            return was_readable

    def front(self):
        with self._lock:
            try:
                return self._queue[0]
            except IndexError:
                raise Again

    def read_available(self):
        with self._lock:
            if len(self._queue) == 0:
                self._readable = False
                return False
            else:
                return True

    def read(self):
        with self._lock:
            if not self.read_available():
                raise Again

            popped = self._queue.popleft()
            self._watermark -= 1
            self._messages_read += 1
            if self._low_watermark is not None:
                # To prevent overflow
                self._messages_read %= self._low_watermark
                low_watermark_reached = self._messages_read % self._low_watermark == 0
            else:
                low_watermark_reached = False
            return popped, low_watermark_reached

    def clear(self):
        with self._lock:
            self._queue.clear()
            self._watermark = 0
