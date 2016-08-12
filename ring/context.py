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


from threading import Thread, Event

from ring.connection import Connection
from ring.io_loop import IOLoop
from ring.utils import get_logger

_logger = get_logger(__name__)


class Context(object):

    def __init__(self):
        self._io_loop = None
        self._io_loop_thread = None
        self._reaper = None
        self._reaper_thread = None
        self._started = False
        self._io_loop_initialized_event = Event()
        self._reaper_initialized_event = Event()
        self._initialize()

    def _initialize(self):

        def start_io_loop():
            self._io_loop = IOLoop.get_thread_instance()
            self._io_loop_initialized_event.set()
            self._io_loop.start()

        def start_reaper():
            self._reaper = IOLoop.get_thread_instance()
            self._reaper_initialized_event.set()
            self._reaper.start()

        self._io_loop_thread = Thread(target=start_io_loop, name='Context IOLoop thread')
        self._io_loop_thread.daemon = True
        self._io_loop_thread.start()

        self._reaper_thread = Thread(target=start_reaper, name='Context Reaper thread')
        self._reaper_thread.daemon = True
        self._reaper_thread.start()

        # This event is to ensure IOLoop is not None
        self._io_loop_initialized_event.wait()
        self._reaper_initialized_event.wait()
        self._started = True

    def stop(self):
        self._io_loop.next_tick(self._io_loop.stop)
        self._io_loop_thread.join(5)
        if self._io_loop_thread.isAlive():
            _logger.warning('Context IOLoop thread failed to stop within 5 secs')

        self._reaper.next_tick(self._reaper.stop)
        self._reaper_thread.join(5)
        if self._reaper_thread.isAlive():
            _logger.warning('Context Reaper thread failed to stop within 5 secs')

    def connection(self, type):
        assert self._started
        return Connection(type, self)

    def run_in_background(self, cb, *args, **kwargs):
        assert self._started
        self._io_loop.next_tick(cb, *args, **kwargs)

    @property
    def io_loop(self):
        assert self._started
        return self._io_loop

    @property
    def reaper(self):
        assert self._started
        return self._reaper
