# Copyright 2016 Facebook Inc.
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
import errno
import traceback
import heapq
import time

import functools

from ring.connection_impl import Again
from ring.poller import get_poller, READ
from ring.utils import errno_from_exception, get_logger
from ring.waker import Waker

logger = get_logger(__name__)

_POLL_TIMEOUT = 1000


def poller_thread_safe(fun):
    @functools.wraps(fun)
    def _inner(self, *args, **kwargs):
        with self._poller_lock:
            if self._waker:
                self._waker.wake()
            fun(self, *args, **kwargs)
    return _inner


_COUNT_LOCK = threading.RLock()


def _count():
    count = 0
    while True:
        yield count
        with _COUNT_LOCK:
            count += 1


_COUNTER = _count()


class Timeout(object):

    def __init__(self, time, cb):
        self.time = (time, next(_COUNTER))
        self.callback = cb

    def __lt__(self, rhs):
        return self.time < rhs.time

    def __le__(self, rhs):
        return self.time <= rhs.time


class IOLoop(object):

    _creation_lock = threading.RLock()

    _instances = {}

    @staticmethod
    def get_thread_instance(*args, **kwargs):
        with IOLoop._creation_lock:
            identifier = threading.currentThread().ident
            if identifier not in IOLoop._instances:
                IOLoop._instances[identifier] = IOLoop(*args, **kwargs)
        return IOLoop._instances[identifier]

    def set_as_thread_instance(self):
        IOLoop._instances[threading.currentThread().ident] = self

    def __init__(self, poller=None, no_waker=False):
        self._poller = poller if poller else get_poller()
        self._started = False
        self._stopping = False
        self._pausing = False

        self._callbacks_lock = threading.RLock()
        self._callbacks = []
        self._poller_callbacks = {}
        self._poller_lock = threading.RLock()
        self._timeouts = []

        self._current_thread_id = None  # Thread ID should be obtained in start.

        self._no_waker = no_waker
        self._waker = None

    def __del__(self):
        self.stop()

    def _deplete_waker(self):
        while 1:
            try:
                self._waker.deplete()
            except Again:
                break

    def _initialize_waker(self):
        if not self._no_waker:
            self._waker = Waker()
            self.register(self._waker.waker_fd, READ, lambda fd, event: self._deplete_waker())

    def _deconstruct_waker(self):
        if not self._no_waker:
            self._waker.wake()
            self.unregister(self._waker.waker_fd)
            self._waker.close()

    def start(self):
        if self._started and not self._pausing:
            return
        if not self._pausing:
            self._initialize_waker()
        self._stopping = False
        self._pausing = False
        self._started = True
        self._current_thread_id = threading.currentThread().ident
        self._loop()

    def stop(self):
        if not self._started or self._stopping:
            return
        with self._callbacks_lock:
            self._stopping = True
        self._callbacks = []
        self._deconstruct_waker()
        self._poller_callbacks = {}
        self._timeouts = []
        self._started = False

    def pause(self):
        if not self._started or self._pausing:
            return
        self._pausing = True
        if not self._no_waker:
            self._waker.wake()

    def _loop(self):
        while 1:
            # Run next tick callbacks first
            with self._callbacks_lock:
                cbs = self._callbacks
                self._callbacks = []

            pending_timeouts = []
            current_time = time.time()
            while len(self._timeouts) != 0:
                if self._timeouts[0].callback is None:
                    heapq.heappop(self._timeouts)
                elif current_time >= self._timeouts[0].time[0]:
                    pending_timeouts.append(heapq.heappop(self._timeouts))
                else:
                    break

            for cb, args, kwargs in cbs:
                try:
                    cb(*args, **kwargs)
                except:
                    # Catch all exceptions.
                    msg = traceback.format_exc()
                    logger.error(msg)
                    traceback.print_exc()
                    pass

            for timeout in pending_timeouts:
                if timeout.callback is not None:
                    try:
                        callback = timeout.callback
                        timeout.callback = None
                        callback()
                    except Exception as e:
                        logger.error(traceback.format_exc())
                        pass

            if self._stopping or self._pausing:
                self._stopping = self._pausing = False
                return

            with self._callbacks_lock:
                num_cbs = len(self._callbacks)
            if num_cbs != 0:
                poll_timeout = 0
            elif len(self._timeouts) != 0:
                poll_timeout = max(0, min(_POLL_TIMEOUT, time.time() - self._timeouts[0].time[0]))
            else:
                poll_timeout = _POLL_TIMEOUT

            # Get a copy of poller callbacks.
            with self._poller_lock:
                poller_callbacks = self._poller_callbacks

            try:
                events = self._poller.poll(poll_timeout)
            except Exception as e:
                if errno_from_exception(e) == errno.EINTR:
                    logger.debug('Interrupted by signals')
                    continue
                else:
                    raise

            for fd, event in events:
                if fd in poller_callbacks:
                    try:
                        poller_callbacks[fd](fd, event)
                    except:
                        logger.error(traceback.format_exc())
                        traceback.print_exc()
                        pass

    @poller_thread_safe
    def register(self, fd, eventmask, cb):
        if fd not in self._poller_callbacks:
            self._poller.register(fd, eventmask)
        else:
            self._poller.modify(fd, eventmask)
        self._poller_callbacks[fd] = cb

    @poller_thread_safe
    def unregister(self, fd):
        if fd in self._poller_callbacks:
            self._poller.unregister(fd)
            del self._poller_callbacks[fd]

    @poller_thread_safe
    def modify(self, fd, eventmask):
        if fd in self._poller_callbacks:
            self._poller.modify(fd, eventmask)

    def next_tick(self, cb, *args, **kwargs):
        if threading.currentThread().ident != self._current_thread_id:
            with self._callbacks_lock:
                if self._stopping:
                    return
                self._callbacks.append((cb, args, kwargs))
                if self._waker:
                    # It is possible that the waker is not set up yet,
                    # hence the extra predicate
                    self._waker.wake()
        else:
            if self._stopping:
                return
            self._callbacks.append((cb, args, kwargs))

    def add_future(self, future, cb):
        future.add_done_callback(lambda f: self.next_tick(cb, f))

    def set_timeout(self, secs, cb):
        timeout_obj = Timeout(time.time() + secs, cb)
        heapq.heappush(self._timeouts, timeout_obj)
        return timeout_obj

    def clear_timeout(self, timeout):
        timeout.callback = None
