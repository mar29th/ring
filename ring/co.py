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
from types import GeneratorType

import sys

from ring.io_loop import IOLoop
from ring.utils import get_logger

_logger = get_logger(__name__)


class Future(object):

    def __init__(self):
        self._done = False
        self._done_callbacks = []
        self._result = None
        self._exc_info = None

    @property
    def done(self):
        return self._done

    def add_done_callback(self, cb):
        if self._done:
            cb(self)
        else:
            self._done_callbacks.append(cb)

    def result(self):
        if self._exc_info:
            raise self._exc_info[0], self._exc_info[1], self._exc_info[2]
        if not self.done:
            raise RuntimeError('Result not available')
        return self._result

    def set_result(self, result):
        self._result = result
        self._finish()

    def set_exc_info(self, exc):
        self._exc_info = exc
        self._finish()

    def set_exception(self, exc):
        self.set_exc_info((exc.__class__, exc, getattr(exc, '__traceback__', None)))

    def _finish(self):
        self._done = True
        for cb in self._done_callbacks:
            try:
                cb(self)
            except Exception as e:
                _logger.warning('Exception raised in Future done callback: %s', e)


class Return(Exception):

    def __init__(self, val=None):
        super(Return, self).__init__('Return class')
        self.value = val


class moment(object):
    pass


class InvalidCoroutineError(Exception):

    def __init__(self):
        super(InvalidCoroutineError, self).__init__('Coroutine should be wrapped as a future')


class Runner(object):

    def __init__(self, gen, future, io_loop):
        self._future = future
        self._local_future = Future()
        self._local_future.set_result(None)
        self._gen = gen
        self._io_loop = io_loop

        self.run()

    def run(self):
        while 1:
            try:
                try:
                    send_val = self._local_future.result()
                except:
                    result = self._gen.throw(*sys.exc_info())
                else:
                    result = self._gen.send(send_val)
            except Return as e:
                # This means the coroutine has returned
                self._future.set_result(e.value)
                return
            except StopIteration:
                # Resolve complete
                self._future.set_result(None)
                return
            except:
                self._future.set_exc_info(sys.exc_info())
                return
            else:
                if isinstance(result, Future):
                    self._local_future = result
                    if not result.done:
                        self._io_loop.add_future(self._local_future, lambda f: self.run())
                        return
                elif isinstance(result, moment):
                    self._local_future = Future()
                    self._local_future.set_result(None)
                    self._io_loop.next_tick(self.run)
                    return
                else:
                    raise InvalidCoroutineError


def with_io_loop(coroutine, loop):
    return functools.partial(coroutine, _gen_io_loop=loop)


def run_sync(future, loop):
    if future.done:
        return future.result()
    else:
        future.add_done_callback(lambda f: loop.stop())
        loop.start()
        return future.result()


def make_coroutine(fun):

    @functools.wraps(fun)
    def wrapped(*args, **kwargs):
        future = Future()
        io_loop = kwargs.get('_gen_io_loop', IOLoop.get_thread_instance())
        try:
            gen = fun(*args, **kwargs)
        except Return as e:
            future.set_result(e.value)
        except:
            future.set_exc_info(sys.exc_info())
        else:
            if isinstance(gen, GeneratorType):
                Runner(gen, future, io_loop)
            else:
                # fun is actually not a generator (has no yield statement), so gen is the result
                future.set_result(gen)
        return future

    return wrapped


def coroutine(fun):
    return make_coroutine(fun)


__all__ = ['Future', 'coroutine', 'make_coroutine', 'with_io_loop', 'run_sync', 'Return']
