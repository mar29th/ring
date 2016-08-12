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


import collections
import socket
import functools
import os
import errno

from ring.co import Future
from ring.poller import READ, WRITE, ERROR
from ring.utils import InconsistentStateError, SocketError
from ring.io_loop import IOLoop
from ring.constants import (
    ERR_WOULD_BLOCK, ERR_CONNRESET, ERR_INPROGRESS
)
from ring.utils import errno_from_exception, get_logger
from ring.reader import BufferReader


logger = get_logger(__name__)

_MAX_BLOCK_SIZE = 1024 * 128


class StreamClosedError(SocketError):

    def __init__(self, msg=None, wrap=None):
        if msg is None:
            msg = 'Stream closed'
        super(StreamClosedError, self).__init__(msg, wrap)


class SocketStream(object):

    def __init__(self, socket, on_close=None, io_loop=None):
        self.socket = socket
        self.socket.setblocking(0)

        self.eventmask = 0

        self.callbacks = 0

        self.read_buffer = collections.deque()
        self.write_buffer = collections.deque()
        self.read_buffer_reader = BufferReader(self.read_buffer)
        self.read_length = 0
        self.read_delimiter = None

        self.io_loop = io_loop or IOLoop.get_thread_instance()

        self.read_callback = None
        self.read_future = None

        self.write_callback = None
        self.write_future = None

        self.connect_callback = None
        self.connect_future = None

        self.close_callback = on_close

        self.error = None
        self.stopping = False
        self.stopped = False
        self.connecting = False

    def _close(self):
        if self.stopping and not self.stopped and self.callbacks == 0:
            if self.error and errno_from_exception(self.error) not in ERR_CONNRESET:
                logger.debug('Closing stream with error %s', self.error)

            futures = []
            if self.connect_future:
                futures.append(self.connect_future)
                self.connect_future = None
            if self.read_future:
                futures.append(self.read_future)
                self.read_future = None
            if self.write_future:
                futures.append(self.write_future)
                self.write_future = None

            # Then let's notify
            for future in futures:
                future.set_exception(StreamClosedError(wrap=self.error))
            if self.close_callback:
                self.io_loop.next_tick(self.close_callback, self.error)
            self.read_callback = self.write_callback = None
            self.read_buffer = self.write_buffer = None

            if not self.close_callback and len(futures) == 0:
                # This close is self incurred, i.e. on_read has detected some error while
                # there's no callback/future to handle the error.
                return
            self.stopped = True

    def close(self):
        """Generally, do not close from outside, especially under exceptional cases.
            Close inherently induces a race if there're still futures pending.
            If you're very sure receiving errors after close wouldn't break your code, then go ahead
        """

        if self.stopping:
            return

        self.stopping = True

        try:
            self.io_loop.unregister(self.socket.fileno())
        except Exception:
            # If the socket induces a close, it's already stale and will raise EBADF during
            # unregister.
            pass

        if self.socket:
            self.socket.close()
            self.socket = None
        self._close()

    @property
    def closed(self):
        return self.stopping

    def _on_connect(self):
        error = self.socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        if error != 0:
            raise socket.error(error, os.strerror(error))
        if self.connect_callback:
            cb = self.connect_callback
            self.connect_callback = None
            self.io_loop.next_tick(cb)
        else:
            future = self.connect_future
            self.connect_future = None
            future.set_result(None)
        self.connecting = False

    def _on_read(self):
        next_read_length = self.read_buffer_reader.buffer_size
        while True:
            try:
                received = self.socket.recv(_MAX_BLOCK_SIZE)
                if len(received) == 0:
                    # EOF
                    raise socket.error(errno.ECONNRESET, os.strerror(errno.ECONNRESET))
                self.read_buffer_reader.add(received)

                if self.read_length != 0:
                    self._read_once()
                elif self.read_delimiter:
                    if next_read_length <= self.read_buffer_reader.buffer_size:
                        self._read_once()
                        next_read_length = 2 * self.read_buffer_reader.buffer_size
            except socket.error as e:
                if errno_from_exception(e) in ERR_WOULD_BLOCK:
                    break
                else:
                    if errno_from_exception(e) not in ERR_CONNRESET:
                        logger.debug(
                            'READ: Socket from %s raises %s',
                            self.socket.getsockname(),
                            e)
                    raise

    def _on_write(self):
        while True:
            if len(self.write_buffer) == 0:
                break
            try:
                bytes_written = self.socket.send(self.write_buffer[0])
                self.write_buffer[0] = self.write_buffer[0][bytes_written:]
                if len(self.write_buffer[0]) == 0:
                    self.write_buffer.popleft()
            except socket.error as e:
                if errno_from_exception(e) in ERR_WOULD_BLOCK:
                    return
                else:
                    if errno_from_exception(e) not in ERR_CONNRESET:
                        logger.debug(
                            'WRITE: Socket from %s raises %s',
                            self.socket.getsockname(),
                            e)
                    raise

        if len(self.write_buffer) == 0:
            if self.write_callback:
                cb = self.write_callback
                self.write_callback = None
                self._run_callback(cb)
            elif self.write_future:
                future = self.write_future
                self.write_future = None
                future.set_result(None)

    def _on_error(self):
        errno = self.socket.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
        self.error = socket.error(errno, os.strerror(errno))
        self.io_loop.next_tick(self.close)

    def _handle_io_event(self, fd, event):
        try:
            if self.connecting:
                self._on_connect()
            if event & READ:
                self._on_read()
            if event & WRITE:
                self._on_write()
            if event & ERROR:
                self._on_error()
                return
            eventmask = READ | ERROR
            if (self.write_callback
                    or self.write_future
                    or self.connect_callback
                    or self.connect_future):
                eventmask |= WRITE
            if eventmask != self.eventmask:
                self.io_loop.modify(self.socket.fileno(), eventmask)
                self.eventmask = eventmask
        except Exception as e:
            self.error = e
            self.close()
            return

    def _add_fd_eventmask(self, event):
        if self.stopped:
            return

        prevmask = self.eventmask
        newmask = self.eventmask | event | ERROR
        if newmask != prevmask:
            self.eventmask = newmask
            if prevmask == 0:
                self.io_loop.register(self.socket.fileno(), self.eventmask, self._handle_io_event)
            else:
                self.io_loop.modify(self.socket.fileno(), self.eventmask)

    def _read_once(self):
        if not self.read_callback and not self.read_future:
            return

        if self.read_length != 0:
            popped = self.read_buffer_reader.read_until_length(self.read_length)
        elif self.read_delimiter:
            popped = self.read_buffer_reader.read_until_delimiter(self.read_delimiter)
        else:
            raise RuntimeError("Shouldn't reach here")

        if popped:
            if self.read_callback:
                cb = self.read_callback
                self.read_callback = None
                self._run_callback(functools.partial(cb, popped))
            else:
                future = self.read_future
                self.read_future = None
                future.set_result(popped)
            self.read_length = 0
            self.read_delimiter = None

    def _wrap_callback(self, cb):
        def wrapped(*args, **kwargs):
            self.callbacks -= 1
            cb(*args, **kwargs)
        return wrapped

    def _run_callback(self, cb):
        self.callbacks += 1
        self.io_loop.next_tick(cb)

    def set_on_close_callback(self, cb):
        if self.stopped:
            cb(StreamClosedError(wrap=self.error))
        else:
            self.close_callback = cb

    def _read_local(self):
        self._read_once()
        if not self.read_callback and not self.read_future:
            return

        # Try to read from socket directly
        try:
            self._on_read()
            if not self.read_callback and not self.read_future:
                return
        except socket.error as e:
            self.error = e
            self.close()
            return
        except AttributeError:
            # We have already called close, so some variables are None. Call _close to
            # ensure callbacks/futures receive the close error
            self._close()
            return

        # Otherwise we would have to wait for io loop to poll
        if self.stopping:
            raise InconsistentStateError('SocketStream closed')

        self._add_fd_eventmask(READ)

    def connect(self, addr, port, cb=None):
        if cb is not None:
            self.connect_callback = cb
            future = None
        else:
            future = self.connect_future = Future()

        try:
            self.socket.connect((addr, port))
        except socket.error as e:
            if errno_from_exception(e) not in ERR_WOULD_BLOCK and \
                    errno_from_exception(e) not in ERR_INPROGRESS:
                self.error = e
                self.close()
            else:
                self._add_fd_eventmask(WRITE)
                self.connecting = True

        return future

    def read_with_length(self, length, cb=None):
        if self.read_callback or self.read_future:
            raise InconsistentStateError('Already reading')
        if cb is not None:
            self.read_callback = self._wrap_callback(cb)
            future = None
        else:
            future = self.read_future = Future()
        self.read_length = length
        self._read_local()

        # We're returning future instead of self.read_future
        # because _read_local may set read future to None
        return future

    def read_with_delimiter(self, delimiter, cb=None):
        if self.read_callback or self.read_future:
            raise InconsistentStateError('Already reading')
        # Run the callback immediately if we have available data in the buffer
        if cb is not None:
            self.read_callback = self._wrap_callback(cb)
            future = None
        else:
            future = self.read_future = Future()
        self.read_delimiter = delimiter
        self._read_local()

        # We're returning future instead of self.read_future
        # because _read_local may set read future to None
        return future

    def write(self, data, cb=None):
        if cb is not None:
            self.write_callback = self._wrap_callback(cb)
            future = None
        else:
            future = self.write_future = Future()

        if self.stopping:
            if not self.stopped:
                # The close is probably not run yet
                self._close()
                return future
            else:
                self.read_callback = self.read_future = None
                raise InconsistentStateError('SocketStream closed')

        # Append to buffer
        for i in range(0, len(data), _MAX_BLOCK_SIZE):
            self.write_buffer.append(data[i:i + _MAX_BLOCK_SIZE])

        try:
            self._on_write()
            if len(self.write_buffer) != 0:
                self._add_fd_eventmask(WRITE)
        except socket.error as e:
            self.error = e
            self.close()

        return future

__all__ = ['SocketStream']
