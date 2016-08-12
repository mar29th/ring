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


import sys
from struct import unpack

import itertools
import threading

from ring.co import Return, coroutine
from ring.connection_impl import Again, Done
from ring.constants import (
    TYPE_CONNECT_SUCCESS, TYPE_ACTIVATE_RECV, TYPE_ACTIVATE_SEND, TYPE_ERROR, TYPE_CLOSED,
    TYPE_FINALIZE
)
from ring.events import Mail
from ring.protocol import LEN_FRAME_HEADER, FMT_FRAME_HEADER, FLAG_MORE, generate_payload_frame

_lock = threading.RLock()
_counter = itertools.count()


class StreamEngine(object):

    def __init__(self, ctx, stream, recv_pipe, send_pipe, mailbox):
        with _lock:
            self._id = next(_counter)
        self._context = ctx
        self._stream = stream
        self._recv_pipe = recv_pipe
        self._send_pipe = send_pipe
        self._mailbox = mailbox

        self._background_sending = False

        self._closed = False

    def _close(self):
        if self._closed:
            # Race condition: _error is spontaneous - the socket can raise an error and close
            # the engine at any time, but _close is manual.
            # We need the _closed flag to prevent the engine from closing twice. However, if
            # the outside world has requested for a manual close, it must be waiting for the
            # TYPE_FINALIZE event. We would have to send the event to the mailbox.
            self._mailbox.send(Mail(TYPE_FINALIZE, self._id, None))
            return

        self._closed = True
        self._stream.close()
        result = Mail(TYPE_CLOSED, self._id, None)
        self._mailbox.send(result)

    def _error(self):
        if self._closed:
            # Race condition handling: for the race condition, see comments in _close().
            # Here, if the engine is closed already, do nothing as the outside world wouldn't
            # care at all. It is only possible that the outside world has already sent Done
            # to the send pipe and closed this engine.
            return

        self._closed = True
        self._stream.close()
        result = Mail(TYPE_ERROR, self._id, sys.exc_info())
        self._mailbox.send(result)

    @coroutine
    def _connect(self, addr):
        yield self._stream.connect(addr[0], addr[1])

        # # Send REQ greeting
        # yield self._stream.write(REQUESTER_GREETING)
        #
        # # Wait for REP greeting
        # server_greeting = yield self._stream.read_with_length(LEN_REPLIER_GREETING)
        #
        # if server_greeting[IDX_MAJOR_VERSION] != MAJOR_VERSION:
        #     raise ProtocolError('Major version does not match')

    @coroutine
    def _recv(self):
        buf = []

        while 1:
            header = yield self._stream.read_with_length(LEN_FRAME_HEADER)
            header = memoryview(header)
            flags, length = unpack(FMT_FRAME_HEADER, header)
            length_remaining = length - LEN_FRAME_HEADER
            more = True if flags & FLAG_MORE else False

            body = yield self._stream.read_with_length(length_remaining)
            buf.append(body)

            if not more:
                break

        raise Return(''.join(buf))

    @coroutine
    def _send(self, data):
        for chunk in generate_payload_frame(data):
            yield self._stream.write(chunk)

    def _attempt_connect(self, addr):

        def on_done(f):
            try:
                f.result()
                result = Mail(TYPE_CONNECT_SUCCESS)
                self._mailbox.send(result)
            except:
                self._error()

        future = self._connect(addr)
        future.add_done_callback(on_done)

    def _attempt_recv(self):

        def on_done(f):
            try:
                working = self._recv_pipe.write(f.result())
                if not working:
                    self._mailbox.send(Mail(TYPE_ACTIVATE_RECV, self._id))
            except:
                self._error()

        future = self._recv()
        if future.done:
            on_done(future)
        else:
            future.add_done_callback(on_done)

    def _attempt_send(self):

        def on_done(f):
            try:
                f.result()

                try:
                    front, lwm_reached = self._send_pipe.read()
                    if isinstance(front, Done):
                        self._close()
                        return
                    if lwm_reached:
                        # If low watermark reached, activate peer
                        self._mailbox.send(Mail(TYPE_ACTIVATE_SEND, self._id))
                    future = self._send(front)
                    future.add_done_callback(on_done)
                except Again:
                    # Queue empty. Pause.
                    self._background_sending = False
            except:
                self._error()

        if self._background_sending:
            # Already running
            return

        # Trigger once
        try:
            front, lwm_reached = self._send_pipe.read()
            if isinstance(front, Done):
                self._close()
                return
            if lwm_reached:
                # If low watermark reached, activate peer
                self._mailbox.send(Mail(TYPE_ACTIVATE_SEND, self._id))
            self._background_sending = True
            future = self._send(front)
            future.add_done_callback(on_done)
        except Again:
            pass

    @property
    def id(self):
        return self._id

    def activate_connect(self, addr):
        self._context.run_in_background(self._attempt_connect, addr)

    def activate_send(self):
        self._context.run_in_background(self._attempt_send)

    def activate_recv(self):
        self._context.run_in_background(self._attempt_recv)
