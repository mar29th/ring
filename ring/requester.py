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


from ring.connection_impl import ConnectionImpl, Again, Done
from ring.constants import TYPE_FINALIZE
from ring.events import Mail
from ring.pipes import Pipe
from ring.stream import SocketStream
from ring.stream_engine import StreamEngine
from ring.utils import InconsistentStateError


class RequesterConnectionImpl(ConnectionImpl):

    def __init__(self, socket, ctx, waker):
        super(RequesterConnectionImpl, self).__init__(socket, ctx, waker)
        self._stream = SocketStream(self._socket, io_loop=self._context.io_loop)

        self._recv_pipe = Pipe()
        self._send_pipe = Pipe()

        self._stream_engine = StreamEngine(
            self._context, self._stream, self._recv_pipe, self._send_pipe, self._mailbox)

        self._recv_activated = True
        self._send_activated = True

        self._should_send = True

    def close(self):
        super(RequesterConnectionImpl, self).close()
        if not self._send_pipe.write(Done()):
            self._stream_engine.activate_send()
        self._send_activated = False

    def connect(self, addr):
        self._stream_engine.activate_connect(addr)

    def recv(self):
        if self._should_send:
            raise InconsistentStateError('Should not recv without send')

        if not self._recv_activated:
            raise Again

        try:
            result, _ = self._recv_pipe.read()
            self._should_send = True
            return result
        except Again:
            self._recv_activated = False

            # Submit read task
            self._stream_engine.activate_recv()
            raise

    def send(self, data):
        if not self._should_send:
            raise InconsistentStateError('Should not send again')

        if not self._send_activated:
            raise Again

        try:
            if not self._send_pipe.write(data):
                # If the pipe returns false, it was previously empty.
                # We would need to resubmit the task
                self._stream_engine.activate_send()
            self._should_send = False
        except Again:
            self._send_activated = False

    def recv_available(self):
        self._recv_activated = self._recv_pipe.read_available()
        return self._recv_activated

    def send_available(self):
        self._send_activated = self._send_pipe.write_available()
        return self._send_activated

    def activate_send(self, engine_id):
        self._send_activated = True

    def activate_recv(self, engine_id):
        self._recv_activated = True

    def connection_close(self, engine_id, err):
        self._recv_pipe.clear()
        self._send_pipe.clear()
        self._mailbox.send(Mail(TYPE_FINALIZE))

    def connection_finalize(self):
        self._recv_pipe = self._send_pipe = None
