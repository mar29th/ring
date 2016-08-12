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

from ring.connection_impl import ConnectionImpl, Again, Done
from ring.constants import TYPE_FINALIZE
from ring.events import Mail
from ring.pipes import Pipe
from ring.poller import READ
from ring.stream import SocketStream
from ring.stream_engine import StreamEngine
from ring.utils import InconsistentStateError


class ReplierConnectionImpl(ConnectionImpl):

    def __init__(self, socket, ctx, mailbox):
        super(ReplierConnectionImpl, self).__init__(socket, ctx, mailbox)
        self._connections = {}
        self._recv_queue = collections.deque()
        self._out_active = {}
        self._last_received_engine_id = -1
        self._context.io_loop.register(self._socket.fileno(), READ, self._on_accept)
        self._should_recv = True
        self._closing = False

    def close(self):
        super(ReplierConnectionImpl, self).close()
        self._closing = True
        self._context.io_loop.unregister(self._socket.fileno())

        if not self._connections:
            # If there's no connection at all, trigger finalize immediately
            self._mailbox.send(Mail(TYPE_FINALIZE))
            return

        for engine, stream, recv_pipe, send_pipe in self._connections.itervalues():
            if not send_pipe.write(Done()):
                engine.activate_send()

    def _on_accept(self, fd, events):
        conn, addr = self._socket.accept()
        stream = SocketStream(conn, io_loop=self._context.io_loop)
        recv_pipe = Pipe()
        send_pipe = Pipe()
        engine = StreamEngine(self._context, stream, recv_pipe, send_pipe, self._mailbox)
        self._connections[engine.id] = (engine, stream, recv_pipe, send_pipe)
        self._out_active[engine.id] = True
        engine.activate_recv()

    def connect(self, addr):
        raise NotImplementedError('Replier does not have connection method')

    def send(self, data):
        if self._should_recv:
            raise InconsistentStateError('Should not send before recv')

        assert self._last_received_engine_id in self._connections
        engine, x, y, send_pipe = self._connections[self._last_received_engine_id]
        if not self._out_active[self._last_received_engine_id]:
            raise Again

        try:
            if not send_pipe.write(data):
                engine.activate_send()
            self._should_recv = True
        except Again:
            self._out_active[self._last_received_engine_id] = False

    def recv(self):
        if not self._should_recv:
            raise InconsistentStateError('Should not recv again')

        if len(self._recv_queue) == 0:
            raise Again

        engine_id = self._recv_queue[0]
        engine, x, recv_pipe, y = self._connections[engine_id]
        assert recv_pipe.read_available()
        read = recv_pipe.read()[0]
        self._last_received_engine_id = engine_id

        if not recv_pipe.read_available():
            self._recv_queue.popleft()
            engine.activate_recv()

        self._should_recv = False

        return read

    def recv_available(self):
        return len(self._recv_queue) != 0

    def send_available(self):
        return self._out_active[self._last_received_engine_id]

    def activate_send(self, engine_id):
        assert engine_id in self._out_active
        assert not self._out_active[engine_id]
        self._out_active[engine_id] = True

    def activate_recv(self, engine_id):
        self._recv_queue.append(engine_id)

    def connection_close(self, engine_id, err):
        if engine_id == -1:
            # -1 means the master socket. We should unregister the master socket and
            # wait for the user to terminate the connection.
            # Terminating connection autonomously brings many hard to find bugs and sometimes
            # subtle race conditions
            self._context.io_loop.unregister(self._socket.fileno())
        else:
            if engine_id == self._last_received_engine_id:
                # Reset state
                self._should_recv = True

            # Erase the connection from cache
            x, y, recv_pipe, send_pipe = self._connections[engine_id]
            recv_pipe.clear()
            send_pipe.clear()
            del self._connections[engine_id]

            if self._closing and not self._connections:
                self._mailbox.send(Mail(TYPE_FINALIZE))

    def connection_finalize(self):
        self._recv_queue = self._connections = self._out_active = None
