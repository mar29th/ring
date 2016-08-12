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


import socket
import os

import threading

import cPickle

from ring.connection_impl import Again
from ring.constants import (
    TYPE_ACTIVATE_SEND, TYPE_ACTIVATE_RECV, BACKLOG, TYPE_ERROR, TYPE_CLOSED, TYPE_FINALIZE,
    TYPE_CONNECT_SUCCESS, ERR_CONNRESET
)
from ring.events import Mailbox
from ring.poller import READ
from ring.puller import PullerConnectionImpl
from ring.pusher import PusherConnectionImpl
from ring.replier import ReplierConnectionImpl
from ring.requester import RequesterConnectionImpl
from ring.utils import RingError, raise_exc_info

_idle = 1
_open = 1 << 1
_closing = 1 << 2
_closed = 1 << 3

REPLIER = 1
REQUESTER = 2
PULLER = 3
PUSHER = 5

NONBLOCK = 1

POLLIN = 1
POLLOUT = 1 << 1


class ConnectionError(RingError):

    def __init__(self, errno):
        super(ConnectionError, self).__init__(self, os.strerror(errno))
        self.errno = errno


class ConnectionInUse(RingError):

    def __init__(self):
        super(ConnectionInUse, self).__init__('Connection in use')


class ConnectionClosedError(RingError):

    def __init__(self):
        super(ConnectionClosedError, self).__init__('Socket closed')


class Connection(object):

    def __init__(self, type, ctx):
        self._socket = None

        self._type = type
        self._state = _idle
        self._context = ctx

        self._target_addr = None
        self._target_port = None

        self._bound_addr = None
        self._bound_port = None

        self._mailbox = Mailbox()

        self._lock = threading.RLock()

    def bind(self, target):
        if self._state & (_closing | _closed):
            raise ConnectionClosedError
        if self._state != _idle:
            raise ConnectionInUse
        if not self._type & (REPLIER | PULLER):
            raise NotImplementedError('Bind is not applicable to such type of socket')

        self._bound_addr = target[0]
        self._bound_port = target[1]

        self._initialize_socket()
        self._state = _open
        self._socket.bind(target)
        self._socket.listen(BACKLOG)
        self._initialize_impl()

    def connect(self, target):
        if self._state & (_closing | _closed):
            raise ConnectionClosedError
        if self._state != _idle:
            raise ConnectionInUse
        if not self._type & (REQUESTER | PUSHER):
            raise NotImplementedError('Connect is not applicable to such type of socket')

        self._target_addr = target[0]
        self._target_port = target[1]
        self._state = _open
        self._initialize_socket()
        self._initialize_impl()

        self._impl.connect(target)
        self._process_commands(None)

    def close(self):
        if self._state != _open:
            raise ConnectionClosedError

        self._impl.close()
        self._context.reaper.register(
            self._mailbox.waker_fd, READ, lambda fd, events: self._process_commands(0))
        self._state = _closing

    def _initialize_socket(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.setblocking(0)
        if self._type & (REPLIER | PULLER):
            self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    def _initialize_impl(self):
        if self._type == REPLIER:
            self._impl = ReplierConnectionImpl(self._socket, self._context, self._mailbox)
        elif self._type == REQUESTER:
            self._impl = RequesterConnectionImpl(self._socket, self._context, self._mailbox)
        elif self._type == PULLER:
            self._impl = PullerConnectionImpl(self._socket, self._context, self._mailbox)
        elif self._type == PUSHER:
            self._impl = PusherConnectionImpl(self._socket, self._context, self._mailbox)
        else:
            raise RuntimeError('Type not implemented')

    def _process_commands(self, timeout):
        while 1:
            try:
                result = self._mailbox.recv(timeout)
            except Again:
                return
            else:
                cmd = result.command
                if cmd == TYPE_ACTIVATE_SEND:
                    self._impl.activate_send(*result.args)
                elif cmd == TYPE_ACTIVATE_RECV:
                    self._impl.activate_recv(*result.args)
                elif cmd == TYPE_CONNECT_SUCCESS:
                    # Nothing to be done. We're just attempting to block here.
                    pass
                elif cmd == TYPE_ERROR:
                    self._impl.connection_close(*result.args)
                    if not getattr(result.args[1][1], 'errno', -1) in ERR_CONNRESET:
                        # Only raise the exception when the error is not connection reset
                        raise_exc_info(result.args[1])
                elif cmd == TYPE_CLOSED:
                    self._impl.connection_close(*result.args)
                elif cmd == TYPE_FINALIZE:
                    self._impl.connection_finalize()
                    self._connection_finalize()

                    # Finalize event should break immediately as everything is closed.
                    break
                else:
                    raise RuntimeError('Received undefined command %s' % (cmd,))

                # Rerun. Set timeout to 0.
                timeout = 0

    def _connection_finalize(self):
        self._socket.close()
        self._context.reaper.unregister(self._mailbox.waker_fd)
        self._mailbox.close()
        self._state = _closed

    def getsockname(self):
        if self._state != _open:
            raise ConnectionClosedError

        return self._socket.getsockname()

    def poll(self, events):
        return (POLLIN & events & self._impl.recv_available()) | \
               (POLLOUT & events & self._impl.send_available()) << 1

    def recv(self, flags=0):
        if self._state != _open:
            raise ConnectionClosedError

        # Process once
        self._process_commands(0)

        # Receive once
        try:
            return self._impl.recv()
        except Again:
            if not flags & NONBLOCK:
                # If the connection should block, wait until recv is activated
                pass
            else:
                # If user wants nonblocking send, just raise Again to user
                raise

            # Let's wait
            while 1:
                self._process_commands(None)
                try:
                    return self._impl.recv()
                except Again:
                    continue

    def send(self, data, flags=0):
        if self._state != _open:
            raise ConnectionClosedError

        # Process once
        self._process_commands(0)

        # Send once
        try:
            self._impl.send(data)
        except Again:
            if not flags & NONBLOCK:
                # If the connection should block, wait until send is activated
                pass
            else:
                # If user wants nonblocking send, just raise Again to user
                raise

            # Let's wait
            while 1:
                self._process_commands(None)
                try:
                    self._impl.send(data)
                    break
                except Again:
                    continue

    def recv_pyobj(self, flags=0):
        return cPickle.loads(self.recv(flags=flags))

    def send_pyobj(self, data, flags=0):
        self.send(cPickle.dumps(data), flags=flags)

__all__ = ['Connection', 'REPLIER', 'REQUESTER', 'PULLER', 'PUSHER', 'NONBLOCK']
