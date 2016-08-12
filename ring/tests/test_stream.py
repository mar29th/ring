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

from ring.stream import SocketStream
from ring.tests.utils import AsyncTestCase, coroutine_test


class TestStream(AsyncTestCase):

    def setUp(self):
        super(TestStream, self).setUp()

        self._server_socket = socket.socket()
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.setblocking(0)
        self._server_socket.bind(('', 0))
        self._server_socket.listen(128)
        self._port = self._server_socket.getsockname()[1]

        self._client_socket = socket.socket()
        self._client_stream = SocketStream(self._client_socket)

    def tearDown(self):
        super(TestStream, self).tearDown()

        self._client_stream.close()
        self._client_socket.close()
        self._server_socket.close()

    @coroutine_test
    def test_connect(self):
        yield self._client_stream.connect('localhost', self._port)
        self._client_stream.close()

    def test_connect_callback(self):

        def after_connect():
            self._client_stream.close()
            self._io_loop.stop()

        self._client_stream.connect('localhost', self._port, after_connect)
        self._io_loop.start()

    @coroutine_test
    def test_simple_send_and_receive(self):
        yield self._client_stream.connect('localhost', self._port)
        conn, _ = self._server_socket.accept()
        server_stream = SocketStream(conn, self._io_loop)
        yield self._client_stream.write('a')
        received = yield server_stream.read_with_length(1)
        self.assertEqual(received, 'a')
        yield server_stream.write('n')
        received = yield self._client_stream.read_with_length(1)
        self.assertEqual(received, 'n')
        self._client_stream.close()
        server_stream.close()
