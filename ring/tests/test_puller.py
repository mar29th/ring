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
import unittest
from threading import Thread

from ring.connection import PULLER
from ring.connection_impl import Again
from ring.constants import TYPE_ACTIVATE_RECV, TYPE_ERROR, ERR_CONNRESET, TYPE_CLOSED, TYPE_FINALIZE
from ring.context import Context
from ring.events import Mailbox
from ring.protocol import generate_payload_frame
from ring.puller import PullerConnectionImpl
from ring.tests.utils import blocking_send, skip_on_ci
from ring.utils import raise_exc_info


class TestPuller(unittest.TestCase):

    def setUp(self):
        self._server_socket = socket.socket()
        self._server_socket.setblocking(0)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind(('', 0))
        self._server_socket.listen(128)

        self._port = self._server_socket.getsockname()[1]

        self._ctx = Context()
        self._mailbox = Mailbox()

        self._puller = PullerConnectionImpl(self._server_socket, self._ctx, self._mailbox)

    def tearDown(self):
        self._puller.close()
        while 1:
            result = self._mailbox.recv()
            if result.command == TYPE_CLOSED:
                self._puller.connection_close(*result.args)
            elif result.command == TYPE_FINALIZE:
                self._puller.connection_finalize()
                break
        self._mailbox.close()
        self._ctx.stop()

    def _test_unidirectional_recv(self, data, num_clients, iterations):

        def send():
            conn = socket.socket()
            conn.connect(('localhost', self._port))
            for _ in xrange(iterations):
                for frame in generate_payload_frame(data):
                    blocking_send(conn, frame)
            conn.close()

        for x in xrange(num_clients):
            th = Thread(target=send)
            th.daemon = True
            th.start()

        for x in xrange(num_clients):
            for _ in xrange(iterations):
                try:
                    received = self._puller.recv()
                except Again:
                    while 1:
                        mail = self._mailbox.recv()
                        if mail.command == TYPE_ACTIVATE_RECV:
                            self._puller.activate_recv(*mail.args)
                            self.assertTrue(self._puller.recv_available())
                            received = self._puller.recv()
                            break
                        elif mail.command == TYPE_ERROR:
                            self._puller.connection_close(*mail.args)
                            if mail.args[1][1].errno not in ERR_CONNRESET:
                                raise_exc_info(mail.args[1])
                        else:
                            continue
                self.assertEqual(received, data)

    def _test_unidirectional_recv_with_connection(self, data, num_clients, iterations):
        connection = self._ctx.connection(PULLER)
        connection.bind(('', 0))
        port = connection.getsockname()[1]

        def send():
            conn = socket.socket()
            conn.connect(('localhost', port))
            for _ in xrange(iterations):
                for frame in generate_payload_frame(data):
                    blocking_send(conn, frame)
            conn.close()

        for x in xrange(num_clients):
            th = Thread(target=send)
            th.daemon = True
            th.start()

        for _ in xrange(iterations * num_clients):
            received = connection.recv()
            self.assertEqual(received, data)

        connection.close()

    def test_unidirectional_recv_1M_with_100_iterations(self):
        self._test_unidirectional_recv('a' * 1024 * 1024, 1, 100)

    def test_unidirectional_recv_1M_with_300_iterations(self):
        self._test_unidirectional_recv('a' * 1024 * 1024, 1, 300)

    @skip_on_ci
    def test_unidirectional_recv_1M_with_100_iterations_50_clients(self):
        self._test_unidirectional_recv('a' * 1024 * 1024, 50, 100)

    @skip_on_ci
    def test_unidirectional_recv_1M_with_100_iterations_50_connections(self):
        self._test_unidirectional_recv_with_connection('a' * 1024 * 1024, 50, 100)

if __name__ == '__main__':
    unittest.main()

