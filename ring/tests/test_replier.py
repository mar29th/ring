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
import threading
import unittest

from ring.connection import REPLIER
from ring.connection_impl import Again
from ring.constants import TYPE_ACTIVATE_RECV, TYPE_ERROR, TYPE_ACTIVATE_SEND, ERR_CONNRESET, \
    TYPE_CLOSED, TYPE_FINALIZE
from ring.context import Context
from ring.events import Mailbox
from ring.protocol import generate_payload_frame
from ring.replier import ReplierConnectionImpl
from ring.tests.utils import blocking_send, blocking_recv, skip_on_ci
from ring.utils import raise_exc_info


class TestReplier(unittest.TestCase):

    def setUp(self):
        self._server_socket = socket.socket()
        self._server_socket.setblocking(0)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind(('', 0))
        self._server_socket.listen(128)

        self._port = self._server_socket.getsockname()[1]

        self._ctx = Context()
        self._mailbox = Mailbox()

        self._replier = ReplierConnectionImpl(self._server_socket, self._ctx, self._mailbox)

    def tearDown(self):
        self._replier.close()
        while 1:
            result = self._mailbox.recv()
            if result.command == TYPE_CLOSED:
                self._replier.connection_close(*result.args)
            elif result.command == TYPE_FINALIZE:
                self._replier.connection_finalize()
                break
        self._mailbox.close()
        self._ctx.stop()

    def _test_simple_receive_and_send(self, data, num_clients):
        mailbox = Mailbox()

        def client():
            conn = socket.socket()
            conn.connect(('localhost', self._port))
            gen_expected = generate_payload_frame(data)
            for expected in gen_expected:
                blocking_send(conn, expected)

            gen_expected = generate_payload_frame(data)
            for expected in gen_expected:
                received = blocking_recv(conn, len(expected))
                self.assertEqual(received, expected)

            conn.close()
            mailbox.send(None)

        for _ in xrange(num_clients):
            th = threading.Thread(target=client)
            th.daemon = True
            th.start()

        for _ in xrange(num_clients):
            try:
                received = self._replier.recv()
            except Again:
                while 1:
                    mail = self._mailbox.recv()
                    if mail.command == TYPE_ACTIVATE_RECV:
                        self._replier.activate_recv(*mail.args)
                        self.assertTrue(self._replier.recv_available())
                        received = self._replier.recv()
                        break
                    elif mail.command == TYPE_ERROR:
                        self._replier.connection_close(*mail.args)
                        if mail.args[1][1].errno not in ERR_CONNRESET:
                            raise_exc_info(mail.args[1])
                    else:
                        continue

            self.assertEqual(received, data)

            try:
                self._replier.send(data)
            except Again:
                while 1:
                    mail = self._mailbox.recv()
                    if mail.command == TYPE_ACTIVATE_SEND:
                        self._replier.activate_send(*mail.args)
                        self.assertTrue(self._replier.send_available())
                        self._replier.send(data)
                        break
                    elif mail.command == TYPE_ERROR:
                        self._replier.connection_close(*mail.args)
                        if mail.args[1][1].errno not in ERR_CONNRESET:
                            raise_exc_info(mail.args[1])
                    else:
                        continue

        for _ in xrange(num_clients):
            mailbox.recv()

        mailbox.close()

    def _test_simple_receive_and_send_with_connection(self, data, num_clients):
        mailbox = Mailbox()
        connection = self._ctx.connection(REPLIER)
        connection.bind(('', 0))
        port = connection.getsockname()[1]

        def client():
            conn = socket.socket()
            conn.connect(('localhost', port))
            gen_expected = generate_payload_frame(data)
            for expected in gen_expected:
                blocking_send(conn, expected)

            gen_expected = generate_payload_frame(data)
            for expected in gen_expected:
                received = blocking_recv(conn, len(expected))
                self.assertEqual(received, expected)

            conn.close()
            mailbox.send(None)

        for _ in xrange(num_clients):
            th = threading.Thread(target=client)
            th.daemon = True
            th.start()

        for _ in xrange(num_clients):
            received = connection.recv()
            self.assertEqual(received, data)
            connection.send(received)

        for _ in xrange(num_clients):
            mailbox.recv()

        connection.close()
        mailbox.close()

    def test_1K_receive_and_send(self):
        self._test_simple_receive_and_send('a' * 1024, 1)

    def test_1M_receive_and_send(self):
        self._test_simple_receive_and_send('a' * 1024 * 1024, 1)

    def test_1K_with_100_iterations(self):
        for _ in xrange(100):
            self._test_simple_receive_and_send('a' * 1024, 1)

    def test_1M_with_100_iterations(self):
        for _ in xrange(100):
            self._test_simple_receive_and_send('a' * 1024 * 1024, 1)

    def test_1M_with_300_iterations(self):
        for _ in xrange(300):
            self._test_simple_receive_and_send('a' * 1024 * 1024, 1)

    @skip_on_ci
    def test_1M_with_100_iterations_50_clients(self):
        for _ in xrange(100):
            self._test_simple_receive_and_send('a' * 1024 * 1024, 50)

    @skip_on_ci
    def test_1M_with_100_iterations_50_connections(self):
        for _ in xrange(100):
            self._test_simple_receive_and_send_with_connection('a' * 1024 * 1024, 50)
