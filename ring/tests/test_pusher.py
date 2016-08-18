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
from threading import Thread

from ring.connection import PUSHER
from ring.connection_impl import Again
from ring.constants import TYPE_ACTIVATE_SEND, TYPE_ERROR, TYPE_CLOSED, TYPE_CONNECT_SUCCESS
from ring.context import Context
from ring.events import Mailbox
from ring.protocol import generate_payload_frame
from ring.pusher import PusherConnectionImpl
from ring.tests.utils import blocking_recv, skip_on_ci
from ring.utils import raise_exc_info


class TestPusher(unittest.TestCase):

    def setUp(self):
        self._server_socket = socket.socket()
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind(('', 0))
        self._server_socket.listen(128)

        self._port = self._server_socket.getsockname()[1]

        self._ctx = Context()

    def tearDown(self):
        self._ctx.stop()

    def _test_unidirectional_send(self, data, iterations, num_clients):
        lock = threading.RLock()
        done_mailbox = Mailbox()

        def receive():
            with lock:
                conn, addr = self._server_socket.accept()
            for _ in xrange(iterations):
                gen_expected = generate_payload_frame(data)
                for expected in gen_expected:
                    received = blocking_recv(conn, len(expected))
                    self.assertEqual(received, expected)
            conn.close()
            done_mailbox.send(None)

        for _ in xrange(num_clients):
            th = Thread(target=receive)
            th.daemon = True
            th.start()

        def pusher():
            mailbox = Mailbox()
            client_socket = socket.socket()
            pusher = PusherConnectionImpl(client_socket, self._ctx, mailbox)

            try:
                pusher.connect(('localhost', self._port))

                while 1:
                    try:
                        result = mailbox.recv(0)
                        if result.command == TYPE_CONNECT_SUCCESS:
                            break
                        elif result.command == TYPE_ERROR:
                            pusher.connection_close(*result.args)
                            raise_exc_info(result.args[1])
                    except Again:
                        continue

                for _ in xrange(iterations):
                    try:
                        pusher.send(data)
                    except Again:
                        while 1:
                            try:
                                result = mailbox.recv()
                                if result.command == TYPE_ACTIVATE_SEND:
                                    pusher.activate_send(*result.args)
                                    self.assertTrue(pusher.send_available())
                                    pusher.send(data)
                                    break
                                elif result.command == TYPE_ERROR:
                                    pusher.connection_close(*result.args)
                                    raise_exc_info(result.args[1])
                                else:
                                    continue
                            except Again:
                                continue

                pusher.close()
                while 1:
                    result = mailbox.recv()
                    if result.command == TYPE_CLOSED:
                        pusher.connection_close(*result.args)
                        break
            finally:
                mailbox.close()

        for _ in xrange(num_clients):
            th = Thread(target=pusher)
            th.daemon = True
            th.start()

        for _ in xrange(num_clients):
            done_mailbox.recv()

        done_mailbox.close()

    def _test_unidirectional_send_with_connection(self, data, iterations, num_clients):
        lock = threading.RLock()
        done_mailbox = Mailbox()

        def receive():
            with lock:
                conn, addr = self._server_socket.accept()
            for _ in xrange(iterations):
                gen_expected = generate_payload_frame(data)
                for expected in gen_expected:
                    received = blocking_recv(conn, len(expected))
                    self.assertEqual(received, expected)
            conn.close()
            done_mailbox.send(None)

        for _ in xrange(num_clients):
            th = Thread(target=receive)
            th.daemon = True
            th.start()

        def pusher():
            connection = self._ctx.connection(PUSHER)

            connection.connect(('localhost', self._port))

            for _ in xrange(iterations):
                connection.send(data)

            connection.close()

        for _ in xrange(num_clients):
            th = Thread(target=pusher)
            th.daemon = True
            th.start()

        for _ in xrange(num_clients):
            done_mailbox.recv()

        done_mailbox.close()

    def test_unidirectional_send_1M_with_100_iterations(self):
        self._test_unidirectional_send('a' * 1024 * 1024, 100, 1)

    def test_unidirectional_send_1M_with_300_iterations(self):
        self._test_unidirectional_send('a' * 1024 * 1024, 300, 1)

    @skip_on_ci
    def test_unidirectional_send_1M_with_100_iterations_25_clients(self):
        self._test_unidirectional_send('a' * 1024 * 1024, 100, 25)

    @skip_on_ci
    def test_unidirectional_send_1M_with_100_iterations_50_connections(self):
        self._test_unidirectional_send_with_connection('a' * 1024 * 1024, 100, 50)

if __name__ == '__main__':
    unittest.main()
