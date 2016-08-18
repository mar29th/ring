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

from ring.connection import REQUESTER
from ring.connection_impl import Again
from ring.constants import TYPE_ACTIVATE_RECV, TYPE_ACTIVATE_SEND, TYPE_ERROR, TYPE_CLOSED, \
    TYPE_CONNECT_SUCCESS
from ring.context import Context
from ring.events import Mailbox
from ring.protocol import generate_payload_frame
from ring.requester import RequesterConnectionImpl
from ring.tests.utils import blocking_recv, blocking_send, skip_on_ci
from ring.utils import raise_exc_info


class TestRequester(unittest.TestCase):

    def setUp(self):
        self._server_socket = socket.socket()
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind(('', 0))
        self._server_socket.listen(128)

        self._port = self._server_socket.getsockname()[1]

        self._ctx = Context()

    def tearDown(self):
        self._server_socket.close()
        self._ctx.stop()

    def _test_simple_send_and_receive(self, data, num_clients):
        lock = threading.RLock()
        done_mailbox = Mailbox()

        def send_and_receive():
            with lock:
                conn, addr = self._server_socket.accept()
            gen_expected = generate_payload_frame(data)
            for expected in gen_expected:
                received = blocking_recv(conn, len(expected))
                self.assertEqual(received, expected)

            gen_reply_string = generate_payload_frame(data)
            for string in gen_reply_string:
                blocking_send(conn, string)

            conn.close()

        for _ in xrange(num_clients):
            th = Thread(target=send_and_receive)
            th.daemon = True
            th.start()

        def requester_thread():
            mailbox = Mailbox()
            client_socket = socket.socket()
            requester = RequesterConnectionImpl(client_socket, self._ctx, mailbox)

            try:
                requester.connect(('localhost', self._port))

                while 1:
                    try:
                        result = mailbox.recv(0)
                        if result.command == TYPE_CONNECT_SUCCESS:
                            break
                        elif result.command == TYPE_ERROR:
                            requester.connection_close(*result.args)
                            raise_exc_info(result.args[1])
                    except Again:
                        continue

                try:
                    requester.send(data)
                except Again:
                    while 1:
                        try:
                            result = mailbox.recv()
                            if result.command == TYPE_ACTIVATE_SEND:
                                requester.activate_send(*result.args)
                                self.assertTrue(requester.send_available())
                                requester.send(data)
                                break
                            elif result.command == TYPE_ERROR:
                                requester.connection_close(*result.args)
                                raise_exc_info(result.args[1])
                            else:
                                continue
                        except Again:
                            continue

                try:
                    result = mailbox.recv(0)
                    if result.command == TYPE_ERROR:
                        raise_exc_info(result.args[1])
                except Again:
                    pass

                try:
                    received = requester.recv()
                except Again:
                    while 1:
                        try:
                            result = mailbox.recv()
                            if result.command == TYPE_ACTIVATE_RECV:
                                requester.activate_recv(*result.args)
                                self.assertTrue(requester.recv_available())
                                received = requester.recv()
                                break
                            elif result.command == TYPE_ERROR:
                                requester.connection_close(*result.args)
                                raise_exc_info(result.args[1])
                            else:
                                continue
                        except Again:
                            continue

                self.assertEqual(received, data)
                requester.close()
                while 1:
                    result = mailbox.recv()
                    if result.command == TYPE_CLOSED:
                        requester.connection_close(*result.args)
                        break
                done_mailbox.send(None)
            finally:
                mailbox.close()

        for _ in xrange(num_clients):
            th = Thread(target=requester_thread)
            th.daemon = True
            th.start()

        for _ in xrange(num_clients):
            done_mailbox.recv()

        done_mailbox.close()

    def _test_simple_send_and_receive_with_connection(self, data, num_clients):
        lock = threading.RLock()
        done_mailbox = Mailbox()

        def send_and_receive():
            with lock:
                conn, addr = self._server_socket.accept()
            gen_expected = generate_payload_frame(data)
            for expected in gen_expected:
                received = blocking_recv(conn, len(expected))
                self.assertEqual(received, expected)

            gen_reply_string = generate_payload_frame(data)
            for string in gen_reply_string:
                blocking_send(conn, string)

            conn.close()

        for _ in xrange(num_clients):
            th = Thread(target=send_and_receive)
            th.daemon = True
            th.start()

        def requester_thread():
            connection = self._ctx.connection(REQUESTER)

            connection.connect(('localhost', self._port))
            connection.send(data)
            received = connection.recv()

            self.assertEqual(received, data)

            connection.close()
            done_mailbox.send(None)

        for _ in xrange(num_clients):
            th = Thread(target=requester_thread)
            th.daemon = True
            th.start()

        for _ in xrange(num_clients):
            done_mailbox.recv()

        done_mailbox.close()

    def test_1M_send_and_receive(self):
        self._test_simple_send_and_receive('a' * 1024 * 1024, 1)

    def test_1K_with_100_iterations(self):
        for _ in xrange(100):
            self._test_simple_send_and_receive('a' * 1024, 1)

    def test_1M_with_100_iterations(self):
        for _ in xrange(100):
            self._test_simple_send_and_receive('a' * 1024 * 1024, 1)

    def test_1M_with_300_iterations(self):
        for _ in xrange(300):
            self._test_simple_send_and_receive('a' * 1024 * 1024, 1)

    @skip_on_ci
    def test_1M_with_100_iterations_25_clients(self):
        for _ in xrange(100):
            self._test_simple_send_and_receive('a' * 1024 * 1024, 25)

    @skip_on_ci
    def test_1M_with_100_iterations_25_connections(self):
        for _ in xrange(100):
            self._test_simple_send_and_receive_with_connection('a' * 1024 * 1024, 25)

if __name__ == '__main__':
    unittest.main()
