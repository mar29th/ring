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


from ring.utils import RingError


class Again(RingError):
    pass


class Done(RingError):
    pass


# Isolated to prevent circular import
class ConnectionImpl(object):

    def __init__(self, socket, ctx, mailbox):
        self._socket = socket
        self._context = ctx
        self._mailbox = mailbox

    def close(self):
        pass

    def connect(self, addr):
        raise NotImplementedError

    def recv(self):
        raise NotImplementedError

    def send(self, data):
        raise NotImplementedError

    def send_available(self):
        raise NotImplementedError

    def recv_available(self):
        raise NotImplementedError

    def activate_recv(self, engine_id):
        raise NotImplementedError

    def activate_send(self, engine_id):
        raise NotImplementedError

    def connection_close(self, engine_id, err):
        raise NotImplementedError

    def connection_finalize(self):
        raise NotImplementedError
