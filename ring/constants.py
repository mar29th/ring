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


import errno
import sys

# From Tornado source

ERR_WOULD_BLOCK = (errno.EWOULDBLOCK, errno.EAGAIN)
if hasattr(errno, 'WSAEWOULDBLOCK'):
    ERR_WOULD_BLOCK += (errno.WSAEWOULDBLOCK,)

ERR_CONNRESET = (errno.ECONNRESET, errno.ECONNABORTED, errno.EPIPE, errno.ETIMEDOUT)

if hasattr(errno, "WSAECONNRESET"):
    ERR_CONNRESET += (errno.WSAECONNRESET, errno.WSAECONNABORTED, errno.WSAETIMEDOUT)

if sys.platform == 'darwin':
    ERR_CONNRESET += (errno.EPROTOTYPE,)

ERR_INPROGRESS = (errno.EINPROGRESS,)

if hasattr(errno, "WSAEINPROGRESS"):
    ERR_INPROGRESS += (errno.WSAEINPROGRESS,)

BACKLOG = 128

# end


# Mailbox opcodes
TYPE_ACTIVATE_SEND = 1
TYPE_ACTIVATE_RECV = 2
TYPE_CONNECT_SUCCESS = 3

TYPE_ERROR = 5
TYPE_CLOSED = 6
TYPE_FINALIZE = 7
