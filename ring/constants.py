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
