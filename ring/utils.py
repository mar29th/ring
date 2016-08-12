import socket
import os
import logging

_log_format = '%(asctime)-15s [%(levelname)s] [%(name)-9s] %(message)s'
_log_level = logging.DEBUG if hasattr(os.environ, 'RING_DEBUG') else logging.INFO
logging.basicConfig(format=_log_format, level=_log_level)


def get_logger(name):
    """ Always use logging.Logger class.

    The user code may change the loggerClass (e.g. pyinotify),
    and will cause exception when format log message.

    This method is taken from DPark.
    """
    old_class = logging.getLoggerClass()
    logging.setLoggerClass(logging.Logger)
    logger = logging.getLogger(name)
    logging.setLoggerClass(old_class)
    return logger


def errno_from_exception(e):
    if not isinstance(e, (OSError, IOError, socket.error)):
        return None
    if hasattr(e, 'errno'):
        return e.errno
    elif e.args:
        return e.args[0]
    else:
        return None


class RingError(Exception):
    pass


class SocketError(RingError):

    def __init__(self, msg=None, wrap=None):
        if msg is None:
            msg = 'Socket error'
        super(SocketError, self).__init__(msg)
        self.wrapped = wrap

    def __str__(self):
        if self.wrapped is not None:
            return super(SocketError, self).__str__() + '. Wrapped error: %s' % (self.wrapped,)
        else:
            return super(SocketError, self).__str__()

    @property
    def errno(self):
        return errno_from_exception(self.wrapped) if self.wrapped else None


class InconsistentStateError(RingError):

    def __init__(self, msg=None):
        if msg is None:
            msg = 'Inconsistent state'
        super(InconsistentStateError, self).__init__(msg)


def protocol_assert(stmnt, msg=None):
    if not stmnt:
        if msg is None:
            msg = 'Protocol assertion failed'
        raise ProtocolError(msg)


class ProtocolError(RingError):

    def __init__(self, msg=None):
        if msg is None:
            msg = 'Message does not conform to protocol'
        super(ProtocolError, self).__init__(msg)


def raise_exc_info(exc):
    raise exc[0], exc[1], exc[2]
