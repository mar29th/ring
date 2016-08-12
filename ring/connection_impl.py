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
