# Copyright 2016 Facebook Inc.
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


import select

# From python select module and Tornado source
_EPOLLIN = 0x001
_EPOLLPRI = 0x002
_EPOLLOUT = 0x004
_EPOLLERR = 0x008
_EPOLLHUP = 0x010
_EPOLLRDHUP = 0x2000
_EPOLLONESHOT = (1 << 30)
_EPOLLET = (1 << 31)

READ = _EPOLLIN
WRITE = _EPOLLOUT
ERROR = _EPOLLERR | _EPOLLHUP
MASK_ALL = 0xFFFF


class PollerImpl(object):

    def poll(self, timeout):
        raise NotImplementedError()

    def register(self, fd, eventmask):
        raise NotImplementedError()

    def unregister(self, fd):
        raise NotImplementedError()

    def modify(self, fd, eventmask):
        raise NotImplementedError()


class EpollImpl(PollerImpl):
    """
    epoll wrapper. Only usable on Linux.
    """

    def __init__(self):
        super(EpollImpl, self).__init__()
        self._epoll = select.epoll()

    def __del__(self):
        try:
            self._epoll.close()
        except:
            # Doesn't matter
            pass

    def register(self, fd, eventmask):
        self._epoll.register(fd, eventmask)

    def unregister(self, fd):
        self._epoll.unregister(fd)

    def poll(self, timeout):
        return self._epoll.poll(timeout)

    def modify(self, fd, eventmask):
        return self._epoll.modify(fd, eventmask)


class KQueueImpl(PollerImpl):
    """
    kqueue wrapper. Only usable on BSD-like systems.
    """
    def __init__(self):
        super(KQueueImpl, self).__init__()
        self._kqueue = select.kqueue()
        self._events = {}

    def __del__(self):
        try:
            self._kqueue.close()
        except:
            # Doesn't matter
            pass

    def _control(self, fd, eventmask, flags):
        events = []
        if eventmask & READ:
            events.append(select.kevent(fd, filter=select.KQ_FILTER_READ, flags=flags))
        if eventmask & WRITE:
            events.append(select.kevent(fd, filter=select.KQ_FILTER_WRITE, flags=flags))

        for ev in events:
            self._kqueue.control([ev], 0)

    def register(self, fd, eventmask):
        assert fd not in self._events, 'File already registered'
        self._events[fd] = eventmask
        if eventmask != 0:
            self._control(fd, eventmask, select.KQ_EV_ADD)

    def unregister(self, fd):
        assert fd in self._events, 'File not registered'
        event = self._events.pop(fd)
        if event != 0:
            self._control(fd, event, select.KQ_EV_DELETE)

    def poll(self, timeout):
        retval = {}
        kevents = self._kqueue.control(None, 1000, timeout)
        for kevent in kevents:
            ident = kevent.ident
            if kevent.filter == select.KQ_FILTER_READ:
                retval[ident] = retval.get(ident, 0) | READ
            if kevent.filter == select.KQ_FILTER_WRITE:
                if kevent.flags & select.KQ_EV_EOF:
                    retval[ident] = ERROR
                else:
                    retval[ident] = retval.get(ident, 0) | WRITE
            if kevent.flags & select.KQ_EV_ERROR:
                retval[ident] = retval.get(ident, 0) | ERROR
        return retval.items()

    def modify(self, fd, eventmask):
        self.unregister(fd)
        self.register(fd, eventmask)


class PollImpl(PollerImpl):

    def __init__(self):
        self._poll = select.poll()

    def __del__(self):
        try:
            self._poll.close()
        except:
            # Doesn't matter
            pass

    def register(self, fd, eventmask):
        self._poll.register(fd, eventmask)

    def unregister(self, fd):
        self._poll.unregister(fd)

    def poll(self, timeout):
        return self._poll.poll(timeout)

    def modify(self, fd, eventmask):
        return self._poll.modify(fd, eventmask)


class SelectImpl(PollerImpl):

    def __init__(self):
        self._reading = set()
        self._writing = set()
        self._error = set()

    def register(self, fd, eventmask):
        if eventmask & READ:
            self._reading.add(fd)
        if eventmask & WRITE:
            self._writing.add(fd)
        if eventmask & ERROR:
            self._error.add(fd)

    def modify(self, fd, eventmask):
        self.unregister(fd)
        self.register(fd, eventmask)

    def poll(self, timeout):
        read, write, err = select.select(self._reading, self._writing, self._error, timeout)
        events = {}
        for fd in read:
            events[fd] = events.get(fd, 0) | READ
        for fd in write:
            events[fd] = events.get(fd, 0) | WRITE
        for fd in err:
            events[fd] = events.get(fd, 0) | ERROR
        return events.items()

    def unregister(self, fd):
        self._reading.discard(fd)
        self._writing.discard(fd)
        self._error.discard(fd)


def get_poller():
    if hasattr(select, 'epoll'):
        # Linux
        return EpollImpl()
    elif hasattr(select, 'kqueue'):
        # BSD
        return KQueueImpl()
    elif hasattr(select, 'poll'):
        # UNIX
        return PollImpl()
    elif hasattr(select, 'select'):
        # Windows et al.
        return SelectImpl()
    else:
        raise OSError('System not supported')
