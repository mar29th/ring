Compatibility
=============

Python version
--------------

Currently, ring has to be run exactly on Python 2.7.

* The version cannot be less than 2.7, since it uses types like
``memoryview()`` which only exists after 2.7. There is no backport available.


* Using Ring on PY3 requires porting (2to3, six, etc.). Ring is currently not
compatible with Python 3, as there are some function usage
(xrange, etc) that exists exclusively below PY3. This would not be a
great issue as the syntax conforms to PY3 conventions.


In particular, when porting to PY3, mind the following:

* ``raise_exc_info()`` function in ``ring.utils``: This helper function raises
exception based on values returned by ``sys.exc_info()``. It is intened to
re-raise an exception bubbled inside coroutines. PY3 has native support
for this - ``from`` keyword that can reconstruct a new exception based on
another exception. When porting, try to use the suitable one on each platform.


* ``xrange()`` saves space on Python 2.7 when enumerating. On PY3 it becomes
``range()``; the original ``range()`` on Python 2.7 is gone.


* There are instances where ``Queue`` is used. PY3 renames it to ``queue``.


Doesn't work on Windows
-----------------------

*Not necessary here in Douban. Plus not enough time to implement.*

Windows has a different set of asynchronous I/O infrastructure. It has
``select()`` and I/O completion ports. ``select()`` fits in our framework, but
its running time is bad. Also some implementations would need to take special
care on Windows, such as ``ring.waker.Waker`` class. Windows does not have
`socketpair()`, so we would need to create two sockets manually.