.. title:: Ring

Ring
====

Ring, an asynchronous I/O based message queue.

This module is intended to substitute ZMQ when ZMQ does not behave on some Python VMs (e.g. PyPy).

Ring is purely implemented in Python, with no external package dependencies. Knowledge with event
loop and asynchronous I/O is a MUST when developing on this framework.


Contents
--------

.. toctree::

  connectiontypes
  compatibility


Quick Start
-----------

.. code-block:: python

  import ring

  ctx = ring.Context()

  server = ctx.connection(ring.REPLIER)
  client = ctx.connection(ring.REQUESTER)

  client.send_pyobj('Greetings')
  print(server.recv_pyobj())
  # Prints Greetings


Requirements
------------

* Linux kernel 2.5.44 *or* BSD systems (incl. macOS) later than publication date of FreeBSD 4.1
* Python 2.7

  **IMPORTANT:** The version should be exactly 2.7, for reason described in :doc:`compatibility`.


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

