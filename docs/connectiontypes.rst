Connection Types
================

Ring's each connection type has its counterpart. One type can only be paired with that particular
counterpart as remote receiver/sender. The following are the connection types.


Requester
---------

Requester implements the following properties:

* Send/Receive Pattern: Send, and receive.
* Durability: Breaks down when socket raises error. Does not automatically restart.
  Create a new one.


Replier
-------

``Replier`` implements the following pattern:

* Send/Receive pattern: Receive, and send.
* Durability: When either send/recv raises error, the connection is reset to receive state and is
  ready to accept messages from another connection.


Pusher
------

``Pusher`` implements the following pattern:

* Send/Receive pattern: Receive continuously. Does not send.
* Durability: Breaks down when socket raises error. Does not automatically restart.
  Create a new one.


Puller
------

``Puller`` implements the following pattern:

* Send/Receive pattern: Send continuously. Receive raises error.
* Durability: When recv raises error, it closes connection with the remote and can resume operation.
