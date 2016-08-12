# Ring
Python-native message queue.

This module is intended to substitute ZMQ when ZMQ does not
behave on some Python VMs (e.g. PyPy). It is originally developed for Douban's
DPark.

Ring is purely implemented in Python, with no
external package dependencies.


## Requirements

* Linux kernel 2.5.44 _or_ BSD systems (incl. macOS) later than 
publication date of FreeBSD 4.1
* Python 2.7


## License

Apache License v2. For details, see [LICENSE](LICENSE).