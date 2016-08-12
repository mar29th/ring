from collections import OrderedDict

import ring
from ring import PUSHER
from ring.benchmark.benchmark import BenchmarkTask


class BenchmarkPusherPuller(BenchmarkTask):

    def setup(self):
        self._ctx = ring.Context()
        self._connection = self._ctx.connection(PUSHER)

    def tear_down(self):
        self._connection.close()
        self._ctx.stop()

    def run_sync(self, host=None, port=None, scale=None, iteration=None, pkg_size=None):
        self._connection.connect((host, port))
        content = 'a' * 1024 * pkg_size
        self.start_timer()
        for _ in xrange(iteration):
            self._connection.send(content)
        self.stop_timer()

    @property
    def args(self):
        return OrderedDict([
            ('--host', {'help': 'host name', 'required': True}),
            ('--port', {'help': 'host port, defaults to 9000', 'default': 9000, 'type': int}),
            ('--iteration', {'help': 'number of iterations', 'type': int, 'required': True}),
            ('--pkg-size', {'help': 'package size in KB', 'type': int, 'required': True})
        ])

    def inspect(self):
        overall_time, system_time = self.results
        transfer_rate = self.params['pkg_size'] * self.params['iteration'] * 8 \
            / overall_time / (10 ** 3)
        return '{}: Overall {}s, Processor time {}s. Transfer rate {} Mb/s' \
            .format(self.name, overall_time, system_time, transfer_rate)

export = BenchmarkPusherPuller()

if __name__ == '__main__':
    print 'This benchmark is not so useful, as it is unidirectional, and one-way send only ' \
          'puts data onto a queue and returns.'
    export.main()
    print export.inspect()
