from collections import OrderedDict
from threading import Thread

import time

import Queue

import ring
from ring.benchmark.benchmark import BenchmarkTask
from ring.benchmark.utils import stat

ctx = ring.Context()

synchronizer = Queue.Queue()


def spawnable(host, port, iteration, pkg_size, time_array):
    global count
    connection = ctx.connection(ring.REQUESTER)
    connection.connect((host, port))
    string = 'a' * 1024 * pkg_size
    starting_overall_time, starting_processor_time = time.time(), time.clock()
    for j in xrange(iteration):
        connection.send_pyobj(string)
        connection.recv_pyobj()
    time_array.append((time.time() - starting_overall_time, time.clock() - starting_processor_time))
    connection.close()
    synchronizer.put_nowait(None)


class MultipleRequesterBenchmarkTask(BenchmarkTask):

    def run_sync(self, host=None, port=None, scale=None, iteration=None, pkg_size=None):
        self.time = []
        for _ in xrange(scale):
            th = Thread(target=spawnable, args=(host, port, iteration, pkg_size, self.time))
            th.daemon = True
            th.start()
        for _ in xrange(scale):
            synchronizer.get()

    @property
    def args(self):
        return OrderedDict([
            ('--host', {'help': 'host name', 'required': True}),
            ('--port', {'help': 'host port, defaults to 9000', 'default': 9000, 'type': int}),
            ('--scale', {'help': 'number of background clients', 'default': 1,
                         'type': int, 'required': True}),
            ('--iteration', {'help': 'number of iterations', 'type': int, 'required': True}),
            ('--pkg-size', {'help': 'package size in KB', 'type': int, 'required': True})
        ])

    def inspect(self):
        scale = self.params['scale']
        iteration = self.params['iteration']
        pkg_size = self.params['pkg_size']
        overall_times = []
        processor_times = []
        transfer_rates = []
        for overall_time, processor_time in self.time:
            overall_times.append(overall_time)
            processor_times.append(processor_time)
            transfer_rates.append(pkg_size * iteration * 8 / overall_time / (10 ** 3))
        overall_stat = stat(overall_times)
        processor_stat = stat(processor_times)
        transfer_stat = stat(transfer_rates)

        return \
            """{}: Completed with {} clients, each with {} iterations, {} KB packets.
                Overall time: min {}s, max {}s, avg {}s, stddev {}s
                Processor time: min {}s, max {}s, avg {}s, stddev {}s
                Transfer rate: min {} Mb/s, max {} Mb/s, avg {} Mb/s, stddev {} Mb/s""" \
                .format(*((self.name, scale, iteration, pkg_size) +
                          overall_stat +
                          processor_stat +
                          transfer_stat))

export = MultipleRequesterBenchmarkTask()

if __name__ == '__main__':
    export.main()
    print export.inspect()
