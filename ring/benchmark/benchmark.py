import Queue
import argparse
import time
import traceback
import logging
import threading

logger = logging.getLogger('Benchmark')
handler = logging.StreamHandler()

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)


class BenchmarkTask(object):

    def __init__(self, name=None):
        self.name = name or self.__class__.__name__

        self.starting_overall_time = None
        self.starting_processor_time = None
        self.stopping_overall_time = None
        self.stopping_processor_time = None
        self.overall_time = None
        self.processor_time = None

        self.synchronizer = Queue.Queue()

        self.params = {}

    @property
    def results(self):
        return self.overall_time, self.processor_time

    def task(self, **kwargs):
        raise NotImplementedError

    @property
    def args(self):
        raise NotImplementedError

    def setup(self):
        pass

    def tear_down(self):
        pass

    def start_timer(self):
        self.starting_overall_time = time.time()
        self.starting_processor_time = time.clock()

    def stop_timer(self):
        self.stopping_overall_time = time.time()
        self.stopping_processor_time = time.clock()
        self.overall_time = self.stopping_overall_time - self.starting_overall_time
        self.processor_time = self.stopping_processor_time - self.starting_processor_time

    def done(self, err=None):
        self.synchronizer.put_nowait(err)

    def run(self, args):
        self.params = args

        self.setup()

        try:
            self.run_sync(**args)
            self.tear_down()
            return
        except NotImplementedError:
            pass
        except Exception as e:
            logger.error('Synchronous benchmark task %s raised error: %s', self.name, e)
            traceback.print_exc()
            return

        try:
            self.run_async(**args)
            err = self.synchronizer.get()
            self.tear_down()
            if err:
                logger.error('Asynchronous benchmark task %s raised error: %s', self.name, err)
        except NotImplementedError:
            logger.error('Neither synchronous nor asynchronous benchmark function is implemented')
        except Exception as e:
            logger.error('Asynchronous benchmark task %s raised error on enter: %s', self.name, e)

    def run_sync(self, **kwargs):
        raise NotImplementedError

    def run_async(self, **kwargs):
        raise NotImplementedError

    def inspect(self):
        raise NotImplementedError

    def main(self):
        parser = argparse.ArgumentParser()
        for kwd, opt in self.args.iteritems():
            parser.add_argument(kwd, **opt)
        args = parser.parse_args()
        self.run(vars(args))


class Benchmark(object):

    @staticmethod
    def from_tasks(tasks):
        benchmark = Benchmark()
        for task in tasks:
            benchmark.add_benchmark(*task)
        return benchmark

    def __init__(self, name=None):
        self.name = name or self.__class__.__name__
        self.benchmarks = []

        self.concurrent_lock = threading.RLock()
        self.concurrent_completed_count = 0

    def add_benchmark(self, b, args):
        self.benchmarks.append((b, args))

    def start(self, concurrent=False):
        logger.info('Running benchmarks')
        for b, args in self.benchmarks:
            if not concurrent:
                b.run(args)
                logger.info(b.inspect())
            else:
                def wrapper(*args, **kwargs):
                    b.run(*args, **kwargs)
                    with self.concurrent_lock:
                        self.concurrent_completed_count += 1

                thread = threading.Thread(
                    name='Benchmark %s runner' % (self.name,), target=wrapper, kwargs=args)
                thread.daemon = True
                thread.start()
        if concurrent:
            while self.concurrent_completed_count != len(self.benchmarks):
                time.sleep(0.1)
        logger.info('Benchmark %s run finished', self.name)
