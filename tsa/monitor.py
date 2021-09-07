"""Recording various runtime metrics into statsd."""
import logging
import time

from tsa.extensions import statsd_client


class Monitor(object):
    """Monitor is recording various runtime metrics into statsd."""

    def __increment(self, key, value, delta=1):
        name = f'tsa.{key}.{value}'
        statsd_client.gauge(name, delta, delta=True)

    def log_format(self, guess):
        """Record distribution format."""
        statsd_client.set('format', guess)

    def log_size(self, size):
        """Record distribution size."""
        if type(size) in [str, int]:
            try:
                size = int(size)
                self.__increment('size', 'sum', size)
                self.__increment('size', 'count')
            except ValueError:
                log = logging.getLogger(__name__)
                log.exception('Failed to log size')

    def log_inspected(self):
        self.__increment('graphs', 'inspected')

    def log_processed(self):
        self.__increment('distributions', 'processed')

    def log_tasks(self, tasks):
        self.__increment('distributions', 'discovered', tasks)

    def log_dereference_request(self):
        self.__increment('dereference', 'requested')

    def log_dereference_processed(self):
        self.__increment('dereference', 'processsed')

    def log_graph_count(self, items):
        statsd_client.gauge('graphs.count', int(items))


monitor = Monitor()


class TimedBlock(object):
    def __init__(self, name):
        self.__name = name

    def __enter__(self):
        self.__start = time.perf_counter_ns()

    def __exit__(self, *args):
        end = time.perf_counter_ns()
        elapsed_ns = end - self.__start
        elapsed_ms = int(elapsed_ns / 1000)
        statsd_client.timing(f'timed_block.{self.__name}', elapsed_ms)
