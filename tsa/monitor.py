"""Recording various runtime metrics into statsd."""
import logging
import time

from tsa.extensions import statsd_client


class Monitor:

    """Monitor is recording various runtime metrics into statsd."""

    @staticmethod
    def __increment(key, value, delta=1):
        name = f'tsa.{key}.{value}'
        statsd_client.gauge(name, delta, delta=True)

    @staticmethod
    def log_format(guess):
        """Record distribution format."""
        statsd_client.set('format', guess)

    @staticmethod
    def log_size(size):
        """Record distribution size."""
        if type(size) in [str, int]:
            try:
                size = int(size)
                Monitor.__increment('size', 'sum', size)
                Monitor.__increment('size', 'count')
            except ValueError:
                log = logging.getLogger(__name__)
                log.exception('Failed to log size')

    @staticmethod
    def log_inspected():
        Monitor.__increment('graphs', 'inspected')

    @staticmethod
    def log_processed():
        Monitor.__increment('distributions', 'processed')

    @staticmethod
    def log_tasks(tasks):
        Monitor.__increment('distributions', 'discovered', tasks)

    @staticmethod
    def log_dereference_request():
        Monitor.__increment('dereference', 'requested')

    @staticmethod
    def log_dereference_processed():
        Monitor.__increment('dereference', 'processsed')

    @staticmethod
    def log_graph_count(items):
        statsd_client.gauge('graphs.count', int(items))


monitor = Monitor()


class TimedBlock:
    def __init__(self, name):
        self.__name = name
        self.__start = None

    def __enter__(self):
        self.__start = time.perf_counter_ns()

    def __exit__(self, *args):
        end = time.perf_counter_ns()
        elapsed_ns = end - self.__start
        elapsed_ms = int(elapsed_ns / 1000)
        statsd_client.timing(f'timed_block.{self.__name}', elapsed_ms)
