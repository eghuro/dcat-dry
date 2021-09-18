class Robots:

    @staticmethod
    def robots_url(*args):  # pylint: disable=unused-argument
        return ''


class StatsClient:
    """Mock client for statsd with empty method implementations."""

    def gauge(self, *args):
        """Intentionally left blank."""

    def set(self, *args):
        """Intentionally left blank."""

    def timing(self, *args):
        """Intentionally left blank."""

    def delta(self, *args, **kwargs):
        """Intentionally left blank."""
