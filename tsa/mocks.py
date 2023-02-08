class Robots:
    @staticmethod
    def robots_url(*args):  # pylint: disable=unused-argument
        return ""


class StatsClient:
    """Mock client for statsd with empty method implementations."""

    def __init__(self, *args, **kwargs):
        """Intentionally left blank."""

    def gauge(self, *args, **kwargs):
        """Intentionally left blank."""

    def set(self, *args, **kwargs):
        """Intentionally left blank."""

    def timing(self, *args, **kwargs):
        """Intentionally left blank."""

    def delta(self, *args, **kwargs):
        """Intentionally left blank."""
