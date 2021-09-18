class Robots:

    @staticmethod
    def robots_url(*args):  # pylint: disable=unused-argument
        return ''


class StatsClient:
    """Mock client for statsd with empty method implementations."""

    def gauge(self, *args):  # pylint: disable=unused-argument
        """Intentionally left blank."""
        pass

    def set(self, *args):  # pylint: disable=unused-argument
        """Intentionally left blank."""
        pass

    def timing(self, *args):  # pylint: disable=unused-argument
        """Intentionally left blank."""
        pass

    def delta(self, *args, **kwargs):  # pylint: disable=unused-argument
        """Intentionally left blank."""
        pass
