import rfc3987

from tsa.robots import session
from tsa.settings import Config


def test_iri(iri: str) -> bool:
    """Checks if the provided iri is a valid http or https IRI."""
    return iri is not None and rfc3987.match(iri) and (iri.startswith('http://') or iri.startswith('https://'))


def message_to_mattermost(message: str) -> None:
    """Sends a message to mattermost.

    Channel and server hook are taken from settings.
    """
    if len(message) > 0:
        channel = Config.MATTERMOST_CHANNEL
        hook = Config.MATTERMOST_HOOK

        payload = { "channel": channel, "text": message }
        session.post(hook, json=payload).raise_for_status()
