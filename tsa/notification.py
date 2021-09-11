from tsa.robots import session
from tsa.settings import Config


def message_to_mattermost(message: str) -> None:
    """Sends a message to mattermost.

    Channel and server hook are taken from settings.
    """
    if len(message) > 0:
        channel = Config.MATTERMOST_CHANNEL
        hook = Config.MATTERMOST_HOOK

        payload = { "channel": channel, "text": message }
        session.post(hook, json=payload).raise_for_status()
