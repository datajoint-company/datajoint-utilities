from . import Notifier
from slack_sdk.webhook import WebhookClient


class SlackWebhookNotifier(Notifier):
    def __init__(self, webhook_url):
        self.webhook = WebhookClient(webhook_url)

    def notify(self, title, message, blocks=None, **kwargs):
        """
        Send a slack notification with the given title and message.
        If blocks is provided, it will be used for the notification. Title and message will be ignored.
        See https://api.slack.com/reference/block-kit/blocks for more information on Slack blocks.
        """
        try:
            if blocks is not None:
                res = self.webhook.send(blocks=blocks)
            else:
                res = self.webhook.send(text=f"#{title}\n```{message}```")
        except Exception as e:
            logger.error(f"Failed to send slack notification: {e}")
        else:
            if res.status_code != 200:
                logger.error(f"Failed to send slack notification: {res.body}")


def create_slack_image_block(image_title, image_url):
    """
    Create a Slack block for displaying an image with url.
    For more information, see https://api.slack.com/reference/block-kit/blocks#image
    """
    return {
        "type": "image",
        "title": {"type": "plain_text", "text": image_title, "emoji": False},
        "image_url": image_url,
        "alt_text": image_title,
    }