from . import Notifier
from slack_sdk.webhook import WebhookClient


class SlackWebhookNotifier(Notifier):

    def __init__(self, webhook_url):
        self.webhook = WebhookClient(webhook_url)

    def notify(self, title, message):
        self.webhook.send(text=f'# {title}\n{message}')
