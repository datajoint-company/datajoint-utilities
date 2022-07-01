from . import Notifier
import requests


class MailgunEmailNotifier(Notifier):

    def __init__(self, mailgun_api_key, mailgun_domain_name,
                 sender_name, sender_email, receiver_emails):
        self.auth = ('api', mailgun_api_key)
        self.request_url = f'https://api.mailgun.net/v3/{mailgun_domain_name}/messages'
        self.body = {
            "from": f"{sender_name} <{sender_email}>",
            "bcc": receiver_emails
        }

    def notify(self, title, message):
        body = {**self.body, 'subject': title, 'text': message}
        requests.post(self.request_url, auth=self.auth, data=body)
