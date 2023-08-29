import requests
import json
import boto3

from . import Notifier


class MailgunEmailNotifier(Notifier):
    def __init__(
        self,
        mailgun_api_key,
        mailgun_domain_name,
        sender_name,
        sender_email,
        receiver_emails,
    ):
        self.auth = ("api", mailgun_api_key)
        self.request_url = f"https://api.mailgun.net/v3/{mailgun_domain_name}/messages"
        self.body = {
            "from": f"{sender_name} <{sender_email}>",
            "to": sender_email,
            "bcc": receiver_emails,
        }

    def notify(self, title, message, **kwargs):
        body = {**self.body, "subject": title, "text": message}
        response = requests.post(self.request_url, auth=self.auth, data=body)
        return response


class HubSpotTemplateEmailNotifier(Notifier):
    def __init__(
        self,
        hubspot_api_key,
        email_template_id,
        primary_recipient_email,
        cc_list=(),
        bcc_list=(),
    ):
        self.request_url = (
            "https://api.hubapi.com/marketing/v3/transactional/single-email/send"
        )
        self.headers = {
            "Authorization": f"Bearer {hubspot_api_key}",
            "Content-Type": "application/json",
        }
        self.body = {
            "emailId": email_template_id,
            "message": {
                "to": primary_recipient_email,
                "cc": cc_list,
                "bcc": bcc_list,
            },
        }

    def notify(self, title, message, **kwargs):
        if "_" in kwargs.get("schema_name", ""):
            # assuming the "schema_name" has a certain namespace hierarchy - seperated by "_"
            # retrieving the first and second level schema namespace
            schema_namespaces = kwargs["schema_name"].split("_")
            (
                kwargs["schema_namespace_0"],
                kwargs["schema_namespace_1"],
            ) = schema_namespaces[:2]
            if len(schema_namespaces) > 2:
                kwargs["schema_name"] = "_".join(schema_namespaces[2:])

        body = {**self.body, "customProperties": {**kwargs, "status_message": message}}
        response = requests.post(
            self.request_url, headers=self.headers, data=json.dumps(body, default=str)
        )
        return response


class SNSNotifier(Notifier):
    def __init__(
        self, aws_region, aws_access_key_id, aws_secret_access_key, target_arn
    ):
        self.aws_region = aws_region
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.target_arn = target_arn

    def notify(self, title, message, **kwargs):
        sns = boto3.client(
            "sns",
            region_name=self.aws_region,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )
        response = sns.publish(
            TargetArn=self.target_arn,
            Subject=(title),
            Message=(message),
        )
        return response
