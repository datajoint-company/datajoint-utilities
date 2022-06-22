# dj-notification

Mechanism to send notifications based on log messages from DataJoint pipeline operation.

Supporting notification with
+ email
+ slack

Supporting custom log handler for `table.populate()` routines - notifying on:
+ start populating
+ success populating
+ error populating


# Usage


## Sending slack and email notifications on start/end status of DJ tables' populate routines

```

import os
import datajoint as dj

from datajoint_utilities.dj_notification.notifier.email_notifier import MailgunEmailNotifier
from datajoint_utilities.dj_notification.notifier.slack_notifier import SlackWebhookNotifier
from datajoint_utilities.dj_notification.loghandler import PopulateHandler

from workflow.pipeline import ephys

# create an email notifier with mailgun
email_notifier = MailgunEmailNotifier(mailgun_api_key=os.getenv('MAILGUN_API_KEY'),
                                      mailgun_domain_name=os.getenv('MAILGUN_DOMAIN_NAME'),
                                      sender_name='datajoint.com',
                                      sender_email='datajoint.info.io',
                                      receiver_emails=['thinh@datajoint.com', 'joseph@datajoint.com'])

# create two slack notifiers (to two different workspace/channel with different webhook urls)
dj_slack_notifier = SlackWebhookNotifier(webhook_url=os.getenv('DJ_SLACK_WEBHOOK_URL'))
project_slack_notifier = SlackWebhookNotifier(webhook_url=os.getenv('PROJECT_SLACK_WEBHOOK_URL'))

# create two log handlers
quiet_handler = PopulateHandler(notifiers=[email_notifier],
                                full_table_names=[ephys.EphysRecording.full_table_name,
                                                  ephys.CuratedClustering.full_table_name],
                                on_start=False, on_success=True, on_error=True)

verbose_handler = PopulateHandler(notifiers=[dj_slack_notifier, project_slack_notifier],
                                  full_table_names=[ephys.EphysRecording.full_table_name,
                                                    ephys.Clustering.full_table_name,
                                                    ephys.CuratedClustering.full_table_name,
                                                    ephys.WaveformSet.full_table_name],
                                  on_start=True, on_success=True, on_error=True)

# add the customer handlers into datajoint's autopopulate logger
logger = dj.logger
logger.setLevel('DEBUG')

quiet_handler.setLevel('DEBUG')
verbose_handler.setLevel('DEBUG')

logger.addHandler(quiet_handler)
logger.addHandler(verbose_handler)

```
