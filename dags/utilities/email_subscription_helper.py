import logging

from airflow.models import Variable
from utilities.msgraph_helper import (
    create_subscription,
    get_mailbox_subscriptions,
    renew_subscription,
)

logger = logging.getLogger(__name__)

SUBSCRIPTION_DURATION_MIN = 8 * 60

client_state = Variable.get("email_monitoring_client_state")
change_notification_url = Variable.get("email_monitoring_apim_change_notification_url")
lifecycle_notification_url = Variable.get(
    "email_monitoring_apim_lifecycle_notification_url"
)


def check_subscription_properties(subscription: dict) -> bool:
    """
    Checks if an MSGraph subscription has details matching those
    desired.
    """
    logger.debug("Checking subscription %s", subscription)

    subscription_notification_url = subscription["notificationUrl"]
    subscription_lifecyle_notification_url = subscription["lifecycleNotificationUrl"]

    logger.debug(
        "Existing properties:\n\tnotificationUrl: %s\n\tlifecyleNotificationURl: %s",
        subscription_notification_url,
        subscription_lifecyle_notification_url,
    )

    logger.debug(
        "Desired properties:\n\tnotificationUrl: %s\n\tlifecyleNotificationURl: %s",
        change_notification_url,
        lifecycle_notification_url,
    )

    subscription_has_desired_properties = (
        subscription["notificationUrl"] == change_notification_url
        and subscription["lifecycleNotificationUrl"] == lifecycle_notification_url
    )

    logger.debug(
        "For subscription %s returning %s",
        subscription,
        subscription_has_desired_properties,
    )
    return subscription_has_desired_properties


def get_existing_subscriptions(mailbox: str) -> [object]:
    logger.info("Checking for existing subscriptions for mailbox %s", mailbox)

    result = get_mailbox_subscriptions(mailbox)

    logger.info("Processing MS Graph response to confirm subscription existence.")
    if len(result["value"]) == 0:
        logger.info("No subscription exists.")
        return False

    subscriptions = result["value"]

    logger.debug("Subscriptions received %s", result["value"])

    subscriptions = [
        subscription
        for subscription in subscriptions
        if check_subscription_properties(subscription)
    ]

    return subscriptions


def create_mailbox_subscription(mailbox: str):
    create_subscription(
        mailbox=mailbox,
        client_state=client_state,
        change_notification_url=change_notification_url,
        lifecycle_notification_url=lifecycle_notification_url,
        subscription_duration_minutes=SUBSCRIPTION_DURATION_MIN,
    )


def renew_mailbox_subscription(subscription_id: str):
    renew_subscription(
        subscription_id=subscription_id,
        subscription_duration_minutes=SUBSCRIPTION_DURATION_MIN,
    )
