import logging
from datetime import UTC, datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.microsoft.azure.operators.msgraph import MSGraphAsyncOperator
from airflow.utils.edgemodifier import Label

NUM_RETRIES = 0
RETRY_DELAY_MINS = 3

MSGRAPH_CONNECTION_ID = "microsoft_graph"

SUBSCRIPTION_DURATION_MIN = 8 * 60

logger = logging.getLogger(__name__)


def check_subscription_properties(subscription: dict) -> bool:
    """
    Checks if an MSGraph subscription has details matching those
    desired.
    """
    client_state = Variable.get("email_monitoring_client_state")
    change_notification_url = Variable.get(
        "email_monitoring_apim_change_notification_url"
    )
    lifecycle_notification_url = Variable.get(
        "email_monitoring_apim_lifecycle_notification_url"
    )
    return (
        subscription["clientState"] == client_state
        and subscription["notificationUrl"] == change_notification_url
        and subscription["lifecycleNotificationUrl"] == lifecycle_notification_url,
    )


def process_msgraph_subscription_response(context, result) -> bool:
    """
    Processes response from retrieving MSGraph subscription details,
     and returns boolean value indicating if a subscription exists.

    Returns:
        bool: Boolean indicating if there is an existing subscription

    """
    logger.info("Processing MS Graph response to confirm subscription existence.")
    if len(result["value"]) == 0:
        logger.info("No subscription exists.")
        return False

    subscriptions = result["value"]

    logger.debug("Subscriptions recieved %s", result["value"])

    subscriptions = [
        subscription
        for subscription in subscriptions
        if check_subscription_properties(subscription)
    ]

    if len(subscriptions) > 0:
        logger.info("Subscription with desired properties already exists.")
        return True
    else:
        logger.info("No subscription with desired properties exists.")
        return False


@dag(
    schedule="@daily",
    start_date=datetime(2025, 4, 1),
    max_active_runs=1,
    default_args={
        "depends_on_past": False,
        "retries": NUM_RETRIES,
        "retry_delay": timedelta(minutes=RETRY_DELAY_MINS),
    },
    tags=["email-monitoring", "scheduled"],
)
def subscribe_to_mailbox():
    """
    ### Subscribe to Mailbox

    This DAG is used to set up the subscription to an MS Graph Mailbox.
    It runs on a schedule, and if the subscription does not already exist will create it.
    """

    subscription_existence_task = MSGraphAsyncOperator(
        task_id="check_subscription_exists",
        conn_id=MSGRAPH_CONNECTION_ID,
        url="subscriptions",
        result_processor=process_msgraph_subscription_response,
    )

    mailbox = Variable.get("email_monitoring_mailbox")
    client_state = Variable.get("email_monitoring_client_state")
    change_notification_url = Variable.get(
        "email_monitoring_apim_change_notification_url"
    )
    lifecycle_notification_url = Variable.get(
        "email_monitoring_apim_lifecycle_notification_url"
    )
    subscription_duration = timedelta(minutes=SUBSCRIPTION_DURATION_MIN)

    subscription_creation_task = MSGraphAsyncOperator(
        task_id="subscribe_to_mailbox",
        conn_id=MSGRAPH_CONNECTION_ID,
        url="subscriptions",
        method="POST",
        data={
            "changeType": "created",
            "resource": f"/users/{mailbox}/mailFolders('Inbox')/messages",
            "expirationDateTime": (
                datetime.now(UTC) + subscription_duration
            ).isoformat(),
            "notificationUrl": change_notification_url,
            "lifecycleNotificationUrl": lifecycle_notification_url,
            "clientState": client_state,
            "includeResourceData": False,
        },
    )

    @task.branch
    def branch(subscription_exists: bool):
        if subscription_exists:
            logger.info("Subscription exists, no action required.")
            return "end"
        else:
            logger.info(
                "Subscription does not exist, new subscription should be created."
            )
            return "subscribe_to_mailbox"

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    subscription_exists = subscription_existence_task.output

    branch_instance = branch(subscription_exists)

    branch_instance >> Label("Subscription exists") >> end
    (
        branch_instance
        >> Label("Subscription does not exist")
        >> subscription_creation_task
        >> end
    )


subscribe_to_mailbox()
