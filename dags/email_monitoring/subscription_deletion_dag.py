import logging
from datetime import timedelta

from airflow.decorators import dag
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.providers.microsoft.azure.operators.msgraph import MSGraphAsyncOperator

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


def process_msgraph_subscription(context, result) -> str | None:
    logger.info("Processing MS Graph response to confirm subscription existence.")
    if len(result["value"]) == 0:
        raise AirflowFailException("There is no existing subscription to delete.")

    subscriptions = result["value"]

    logger.info("Subscriptions receieved %s", result["value"])

    subscriptions = [
        subscription
        for subscription in subscriptions
        if check_subscription_properties(subscription)
    ]

    logger.info("Filtered subscriptions %s", subscriptions)

    if len(subscriptions) != 1:
        raise AirflowFailException(
            "Found %i matching subscriptions, expected 1", len(subscriptions)
        )

    return subscriptions[0]["id"]


@dag(
    default_args={
        "depends_on_past": False,
        "retries": NUM_RETRIES,
        "retry_delay": timedelta(minutes=RETRY_DELAY_MINS),
    },
    schedule=None,
    tags=["email-monitoring"],
)
def delete_subscription():
    """
    ### Delete Subscription

    This DAG can be used to delete the subscription between airflow and the mailbox.
    It is not run on a schedule, and will only run if triggered manually from airflow.
    """

    subscription_existence_task = MSGraphAsyncOperator(
        task_id="check_subscription_exists",
        conn_id=MSGRAPH_CONNECTION_ID,
        url="subscriptions",
        result_processor=process_msgraph_subscription,
    )

    subscription_id = subscription_existence_task.output

    MSGraphAsyncOperator(
        task_id="delete_subscription",
        conn_id=MSGRAPH_CONNECTION_ID,
        url="subscriptions/{subscription_id}",
        path_parameters={"subscription_id": subscription_id},
        method="DELETE",
    )


delete_subscription()
