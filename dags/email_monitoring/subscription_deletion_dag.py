import logging
from datetime import timedelta

import utilities.email_subscription_helper as email_subscription_helper
import utilities.msgraph_helper as msgraph_helper
from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable

NUM_RETRIES = 2
RETRY_DELAY_MINS = 3


logger = logging.getLogger(__name__)


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

    @task
    def get_subscription_id_if_exists() -> str:
        """
        Get the id of existing subscription.

        Fails if there is no existing subscription, or if more than one subscription
        with the desired properties exists.

        Returns:

            str: id of subscription
        """
        mailbox = Variable.get("email_monitoring_mailbox")
        subscriptions = email_subscription_helper.get_existing_subscriptions(mailbox)

        if len(subscriptions) == 0:
            raise AirflowFailException("There is no existing subscription to delete.")
        elif len(subscriptions) != 1:
            raise AirflowFailException(
                "Found %i matching subscriptions, expected 1", len(subscriptions)
            )

        return subscriptions[0]["id"]

    @task
    def delete_subscription(subscription_id: str):
        """
        Delete subscription based on id.

        Args:
            subscription_id (str): The id of the subscription to delete
        """
        msgraph_helper.delete_subscription(subscription_id)

    delete_subscription(subscription_id=get_subscription_id_if_exists())


delete_subscription()
