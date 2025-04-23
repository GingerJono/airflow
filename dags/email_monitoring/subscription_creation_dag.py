import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label
from utilities.email_subscription_helper import (
    create_mailbox_subscription,
    get_existing_subscriptions,
)

NUM_RETRIES = 2
RETRY_DELAY_MINS = 3

MSGRAPH_CONNECTION_ID = "microsoft_graph"


logger = logging.getLogger(__name__)


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

    It runs on a schedule, and if the subscription does not already exist will
    create it.
    """

    @task
    def check_subscription_exists() -> bool:
        """
        Check for existing subscription with the desired properties.

        Returns:
            bool: True if there is an existing subscription with the desired properties,
                  False otherwise.
        """
        mailbox = Variable.get("email_monitoring_mailbox")
        subscriptions = get_existing_subscriptions(mailbox)
        if subscriptions and len(subscriptions) > 0:
            logger.info("Subscription with desired properties already exists.")
            return True
        else:
            logger.info("No subscription with desired properties exists.")
            return False

    @task.branch
    def branch(subscription_exists: bool) -> str:
        """
        A branching task, which returns the id of the task which should run next,
        based on whether or not a subscription with the desired properties already
        exists.

        Args:
            subscription_exists (bool): Boolean value indicating whether there is
                                        an existing subscription

        Returns:
            str: The task id of the the next task which should run
        """
        if subscription_exists:
            logger.info("Subscription exists, no action required.")
            return "end"
        else:
            logger.info(
                "Subscription does not exist, new subscription should be created."
            )
            return "subscribe_to_mailbox"

    @task
    def subscribe_to_mailbox():
        """
        Creates MS Graph subscription to the mailbox, with the configured change
        and lifecycle notication URLs.
        """
        mailbox = Variable.get("email_monitoring_mailbox")

        create_mailbox_subscription(mailbox=mailbox)

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")

    branch_instance = branch(check_subscription_exists())

    branch_instance >> Label("Subscription exists") >> end
    (
        branch_instance
        >> Label("Subscription does not exist")
        >> subscribe_to_mailbox()
        >> end
    )


subscribe_to_mailbox()
