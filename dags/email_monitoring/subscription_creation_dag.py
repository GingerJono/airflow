import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label

NUM_RETRIES = 2
RETRY_DELAY_MINS = 3


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

    @task
    def check_subscription_exists() -> bool:
        logging.info("Checking if subscription exists")
        subscription_exists = True
        return subscription_exists

    @task.branch
    def branch(subscription_exists: bool):
        if subscription_exists:
            return "end"
        else:
            return "subscribe_to_mailbox"

    @task
    def subscribe_to_mailbox():
        logging.info("Creating subscription to mailbox")

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
