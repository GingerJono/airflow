import logging
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label

NUM_RETRIES = 2
RETRY_DELAY_MINS = 3


@dag(
    params={
        "notifications": Param(
            [{"lifecycle_action": "unset", "subscription_id": "unset"}],
            type="array",
            items={
                "type": "object",
                "properties": {
                    "lifecycle_action": {"type": "string"},
                    "subscription_id": {"type": "string"},
                },
            },
        )
    },
    schedule=None,
    default_args={
        "depends_on_past": False,
        "retries": NUM_RETRIES,
        "retry_delay": timedelta(minutes=RETRY_DELAY_MINS),
    },
    tags=["email-monitoring"],
)
def process_lifecycle_notifications():
    """
    ### Process Lifecycle Notifications

      This DAG is used to process notifications from MS Graph about the lifecycle of the email subscription.
    """

    @task
    def check_params(params: dict):
        notifications = params["notifications"]
        if not notifications:
            raise AirflowFailException("DAG parameters not correctly specified")
        return notifications

    @task
    def renew_subscription(notification: dict[str, str]):
        subscription_id = notification["subscription_id"]
        logging.info(f"Renewing subscription {subscription_id}")
        return subscription_id

    @task
    def log_unhandled_action(notification: dict[str, str]):
        lifecycle_action = notification["lifecycle_action"]
        logging.warning(f"Unhandled lifecycle action {lifecycle_action}")

    @task.branch
    def branch(notification: dict[str, str]):
        lifecycle_action = notification["lifecycle_action"]

        # TODO: Handle 'subscriptionRemoved' and 'missed' lifecycle notifications
        # see: https://learn.microsoft.com/en-us/graph/change-notifications-lifecycle-events
        if lifecycle_action == "reauthorizationRequired":
            return "renew_subscription"
        else:
            return "log_unhandled_action"

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")
    notifications = check_params()
    branch_instance = branch.expand(notification=notifications)

    (
        branch_instance
        >> Label("reauthorizationRequired")
        >> renew_subscription.expand(notification=notifications)
        >> end
    )
    (
        branch_instance
        >> Label("Action not recognised")
        >> log_unhandled_action.expand(notification=notifications)
        >> end
    )


process_lifecycle_notifications()
