import logging
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param

NUM_RETRIES = 2
RETRY_DELAY_MINS = 3


@dag(
    params={"email_ids": Param(["email-1", "email-2"], type="array")},
    default_args={
        "depends_on_past": False,
        "retries": NUM_RETRIES,
        "retry_delay": timedelta(minutes=RETRY_DELAY_MINS),
    },
    schedule=None,
    tags=["email-monitoring"],
)
def process_email_change_notifications():
    """
    ### Process Email Changes

    This DAG is used to process notifications from MS Graph about the receipt of emails.
    """

    @task
    def get_email_ids(params: dict):
        email_ids = params["email_ids"]
        if not email_ids:
            raise AirflowFailException("No email IDs provided")
        return email_ids

    @task
    def get_email(email_id: str):
        """
        #### Get Email task

        This task gets the details of an email from MS Graph, using the id. It then saves the contents of the email to blob storage.
        """

        logging.info(f"Getting email for {email_id}")
        return email_id

    @task
    def get_llm_response(email_id: str):
        """
        #### Get LLM Response task

        This task passes the email contents to an LLM for enrichment. It saves the response to blob storage.
        """

        logging.info(f"Getting LLM response for {email_id}")
        return email_id

    @task
    def send_email_to_inbox(email_id: str):
        """
        #### Send Email task

        This task sends an email back to the inbox with the LLM enriched version of the email.
        """
        logging.info(f"Sending email about {email_id}")
        return

    email_ids = get_email_ids()
    (
        get_email.expand(email_id=email_ids)
        >> get_llm_response.expand(email_id=email_ids)
        >> send_email_to_inbox.expand(email_id=email_ids)
    )


process_email_change_notifications()
