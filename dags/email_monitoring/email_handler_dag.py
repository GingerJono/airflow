import json
import logging
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.models.dagrun import DagRun
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.microsoft.azure.operators.msgraph import MSGraphAsyncOperator
from html2text import html2text
from utilities.blob_storage_helper import (
    read_file_as_string,
    write_string_to_file,
)
from utilities.msgraph_helper import send_email
from utilities.open_ai_helper import get_llm_chat_response

NUM_RETRIES = 0
RETRY_DELAY_MINS = 3


MSGRAPH_CONNECTION_ID = "microsoft_graph"

BLOB_CONTAINER = "email-monitoring-data"

GRAPH_RESPONSE_FILENAME = "graph_message_response_raw"
EMAIL_BODY_FILENAME = "email_body_text"
LLM_RESPONSE_FILENAME = "llm_response"
EMAIL_RESPONSE_FILENAME = "email_response"

logger = logging.getLogger(__name__)


def save_email_content_to_blob_storage(context, result):
    logger.info("Saving email content to blob storage...")
    run_id = context["dag_run"].run_id

    email_id = result["id"]

    blob_path = f"{run_id}/{email_id}/{GRAPH_RESPONSE_FILENAME}"

    write_string_to_file(BLOB_CONTAINER, blob_path, json.dumps(result))

    logger.info("Email content saved to blob storage: %s", blob_path)
    return email_id


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

    This DAG is used to process notifications from MS Graph about
    the receipt of emails.
    """

    @task
    def get_email_ids(params: dict) -> [str]:
        email_ids = params["email_ids"]
        if not email_ids:
            raise AirflowFailException("No email IDs provided")
        return email_ids

    @task
    def get_graph_email_url(email_id: str) -> str:
        mailbox = Variable.get("email_monitoring_mailbox")

        url = f"users/{mailbox}/messages/{email_id}"
        logger.info(f"Received {email_id}, returning {url}")
        return url

    @task.branch
    def check_and_parse_email(email_id: str, dag_run: DagRun | None = None):
        """
        #### Check and Parse Email task

        This task reviews the MSGraph response containing the email.

        If the email was received from the mailbox which is monitored
        then no futher processing should be done, as this may an email
        sent by this DAG.

        Otherwise, the body of the email is retrieved and processed to
        plain text, before being saved to the storage account for the
        next task.
        """
        logger.info("Parsing email with id %s", email_id)

        run_id = dag_run.run_id

        msgraph_response = read_file_as_string(
            container_name=BLOB_CONTAINER,
            blob_name=f"{run_id}/{email_id}/{GRAPH_RESPONSE_FILENAME}",
        )

        logger.info("Got msgraph response")
        logger.debug(msgraph_response)

        msgraph_response = json.loads(msgraph_response)

        email_sender = msgraph_response["sender"]["emailAddress"]["address"]

        mailbox = Variable.get("email_monitoring_mailbox")
        if email_sender == mailbox:
            logger.info(
                "Email was sent from same mailbox, no futher processing required."
            )
            return "end"

        email_body_content = msgraph_response["body"]["content"]
        email_body_content_type = msgraph_response["body"]["contentType"]

        logger.debug("Email body content of type %s", email_body_content_type)

        parsed_email_content: str

        match email_body_content_type:
            case "text":
                parsed_email_content = email_body_content
            case "html":
                parsed_email_content = html2text(email_body_content)
            case _:
                logger.error(
                    "Unexpected email body type %s for email with id %s",
                    email_body_content_type,
                    email_id,
                )
                raise Exception("Unexpected email content type")

        logger.info(
            "Email content successfully parsed, writing parsed content to storage."
        )
        write_string_to_file(
            container_name=BLOB_CONTAINER,
            blob_name=f"{run_id}/{email_id}/{EMAIL_BODY_FILENAME}",
            string_data=parsed_email_content,
        )

        return "get_llm_response"

    @task
    def get_llm_response(email_id: str, dag_run: DagRun | None = None):
        """
        #### Get LLM Response task

        This task passes the email contents to an LLM for enrichment. It saves the response to blob storage.
        """
        logger.info("Getting LLM response for %s", email_id)

        run_id = dag_run.run_id

        email_body = read_file_as_string(
            container_name=BLOB_CONTAINER,
            blob_name=f"{run_id}/{email_id}/{EMAIL_BODY_FILENAME}",
        )

        logger.info("Retrieved email body")

        msgraph_response = json.loads(
            read_file_as_string(
                container_name=BLOB_CONTAINER,
                blob_name=f"{run_id}/{email_id}/{GRAPH_RESPONSE_FILENAME}",
            )
        )

        logger.info("Retrieved graph response")

        email_subject = msgraph_response["subject"]
        augmented_email_subject = f"[LLM Enriched] {email_subject}"

        llm_response = get_llm_chat_response(
            email_subject=email_subject, email_contents=email_body
        )
        augmented_email_body = f"{llm_response}<br/><hr><br/>{email_body}"

        email_object = {
            "subject": augmented_email_subject,
            "body": augmented_email_body,
        }

        email_object_path = f"{run_id}/{email_id}/{EMAIL_RESPONSE_FILENAME}"
        logger.info(
            "Would send email with title %s and body %s",
            augmented_email_subject,
            augmented_email_body,
        )

        write_string_to_file(
            container_name=BLOB_CONTAINER,
            blob_name=f"{run_id}/{email_id}/{LLM_RESPONSE_FILENAME}",
            string_data=llm_response,
        )

        write_string_to_file(
            container_name=BLOB_CONTAINER,
            blob_name=email_object_path,
            string_data=json.dumps(email_object),
        )

        return email_object_path

    @task
    def send_email_to_inbox(email_object_path: str):
        """
        #### Send Email task

        This task sends an email back to the inbox with the LLM enriched version of the email.
        """
        logging.info("Sending LLM enriched email")
        email_details = json.loads(
            read_file_as_string(BLOB_CONTAINER, email_object_path)
        )

        mailbox = Variable.get("email_monitoring_mailbox")

        logger.info("mailbox %s", mailbox)

        send_email(
            subject=email_details["subject"],
            html_content=email_details["body"],
            to_address=mailbox,
            sending_mailbox=mailbox,
        )

        return

    email_ids = get_email_ids()
    email_urls = get_graph_email_url.expand(email_id=email_ids)

    get_email_task = MSGraphAsyncOperator.partial(
        task_id="get_email",
        conn_id=MSGRAPH_CONNECTION_ID,
        result_processor=save_email_content_to_blob_storage,
    ).expand(url=email_urls)

    parse_email_task_instance = check_and_parse_email.expand(email_id=email_ids)

    get_email_task >> parse_email_task_instance
    email_object_paths = get_llm_response.expand(email_id=email_ids)

    (parse_email_task_instance >> email_object_paths)

    send_email_to_inbox.expand(email_object_path=email_object_paths)

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")
    parse_email_task_instance >> end


process_email_change_notifications()
