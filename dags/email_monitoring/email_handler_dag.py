import json
import logging
from datetime import timedelta
import markdown as md

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.models.dagrun import DagRun
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from html2text import html2text
from utilities.blob_storage_helper import (
    read_file_as_string,
    write_string_to_file,
)
from utilities.email_attachments_helper import get_attachments_text
from utilities.msgraph_helper import (
    get_attachments_from_email_id,
    get_email_from_id,
    send_email,
)
from utilities.open_ai_helper import get_llm_chat_response

NUM_RETRIES = 2
RETRY_DELAY_MINS = 3


BLOB_CONTAINER = "email-monitoring-data"

GRAPH_EMAIL_RESPONSE_FILENAME = "graph_message_response_raw"
GRAPH_ATTACHMENTS_RESPONSE_FILENAME = "graph_attachments_response_raw"
LLM_RESPONSE_FILENAME = "llm_response"
EMAIL_RESPONSE_FILENAME = "email_response"

logger = logging.getLogger(__name__)


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
    def get_email_ids(params: dict) -> list[str]:
        """
        Gets the list of email ids from the DAG parameters

        Returns:
            lists[str]: Array of email ids that need handling
        """
        email_ids = params["email_ids"]
        if not email_ids:
            raise AirflowFailException("No email IDs provided")
        return email_ids

    @task
    def get_email_content(email_id: str, dag_run: DagRun | None = None):
        """
        Retrieves the content of an email from MS Graph, and saves the raw response
        to Azure Blob Storage.

        Args:
            email_id (str): The id of the email to retrieve from MS Graph
        """
        logger.info("Retrieving email with id %s", email_id)

        mailbox = Variable.get("email_monitoring_mailbox")
        result = get_email_from_id(email_id=email_id, mailbox=mailbox)

        logger.info("Saving email content to blob storage...")
        run_id = dag_run.run_id

        email_blob_path = f"{run_id}/{email_id}/{GRAPH_EMAIL_RESPONSE_FILENAME}"

        write_string_to_file(BLOB_CONTAINER, email_blob_path, json.dumps(result))

        logger.info("Email content saved to blob storage: %s", email_blob_path)

    @task
    def get_email_attachments(email_id: str, dag_run: DagRun | None = None):
        """
        Retrieves the content of files attached to an email from MS Graph,
        and saves the raw response to Azure Blob Storage.

        Args:
            email_id (str): The id of the email to retrieve the attachments
            of from MS Graph
        """
        run_id = dag_run.run_id

        msgraph_response = json.loads(
            read_file_as_string(
                container_name=BLOB_CONTAINER,
                blob_name=f"{run_id}/{email_id}/{GRAPH_EMAIL_RESPONSE_FILENAME}",
            )
        )

        if msgraph_response["hasAttachments"]:
            logger.info("Retrieving attachments for email with id %s", email_id)
            mailbox = Variable.get("email_monitoring_mailbox")
            attachments = get_attachments_from_email_id(
                email_id=email_id, mailbox=mailbox
            )

            logger.info("Saving email attachments to blob storage...")
            attachments_blob_path = (
                f"{run_id}/{email_id}/{GRAPH_ATTACHMENTS_RESPONSE_FILENAME}"
            )
            write_string_to_file(
                BLOB_CONTAINER, attachments_blob_path, json.dumps(attachments)
            )

            logger.info(
                "Email attachments saved to blob storage blob: %s",
                f"{attachments_blob_path}",
            )

    @task.branch
    def check_and_parse_email(email_id: str, dag_run: DagRun | None = None):
        """
        Processes the MSGraph response containing the email.

        If the email was received from the mailbox which is monitored
        then no further processing should be done, as this may an email
        sent by this DAG.

        Otherwise, the body of the email is retrieved and processed to
        plain text, before being saved to the storage account for the
        next task.

        Args:
            email_id (str): The id of the email to retrieve from MS Graph
        """
        logger.info("Parsing email with id %s", email_id)

        run_id = dag_run.run_id

        msgraph_response = read_file_as_string(
            container_name=BLOB_CONTAINER,
            blob_name=f"{run_id}/{email_id}/{GRAPH_EMAIL_RESPONSE_FILENAME}",
        )

        logger.debug(msgraph_response)

        msgraph_response = json.loads(msgraph_response)

        email_sender = msgraph_response["sender"]["emailAddress"]["address"]

        mailbox = Variable.get("email_monitoring_mailbox")
        if email_sender == mailbox:
            logger.info(
                "Email was sent from same mailbox, no futher processing required."
            )
            return "end"

        email_body_content_type = msgraph_response["body"]["contentType"]

        logger.debug("Email body content of type %s", email_body_content_type)

        if email_body_content_type != "text" and email_body_content_type != "html":
            logger.error(
                "Unexpected email body type %s for email with id %s",
                email_body_content_type,
                email_id,
            )
            raise Exception("Unexpected email content type")

        return "get_llm_response"

    @task
    def get_llm_response(email_id: str, dag_run: DagRun | None = None):
        """
        Passes email contents to an LLM for enrichment, then saves the response to blob storage.

        Args:
            email_id (str): The id of the email to retrieve from MS Graph
        """
        logger.info("Getting LLM response for %s", email_id)

        run_id = dag_run.run_id

        base_path = f"{run_id}/{email_id}"

        msgraph_response = json.loads(
            read_file_as_string(
                container_name=BLOB_CONTAINER,
                blob_name=f"{base_path}/{GRAPH_EMAIL_RESPONSE_FILENAME}",
            )
        )

        logger.info("Retrieved graph response")

        attachments_text = []
        attachments_for_email = []
        if msgraph_response["hasAttachments"]:
            msgraph_attachments_response = json.loads(
                read_file_as_string(
                    container_name=BLOB_CONTAINER,
                    blob_name=f"{base_path}/{GRAPH_ATTACHMENTS_RESPONSE_FILENAME}",
                )
            )

            attachments_text = get_attachments_text(msgraph_attachments_response)

            attachments_for_email = [
                {
                    "@odata.type": attachment["@odata.type"],
                    "contentType": attachment["contentType"],
                    "contentBytes": attachment["contentBytes"],
                    "contentId": attachment["contentId"],
                    "name": attachment["name"],
                    "isInline": attachment["isInline"],
                }
                for attachment in msgraph_attachments_response["value"]
            ]

        logger.info("Retrieved email attachments")

        email_subject = msgraph_response["subject"]
        augmented_email_subject = f"[LLM Enriched] {email_subject}"

        email_body = msgraph_response["body"]["content"]
        email_body_content_type = msgraph_response["body"]["contentType"]
        parsed_email_body = (
            html2text(email_body) if email_body_content_type == "html" else email_body
        )

        llm_response = get_llm_chat_response(
            email_subject=email_subject,
            email_contents=parsed_email_body,
            attachments_text=attachments_text,
        )
        augmented_email_body = f"{md.markdown(llm_response)}<br/><hr><br/>{email_body}"

        email_object = {
            "subject": augmented_email_subject,
            "body": augmented_email_body,
            "attachments": attachments_for_email,
        }

        email_object_path = f"{run_id}/{email_id}/{EMAIL_RESPONSE_FILENAME}"

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
        Sends an email back to the inbox with the LLM enriched version of the email.

        Args:
            email_object_path (str): The path to the blob containing details
                                     of the email to be sent
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
            attachments=email_details["attachments"],
        )

        return

    email_ids = get_email_ids()

    email_content_task_instance = get_email_content.expand(email_id=email_ids)

    email_attachments_task_instance = get_email_attachments.expand(email_id=email_ids)

    parse_email_task_instance = check_and_parse_email.expand(email_id=email_ids)

    email_object_paths = get_llm_response.expand(email_id=email_ids)

    (
        email_content_task_instance
        >> email_attachments_task_instance
        >> parse_email_task_instance
        >> email_object_paths
    )

    send_email_to_inbox.expand(email_object_path=email_object_paths)

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")
    parse_email_task_instance >> end


process_email_change_notifications()
