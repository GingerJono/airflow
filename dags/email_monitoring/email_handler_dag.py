import json
import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.models import Variable
from airflow.models.dagrun import DagRun
from airflow.models.param import Param
from utilities.blob_storage_helper import (
    read_file_as_bytes,
    write_bytes_to_file,
    write_string_to_file,
)
from utilities.cytora_helper import CytoraHook
from utilities.msgraph_helper import (
    get_eml_file_from_email_id,
)

NUM_RETRIES = 2
RETRY_DELAY_MINS = 3


BLOB_CONTAINER = "email-monitoring-data"
OUTPUT_BLOB_CONTAINER = "cytora-output"

GRAPH_EMAIL_RESPONSE_FILENAME = "graph_message_response_raw"
GRAPH_EMAIL_EML_FILE_RESPONSE_FILENAME = "graph_message_eml_response_raw"
GRAPH_ATTACHMENTS_RESPONSE_FILENAME = "graph_attachments_response_raw"
LLM_RESPONSE_FILENAME = "llm_response"
EMAIL_RESPONSE_FILENAME = "email_response"
MEDIA_TYPE = "message/rfc822"
SCHEMA_MAIN = "ds:cfg:wr2pxXtxctBgFaZP"

OUTPUTS_PREFIX = "outputs"

logger = logging.getLogger(__name__)


def upload_stream_to_cytora(email_id: str, media_type: str, cytora_instance: CytoraHook, dag_run_id: str = None):
    file_bytes = read_file_as_bytes(
        container_name=BLOB_CONTAINER,
        blob_name=f"{dag_run_id}/{email_id}/{GRAPH_EMAIL_EML_FILE_RESPONSE_FILENAME}",
    )
    upload_url, upload_id = cytora_instance.get_presigned_url()
    status = cytora_instance.upload_file(upload_url, file_stream=file_bytes, content_type=media_type)
    return status, upload_id

def start_cytora_job(cytora_instance: CytoraHook, upload_id: str, file_name: str, media_type: str):
    file_id = cytora_instance.create_file(upload_id, file_name, media_type)
    job_name = f"API {datetime.now().strftime('%Y%m%d')}: {file_name}"
    job_id = cytora_instance.create_schema_job(file_id, job_name)
    return job_id

def save_cytora_output_to_blob_storage(output: dict, key_prefix:str):
    ts = datetime.now().strftime('%Y%m%d%H%M%S%f')[:-3]
    output["TimeProcessed"] = ts
    key = f"{key_prefix}/{ts}_{output['job_id']}.json"
    output_json = json.dumps(output, indent=2)
    write_string_to_file(OUTPUT_BLOB_CONTAINER, key, output_json)
    return key

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
    def get_email_eml_file(email_id: str, dag_run: DagRun | None = None):
        """
        Retrieves the eml file of an email from MS Graph, and saves the raw response
        to Azure Blob Storage.

        Args:
            email_id (str): The id of the email to retrieve from MS Graph
        """
        logger.info("Retrieving email with id %s", email_id)

        mailbox = Variable.get("email_monitoring_mailbox")
        result = get_eml_file_from_email_id(email_id=email_id, mailbox=mailbox)

        logger.info("Saving email eml file to blob storage...")
        run_id = dag_run.run_id

        email_blob_path = f"{run_id}/{email_id}/{GRAPH_EMAIL_EML_FILE_RESPONSE_FILENAME}"

        try:
            write_bytes_to_file(BLOB_CONTAINER, email_blob_path, result)
        except Exception as e:
            raise AirflowException(f"Failed to save eml file for email {email_id} to blob storage: {e}")

        logger.info("Email eml file saved to blob storage: %s", email_blob_path)

    @task
    def upload_file_for_cytora_main_job(email_id: str, dag_run: DagRun | None = None):
        """
        Uploads an email EML file from blob storage to Cytora for processing.

        Args:
            email_id (str): The ID of the email whose EML file should be uploaded.
        """
        run_id = dag_run.run_id
        cytora_main = CytoraHook(SCHEMA_MAIN)
        status, upload_id = upload_stream_to_cytora(email_id=email_id, media_type=MEDIA_TYPE, cytora_instance = cytora_main, dag_run_id=run_id)
        if status != 200:
            raise AirflowException(f"Failed to upload file to Cytora with HTTP status: {status}")

        return upload_id

    @task
    def start_cytora_main_job(upload_id: str):

        """
        Starts a Cytora schema job using a previously uploaded file.

        Args:
            upload_id (str): The ID of the uploaded file in Cytora.
        """

        cytora_main = CytoraHook(SCHEMA_MAIN)

        try:
            main_job_id = start_cytora_job(cytora_instance=cytora_main, upload_id=upload_id, file_name=GRAPH_EMAIL_EML_FILE_RESPONSE_FILENAME, media_type=MEDIA_TYPE)
        except Exception as e:
            raise AirflowException(f"Failed to start Cytora main job: {e}")

        if not main_job_id:
            raise AirflowFailException("No main job id provided.")

        logger.info(f"Starting cytora main job with id {main_job_id}")

        return main_job_id

    @task
    def save_cytora_job_output(job_id: str, ):
        """
        Waits for a Cytora job to complete, retrieves its output, and saves the result to blob storage.

        Args:
            job_id (str): The Cytora job ID to poll and fetch output for.
        """
        cytora_main = CytoraHook(SCHEMA_MAIN)
        output = cytora_main.wait_for_schema_job(job_id)

        if not output:
            raise AirflowFailException(f"No output returned for Cytora job {job_id}. Status may be errored or under human review.")

        try:
            key = save_cytora_output_to_blob_storage(output=output, key_prefix=OUTPUTS_PREFIX)
        except Exception as e:
            raise AirflowException(f"Failed to save output for job {job_id} to blob storage: {e}")

        return key

    email_ids = get_email_ids()

    email_eml_file_task_instance = get_email_eml_file.expand(email_id=email_ids)
    cytora_upload_ids = upload_file_for_cytora_main_job.expand(email_id=email_ids)
    cytora_main_job_ids = start_cytora_main_job.expand(upload_id=cytora_upload_ids)
    cytora_output_keys = save_cytora_job_output.expand(job_id=cytora_main_job_ids)

    (
        email_eml_file_task_instance
        >> cytora_upload_ids
        >> cytora_main_job_ids
        >> cytora_output_keys
    )


process_email_change_notifications()
