import json
import logging
import re
from collections import defaultdict
from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.models import Variable
from airflow.models.dagrun import DagRun
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from email_monitoring.cytora_api_status_sensor_operator import (
    CytoraApiStatusSensorOperator,
)
from helpers.cytora_helper import CYTORA_SCHEMA_MAIN, CYTORA_SCHEMA_SOV, CytoraHook
from helpers.cytora_mappings import CYTORA_OUTPUT_FIELD_MAP_MAIN
from helpers.utils import get_field_value
from utilities.blob_storage_helper import (
    read_file_as_bytes,
    write_bytes_to_file,
    write_string_to_file,
)
from utilities.msgraph_helper import (
    get_eml_file_from_email_id,
)

NUM_RETRIES = 2
RETRY_DELAY_MINS = 3


MONITORING_BLOB_CONTAINER = "email-monitoring-data"
OUTPUT_BLOB_CONTAINER = "cytora-output"

GRAPH_EMAIL_EML_FILE_RESPONSE_FILENAME = "graph_message_eml_response_raw"
MEDIA_TYPE = "message/rfc822"

MAIN_FULL_OUTPUTS_PREFIX = "outputs_main/full_output"
MAIN_EXTRACTED_OUTPUTS_PREFIX = "outputs_main/extracted_output"
SOV_FULL_OUTPUTS_PREFIX = "outputs_sov/full_output"
SOV_EXTRACTED_OUTPUTS_PREFIX = "outputs_sov/extracted_output"
CYTORA_MAIN_FILE_NAME = f"{GRAPH_EMAIL_EML_FILE_RESPONSE_FILENAME}_main"
CYTORA_SOV_FILE_NAME = f"{GRAPH_EMAIL_EML_FILE_RESPONSE_FILENAME}_sov"

TIMESTAMP_FORMAT_MILLISECONDS = "%Y%m%d%H%M%S%f"
DEFAULT_DATE_FORMAT = "%Y%m%d"

logger = logging.getLogger(__name__)


def upload_stream_to_cytora(
    email_id: str, media_type: str, cytora_instance: CytoraHook, dag_run_id: str = None
):
    file_bytes = read_file_as_bytes(
        container_name=MONITORING_BLOB_CONTAINER,
        blob_name=f"{dag_run_id}/{email_id}/{GRAPH_EMAIL_EML_FILE_RESPONSE_FILENAME}",
    )
    upload_url, upload_id = cytora_instance.get_presigned_url()
    status = cytora_instance.upload_file(
        upload_url, file_stream=file_bytes, content_type=media_type
    )
    return status, upload_id


def start_cytora_job(
    cytora_instance: CytoraHook, upload_id: str, file_name: str, media_type: str
):
    file_id = cytora_instance.create_file(upload_id, file_name, media_type)
    job_name = f"API {datetime.now().strftime(DEFAULT_DATE_FORMAT)}: {file_name}"
    job_id = cytora_instance.create_schema_job(file_id, job_name)
    return job_id


def save_cytora_output_to_blob_storage(output: dict, key_prefix: str, job_id: str):
    ts = datetime.now().strftime(TIMESTAMP_FORMAT_MILLISECONDS)[:-3]
    output["TimeProcessed"] = ts
    key = f"{key_prefix}/{ts}_{job_id}.json"
    output_json = json.dumps(output, indent=2)
    write_string_to_file(OUTPUT_BLOB_CONTAINER, key, output_json)
    return key


def extract_main_outputs(output: dict):
    def get_output_value_from_key(key):
        return get_field_value(output, key)

    extracted_outputs = {}

    try:
        for output_key, (
            input_key,
            transform_fn,
        ) in CYTORA_OUTPUT_FIELD_MAP_MAIN.items():
            extracted_outputs[output_key] = transform_fn(
                get_output_value_from_key(input_key)
            )
        extracted_outputs["job_id"] = output["job_id"]
    except Exception as e:
        raise AirflowException(f"Failed to extract outputs: {e}")

    return extracted_outputs


def get_missing_mapping_keys(cytora_instance: CytoraHook) -> set[str]:
    """
    Return the set of mapping keys that are not present in the required
    output fields of the Cytora schema.

    Mapping keys are taken from the first element of each tuple in
    CYTORA_OUTPUT_FIELD_MAP_MAIN.values().
    """

    mapping_keys = [value[0] for value in CYTORA_OUTPUT_FIELD_MAP_MAIN.values()]
    required_output_fields = cytora_instance.get_schema_required_output_fields()
    return set(mapping_keys) - set(required_output_fields)


def extract_sov_outputs(output: dict):
    sov_fields = output.get("fields", {}) or {}
    sov_rows = extract_sov_from_flat_fields(sov_fields)

    return sov_rows


def extract_sov_from_flat_fields(fields: dict):
    row_map = defaultdict(dict)
    pattern = re.compile(r"^property_list\.property_details\[(\d+)\]\.(.+)$")
    for key, field_obj in fields.items():
        m = pattern.match(key)
        if not m:
            continue
        idx = int(m.group(1))
        col = m.group(2)
        if isinstance(field_obj, dict):
            row_map[idx][col] = field_obj.get("value")
        else:
            row_map[idx][col] = field_obj
    return [row_map[i] for i in sorted(row_map.keys())]


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
    the receipt of emails. The email content and attachment is uploaded to Cytora
    for processing, and the resulting output is saved in Azure Blob Storage.
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
    def download_email_eml_file(email_id: str, dag_run: DagRun | None = None):
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

        email_blob_path = (
            f"{run_id}/{email_id}/{GRAPH_EMAIL_EML_FILE_RESPONSE_FILENAME}"
        )

        try:
            write_bytes_to_file(MONITORING_BLOB_CONTAINER, email_blob_path, result)
        except Exception as e:
            raise AirflowException(
                f"Failed to save eml file for email {email_id} to blob storage: {e}"
            )

        logger.info("Email eml file saved to blob storage: %s", email_blob_path)

    @task_group(group_id="main_flow")
    def run_main_flow(email_ids: list[str]):
        """
        TaskGroup: main_flow

        This task group includes the tasks for the cytora main flow.
        Including uploading the eml files to Cytora,
        starting the main schema job, waiting for job completion,
        and saving the processed output to blob storage.
        """

        @task
        def start_cytora_main_job(email_id: str, dag_run: DagRun | None = None):
            """
            Uploads an email EML file from blob storage to Cytora for processing, and starts the cytora main job.

            Args:
                email_id (str): The ID of the email whose EML file should be uploaded.
            """
            run_id = dag_run.run_id
            cytora_main = CytoraHook(CYTORA_SCHEMA_MAIN)

            missing_mapping_keys = get_missing_mapping_keys(cytora_instance=cytora_main)

            if missing_mapping_keys:
                raise AirflowFailException(
                    f"Invalid Cytora field mapping: missing keys {sorted(missing_mapping_keys)}. "
                    f"Update the schema or CYTORA_OUTPUT_FIELD_MAP_MAIN."
                )

            status, upload_id = upload_stream_to_cytora(
                email_id=email_id,
                media_type=MEDIA_TYPE,
                cytora_instance=cytora_main,
                dag_run_id=run_id,
            )
            if status != 200:
                raise AirflowException(
                    f"Failed to upload file to Cytora with HTTP status: {status}"
                )

            try:
                main_job_id = start_cytora_job(
                    cytora_instance=cytora_main,
                    upload_id=upload_id,
                    file_name=CYTORA_MAIN_FILE_NAME,
                    media_type=MEDIA_TYPE,
                )
            except Exception as e:
                raise AirflowException(f"Failed to start Cytora main job: {e}")

            logger.info(f"Starting cytora main job with id {main_job_id}")

            return main_job_id

        @task
        def save_cytora_main_job_output(job_id: str):
            """
            Retrieve Cytora output and saves the result to blob storage.

            Args:
                job_id (str): The Cytora job ID to fetch output for.
            """
            cytora_main = CytoraHook(CYTORA_SCHEMA_MAIN)
            output = cytora_main.get_result_for_schema_job(job_id)

            if not output:
                raise AirflowFailException(
                    f"No output returned for Cytora job {job_id}. Status may be errored or under human review."
                )

            try:
                full_output_key = save_cytora_output_to_blob_storage(
                    output=output, key_prefix=MAIN_FULL_OUTPUTS_PREFIX, job_id=job_id
                )
            except Exception as e:
                raise AirflowException(
                    f"Failed to save full output for job {job_id} to blob storage: {e}"
                )

            extracted_output = extract_main_outputs(output)
            try:
                extracted_output_key = save_cytora_output_to_blob_storage(
                    output=extracted_output,
                    key_prefix=MAIN_EXTRACTED_OUTPUTS_PREFIX,
                    job_id=job_id,
                )
            except Exception as e:
                raise AirflowException(
                    f"Failed to save extracted output for job {job_id} to blob storage: {e}"
                )

            return full_output_key, extracted_output_key

        cytora_main_job_ids = start_cytora_main_job.expand(email_id=email_ids)
        main_output_keys = save_cytora_main_job_output.expand(
            job_id=cytora_main_job_ids
        )
        wait_for_main_job = CytoraApiStatusSensorOperator.partial(
            task_id="wait_for_main_cytora_api_status",
            cytora_schema=CYTORA_SCHEMA_MAIN,
        ).expand(job_id=cytora_main_job_ids)

        cytora_main_job_ids >> wait_for_main_job >> main_output_keys

    @task_group(group_id="sov_flow")
    def run_sov_flow(email_ids: list[str]):
        """
        TaskGroup: sov_flow

        This task group includes the tasks for the cytora sov flow.
        Including uploading the eml files to Cytora,
        starting the sov schema job, waiting for job completion,
        and saving the processed output to blob storage.
        """

        @task
        def start_cytora_sov_job(email_id: str, dag_run: DagRun | None = None):
            """
            Uploads an email EML file from blob storage to Cytora for processing, and starts the cytora sov job.

            Args:
                email_id (str): The ID of the email whose EML file should be uploaded.
            """
            run_id = dag_run.run_id
            cytora_sov = CytoraHook(CYTORA_SCHEMA_SOV)
            status, upload_id = upload_stream_to_cytora(
                email_id=email_id,
                media_type=MEDIA_TYPE,
                cytora_instance=cytora_sov,
                dag_run_id=run_id,
            )
            if status != 200:
                raise AirflowException(
                    f"Failed to upload file to Cytora with HTTP status: {status}"
                )

            try:
                sov_job_id = start_cytora_job(
                    cytora_instance=cytora_sov,
                    upload_id=upload_id,
                    file_name=CYTORA_SOV_FILE_NAME,
                    media_type=MEDIA_TYPE,
                )
            except Exception as e:
                raise AirflowException(f"Failed to start Cytora sov job: {e}")

            logger.info(f"Starting cytora sov job with id {sov_job_id}")

            return sov_job_id

        @task
        def save_cytora_sov_job_output(job_id: str):
            """
            Retrieve Cytora output and saves the result to blob storage.

            Args:
                job_id (str): The Cytora job ID to fetch output for.
            """
            cytora_sov = CytoraHook(CYTORA_SCHEMA_SOV)
            output = cytora_sov.get_result_for_schema_job(job_id)

            if not output:
                raise AirflowFailException(
                    f"No output returned for Cytora job {job_id}. Status may be errored or under human review."
                )

            try:
                full_output_key = save_cytora_output_to_blob_storage(
                    output=output, key_prefix=SOV_FULL_OUTPUTS_PREFIX, job_id=job_id
                )
            except Exception as e:
                raise AirflowException(
                    f"Failed to save full output for job {job_id} to blob storage: {e}"
                )

            extracted_output_list = extract_sov_outputs(output)
            extracted_output_dict = {
                f"sov_row_{i + 1}": row for i, row in enumerate(extracted_output_list)
            }

            if not extracted_output_dict:
                return full_output_key, None

            try:
                extracted_output_key = save_cytora_output_to_blob_storage(
                    output=extracted_output_dict,
                    key_prefix=SOV_EXTRACTED_OUTPUTS_PREFIX,
                    job_id=job_id,
                )
            except Exception as e:
                raise AirflowException(
                    f"Failed to save extracted output for job {job_id} to blob storage: {e}"
                )

            return full_output_key, extracted_output_key

        cytora_sov_job_ids = start_cytora_sov_job.expand(email_id=email_ids)
        sov_output_keys = save_cytora_sov_job_output.expand(job_id=cytora_sov_job_ids)
        wait_for_sov_job = CytoraApiStatusSensorOperator.partial(
            task_id="wait_for_sov_cytora_api_status",
            cytora_schema=CYTORA_SCHEMA_SOV,
        ).expand(job_id=cytora_sov_job_ids)

        cytora_sov_job_ids >> wait_for_sov_job >> sov_output_keys

    email_ids = get_email_ids()

    email_eml_file_task_instance = download_email_eml_file.expand(email_id=email_ids)
    cytora_main_flow = run_main_flow(email_ids=email_ids)
    cytora_sov_flow = run_sov_flow(email_ids=email_ids)

    (email_ids >> email_eml_file_task_instance >> [cytora_main_flow, cytora_sov_flow])

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")
    [cytora_main_flow, cytora_sov_flow] >> end


process_email_change_notifications()
