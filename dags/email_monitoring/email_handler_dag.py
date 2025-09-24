import json
import logging
import mimetypes
import re
from collections import defaultdict
from datetime import datetime, timedelta, UTC

from airflow.decorators import dag, task, task_group
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from email_monitoring.cytora_api_status_sensor_operator import (
    CytoraApiStatusSensorOperator,
)
from helpers.cytora_helper import (
    CYTORA_SCHEMA_MAIN,
    CYTORA_SCHEMA_RENEWAL,
    CYTORA_SCHEMA_SOV,
    CytoraHook,
)
from helpers.cytora_mappings import CYTORA_OUTPUT_FIELD_MAP_MAIN
from helpers.utils import get_field_value
from utilities.blob_storage_helper import (
    MONITORING_BLOB_CONTAINER,
    OUTPUT_BLOB_CONTAINER,
    read_file_as_bytes,
    read_file_as_string,
    write_bytes_to_file,
    write_string_to_file,
)
from utilities.msgraph_helper import (
    get_eml_file_from_email_id,
)
from utilities.sharepoint_helper import find_expiring_slip

NUM_RETRIES = 2
RETRY_DELAY_MINS = 3

GRAPH_EMAIL_EML_FILE_RESPONSE_FILENAME = "graph_message_eml_response_raw"
MEDIA_TYPE = "message/rfc822"

MAIN_FULL_OUTPUTS_PREFIX = "full_output_main"
MAIN_EXTRACTED_OUTPUTS_PREFIX = "extracted_output_main"
SOV_FULL_OUTPUTS_PREFIX = "full_output_sov"
SOV_EXTRACTED_OUTPUTS_PREFIX = "extracted_output_sov"
RENEWAL_FULL_OUTPUTS_PREFIX = "full_output_renewal"
RENEWAL_EXTRACTED_OUTPUTS_PREFIX = "extracted_output"
CYTORA_MAIN_FILE_NAME = f"{GRAPH_EMAIL_EML_FILE_RESPONSE_FILENAME}_main"
CYTORA_SOV_FILE_NAME = f"{GRAPH_EMAIL_EML_FILE_RESPONSE_FILENAME}_sov"
CYTORA_RENEWAL_FILE_NAME = f"{GRAPH_EMAIL_EML_FILE_RESPONSE_FILENAME}_renewal"
CYTORA_RENEWAL_SLIP_FILE_NAME = "slip_file"

TIMESTAMP_FORMAT_MILLISECONDS = "%Y%m%d%H%M%S%f"
DEFAULT_DATE_FORMAT = "%Y%m%d"

logger = logging.getLogger(__name__)


def upload_stream_to_cytora(
    blob_name: str, container_name: str, media_type: str, cytora_instance: CytoraHook
):
    file_bytes = read_file_as_bytes(
        container_name=container_name,
        blob_name=blob_name,
    )
    upload_url, upload_id = cytora_instance.get_presigned_url()
    status = cytora_instance.upload_file(
        upload_url, file_stream=file_bytes, content_type=media_type
    )
    return status, upload_id


def start_cytora_job(
    cytora_instance: CytoraHook, file_names: list[str], file_ids: list[str]
):
    job_name = (
        f"API {datetime.now().strftime(DEFAULT_DATE_FORMAT)}: {' + '.join(file_names)}"
    )
    job_id = cytora_instance.create_schema_job(file_ids, job_name)
    return job_id


def save_cytora_output_to_blob_storage(output: dict, key_prefix: str, job_id: str):
    ts = datetime.now().strftime(TIMESTAMP_FORMAT_MILLISECONDS)[:-3]
    output["TimeProcessed"] = ts
    key = f"{key_prefix}_{ts}_{job_id}.json"
    output_json = json.dumps(output, indent=2)
    write_string_to_file(MONITORING_BLOB_CONTAINER, key, output_json)
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


def parse_programme_ref_and_year_of_account(
    renewed_from: str,
) -> tuple[str | None, int | None]:
    if not renewed_from:
        return None, None
    token = renewed_from.split(";", 1)[0].strip()
    if len(token) < 9:
        return None, None
    programme_ref = token[:7]
    yy = token[7:9]
    if not yy.isdigit():
        return programme_ref, None
    y = int(yy)
    year = 2000 + y
    return programme_ref, year


def check_is_renewal(output: dict) -> bool:
    val = get_field_value(output, "new_vs_renewal")
    if not val:
        return False
    return str(val).strip().lower() in {
        "renewal",
        "renewed",
        "is_renewal",
        "yes",
        "true",
    }


def guess_media_type(filename: str) -> str:
    mt, _ = mimetypes.guess_type(filename or "")
    return mt or "application/octet-stream"


def extract_renewal_outputs(output: dict):
    renewal_terms = get_field_value(output, "renewal_terms_comparison")
    extracted_renewal_output = {"renewal_terms_comparison": renewal_terms}
    return extracted_renewal_output


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
    def init_email_processing_jobs(params: dict) -> list[dict]:
        """
        Gets the list of email ids from the DAG parameters

        Returns:
            lists[str]: Array of email ids that need handling
        """
        email_ids = params["email_ids"]
        if not email_ids:
            raise AirflowFailException("No email IDs provided")

        email_processing_jobs = []
        for email_id in email_ids:
            email_processing_job = {
                "email_id": email_id,
                "blob_folder": f"{email_id}_{datetime.now(UTC)}",
                "main_job_id": None,
                "sov_job_id": None,
                "renewal_job_id": None,
                "email_eml_file_id": None,
                "main_output_key": None,
                "renewal_metadata": None,
                "slip_info": None,
            }
            email_processing_jobs.append(email_processing_job)
        return email_processing_jobs

    @task
    def download_email_eml_file(email_processing_job: dict):
        """
        Retrieves the eml file of an email from MS Graph, and saves the raw response
        to Azure Blob Storage.

        Args:
            email_id (str): The id of the email to retrieve from MS Graph
        """
        email_id = email_processing_job.get("email_id")
        blob_folder = email_processing_job.get("blob_folder")
        logger.info("Retrieving email with id %s", email_id)

        mailbox = Variable.get("email_monitoring_mailbox")
        result = get_eml_file_from_email_id(email_id=email_id, mailbox=mailbox)

        logger.info("Saving email eml file to blob storage...")

        email_blob_path = (
            f"{blob_folder}/{GRAPH_EMAIL_EML_FILE_RESPONSE_FILENAME}"
        )

        try:
            write_bytes_to_file(MONITORING_BLOB_CONTAINER, email_blob_path, result)
        except Exception as e:
            raise AirflowException(
                f"Failed to save eml file for email {email_id} to blob storage: {e}"
            )

        logger.info("Email eml file saved to blob storage: %s", email_blob_path)

        return email_processing_job

    @task
    def upload_email_eml_file_to_cytora(email_processing_job: dict):
        """
        Retrieves the eml file of an email from blob storage, upload it and create
        a file on Cytora.

        Args:
            email_id (str): The id of the email to retrieve from MS Graph
        """
        cytora_main = CytoraHook(CYTORA_SCHEMA_MAIN)
        blob_folder = email_processing_job.get("blob_folder")
        blob_name = f"{blob_folder}/{GRAPH_EMAIL_EML_FILE_RESPONSE_FILENAME}"
        status, upload_id = upload_stream_to_cytora(
            blob_name=blob_name,
            cytora_instance=cytora_main,
            container_name=MONITORING_BLOB_CONTAINER,
            media_type=MEDIA_TYPE,
        )
        if status != 200:
            raise AirflowException(
                f"Failed to upload file to Cytora with HTTP status: {status}"
            )

        try:
            file_id = cytora_main.create_file(
                upload_id, CYTORA_MAIN_FILE_NAME, MEDIA_TYPE
            )
        except Exception as e:
            raise AirflowException(f"Failed to create file on Cytora: {e}")

        email_processing_job["email_eml_file_id"] = file_id
        return email_processing_job

    @task_group(group_id="main_flow")
    def run_main_flow(email_processing_job: dict) -> dict:
        """
        TaskGroup: main_flow

        This task group includes the tasks for the cytora main flow.
        Including starting the main schema job, waiting for job completion,
        and saving the processed output to blob storage.
        """

        @task
        def start_cytora_main_job(email_processing_job: dict):
            """
            Starts the cytora main job.

            Args:
                email_id (str): The ID of the email whose EML file should be uploaded.
            """
            cytora_main = CytoraHook(CYTORA_SCHEMA_MAIN)
            email_eml_file_id = email_processing_job.get("email_eml_file_id")

            try:
                main_job_id = start_cytora_job(
                    cytora_instance=cytora_main,
                    file_names=[CYTORA_MAIN_FILE_NAME],
                    file_ids=[email_eml_file_id],
                )
            except Exception as e:
                raise AirflowException(f"Failed to start Cytora main job: {e}")

            logger.info(f"Starting cytora main job with id {main_job_id}")

            email_processing_job["main_job_id"] = main_job_id

            return email_processing_job

        @task
        def save_cytora_main_job_output(email_processing_job: dict) -> dict:
            """
            Retrieve Cytora output and saves the result to blob storage.

            Args:
                job_id (str): The Cytora job ID to fetch output for.
            """
            cytora_main = CytoraHook(CYTORA_SCHEMA_MAIN)
            job_id = email_processing_job.get("main_job_id")
            blob_folder = email_processing_job.get("blob_folder")
            output = cytora_main.get_result_for_schema_job(job_id)

            if not output:
                raise AirflowFailException(
                    f"No output returned for Cytora job {job_id}. Status may be errored or under human review."
                )

            try:
                full_output_key = save_cytora_output_to_blob_storage(
                    output=output, key_prefix=f"{blob_folder}/{MAIN_FULL_OUTPUTS_PREFIX}", job_id=job_id
                )
            except Exception as e:
                raise AirflowException(
                    f"Failed to save full output for job {job_id} to blob storage: {e}"
                )

            extracted_output = extract_main_outputs(output)
            try:
                extracted_output_key = save_cytora_output_to_blob_storage(
                    output=extracted_output,
                    key_prefix=f"{blob_folder}/{MAIN_EXTRACTED_OUTPUTS_PREFIX}",
                    job_id=job_id,
                )
            except Exception as e:
                raise AirflowException(
                    f"Failed to save extracted output for job {job_id} to blob storage: {e}"
                )

            main_output_key = {
                "fullOutput": full_output_key,
                "extractedOutput": extracted_output_key,
            }

            email_processing_job["main_output_key"] = main_output_key

            return email_processing_job

        started = start_cytora_main_job(email_processing_job=email_processing_job)
        wait_for_main_job = CytoraApiStatusSensorOperator(
            task_id="wait_for_main_cytora_api_status",
            cytora_schema=CYTORA_SCHEMA_MAIN,
            job_id="{{ ti.xcom_pull(task_ids='main_flow.start_cytora_main_job')[ti.map_index]['main_job_id'] }}",
        )
        save = save_cytora_main_job_output(email_processing_job=started)

        started >> wait_for_main_job >> save

        return save

    @task_group(group_id="sov_flow")
    def run_sov_flow(email_processing_job: dict):
        """
        TaskGroup: sov_flow

        This task group includes the tasks for the cytora sov flow.
        Including starting the sov schema job, waiting for job completion,
        and saving the processed output to blob storage.
        """

        @task
        def start_cytora_sov_job(email_processing_job: dict):
            """
            Starts the cytora sov job.

            Args:
                email_id (str): The ID of the email whose EML file should be uploaded.
            """
            cytora_sov = CytoraHook(CYTORA_SCHEMA_SOV)
            file_id = email_processing_job.get("email_eml_file_id")
            try:
                sov_job_id = start_cytora_job(
                    cytora_instance=cytora_sov,
                    file_names=[CYTORA_SOV_FILE_NAME],
                    file_ids=[file_id],
                )
            except Exception as e:
                raise AirflowException(f"Failed to start Cytora sov job: {e}")

            logger.info(f"Starting cytora sov job with id {sov_job_id}")
            email_processing_job["sov_job_id"] = sov_job_id

            return email_processing_job

        @task
        def save_cytora_sov_job_output(email_processing_job: dict):
            """
            Retrieve Cytora output and saves the result to blob storage.

            Args:
                job_id (str): The Cytora job ID to fetch output for.
            """
            cytora_sov = CytoraHook(CYTORA_SCHEMA_SOV)
            job_id = email_processing_job.get("sov_job_id")
            blob_folder = email_processing_job.get("blob_folder")
            output = cytora_sov.get_result_for_schema_job(job_id)

            if not output:
                raise AirflowFailException(
                    f"No output returned for Cytora job {job_id}. Status may be errored or under human review."
                )

            try:
                full_output_key = save_cytora_output_to_blob_storage(
                    output=output, key_prefix=f"{blob_folder}/{SOV_FULL_OUTPUTS_PREFIX}", job_id=job_id
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
                    key_prefix=f"{blob_folder}/{SOV_EXTRACTED_OUTPUTS_PREFIX}",
                    job_id=job_id,
                )
            except Exception as e:
                raise AirflowException(
                    f"Failed to save extracted output for job {job_id} to blob storage: {e}"
                )

            return full_output_key, extracted_output_key

        started = start_cytora_sov_job(email_processing_job=email_processing_job)
        save = save_cytora_sov_job_output(email_processing_job=started)
        wait_for_sov_job = CytoraApiStatusSensorOperator(
            task_id="wait_for_sov_cytora_api_status",
            cytora_schema=CYTORA_SCHEMA_SOV,
            job_id="{{ ti.xcom_pull(task_ids='sov_flow.start_cytora_sov_job')[ti.map_index]['sov_job_id'] }}"
        )

        started >> wait_for_sov_job >> save

    @task_group(group_id="renewal_flow")
    def run_renewal_flow(email_processing_job: dict):
        """
        TaskGroup: renewal_flow

        This task group includes the tasks for the cytora renewal flow.
        Including extracting renewal metadata, checking renewal flow trigger condition,
        retrieving slip files, starting the renewal schema job (with or without slip file),
        waiting for job completion, and saving the processed output to blob storage.
        """

        @task
        def extract_renewal_metadata(email_processing_job: dict):
            """
            Extract renewal metadata from the full Cytora main job output.

            Args:
               main_output_key (dict[str, str]): Dictionary containing blob storage keys
               for main output files. Expected to include a "full" key.
            """
            try:
                main_output_key = email_processing_job.get("main_output_key")
                full_output_key = main_output_key.get("fullOutput")
                if not full_output_key:
                    raise KeyError("Missing required key 'full' in main_output_key")

                logger.info(
                    "Reading full main output from blob storage: %s", full_output_key
                )
                main_output_json_str = read_file_as_string(
                    MONITORING_BLOB_CONTAINER, full_output_key
                )

                try:
                    main_output = json.loads(main_output_json_str)
                except json.JSONDecodeError as e:
                    logger.error("Invalid JSON in %s: %s", full_output_key, e)
                    raise AirflowFailException("Invalid JSON in main output") from e

                renewed_from_val = get_field_value(main_output, "renewed_from")
                programme_ref, year_of_account = (
                    parse_programme_ref_and_year_of_account(renewed_from_val)
                )
                is_renewal = check_is_renewal(main_output)

                metadata = {
                    "programme_ref": programme_ref,
                    "year_of_account": year_of_account,
                    "is_renewal": is_renewal,
                }
                logger.info("Extracted renewal metadata: %s", metadata)
                email_processing_job["renewal_metadata"] = metadata
                return email_processing_job

            except Exception as e:
                logger.error("Failed to extract renewal metadata: %s", e, exc_info=True)
                raise AirflowFailException(
                    f"extract_renewal_metadata failed: {e}"
                ) from e

        @task.short_circuit
        def check_if_should_start_renewal_flow(email_processing_job: dict):
            metadata = email_processing_job.get("renewal_metadata")
            if (
                metadata["is_renewal"]
                and metadata["programme_ref"]
                and metadata["year_of_account"]
            ):
                return True
            return False

        @task
        def fetch_slip_files(email_processing_job: dict):
            metadata = email_processing_job.get("renewal_metadata")
            expired_file = find_expiring_slip(
                programme_reference=metadata["programme_ref"],
                year_of_account=metadata["year_of_account"],
                blob_folder=email_processing_job.get("blob_folder"),
            )
            logger.info("Extracted slip files: %s", expired_file)
            if expired_file:
                file_key = expired_file.get("file_key")
                slip_name = expired_file.get("name")
                slip_info = {
                    "slip_blob_name": file_key,
                    "slip_name": slip_name,
                }
                email_processing_job["slip_info"] = slip_info

            return email_processing_job


        @task
        def start_cytora_renewal_job(email_processing_job: dict):
            """
            Upload slip to Cytora, create files, and start a renewal schema job.
            Includes logic for both with and without slip file.
            Returns the renewal job_id.
            """
            cytora_renewal = CytoraHook(CYTORA_SCHEMA_RENEWAL)
            email_file_id = email_processing_job.get("email_eml_file_id")
            slip_info = email_processing_job.get("slip_info")

            if slip_info:
                slip_blob_name = slip_info.get("slip_blob_name")
                slip_name = slip_info.get("slip_name")
                slip_media_type = guess_media_type(slip_name or "document.pdf")

                status, slip_file_upload_id = upload_stream_to_cytora(
                    blob_name=slip_blob_name,
                    container_name=MONITORING_BLOB_CONTAINER,
                    cytora_instance=cytora_renewal,
                    media_type=slip_media_type,
                )
                if status != 200:
                    raise RuntimeError(f"Slip upload to Cytora failed: HTTP {status}")

                slip_file_id = cytora_renewal.create_file(
                    upload_id=slip_file_upload_id,
                    file_name=CYTORA_RENEWAL_SLIP_FILE_NAME,
                    media_type=slip_media_type,
                )

                renewal_job_id = start_cytora_job(
                    cytora_instance=cytora_renewal,
                    file_ids=[email_file_id, slip_file_id],
                    file_names=[
                        CYTORA_RENEWAL_FILE_NAME,
                        CYTORA_RENEWAL_SLIP_FILE_NAME,
                    ],
                )
                logger.info(
                    "Started cytora renewal job with slip file: %s", renewal_job_id
                )
            else:
                renewal_job_id = start_cytora_job(
                    cytora_instance=cytora_renewal,
                    file_ids=[email_file_id],
                    file_names=[CYTORA_RENEWAL_FILE_NAME],
                )
                logger.info(
                    "Started cytora renewal job with email file: %s", renewal_job_id
                )

            email_processing_job["renewal_job_id"] = renewal_job_id
            return email_processing_job

        @task
        def save_cytora_renewal_output(email_processing_job: dict):
            """
            Retrieve Cytora output and saves the result to blob storage.

            Args:
                job_id (str): The Cytora job ID to fetch output for.
            """
            cytora_renewal = CytoraHook(CYTORA_SCHEMA_RENEWAL)
            job_id = email_processing_job.get("renewal_job_id")
            output = cytora_renewal.get_result_for_schema_job(job_id)

            if not output:
                raise AirflowFailException(
                    f"No output returned for Cytora job {job_id}. Status may be errored or under human review."
                )

            try:
                full_output_key = save_cytora_output_to_blob_storage(
                    output=output, key_prefix=RENEWAL_FULL_OUTPUTS_PREFIX, job_id=job_id
                )
            except Exception as e:
                raise AirflowException(
                    f"Failed to save full output for job {job_id} to blob storage: {e}"
                )

            extracted_output = extract_renewal_outputs(output)
            try:
                extracted_output_key = save_cytora_output_to_blob_storage(
                    output=extracted_output,
                    key_prefix=RENEWAL_EXTRACTED_OUTPUTS_PREFIX,
                    job_id=job_id,
                )
            except Exception as e:
                raise AirflowException(
                    f"Failed to save extracted output for job {job_id} to blob storage: {e}"
                )

            return full_output_key, extracted_output_key

        metadata = extract_renewal_metadata(email_processing_job=email_processing_job)
        should_start_renewal_flow = check_if_should_start_renewal_flow(
            email_processing_job=metadata
        )
        slip_info = fetch_slip_files(email_processing_job=metadata)
        started = start_cytora_renewal_job(
            email_processing_job=slip_info
        )
        saved = save_cytora_renewal_output(email_processing_job=started)
        wait_for_cytora_renewal_job = CytoraApiStatusSensorOperator(
            task_id="wait_for_renewal_cytora_api_status",
            cytora_schema=CYTORA_SCHEMA_RENEWAL,
            job_id="{{ ti.xcom_pull(task_ids='renewal_flow.start_cytora_renewal_job')[ti.map_index]['renewal_job_id'] }}",
        )

        metadata >> should_start_renewal_flow >> slip_info >> started >> wait_for_cytora_renewal_job >> saved

    @task
    def get_main_flow_email_processing_jobs(email_processing_jobs: list[dict]):
        # This intermediate task is required for things to work properly.
        # Without it Airflow will mark downstream task groups as upstream_failed,
        # potentially due to how dependencies/XCom passing are handled between groups.
        # Keeping this ensures jobs are properly handed off to the next flow.
        return email_processing_jobs

    init_email_processing_jobs = init_email_processing_jobs()

    email_processing_jobs = download_email_eml_file.expand(email_processing_job=init_email_processing_jobs)
    uploaded_jobs = upload_email_eml_file_to_cytora.expand(
        email_processing_job=email_processing_jobs
    )
    main_flow_jobs = run_main_flow.expand(
        email_processing_job=uploaded_jobs
    )
    returned_main_flow_jobs = get_main_flow_email_processing_jobs(main_flow_jobs)
    cytora_renewal_flow = run_renewal_flow.expand(email_processing_job=returned_main_flow_jobs)
    cytora_sov_flow = run_sov_flow.expand(email_processing_job=uploaded_jobs)

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")
    [cytora_renewal_flow, cytora_sov_flow] >> end


process_email_change_notifications()
