import json
import logging
import mimetypes
import os
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

MAIN_FULL_OUTPUTS_PREFIX = "outputs_main/full_output"
MAIN_EXTRACTED_OUTPUTS_PREFIX = "outputs_main/extracted_output"
SOV_FULL_OUTPUTS_PREFIX = "outputs_sov/full_output"
SOV_EXTRACTED_OUTPUTS_PREFIX = "outputs_sov/extracted_output"
RENEWAL_FULL_OUTPUTS_PREFIX = "outputs_renewal/full_output"
RENEWAL_EXTRACTED_OUTPUTS_PREFIX = "outputs_renewal/extracted_output"
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


def fetch_expiring_slip_to_blob_storage() -> tuple[
    str | None, bytes | None, str | None
]:
    sample_file_name = "Slip 2023.pdf"

    dag_dir = os.path.dirname(__file__)
    file_path = os.path.join(dag_dir, sample_file_name)

    if not os.path.exists(file_path):
        logger.warning("Test slip file not found at %s", file_path)
        return None, None, None

    with open(file_path, "rb") as f:
        slip_bytes = f.read()

    file_name = os.path.basename(file_path)

    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S%f")[:-3]
    blob_name = f"{timestamp}_{file_name}"

    logger.info("Uploading test slip file to blob storage: %s", blob_name)

    write_bytes_to_file(
        container_name=MONITORING_BLOB_CONTAINER,
        blob_name=blob_name,
        bytes_data=slip_bytes,
    )

    logger.info("Slip file uploaded to blob storage: %s", blob_name)

    slip_present = bool(slip_bytes)
    return blob_name, slip_present, file_name


def parse_programme_ref_and_yoa(renewed_from: str) -> tuple[str | None, int | None]:
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

    @task
    def upload_email_eml_file_to_cytora(email_id: str, dag_run: DagRun | None = None):
        """
        Retrieves the eml file of an email from blob storage, upload it and create
        a file on Cytora.

        Args:
            email_id (str): The id of the email to retrieve from MS Graph
        """
        cytora_main = CytoraHook(CYTORA_SCHEMA_MAIN)
        run_id = dag_run.run_id
        blob_name = f"{run_id}/{email_id}/{GRAPH_EMAIL_EML_FILE_RESPONSE_FILENAME}"
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

        return file_id

    @task_group(group_id="main_flow")
    def run_main_flow(file_id: str):
        """
        TaskGroup: main_flow

        This task group includes the tasks for the cytora main flow.
        Including starting the main schema job, waiting for job completion,
        and saving the processed output to blob storage.
        """

        @task
        def start_cytora_main_job(file_id: str):
            """
            Starts the cytora main job.

            Args:
                email_id (str): The ID of the email whose EML file should be uploaded.
            """
            cytora_main = CytoraHook(CYTORA_SCHEMA_MAIN)

            try:
                main_job_id = start_cytora_job(
                    cytora_instance=cytora_main,
                    file_names=[CYTORA_MAIN_FILE_NAME],
                    file_ids=[file_id],
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

            return {
                "full": full_output_key,
                "extracted": extracted_output_key,
            }

        cytora_main_job_id = start_cytora_main_job(file_id=file_id)
        wait_for_main_job = CytoraApiStatusSensorOperator(
            task_id="wait_for_main_cytora_api_status",
            cytora_schema=CYTORA_SCHEMA_MAIN,
            job_id=cytora_main_job_id,
        )
        main_output_key = save_cytora_main_job_output(job_id=cytora_main_job_id)

        cytora_main_job_id >> wait_for_main_job >> main_output_key

        return main_output_key

    @task_group(group_id="sov_flow")
    def run_sov_flow(file_id: str):
        """
        TaskGroup: sov_flow

        This task group includes the tasks for the cytora sov flow.
        Including starting the sov schema job, waiting for job completion,
        and saving the processed output to blob storage.
        """

        @task
        def start_cytora_sov_job(file_id: str):
            """
            Starts the cytora sov job.

            Args:
                email_id (str): The ID of the email whose EML file should be uploaded.
            """
            cytora_sov = CytoraHook(CYTORA_SCHEMA_SOV)
            try:
                sov_job_id = start_cytora_job(
                    cytora_instance=cytora_sov,
                    file_names=[CYTORA_SOV_FILE_NAME],
                    file_ids=[file_id],
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

        cytora_sov_job_id = start_cytora_sov_job(file_id=file_id)
        sov_output_keys = save_cytora_sov_job_output(job_id=cytora_sov_job_id)
        wait_for_sov_job = CytoraApiStatusSensorOperator(
            task_id="wait_for_sov_cytora_api_status",
            cytora_schema=CYTORA_SCHEMA_SOV,
            job_id=cytora_sov_job_id,
        )

        cytora_sov_job_id >> wait_for_sov_job >> sov_output_keys

    @task_group(group_id="renewal_flow")
    def run_renewal_flow(main_output_key: dict[str], email_eml_file_cytora_id: str):
        """
        TaskGroup: renewal_flow

        This task group includes the tasks for the cytora renewal flow.
        Including extracting renewal metadata, checking renewal flow trigger condition,
        retrieving slip files, starting the  schema job (with or without slip file),
        waiting for job completion, and saving the processed output to blob storage.
        """

        @task
        def extract_renewal_metadata(main_output_key: dict[str]):
            """
            Extract renewal metadata from the full Cytora main job output.

            Args:
               main_output_key (dict[str, str]): Dictionary containing blob storage keys
               for main output files. Expected to include a "full" key.
            """
            try:
                full_output_key = main_output_key.get("full")
                if not full_output_key:
                    raise KeyError("Missing required key 'full' in main_output_key")

                logger.info(
                    "Reading full main output from blob storage: %s", full_output_key
                )
                main_output_json_str = read_file_as_string(
                    OUTPUT_BLOB_CONTAINER, full_output_key
                )

                try:
                    main_output = json.loads(main_output_json_str)
                except json.JSONDecodeError as e:
                    logger.error("Invalid JSON in %s: %s", full_output_key, e)
                    raise AirflowFailException("Invalid JSON in main output") from e

                renewed_from_val = get_field_value(main_output, "renewed_from")
                programme_ref, yoa = parse_programme_ref_and_yoa(renewed_from_val)
                is_renewal = check_is_renewal(main_output)

                metadata = {
                    "programme_ref": programme_ref,
                    "yoa": yoa,
                    "is_renewal": is_renewal,
                }
                logger.info("Extracted renewal metadata: %s", metadata)
                return metadata

            except Exception as e:
                logger.error("Failed to extract renewal metadata: %s", e, exc_info=True)
                raise AirflowFailException(
                    f"extract_renewal_metadata failed: {e}"
                ) from e

        @task.short_circuit
        def check_if_should_start_renewal_flow(metadata: dict):
            if metadata["is_renewal"] and metadata["programme_ref"] and metadata["yoa"]:
                return True
            return False

        @task
        def fetch_slip_files(metadata: dict):
            expired_file = find_expiring_slip(
                programme_reference=metadata["programme_ref"],
                year_of_account=metadata["yoa"],
            )
            logger.info("Extracted slip files: %s", expired_file)
            if expired_file:
                file_key = expired_file.get("file_key")
                slip_is_present = True
                slip_name = expired_file.get("name")
                return file_key, slip_is_present, slip_name
            else:
                return None, None, None

        @task
        def start_cytora_renewal_job(email_file_id: str, slip_info: tuple) -> str:
            """
            Upload slip to Cytora, create files, and start a renewal schema job (email + slip).
            Returns the renewal job_id.
            """
            cytora_renewal = CytoraHook(CYTORA_SCHEMA_RENEWAL)
            slip_blob_name, slip_is_present, slip_name = slip_info

            if slip_is_present:
                slip_media_type = guess_media_type(slip_name or "document.pdf")

                status, slip_file_upload_id = upload_stream_to_cytora(
                    blob_name=slip_blob_name,
                    container_name=OUTPUT_BLOB_CONTAINER,
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

            return renewal_job_id

        @task
        def save_cytora_renewal_output(job_id: str):
            """
            Retrieve Cytora output and saves the result to blob storage.

            Args:
                job_id (str): The Cytora job ID to fetch output for.
            """
            cytora_renewal = CytoraHook(CYTORA_SCHEMA_RENEWAL)
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

        @task(trigger_rule="all_done")
        def get_renewal_job_id(
            job_id_with_slip: str | None, job_id_email_only: str | None
        ) -> str:
            return job_id_with_slip or job_id_email_only

        metadata = extract_renewal_metadata(main_output_key=main_output_key)
        should_start_renewal_flow = check_if_should_start_renewal_flow(
            metadata=metadata
        )
        slip_info = fetch_slip_files(metadata=metadata)
        cytora_renewal_job_id = start_cytora_renewal_job(
            email_file_id=email_eml_file_cytora_id, slip_info=slip_info
        )
        renewal_output_keys = save_cytora_renewal_output(job_id=cytora_renewal_job_id)
        wait_for_cytora_renewal_job = CytoraApiStatusSensorOperator(
            task_id="wait_for_renewal_cytora_api_status",
            cytora_schema=CYTORA_SCHEMA_RENEWAL,
            job_id=cytora_renewal_job_id,
        )

        (
            metadata
            >> should_start_renewal_flow
            >> slip_info
            >> cytora_renewal_job_id
            >> wait_for_cytora_renewal_job
            >> renewal_output_keys
        )

    @task
    def zip_to_dicts(
        main_output_keys: list[str], cytora_ids: list[str]
    ) -> list[dict[str, str]]:
        return [
            {
                "main_output_key": mk,
                "email_eml_file_cytora_id": cid,
            }
            for mk, cid in zip(main_output_keys, cytora_ids)
        ]

    email_ids = get_email_ids()

    email_eml_file_task_instance = download_email_eml_file.expand(email_id=email_ids)
    email_eml_file_cytora_ids = upload_email_eml_file_to_cytora.expand(
        email_id=email_ids
    )
    cytora_main_flow_output_keys_list = run_main_flow.expand(
        file_id=email_eml_file_cytora_ids
    )
    zipped_inputs = zip_to_dicts(
        main_output_keys=cytora_main_flow_output_keys_list,
        cytora_ids=email_eml_file_cytora_ids,
    )
    cytora_renewal_flow = run_renewal_flow.expand_kwargs(zipped_inputs)
    cytora_sov_flow = run_sov_flow.expand(file_id=email_eml_file_cytora_ids)

    (
        email_ids
        >> email_eml_file_task_instance
        >> email_eml_file_cytora_ids
        >> [cytora_main_flow_output_keys_list, cytora_sov_flow]
    )
    cytora_main_flow_output_keys_list >> cytora_renewal_flow

    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")
    [cytora_renewal_flow, cytora_sov_flow] >> end


process_email_change_notifications()
