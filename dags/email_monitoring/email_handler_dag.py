import json
import logging
from datetime import UTC, datetime, timedelta

import requests
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
from helpers.utils import get_field_value
from utilities.blob_storage_helper import (
    MONITORING_BLOB_CONTAINER,
    read_file_as_string,
    write_bytes_to_file,
)
from utilities.constants import (
    CYTORA_MAIN_FILE_NAME,
    CYTORA_RENEWAL_FILE_NAME,
    CYTORA_RENEWAL_SLIP_FILE_NAME,
    CYTORA_SOV_FILE_NAME,
    GRAPH_EMAIL_EML_FILE_RESPONSE_FILENAME,
    MAIN_EXTRACTED_OUTPUTS_PREFIX,
    MAIN_FULL_OUTPUTS_PREFIX,
    MEDIA_TYPE,
    RENEWAL_EXTRACTED_OUTPUTS_PREFIX,
    RENEWAL_FULL_OUTPUTS_PREFIX,
    SOV_EXTRACTED_OUTPUTS_PREFIX,
    SOV_FULL_OUTPUTS_PREFIX,
)
from utilities.cytora_helper.client import (
    guess_media_type,
    save_cytora_output_to_blob_storage,
    start_cytora_job,
    upload_stream_to_cytora,
)
from utilities.cytora_helper.output_extractors import (
    extract_main_outputs,
    extract_renewal_outputs,
    extract_sov_outputs,
)
from utilities.cytora_helper.parsers import (
    check_is_renewal,
    parse_programme_ref_and_year_of_account,
)
from utilities.msgraph_helper import (
    get_eml_file_from_email_id,
)
from utilities.sharepoint_helper import find_expiring_slip

from utilities.database_helper import (
    create_email_processing_job,
    set_cytora_job_status, save_cytora_output_to_db,
)

from utilities.utils import bytes_to_megabytes

from utilities.constants import TIMESTAMP_FORMAT_READABLE_MICROSECONDS
from utilities.database_helper import end_email_processing_job_in_db

NUM_RETRIES = 2
RETRY_DELAY_MINS = 3

FUNCTION_APP_API = "http://host.docker.internal:7071"

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
    the receipt of emails. The email content and attachment is uploaded to Cytora
    for processing, and the resulting output is saved in Azure Blob Storage.
    """

    @task
    def init_email_processing_jobs(params: dict) -> list[dict]:
        """
        Initialises a list of email processing jobs objects with
        email_id and blob_folder

        Returns:
            lists[str]: Array of email_processing_jobs
        """
        email_ids = params["email_ids"]
        if not email_ids:
            raise AirflowFailException("No email IDs provided")

        email_processing_jobs = []
        for email_id in email_ids:
            start_time = datetime.now(UTC).strftime(TIMESTAMP_FORMAT_READABLE_MICROSECONDS)[:-3]
            response = create_email_processing_job(
                base_url=FUNCTION_APP_API, email_id=email_id, start_time=start_time
            )
            job_id = response.json()["id"]

            email_processing_job = {
                "id": job_id,
                "email_id": email_id,
                "blob_folder": f"{email_id}_{start_time}",
                "main_job_id": None,
                "sov_job_id": None,
                "renewal_job_id": None,
                "email_eml_file_id": None,
                "email_eml_file_size": None,
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
            email_processing_job (dict): Email processing job object
        """
        email_id = email_processing_job.get("email_id")
        blob_folder = email_processing_job.get("blob_folder")
        logger.info("Retrieving email with id %s", email_id)

        mailbox = Variable.get("email_monitoring_mailbox")
        result = get_eml_file_from_email_id(email_id=email_id, mailbox=mailbox)
        email_processing_job["email_eml_file_size"] = bytes_to_megabytes(len(result))
        if not result:
            raise AirflowException(f"No content retrieved for email {email_id}")

        logger.info("Saving email eml file to blob storage...")

        email_blob_path = f"{blob_folder}/{GRAPH_EMAIL_EML_FILE_RESPONSE_FILENAME}"

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
            email_processing_job (dict): Email processing job object
        """
        cytora_main = CytoraHook(CYTORA_SCHEMA_MAIN)
        blob_folder = email_processing_job.get("blob_folder")
        blob_name = f"{blob_folder}/{GRAPH_EMAIL_EML_FILE_RESPONSE_FILENAME}"

        logger.info("Uploading email eml file to Cytora...")
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
        logger.info("Email eml file uploaded to Cytora: %s", upload_id)

        logger.info("Creating file on Cytora...")
        try:
            file_id = cytora_main.create_file(
                upload_id, CYTORA_MAIN_FILE_NAME, MEDIA_TYPE
            )
        except Exception as e:
            raise AirflowException(f"Failed to create file on Cytora: {e}")
        logger.info("File created on Cytora: %s", file_id)

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
                email_processing_job (dict): Email processing job object
            """
            cytora_main = CytoraHook(CYTORA_SCHEMA_MAIN)
            email_processing_job_id = email_processing_job.get("id")
            email_eml_file_id = email_processing_job.get("email_eml_file_id")

            logger.info("Starting cytora main flow...")
            try:
                main_job_id = start_cytora_job(
                    cytora_instance=cytora_main,
                    file_names=[CYTORA_MAIN_FILE_NAME],
                    file_ids=[email_eml_file_id],
                )
            except Exception as e:
                set_cytora_job_status(
                    base_url=FUNCTION_APP_API,
                    email_processing_job_id=email_processing_job_id,
                    status="Failed to start",
                    job_type="main",
                )
                raise AirflowException(f"Failed to start Cytora main job: {e}")

            logger.info(f"Started cytora main job with id {main_job_id}")
            set_cytora_job_status(
                base_url=FUNCTION_APP_API,
                email_processing_job_id=email_processing_job_id,
                status="In Progress",
                job_type="main",
                cytora_job_id=main_job_id,
            )

            email_processing_job["main_job_id"] = main_job_id

            return email_processing_job

        @task
        def save_cytora_main_job_output(email_processing_job: dict) -> dict:
            """
            Retrieve Cytora output and saves the result to blob storage.

            Args:
                email_processing_job (dict): Email processing job object
            """
            cytora_main = CytoraHook(CYTORA_SCHEMA_MAIN)
            email_processing_job_id = email_processing_job.get("id")
            job_id = email_processing_job.get("main_job_id")
            blob_folder = email_processing_job.get("blob_folder")
            output = cytora_main.get_result_for_schema_job(job_id)

            if not output:
                set_cytora_job_status(
                    base_url=FUNCTION_APP_API,
                    email_processing_job_id=email_processing_job_id,
                    status="Failed - Status might be errored or under human review",
                    job_type="main",
                    cytora_job_id=email_processing_job_id,
                )

                raise AirflowFailException(
                    f"No output returned for Cytora job {job_id}. Status may be errored or under human review."
                )
            logger.info(f"Retrieved output for Cytora job {job_id}")

            logger.info(
                f"Saving full output for main flow job to blob storage: {job_id}"
            )
            try:
                full_output_key = save_cytora_output_to_blob_storage(
                    output=output,
                    key_prefix=f"{blob_folder}/{MAIN_FULL_OUTPUTS_PREFIX}",
                    job_id=job_id,
                )
            except Exception as e:
                set_cytora_job_status(
                    base_url=FUNCTION_APP_API,
                    email_processing_job_id=email_processing_job_id,
                    status="Failed to save full output to blob storage",
                    job_type="main",
                    cytora_job_id=email_processing_job_id,
                )

                raise AirflowException(
                    f"Failed to save full output for job {job_id} to blob storage: {e}"
                )
            logger.info(f"Full output for job {job_id} saved to blob storage.")

            logger.info(f"Extracting trimmed output for main flow job: {job_id}")
            extracted_output = extract_main_outputs(output)
            extracted_output["EmailFileSizeInMB"] = email_processing_job["email_eml_file_size"]

            logger.info(
                f"Saving main job extracted output to DB: {email_processing_job_id}"
            )
            save_cytora_output_to_db(
                base_url=FUNCTION_APP_API,
                endpoint="/api/main-job-output",
                email_processing_job_id=email_processing_job_id,
                extracted_output=extracted_output,
            )
            logger.info(f"Extracted output for main job {job_id} saved to DB.")

            logger.info(
                f"Saving extracted output for main flow job to blob storage: {job_id}"
            )
            try:
                extracted_output_key = save_cytora_output_to_blob_storage(
                    output=extracted_output,
                    key_prefix=f"{blob_folder}/{MAIN_EXTRACTED_OUTPUTS_PREFIX}",
                    job_id=job_id,
                )
            except Exception as e:
                set_cytora_job_status(
                    base_url=FUNCTION_APP_API,
                    email_processing_job_id=email_processing_job_id,
                    status="Failed to save extracted output to blob storage",
                    job_type="main",
                    cytora_job_id=job_id,
                )
                raise AirflowException(
                    f"Failed to save extracted output for job {job_id} to blob storage: {e}"
                )

            set_cytora_job_status(
                base_url=FUNCTION_APP_API,
                email_processing_job_id=email_processing_job_id,
                status="Completed",
                job_type="main",
                cytora_job_id=job_id,
            )
            logger.info(f"Extracted output for job {job_id} saved to blob storage.")

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
            email_processing_job_id = email_processing_job.get("id")
            file_id = email_processing_job.get("email_eml_file_id")
            logger.info("Starting cytora sov flow...")
            try:
                sov_job_id = start_cytora_job(
                    cytora_instance=cytora_sov,
                    file_names=[CYTORA_SOV_FILE_NAME],
                    file_ids=[file_id],
                )
            except Exception as e:
                set_cytora_job_status(
                    base_url=FUNCTION_APP_API,
                    email_processing_job_id=email_processing_job_id,
                    status="Failed to start",
                    job_type="sov",
                )
                raise AirflowException(f"Failed to start Cytora sov job: {e}")

            set_cytora_job_status(
                base_url=FUNCTION_APP_API,
                email_processing_job_id=email_processing_job_id,
                status="In Progress",
                job_type="sov",
                cytora_job_id=sov_job_id,
            )
            logger.info(f"Started cytora sov job with id {sov_job_id}")

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
            email_processing_job_id = email_processing_job.get("id")
            job_id = email_processing_job.get("sov_job_id")
            blob_folder = email_processing_job.get("blob_folder")
            output = cytora_sov.get_result_for_schema_job(job_id)

            if not output:
                set_cytora_job_status(
                    base_url=FUNCTION_APP_API,
                    email_processing_job_id=email_processing_job_id,
                    status="Failed - Status might be errored or under human review",
                    job_type="sov",
                    cytora_job_id=job_id,
                )
                raise AirflowFailException(
                    f"No output returned for Cytora job {job_id}. Status may be errored or under human review."
                )
            logger.info(f"Retrieved output for Cytora job {job_id}")

            logger.info(
                f"Saving full output for sov flow job to blob storage: {job_id}"
            )
            try:
                full_output_key = save_cytora_output_to_blob_storage(
                    output=output,
                    key_prefix=f"{blob_folder}/{SOV_FULL_OUTPUTS_PREFIX}",
                    job_id=job_id,
                )
            except Exception as e:
                set_cytora_job_status(
                    base_url=FUNCTION_APP_API,
                    email_processing_job_id=email_processing_job_id,
                    status="Failed to save full output to blob storage",
                    job_type="sov",
                    cytora_job_id=job_id,
                )
                raise AirflowException(
                    f"Failed to save full output for job {job_id} to blob storage: {e}"
                )

            logger.info(f"Full output for job {job_id} saved to blob storage.")

            logger.info(f"Extracting trimmed output for sov flow job: {job_id}")
            extracted_output_list = extract_sov_outputs(output)
            extracted_output_dict = {
                f"sov_row_{i + 1}": row for i, row in enumerate(extracted_output_list)
            }

            if not extracted_output_dict:
                set_cytora_job_status(
                    base_url=FUNCTION_APP_API,
                    email_processing_job_id=email_processing_job_id,
                    status="Completed - no SOV fields found",
                    job_type="sov",
                    cytora_job_id=job_id,
                )
                return full_output_key, None

            logger.info(
                f"Saving sov job extracted output to DB: {email_processing_job_id}"
            )
            save_cytora_output_to_db(
                base_url=FUNCTION_APP_API,
                endpoint="/api/sov-job-output",
                email_processing_job_id=email_processing_job_id,
                extracted_output=extracted_output_dict,
            )
            logger.info(f"Extracted output for sov job {job_id} saved to DB.")

            logger.info(
                f"Saving extracted output for sov flow job to blob storage: {job_id}"
            )
            try:
                extracted_output_key = save_cytora_output_to_blob_storage(
                    output=extracted_output_dict,
                    key_prefix=f"{blob_folder}/{SOV_EXTRACTED_OUTPUTS_PREFIX}",
                    job_id=job_id,
                )
            except Exception as e:
                set_cytora_job_status(
                    base_url=FUNCTION_APP_API,
                    email_processing_job_id=email_processing_job_id,
                    status="Failed to save extracted output to blob storage",
                    job_type="sov",
                    cytora_job_id=job_id,
                )

                raise AirflowException(
                    f"Failed to save extracted output for job {job_id} to blob storage: {e}"
                )

            logger.info(f"Extracted output for job {job_id} saved to blob storage.")
            set_cytora_job_status(
                base_url=FUNCTION_APP_API,
                email_processing_job_id=email_processing_job_id,
                status="Completed",
                job_type="sov",
                cytora_job_id=job_id,
            )

            return full_output_key, extracted_output_key

        started = start_cytora_sov_job(email_processing_job=email_processing_job)
        save = save_cytora_sov_job_output(email_processing_job=started)
        wait_for_sov_job = CytoraApiStatusSensorOperator(
            task_id="wait_for_sov_cytora_api_status",
            cytora_schema=CYTORA_SCHEMA_SOV,
            job_id="{{ ti.xcom_pull(task_ids='sov_flow.start_cytora_sov_job')[ti.map_index]['sov_job_id'] }}",
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
                    raise KeyError(
                        "Missing required key 'fullOutput' in main_output_key"
                    )

                logger.info(
                    "Loading full main output from blob storage: %s", full_output_key
                )
                main_output_json_str = read_file_as_string(
                    MONITORING_BLOB_CONTAINER, full_output_key
                )
                try:
                    main_output = json.loads(main_output_json_str)
                except json.JSONDecodeError as e:
                    logger.error("Invalid JSON in %s: %s", full_output_key, e)
                    raise AirflowFailException("Invalid JSON in main output") from e

                logger.info("Extracting renewal metadata...")
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
            logger.info("Checking if renewal flow should start renewal flow...")
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

            logger.info("Fetching slip files for renewal flow...")
            try:
                expired_file = find_expiring_slip(
                    programme_reference=metadata["programme_ref"],
                    year_of_account=metadata["year_of_account"],
                    blob_folder=email_processing_job.get("blob_folder"),
                )
            except Exception as e:
                raise AirflowFailException(
                    f"Failed to fetch slip files for renewal flow: {e}"
                )

            if expired_file:
                logger.info("Extracted slip file: %s", expired_file)
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
            email_processing_job_id = email_processing_job.get("id")
            email_file_id = email_processing_job.get("email_eml_file_id")
            slip_info = email_processing_job.get("slip_info")

            if slip_info:
                slip_blob_name = slip_info.get("slip_blob_name")
                slip_name = slip_info.get("slip_name")
                slip_media_type = guess_media_type(slip_name or "document.pdf")

                logger.info("Uploading slip to Cytora...")
                status, slip_file_upload_id = upload_stream_to_cytora(
                    blob_name=slip_blob_name,
                    container_name=MONITORING_BLOB_CONTAINER,
                    cytora_instance=cytora_renewal,
                    media_type=slip_media_type,
                )
                if status != 200:
                    raise RuntimeError(f"Slip upload to Cytora failed: HTTP {status}")
                logger.info(f"Uploaded slip to Cytora: {slip_file_upload_id}")

                logger.info("Createing slip file on Cytora...")
                slip_file_id = cytora_renewal.create_file(
                    upload_id=slip_file_upload_id,
                    file_name=CYTORA_RENEWAL_SLIP_FILE_NAME,
                    media_type=slip_media_type,
                )
                logger.info(f"Created slip file on Cytora: {slip_file_id}")

                logger.info("Starting renewal flow with slip...")
                try:
                    renewal_job_id = start_cytora_job(
                        cytora_instance=cytora_renewal,
                        file_ids=[email_file_id, slip_file_id],
                        file_names=[
                            CYTORA_RENEWAL_FILE_NAME,
                            CYTORA_RENEWAL_SLIP_FILE_NAME,
                        ],
                    )
                except Exception as e:
                    set_cytora_job_status(
                        base_url=FUNCTION_APP_API,
                        status="Failed to start",
                        email_processing_job_id=email_processing_job_id,
                        job_type="renewal",
                    )
                    raise AirflowException(
                        f"Failed to start Cytora renewal job with slip: {e}"
                    )
                logger.info(
                    "Started cytora renewal job with slip file: %s", renewal_job_id
                )
            else:
                try:
                    logger.info("Starting renewal flow with email only...")
                    renewal_job_id = start_cytora_job(
                        cytora_instance=cytora_renewal,
                        file_ids=[email_file_id],
                        file_names=[CYTORA_RENEWAL_FILE_NAME],
                    )
                except Exception as e:
                    set_cytora_job_status(
                        base_url=FUNCTION_APP_API,
                        status="Failed to start",
                        email_processing_job_id=email_processing_job_id,
                        job_type="renewal",
                    )
                    raise AirflowException(
                        f"Failed to start Cytora renewal job without slip: {e}"
                    )
                logger.info(
                    "Started cytora renewal job with email file only: %s",
                    renewal_job_id,
                )

            set_cytora_job_status(
                base_url=FUNCTION_APP_API,
                status="In Progress",
                email_processing_job_id=email_processing_job_id,
                job_type="renewal",
                cytora_job_id=renewal_job_id,
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
            email_processing_job_id = email_processing_job.get("id")
            job_id = email_processing_job.get("renewal_job_id")
            output = cytora_renewal.get_result_for_schema_job(job_id)

            if not output:
                set_cytora_job_status(
                    base_url=FUNCTION_APP_API,
                    status="Failed - Status may be errored or under human review",
                    email_processing_job_id=email_processing_job_id,
                    job_type="renewal",
                    cytora_job_id=job_id,
                )
                raise AirflowFailException(
                    f"No output returned for Cytora job {job_id}. Status may be errored or under human review."
                )
            logger.info(f"Retrieved output for Cytora job {job_id}")

            logger.info(
                f"Saving full output for renewal flow job to blob storage: {job_id}"
            )
            try:
                full_output_key = save_cytora_output_to_blob_storage(
                    output=output, key_prefix=RENEWAL_FULL_OUTPUTS_PREFIX, job_id=job_id
                )
            except Exception as e:
                set_cytora_job_status(
                    base_url=FUNCTION_APP_API,
                    email_processing_job_id=email_processing_job_id,
                    status="Failed to save full output to blob storage",
                    job_type="renewal",
                    cytora_job_id=job_id,
                )
                raise AirflowException(
                    f"Failed to save full output for job {job_id} to blob storage: {e}"
                )
            logger.info(f"Full output for job {job_id} saved to blob storage")

            logger.info(f"Extracting trimmed output for renewal flow job: {job_id}")
            extracted_output = extract_renewal_outputs(output)

            logger.info(
                f"Saving renewal job extracted output to DB: {email_processing_job_id}"
            )
            save_cytora_output_to_db(
                base_url=FUNCTION_APP_API,
                endpoint="/api/renewal-job-output",
                email_processing_job_id=email_processing_job_id,
                extracted_output=extracted_output,
            )
            logger.info(f"Extracted output for renewal job {job_id} saved to DB.")

            logger.info(f"Saving extracted output for renewal flow job: {job_id}")
            try:
                extracted_output_key = save_cytora_output_to_blob_storage(
                    output=extracted_output,
                    key_prefix=RENEWAL_EXTRACTED_OUTPUTS_PREFIX,
                    job_id=job_id,
                )
            except Exception as e:
                set_cytora_job_status(
                    base_url=FUNCTION_APP_API,
                    email_processing_job_id=email_processing_job_id,
                    status="Failed to save extracted output to blob storage",
                    job_type="renewal",
                    cytora_job_id=job_id,
                )
                raise AirflowException(
                    f"Failed to save extracted output for job {job_id} to blob storage: {e}"
                )
            logger.info(f"Extracted output for job {job_id} saved to blob storage")

            set_cytora_job_status(
                base_url=FUNCTION_APP_API,
                email_processing_job_id=email_processing_job_id,
                status="Completed",
                job_type="renewal",
                cytora_job_id=job_id,
            )

            return full_output_key, extracted_output_key

        metadata = extract_renewal_metadata(email_processing_job=email_processing_job)
        should_start_renewal_flow = check_if_should_start_renewal_flow(
            email_processing_job=metadata
        )
        slip_info = fetch_slip_files(email_processing_job=metadata)
        started = start_cytora_renewal_job(email_processing_job=slip_info)
        saved = save_cytora_renewal_output(email_processing_job=started)
        wait_for_cytora_renewal_job = CytoraApiStatusSensorOperator(
            task_id="wait_for_renewal_cytora_api_status",
            cytora_schema=CYTORA_SCHEMA_RENEWAL,
            job_id="{{ ti.xcom_pull(task_ids='renewal_flow.start_cytora_renewal_job')[ti.map_index]['renewal_job_id'] }}",
        )

        (
            metadata
            >> should_start_renewal_flow
            >> slip_info
            >> started
            >> wait_for_cytora_renewal_job
            >> saved
        )

    @task
    def get_main_flow_email_processing_jobs(email_processing_jobs: list[dict]):
        # This intermediate task is required for things to work properly.
        # Without it Airflow will mark downstream task groups as upstream_failed,
        # potentially due to how dependencies/XCom passing are handled between groups.
        # Keeping this ensures jobs are properly handed off to the next flow.
        return email_processing_jobs

    @task(trigger_rule="all_done")
    def end_email_processing_job(email_processing_job: dict):
        end_email_processing_job_in_db(
            base_url=FUNCTION_APP_API,
            overall_job_status="Completed",
            end_time=datetime.now(UTC).strftime(TIMESTAMP_FORMAT_READABLE_MICROSECONDS)[:-3],
            email_processing_job_id=email_processing_job["id"],
        )
        return email_processing_job

    init_email_processing_jobs = init_email_processing_jobs()

    email_processing_jobs = download_email_eml_file.expand(
        email_processing_job=init_email_processing_jobs
    )
    uploaded_jobs = upload_email_eml_file_to_cytora.expand(
        email_processing_job=email_processing_jobs
    )
    main_flow_jobs = run_main_flow.expand(email_processing_job=uploaded_jobs)
    returned_main_flow_jobs = get_main_flow_email_processing_jobs(main_flow_jobs)
    cytora_renewal_flow = run_renewal_flow.expand(
        email_processing_job=returned_main_flow_jobs
    )
    cytora_sov_flow = run_sov_flow.expand(email_processing_job=uploaded_jobs)

    ended_jobs = end_email_processing_job.expand(
        email_processing_job=uploaded_jobs
    )
    [cytora_renewal_flow, cytora_sov_flow] >> ended_jobs
    end = EmptyOperator(task_id="end", trigger_rule="none_failed_min_one_success")
    ended_jobs >> end


process_email_change_notifications()
