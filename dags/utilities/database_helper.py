import requests
from airflow.models import Variable

FUNCTION_APP_API = Variable.get("function_app_api")
FUNCTION_APP_API_KEY = Variable.get("function_app_api_key")

base_url = FUNCTION_APP_API
api_access_key_url = "?code=" + FUNCTION_APP_API_KEY
function_app_request_timeout = 60


def create_api_path(path: str):
    return base_url + path + api_access_key_url


def create_email_processing_job(email_id: str, start_time: str):
    response = requests.post(
        create_api_path("/api/new-email-received"),
        json={"msGraphEmailId": email_id, "startTime": start_time},
        timeout=function_app_request_timeout,
    )
    response.raise_for_status()  # will raise an exception if not 2xx
    return response


def set_cytora_job_status(
    email_processing_job_id: str,
    status: str,
    job_type: str,
    cytora_job_id: str = None,
):
    payload = {
        "id": email_processing_job_id,
        "jobStatus": status,
        "cytoraJobType": job_type,
    }

    if cytora_job_id:
        payload["cytoraJobID"] = cytora_job_id

    response = requests.post(
        create_api_path("/api/cytora-job-status"),
        json=payload,
        timeout=function_app_request_timeout,
    )
    response.raise_for_status()
    return response


def save_cytora_output_to_db(
    endpoint: str,
    email_processing_job_id: str,
    extracted_output: str | dict[str, str],
):
    response = requests.post(
        create_api_path(endpoint),
        json={"id": email_processing_job_id, "output": extracted_output},
        timeout=function_app_request_timeout,
    )
    response.raise_for_status()  # will raise an exception if not 2xx


def end_email_processing_job_in_db(
    email_processing_job_id: str, end_time: str, overall_job_status: str
):
    response = requests.post(
        create_api_path("/api/email-processing-finished"),
        json={
            "id": email_processing_job_id,
            "endTime": end_time,
            "overallJobStatus": overall_job_status,
        },
        timeout=function_app_request_timeout,
    )
    response.raise_for_status()
