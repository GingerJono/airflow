import requests


def create_email_processing_job(base_url: str, email_id: str, start_time: str):
    response = requests.post(
        base_url + "/api/new-email-received",
        json={"msGraphEmailId": email_id, "startTime": start_time},
    )
    response.raise_for_status()  # will raise an exception if not 2xx
    return response


def set_cytora_job_status(
    base_url: str,
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

    response = requests.post(base_url + "/api/cytora-job-status", json=payload)
    response.raise_for_status()
    return response


def save_cytora_output_to_db(
    base_url: str,
    endpoint: str,
    email_processing_job_id: str,
    extracted_output: str | dict[str, str],
):
    response = requests.post(
        f"{base_url}{endpoint}",
        json={"id": email_processing_job_id, "output": extracted_output},
    )
    response.raise_for_status()  # will raise an exception if not 2xx


def end_email_processing_job_in_db(
    base_url: str, email_processing_job_id: str, end_time: str, overall_job_status: str
):
    response = requests.post(
        base_url + "/api/email-processing-finished",
        json={
            "id": email_processing_job_id,
            "endTime": end_time,
            "overallJobStatus": overall_job_status,
        },
    )
    response.raise_for_status()
