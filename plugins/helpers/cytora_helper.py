import json
import logging
import time

import requests
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.hooks.base import BaseHook

CYTORA_CONNECTION_ID = "cytora"
CYTORA_AUTH_URL = "https://token.cytora.com/oauth/token"
CYTORA_SCHEMA_MAIN = "ds:cfg:wr2pxXtxctBgFaZP"

CYTORA_API_POLL_INTERVAL = 30
CYTORA_API_TIMEOUT = 1800

logger = logging.getLogger(__name__)

DATETIME_FORMATS = [
    "%d/%m/%Y",
    "%Y-%m-%d",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%fZ",
]


def get_field_value(output: dict, key: str):
    """Safely extract output["fields"][key]["value"], or return None."""
    try:
        node = output.get("fields", {}).get(key, {})
        return node.get("value")
    except Exception:
        return None


def safe_stringify(v):
    """Return str(value) if value is not None, else None."""
    return None if v is None else str(v)


def safe_parse_date(v):
    """Try to parse a string using known datetime formats; return date or None."""
    if not v:
        return None

    s = str(v).strip()
    from datetime import datetime as dt

    for fmt in DATETIME_FORMATS:
        try:
            return dt.strptime(s[: len(fmt)], fmt).date()
        except Exception:
            continue
    return None


CYTORA_OUTPUT_FIELD_MAP_MAIN = {
    "OutputInsuredName": ("insured_name", safe_stringify),
    "OutputInsuredDomicile": ("insured_domicile", safe_stringify),
    "OutputInsuredState": ("insured_state", safe_stringify),
    "OutputReinsuredName": ("reinsured_name", safe_stringify),
    "OutputReinsuredDomicile": ("reinsured_domicile", safe_stringify),
    "OutputReinsuredState": ("reinsured_state", safe_stringify),
    "OutputBrokerCompany": ("broker_company", safe_stringify),
    "OutputBrokerContact": ("broker_contact", safe_stringify),
    "OutputLineOfBusiness": ("line_of_business", safe_stringify),
    "OutputPolicyType": ("policy_type", safe_stringify),
    "OutputUnderwriter": ("underwriter", safe_stringify),
    "OutputInceptionDate": ("inception_date", safe_parse_date),
    "OutputExpiryDate": ("expiry_date", safe_parse_date),
    "OutputRiskLocation": ("risk_location", safe_stringify),
    "OutputAdditionalParties": ("additional_parties", safe_stringify),
    "OutputSubmissionSummary": ("submission_summary", safe_stringify),
    "OutputNewRenewal": ("new_vs_renewal", safe_stringify),
    "OutputRenewedFrom": ("renewed_from", safe_stringify),
}


class CytoraHook:
    def __init__(self, schema_config_id: str, conn_id=CYTORA_CONNECTION_ID):
        self.schema_config_id = schema_config_id
        self.conn = BaseHook.get_connection(conn_id)

        self.client_id = self.conn.login
        self.client_secret = self.conn.password
        self.auth_audience = self.conn.extra_dejson["auth_audience"]
        self.workspace = self.conn.extra_dejson["workspace"]
        self.cytora_url_prefix = self.conn.extra_dejson["url_prefix"]

        self.token = None
        self.token_expiry = 0

        self._authenticate()

    def _authenticate(self):
        headers = {"accept": "application/json", "content-type": "application/json"}
        payload = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "audience": self.auth_audience,
            "grant_type": "client_credentials",
        }

        response = requests.post(CYTORA_AUTH_URL, headers=headers, json=payload)
        response.raise_for_status()

        data = response.json()
        self.token = data["access_token"]
        self.token_expiry = time.time() + data["expires_in"]
        logger.info("Authenticated with Cytora.")

    def _get_headers(self):
        if time.time() >= self.token_expiry:
            logger.info("Token expired. Re-authenticating...")
            self._authenticate()

        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    def get_presigned_url(self):
        logger.info("Requesting pre-signed upload URL...")
        url = f"{self.cytora_url_prefix}/files/workspaces/{self.workspace}/uploads/presigned-url"
        headers = self._get_headers()

        response = requests.get(url, headers=headers)
        response.raise_for_status()

        data = response.json()
        logger.debug("Full response: %s", json.dumps(data, indent=2))

        if "url" not in data or "id" not in data:
            raise AirflowFailException(
                f"Presigned URL response missing required keys: {json.dumps(data)}"
            )

        logger.info("Pre-signed URL obtained.")
        return data["url"], data["id"]

    def upload_file(self, upload_url, file_stream=None, content_type="text/plain"):
        logger.info("Uploading file to Cytora: [streamed]")
        headers = {"Content-Type": content_type}

        response = requests.put(upload_url, headers=headers, data=file_stream)
        response.raise_for_status()
        logger.info("File uploaded successfully.")
        return response.status_code

    def create_file(self, upload_id, file_name, media_type):
        logger.info(f"Registering uploaded file: {file_name}")
        url = f"{self.cytora_url_prefix}/files/workspaces/{self.workspace}/files"
        payload = {"upload_id": upload_id, "name": file_name, "media_type": media_type}
        headers = self._get_headers()
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()

        data = response.json()
        logger.debug(
            "Full response from file registration: %s", json.dumps(data, indent=2)
        )

        if "id" not in data:
            raise AirflowFailException(f"'id' not found in response: {data}")

        logger.info("File registered with Cytora.")
        return data["id"]

    def create_schema_job(self, file_id, job_name):
        logger.info(f"Creating schema job for file_id: {file_id}")
        url = f"{self.cytora_url_prefix}/digitize/workspaces/{self.workspace}/schemas/jobs"
        payload = {
            "schema_config_id": self.schema_config_id,
            "file_ids": [file_id],
            "name": job_name,
        }
        headers = self._get_headers()
        response = requests.post(url, headers=headers, json=payload)
        if response.status_code not in [200, 201]:
            raise AirflowException(
                f"Schema job creation failed: {response.status_code} - {response.text}"
            )
        logger.info("Schema job created.")
        return response.json()["id"]

    def get_schema_job_status(self, job_id):
        url = f"{self.cytora_url_prefix}/digitize/workspaces/{self.workspace}/schemas/jobs/{job_id}"
        headers = self._get_headers()
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data["status"], data["output_status"]

    def get_schema_job_output(self, job_id):
        url = f"{self.cytora_url_prefix}/digitize/workspaces/{self.workspace}/schemas/jobs/{job_id}/output"
        headers = self._get_headers()
        response = requests.get(url, headers=headers)
        if response.status_code == 400:
            raise AirflowException(
                "Job is not finished yet. Wait for completion first."
            )
        response.raise_for_status()
        return response.json()

    def get_result_for_schema_job(self, job_id):
        status, output_status = self.get_schema_job_status(job_id)

        if status == "finished" and output_status == "confirmed":
            logger.info("Job completed. Fetching output...")
            output = self.get_schema_job_output(job_id)
            return output

        elif status == "errored":
            logger.warning(
                f"Job {job_id} finished with error status. Output status: {output_status}"
            )
            # Optionally fetch and log any available partial output
            try:
                output = self.get_schema_job_output(job_id)
                logger.debug(f"Partial output: {json.dumps(output, indent=2)}")
            except Exception as fetch_err:
                logger.debug(f"No output could be retrieved: {fetch_err}")
            return None
