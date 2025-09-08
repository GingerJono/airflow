import json
import logging
import time

import requests
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.hooks.base import BaseHook

CYTORA_CONNECTION_ID = "cytora"
CYTORA_AUTH_URL = "https://token.cytora.com/oauth/token"

logger = logging.getLogger(__name__)


class CytoraHook:
    def __init__(self, schema_config_id: str, conn_id=CYTORA_CONNECTION_ID):
        self.schema_config_id = schema_config_id
        self.conn = BaseHook.get_connection(conn_id)

        self.client_id = self.conn.login
        self.client_secret = self.conn.password
        self.auth_audience = self.conn.extra_dejson["auth_audience"]
        self.region = self.conn.extra_dejson["region"]
        self.workspace = self.conn.extra_dejson["workspace"]

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
        url = f"https://{self.region}.gateway.cytora-prod.com/files/workspaces/{self.workspace}/uploads/presigned-url"
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
        url = f"https://{self.region}.gateway.cytora-prod.com/files/workspaces/{self.workspace}/files"
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
        url = f"https://{self.region}.gateway.cytora-prod.com/digitize/workspaces/{self.workspace}/schemas/jobs"
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
        url = f"https://{self.region}.gateway.cytora-prod.com/digitize/workspaces/{self.workspace}/schemas/jobs/{job_id}"
        headers = self._get_headers()
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        return data["status"], data["output_status"]

    def get_schema_job_output(self, job_id):
        url = f"https://{self.region}.gateway.cytora-prod.com/digitize/workspaces/{self.workspace}/schemas/jobs/{job_id}/output"
        headers = self._get_headers()
        response = requests.get(url, headers=headers)
        if response.status_code == 400:
            raise AirflowException(
                "Job is not finished yet. Wait for completion first."
            )
        response.raise_for_status()
        return response.json()

    def wait_for_schema_job(
        self, job_id, poll_interval=10, max_attempts=250, output_file_path=None
    ):
        logger.info(f"Waiting for schema job {job_id} to complete...")
        attempt = 0
        while attempt < max_attempts:
            try:
                status, output_status = self.get_schema_job_status(job_id)
                logger.info(
                    f"Attempt {attempt + 1}: Status = {status}, Output Status = {output_status}"
                )

                if status == "finished" and output_status == "confirmed":
                    logger.info("Job completed. Fetching output...")
                    output = self.get_schema_job_output(job_id)
                    if output_file_path:
                        with open(output_file_path, "w") as f:
                            json.dump(output, f, indent=2)
                        logger.info(f"Output saved to {output_file_path}")
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
                    return None  # End polling gracefully

                time.sleep(poll_interval)
                attempt += 1

            except Exception as e:
                raise AirflowException(
                    f"Polling error for job {job_id}: {e}", exc_info=True
                )

        raise AirflowFailException(
            f"Job {job_id} did not complete within {max_attempts * poll_interval} seconds."
        )
