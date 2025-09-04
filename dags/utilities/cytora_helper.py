import logging
import time

import requests
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
