import logging

from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

logger = logging.getLogger(__name__)

WASB_CONNECTION_ID = "wasb"


def write_string_to_file(container_name: str, blob_name: str, string_data: str):
    logger.info("Preparing to write string to blob %s", blob_name)
    logger.debug(
        "Preparing to write string to blob.\nContainer: %s\nBlob: %s\nData: %s",
        container_name,
        blob_name,
        string_data,
    )
    conn = WasbHook(wasb_conn_id=WASB_CONNECTION_ID)
    conn.load_string(
        string_data=string_data,
        container_name=container_name,
        blob_name=blob_name,
        create_container=False,
    )


def read_file_as_string(container_name: str, blob_name: str) -> str:
    logger.info("Preparing to read blob as string %s", blob_name)
    conn = WasbHook(wasb_conn_id=WASB_CONNECTION_ID)
    return conn.read_file(container_name=container_name, blob_name=blob_name)
