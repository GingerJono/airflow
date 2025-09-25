import json
import mimetypes
from datetime import datetime

from helpers.cytora_helper import CytoraHook
from utilities.blob_storage_helper import (
    MONITORING_BLOB_CONTAINER,
    read_file_as_bytes,
    write_string_to_file,
)
from utilities.constants import (
    DEFAULT_DATE_FORMAT,
    TIMESTAMP_FORMAT_MILLISECONDS,
)


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


def guess_media_type(filename: str) -> str:
    mt, _ = mimetypes.guess_type(filename or "")
    return mt or "application/octet-stream"
