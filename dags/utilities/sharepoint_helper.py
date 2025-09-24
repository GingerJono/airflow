import logging
from asyncio import get_event_loop
from urllib.parse import quote

import requests
from airflow.models import Variable
from airflow.providers.microsoft.azure.hooks.msgraph import (
    KiotaRequestAdapterHook as MSGraphHook,
)
from utilities.blob_storage_helper import MONITORING_BLOB_CONTAINER, write_bytes_to_file

logger = logging.getLogger(__name__)

SHAREPOINT_MSGRAPH_CONNECTION_ID = "sharepoint_microsoft_graph"


def _run_sharepoint_msgraph_query(
    url: str,
    path_parameters: dict[str, any] | None = None,
    method: str = "GET",
    query_parameters: dict[str, any] | None = None,
    headers: dict[str, str] | None = None,
    data: dict[str, any] | str | None = None,
):
    logger.debug(
        "Submitting %s request to %s.\n\tPath Parameters: %s\n\tQuery Parameters %s\n\tHeaders: %s\n\tData: %s",
        method,
        url,
        path_parameters,
        query_parameters,
        headers,
        data,
    )
    conn = MSGraphHook(conn_id=SHAREPOINT_MSGRAPH_CONNECTION_ID)
    loop = get_event_loop()

    result = loop.run_until_complete(
        conn.run(
            url=url,
            path_parameters=path_parameters,
            method=method,
            query_parameters=query_parameters,
            headers=headers,
            data=data,
        )
    )

    logger.debug("Received result %s", result)

    return result


def get_sharepoint_site_id():
    sharepoint_site_hostname = Variable.get("sharepoint_site_hostname")
    sharepoint_site_path = Variable.get("sharepoint_site_path")

    url = f"sites/{sharepoint_site_hostname}:/{sharepoint_site_path}"
    data = _run_sharepoint_msgraph_query(url=url)
    site_id = data.get("id")

    if not site_id:
        raise RuntimeError(
            f"Unable to resolve site ID for {sharepoint_site_hostname}/{sharepoint_site_path}"
        )

    logging.info(f"Found siteId {site_id}")
    return site_id


def get_sharepoint_drive_id(site_id: str):
    sharepoint_library_name = Variable.get("sharepoint_library_name")

    url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
    logging.info(f"URL: {url}")

    drives = _run_sharepoint_msgraph_query(url=url).get("value", [])
    logging.info(f"Drives: {drives}")

    for d in drives:
        if d.get("name") == sharepoint_library_name:
            return d.get("id")

    raise RuntimeError(
        f"Drive/Library '{sharepoint_library_name}' not found on site {site_id}"
    )


def get_folder_by_programme_reference(drive_id: str, programme_reference: str):
    folder_url = f"drives/{drive_id}/root:/{quote(programme_reference)}"
    folder_data = _run_sharepoint_msgraph_query(url=folder_url)

    if "id" not in folder_data:
        return None

    return folder_data["id"]


def get_folder_items(drive_id: str, folder_id: str):
    children_url = f"drives/{drive_id}/items/{folder_id}/children"
    items = _run_sharepoint_msgraph_query(url=children_url).get("value", [])
    return items


def normalise(val):
    if isinstance(val, dict):
        return val.get("Value") or val.get("Label") or ""
    return val or ""


def get_filtered_folder_items(
    drive_id: str, yearOfAccount: str, document_type: str, folder_items
):
    filtered_folder_items = []
    fields = {}
    for item in folder_items:
        logging.info(f"Item: {item}")
        if "file" not in item:
            continue

        fields_url = f"drives/{drive_id}/items/{item['id']}/listItem?$expand=fields"
        try:
            fields_data = _run_sharepoint_msgraph_query(url=fields_url)
            fields = fields_data.get("fields", {})
            logging.info(f"Fields: {fields}")
        except Exception:
            fields = {}

        year_of_account_val = normalise(fields.get("YearOfAccount", ""))
        doc_type_val = normalise(fields.get("DocumentType", ""))

        year_of_account_match = (not year_of_account_val) or (
            str(year_of_account_val) == str(yearOfAccount)
        )
        doc_type_match = (not doc_type_val) or (
            str(doc_type_val).lower() == str(document_type).lower()
        )

        logging.info(f"Year of Account match: {year_of_account_match}")
        logging.info(f"Document Type match: {doc_type_match}")

        if year_of_account_match and doc_type_match:
            filtered_folder_items.append(item)

    if not filtered_folder_items:
        return None

    logging.info(f"Filtered items: {filtered_folder_items}")
    return [filtered_folder_items, fields]


def get_latest_file(files):
    files.sort(key=lambda f: f.get("lastModifiedDateTime", ""), reverse=True)
    return files[0]


def save_expired_file_to_blob_storage(file, blob_folder):
    download_url = file["@microsoft.graph.downloadUrl"]
    logging.info(f"Download URL: {download_url}")

    response = requests.get(download_url)
    response.raise_for_status()
    content_bytes = response.content

    path = f"{blob_folder}/{file['name']}"

    write_bytes_to_file(MONITORING_BLOB_CONTAINER, path, content_bytes)

    return path


def find_expiring_slip(
    programme_reference: str,
    year_of_account: int,
    document_type: str = "Slip",
    blob_folder: str = "",
):
    site_id = get_sharepoint_site_id()
    drive_id = get_sharepoint_drive_id(site_id=site_id)
    folder_id = get_folder_by_programme_reference(drive_id, programme_reference)

    if folder_id is None:
        return None

    folder_items = get_folder_items(drive_id, folder_id)
    if not folder_items:
        return None

    [filtered_folder_items, fields] = get_filtered_folder_items(
        drive_id, year_of_account, document_type, folder_items
    )

    latest_file = get_latest_file(filtered_folder_items)
    file_key = save_expired_file_to_blob_storage(latest_file, blob_folder)

    return {
        "drive_id": drive_id,
        "drive_item_id": latest_file["id"],
        "name": latest_file["name"],
        "lastModifiedDateTime": latest_file.get("lastModifiedDateTime"),
        "fields": fields if filtered_folder_items else {},
        "file_key": file_key,
    }
