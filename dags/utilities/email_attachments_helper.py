import base64
import io
import json
import logging
from typing import Callable, Optional

import docx2txt

logger = logging.getLogger(__name__)

SUPPORTED_FILE_TYPES: dict[str, Callable[[io.BytesIO], str]] = {
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": docx2txt.process
}


def get_attachments_text(msgraph_attachments_response: dict) -> list[str]:
    attachments = []
    for msgraph_attachment in msgraph_attachments_response["value"]:
        attachment = _parse_attachment(msgraph_attachment)
        if attachment:
            attachments.append(json.dumps(attachment))

    return attachments


def _parse_attachment(msgraph_attachment: dict) -> Optional[dict]:
    file_type = msgraph_attachment["contentType"]

    if msgraph_attachment["isInline"]:
        logger.debug(
            "Attached file '%s' is an inline attachment and is therefore not included in the prompt",
            msgraph_attachment["name"],
        )
        return None

    if file_type not in SUPPORTED_FILE_TYPES:
        logger.warning(
            "Attached file '%s' is of an unsupported file type: %s",
            msgraph_attachment["name"],
            file_type,
        )
        return None

    attachment_bytes = base64.b64decode(msgraph_attachment["contentBytes"])

    attachment_file = io.BytesIO(attachment_bytes)

    attachment_text = SUPPORTED_FILE_TYPES[file_type](attachment_file)

    return {
        "original_content_type": file_type,
        "file_name": msgraph_attachment["name"],
        "text": attachment_text,
    }
