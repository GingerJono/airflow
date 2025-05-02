import base64
import io
import json
import logging
from typing import Callable, Optional

import docx2txt
import pandas as pd
import pptx
import pymupdf

logger = logging.getLogger(__name__)


def _extract_text_pdf(pdf_file: io.BytesIO) -> str:
    with pymupdf.open(stream=pdf_file, filetype="pdf") as doc:
        text = ""
        for page_num in range(len(doc)):
            page = doc[page_num]
            text += f"Page {page_num + 1}:\n{page.get_text()}\n"

    return text


def _extract_text_excel(excel_file: io.BytesIO) -> str:
    all_sheets = pd.read_excel(excel_file, sheet_name=None)

    csv_outputs = {}
    for sheet_name, df in all_sheets.items():
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_outputs[sheet_name] = csv_buffer.getvalue()

    return json.dumps(csv_outputs)


def _extract_text_powerpoint(powerpoint_file: io.BytesIO) -> str:
    presentation = pptx.Presentation(powerpoint_file)

    all_slides = []
    for i, slide in enumerate(presentation.slides):
        shapes_on_slide = []
        for shape in slide.shapes:
            shapes_on_slide.append({"shape_text": shape.text})
        all_slides.append({"slide_number": i + 1, "shapes": shapes_on_slide})

    return json.dumps({"presentation": all_slides})


SUPPORTED_FILE_TYPES: dict[str, Callable[[io.BytesIO], str]] = {
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": docx2txt.process,
    "application/pdf": _extract_text_pdf,
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": _extract_text_excel,
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": _extract_text_powerpoint,
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
        "original_file_type": file_type,
        "file_name": msgraph_attachment["name"],
        "text": attachment_text,
    }
