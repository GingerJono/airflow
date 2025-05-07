import logging

import requests
from airflow.models import Variable

logger = logging.getLogger(__name__)


ROLE_ASSIGNMENT = "You are an assistant to an insurance underwriting team. Your job is to write email summaries of submissions for prospective insurance policies to help underwriters assess opportunities."
INFORMATION_OF_INTEREST = """Information of interest includes but is not limited to:
+ Company details (company name, industry, size, location).
+ Type of insurance being offered.
+ Proposed clauses/terms and conditions.
+ Deductibles being proposed by the broker.
+ Claims history summary (if available).
+ Risk assessment factors highlighted in the submission.
+ Information that may affect the underwriters interest."""
LANGUAGE_AND_STRUCTURE = """Language and structure:
+ Structure with clear headings for each section.
+ Use the UK English spellings of words.
+ Format using markdown syntax."""
TONE = """Tone of responses:
+ Be professional and objective.
+ Factual rather than speculative.
+ Flag areas of concern without making recommendations.
+ Be concise in your summarisation."""

SYSTEM_PROMPT = (
    f"{ROLE_ASSIGNMENT}\n{INFORMATION_OF_INTEREST}\n{LANGUAGE_AND_STRUCTURE}\n{TONE}"
)


def _get_user_prompt(email_subject, email_contents, attachments):
    subject = f"Email subject: {email_subject}"
    email_text = f"Email body:\n{email_contents}"

    attachments_text = ""
    if attachments:
        attachments_text = f"Email attachments:\n{',\n'.join(attachments)}"

    return f"{subject}\n{email_text}\n{attachments_text}"


def get_llm_chat_response(email_subject, email_contents, attachments_text) -> str:
    azureAi_api_key = Variable.get("azureai_api_key")
    azureAI_endpoint = Variable.get("azureai_endpoint")

    headers = {"Content-Type": "application/json", "api-key": azureAi_api_key}

    payload = {
        "messages": [
            {
                "role": "system",
                "content": SYSTEM_PROMPT,
            },
            {
                "role": "user",
                "content": _get_user_prompt(
                    email_subject, email_contents, attachments_text
                ),
            },
        ]
    }

    logger.debug(
        "Submitting LLM chat request\nHeader: %s\nPayload:%s", headers, payload
    )

    response = requests.post(azureAI_endpoint, headers=headers, json=payload)
    response.raise_for_status()

    response_json = response.json()

    logger.debug("Azure Open AI response %s", response_json)

    if "choices" in response_json and len(response_json["choices"]) > 0:
        content = response_json["choices"][0]["message"]["content"]

        return content
