import logging

import requests
from airflow.models import Variable

logger = logging.getLogger(__name__)


def get_llm_chat_response(email_subject, email_contents):
    azureAi_api_key = Variable.get("azureai_api_key")
    azureAI_endpoint = Variable.get("azureai_endpoint")

    headers = {"Content-Type": "application/json", "api-key": azureAi_api_key}

    payload = {
        "messages": [
            {
                "role": "system",
                "content": _get_prompt(email_subject, email_contents),
            }
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


def _get_prompt(email_subject, email_contents):
    return f"""Attached is the text of an email message. It contains an insurance submission. Summarise this into a few lines and return it to me.
Also, Tell me if you know anything about the prospective insured, i.e. have they had any big events that might give rise to an insurance claim in the news? 
Don't guess though. Keep it to at most 10 bullet points. Use British English in your response.
Subject of the email was {email_subject}. Text from the email body was: {email_contents}"""
