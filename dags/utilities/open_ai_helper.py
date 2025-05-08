import logging

import requests
from airflow.models import Variable

logger = logging.getLogger(__name__)


ROLE_ASSIGNMENT = "You are an assistant to an insurance underwriting team, working for Dale Underwriting Partners. Your job is to write email summaries of submissions for prospective insurance policies to help underwriters assess opportunities."
INFORMATION_ABOUT_DALE = """Dale are a midsize Lloyd's Specialty Insurer, specialising in the following classes of business (subclasses listed in brackets):
- Casualty (Automobile Liability, General Liability, GL - Contractors, Medical Malpractice PICA, MPL Treaty & Binders, Personal Accident, Professional Liab (Non-Med),Sports & Leisure, Workers Compensation)
- Energy (Construction, Control of Well, Energy Liability, Gulf of Mexico Wind, Physical Damage)
- Healthcare (Healthcare PI, International Medical Malpractice, US Healthcare Open Market)
- Marine Reinsurance (Marine Pro Rata, Marine XL)
- Portfolio Solutions (Bloodstock, Package,Political Risk, Power, Space, Terrorism)
- Professional Lines (Construction PI, Financial Lines, Management Liability, Non-Construction PI, Public Liability)
- Property Facilities (Facilities International, Facilities US, Facility, Liability)
- Property Open Market (Open Market, Open Market International, Open Market US, Specialty Insurance, Transportation Insurance)
- Property Reinsurance (Quota Share CHECK, Catastrophe International, Catastrophe US, Catastrophe XL, Nuclear, Risk XL, Satellite, Specialty Reinsurance Other)
- Specialty Insurance (Accident & Health, Aviation Reinsurance, Contingency DUA, Contingency Open Market, Special Risks, Specialty Property, Sports PA DUA, Sports PA Open Market)"""
INFORMATION_OF_INTEREST = """Information of interest includes but is not limited to:
- Company details (company name, industry, size, location).
- Type of insurance being offered.
- Geographical exposure profile (if available). For a worldwide geographical spread, list out the top countries with their % of the overall exposure, but for a US-specific geographical spread, list out the top States with their % of the overall exposure.
- Proposed clauses/terms and conditions.
- Deductibles being proposed by the broker.
- Claims history summary (if available). Highlighting the length of the claims record provided, any "as-if"ing on deductible structure that has taken place, details about key claims and an overall summary of the claims activity.
- Details on historical placement, i.e. if this has previously been placed at Lloyd's, whether Dale has participated.
- Risk management factors highlighted in the submission.
- Information that may affect the underwriters interest."""
LANGUAGE_AND_STRUCTURE = """Language and structure:
- Structure with clear headings for each section.
- Use the UK English spellings of words.
- Format using markdown syntax. Use headings where appropriate e.g. '# H1' and '## H2'"""
TONE = """Tone of responses:
- Be professional and objective.
- Factual rather than speculative.
- Flag areas of concern without making recommendations.
- Be concise in your summarisation."""

SYSTEM_PROMPT = f"{ROLE_ASSIGNMENT}\n{INFORMATION_ABOUT_DALE}\n{INFORMATION_OF_INTEREST}\n{LANGUAGE_AND_STRUCTURE}\n{TONE}"


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
