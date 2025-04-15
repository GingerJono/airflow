import asyncio
import logging

from airflow.providers.microsoft.azure.hooks.msgraph import (
    KiotaRequestAdapterHook as MSGraphHook,
)

logger = logging.getLogger(__name__)

MSGRAPH_CONNECTION_ID = "microsoft_graph"


async def get_email_from_id(email_id: str, mailbox: str):
    conn = MSGraphHook(conn_id=MSGRAPH_CONNECTION_ID)

    return await conn.run(url=f"/users/{mailbox}/messages/{email_id}")


async def delete_subscription(subscription_id: str):
    logger.info("Attempting to delete subscription %s", subscription_id)

    try:
        conn = MSGraphHook("msgraph")
        await conn.run(url=f"/subscriptions/{subscription_id}", method="DELETE")
    except Exception:
        logger.error("Exception occurred when deleting subscription", exc_info=True)

    logger.info("Done")


def send_email(subject: str, html_content: str, to_address: str, sending_mailbox: str):
    logger.info(f"Attempting to send email with subject {subject}")
    request_body = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "html",
                "content": html_content,
            },
            "toRecipients": [{"emailAddress": {"address": to_address}}],
        }
    }

    conn = MSGraphHook(conn_id=MSGRAPH_CONNECTION_ID)
    loop = asyncio.get_event_loop()

    result = loop.run_until_complete(
        conn.run(
            url=f"users/{sending_mailbox}/microsoft.graph.sendMail",
            method="POST",
            headers={"content-type": "application/json"},
            data=request_body,
        )
    )

    logger.info(result)
