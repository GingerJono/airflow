import asyncio
import logging
from datetime import UTC, datetime, timedelta

from airflow.providers.microsoft.azure.hooks.msgraph import (
    KiotaRequestAdapterHook as MSGraphHook,
)

logger = logging.getLogger(__name__)

MSGRAPH_CONNECTION_ID = "microsoft_graph"


def _run_msgraph_query(
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
    conn = MSGraphHook(conn_id=MSGRAPH_CONNECTION_ID)
    loop = asyncio.get_event_loop()

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


def get_mailbox_subscriptions(mailbox: str):
    logging.info("Retrieving subscriptions for %s", mailbox)
    return _run_msgraph_query(url="subscriptions")


def delete_subscription(subscription_id: str):
    logger.info("Deleting subscription %s", subscription_id)

    _run_msgraph_query(url=f"/subscriptions/{subscription_id}", method="DELETE")


def renew_subscription(subscription_id: str, subscription_duration_minutes: int):
    logger.info("Renewing subscription %s", subscription_id)

    subscription_renewal_data = {
        "expirationDateTime": (
            datetime.now(UTC) + timedelta(minutes=subscription_duration_minutes)
        ).isoformat(),
    }
    _run_msgraph_query(
        url=f"subscriptions/{subscription_id}",
        method="PATCH",
        data=subscription_renewal_data,
    )


def create_subscription(
    mailbox: str,
    client_state: str,
    change_notification_url: str,
    lifecycle_notification_url: str,
    subscription_duration_minutes: int,
):
    logger.info("Creating subscription to mailbox %s", mailbox)
    subscription_change_data = {
        "changeType": "created",
        "resource": f"/users/{mailbox}/mailFolders('Inbox')/messages",
        "expirationDateTime": (
            datetime.now(UTC) + timedelta(minutes=subscription_duration_minutes)
        ).isoformat(),
        "notificationUrl": change_notification_url,
        "lifecycleNotificationUrl": lifecycle_notification_url,
        "clientState": client_state,
        "includeResourceData": False,
    }
    _run_msgraph_query(
        url="subscriptions", method="POST", data=subscription_change_data
    )


def get_email_from_id(email_id: str, mailbox: str):
    logger.info("Getting email from MS Graph based on ID: %s", email_id)
    return _run_msgraph_query(url=f"/users/{mailbox}/messages/{email_id}")


def get_attachments_from_email_id(email_id: str, mailbox: str):
    logger.info("Getting attachments info from MS Graph based on ID: %s", email_id)
    return _run_msgraph_query(url=f"/users/{mailbox}/messages/{email_id}/attachments")


def send_email(
    subject: str,
    html_content: str,
    to_addresses: list[str],
    sending_mailbox: str,
    attachments: list[dict],
):
    logger.info(f"Sending email with subject {subject}")
    request_body = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "html",
                "content": html_content,
            },
            "toRecipients": [
                {"emailAddress": {"address": to_address}} for to_address in to_addresses
            ],
            "attachments": attachments,
        }
    }

    _run_msgraph_query(
        url=f"users/{sending_mailbox}/microsoft.graph.sendMail",
        method="POST",
        headers={"content-type": "application/json"},
        data=request_body,
    )
