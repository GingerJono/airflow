import asyncio
import logging
from datetime import datetime

from airflow.triggers.base import BaseTrigger, TriggerEvent
from helpers.cytora_helper import (
    CYTORA_API_POLL_INTERVAL,
    CYTORA_API_TIMEOUT,
    CytoraHook,
)


class CytoraApiStatusTrigger(BaseTrigger):
    def __init__(
        self,
        cytora_schema: str,
        job_id: str,
        poll_interval: int = CYTORA_API_POLL_INTERVAL,
        timeout: int = CYTORA_API_TIMEOUT,
    ):
        super().__init__()
        self.cytora_schema = cytora_schema
        self.job_id = job_id
        self.poll_interval = poll_interval
        self.timeout = timeout

    def serialize(self):
        return (
            "email_monitoring.cytora_api_status_trigger.CytoraApiStatusTrigger",
            {
                "cytora_schema": self.cytora_schema,
                "job_id": self.job_id,
                "poll_interval": self.poll_interval,
                "timeout": self.timeout,
            },
        )

    async def run(self):
        logging.info(self.job_id)
        start_time = datetime.now()

        while True:
            # Check timeout
            if (datetime.now() - start_time).total_seconds() > self.timeout:
                yield TriggerEvent({"status": "timeout"})
                return

            # Poll API
            # The status can be one of: started, pending, finished, errored
            # The output_status can be one of: building, incomplete, ready, external_review, validating, human_review, confirming, confirmed, confirmed_with_error
            cytora_main = CytoraHook(self.cytora_schema)
            status, output_status = cytora_main.get_schema_job_status(self.job_id)

            if status == "finished":
                yield TriggerEvent({"status": status, "output_status": output_status})
                return
            elif status == "errored":
                yield TriggerEvent({"status": status, "output_status": output_status})
                return

            await asyncio.sleep(self.poll_interval)
