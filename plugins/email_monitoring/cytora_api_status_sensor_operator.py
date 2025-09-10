from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from email_monitoring.cytora_api_status_trigger import CytoraApiStatusTrigger
from helpers.cytora_helper import CYTORA_API_POLL_INTERVAL, CYTORA_API_TIMEOUT


class CytoraApiStatusSensorOperator(BaseOperator):
    def __init__(
        self,
        cytora_schema: str,
        job_id: str,
        poll_interval: int = CYTORA_API_POLL_INTERVAL,
        timeout: int = CYTORA_API_TIMEOUT,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.cytora_schema = cytora_schema
        self.job_id = job_id
        self.poll_interval = poll_interval
        self.timeout = timeout

    def execute(self, context: Context):
        self.defer(
            trigger=CytoraApiStatusTrigger(
                cytora_schema=self.cytora_schema,
                job_id=self.job_id,
                poll_interval=self.poll_interval,
                timeout=self.timeout,
            ),
            method_name="execute_complete",
        )

    # The execute_complete function is running after the trigger completes.
    # It is set up in the execute function above as the method to run on completion.
    def execute_complete(self, context: Context, event=None):
        if not event:
            raise AirflowException("No event received from trigger.")

        status = event.get("status")
        output_status = event.get("output_status")
        if status == "finished":
            self.log.info(
                f"Cytora Job returned status={status} and output_status={output_status}."
            )
            return "complete"
        elif output_status == "timeout":
            raise AirflowException(
                f"Cytora Job did not return complete within {self.timeout} seconds."
            )
        else:
            raise AirflowException(
                f"Unexpected status from trigger. Status: {status} and output status: {output_status}."
            )
