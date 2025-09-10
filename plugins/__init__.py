from airflow.plugins_manager import AirflowPlugin
from email_monitoring.cytora_api_status_sensor_operator import (
    CytoraApiStatusSensorOperator,
)
from email_monitoring.cytora_api_status_trigger import CytoraApiStatusTrigger


class CytoraPlugin(AirflowPlugin):
    name = "cytora_plugin"
    operators = [CytoraApiStatusSensorOperator]
    triggers = [CytoraApiStatusTrigger]
