from datetime import datetime

from airflow.decorators import dag, task
from utilities.cytora_helper import CytoraHook


@task
def test_cytora_auth(schema_config_id: str = "dummy"):
    CytoraHook(schema_config_id=schema_config_id)


@dag(schedule=None, start_date=datetime(2023, 1, 1), catchup=False, tags=["test"])
def test_cytora_auth_dag():
    test_cytora_auth()


dag = test_cytora_auth_dag()
