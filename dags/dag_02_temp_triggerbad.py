from datetime import datetime
from airflow.sdk import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    dag_id="dag_02_temp_triggerbad",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lab", "temp"],
) as dag:
    trigger_bad = TriggerDagRunOperator(
        task_id="trigger_bad",
        trigger_dag_id="dag_02_target_does_not_exist",
    )
