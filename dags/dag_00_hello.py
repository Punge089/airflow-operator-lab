from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dag_00_hello",
    description="DAG แรก - เข้าใจ DAG object + task dependency",
    schedule=None,          # ไม่มี schedule อัตโนมัติ ต้องกด trigger เองใน UI
    start_date=datetime(2026, 1, 1),
    catchup=False,           # ไม่ต้อง backfill ย้อนหลัง
    tags=["lab", "intro"],
) as dag:

    start = EmptyOperator(task_id="start")

    say_hello = BashOperator(
        task_id="say_hello",
        bash_command='echo "Hello from Airflow 3, user: $(whoami)"',
    )

    end = EmptyOperator(task_id="end")

    start >> say_hello >> end
