import os
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.empty import EmptyOperator

SIGNAL_DIR = os.path.join(os.environ["AIRFLOW_HOME"], "signals")

# ใช้ FileSensor ตัวเดียวกับ dag_03_sensor.py เป๊ะ ต่างกันแค่ deferrable=True
# ต้องมี connection "fs_default" ก่อน (ดู dag_03_sensor.py)

with DAG(
    dag_id="dag_05_deferrable",
    description="Deferrable: FileSensor ตัวเดิม แค่ deferrable=True",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lab", "deferrable"],
) as dag:

    wait_file_deferred = FileSensor(
        task_id="wait_file_deferred",
        filepath=f"{SIGNAL_DIR}/deferred.txt",
        fs_conn_id="fs_default",
        poke_interval=5,
        timeout=300,
        mode="reschedule",   # ไม่มีผลอะไรถ้า deferrable=True แต่ใส่ไว้เผื่อ fallback
        deferrable=True,     # <- บรรทัดเดียวที่ต่างจาก dag_03_sensor.py
    )

    done_deferred = EmptyOperator(task_id="done_deferred")

    wait_file_deferred >> done_deferred
