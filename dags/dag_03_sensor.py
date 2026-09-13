import os
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.providers.standard.operators.empty import EmptyOperator

SIGNAL_DIR = os.path.join(os.environ["AIRFLOW_HOME"], "signals")


def file_exists(path):
    def _check(**context):
        return os.path.exists(path)
    return _check


with DAG(
    dag_id="dag_03_sensor",
    description="Sensor: mode=poke vs mode=reschedule + FileSensor ตัวจริง",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lab", "sensor"],
) as dag:

    # --- ส่วนที่ 1: PythonSensor เทียบ poke vs reschedule ---
    wait_poke = PythonSensor(
        task_id="wait_poke",
        python_callable=file_exists(f"{SIGNAL_DIR}/poke.txt"),
        poke_interval=5,
        timeout=300,
        mode="poke",
    )

    wait_reschedule = PythonSensor(
        task_id="wait_reschedule",
        python_callable=file_exists(f"{SIGNAL_DIR}/reschedule.txt"),
        poke_interval=5,
        timeout=300,
        mode="reschedule",
    )

    done_poke = EmptyOperator(task_id="done_poke")
    done_reschedule = EmptyOperator(task_id="done_reschedule")

    wait_poke >> done_poke
    wait_reschedule >> done_reschedule

    # --- ส่วนที่ 2: FileSensor ของจริง (built-in ไม่ต้องเขียน os.path.exists เอง) ---
    # ต้องมี connection "fs_default" ก่อน:
    #   airflow connections add fs_default --conn-type fs --conn-extra '{"path": "/"}'
    wait_file = FileSensor(
        task_id="wait_file",
        filepath=f"{SIGNAL_DIR}/file_sensor_signal.txt",
        fs_conn_id="fs_default",
        poke_interval=5,
        timeout=300,
        mode="reschedule",
    )

    done_file = EmptyOperator(task_id="done_file")

    wait_file >> done_file
