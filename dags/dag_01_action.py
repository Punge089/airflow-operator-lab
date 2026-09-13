from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.bash import BashOperator


def greet(**context):
    name = context["params"]["name"]
    print(f"Hello, {name}! รันจาก PythonOperator ตรง ๆ")
    return f"greeted {name}"


def check_pandas_version():
    # ฟังก์ชันนี้รันใน venv แยกที่สร้างขึ้นใหม่ทุกครั้ง
    # ต้อง import ข้างในฟังก์ชันเท่านั้น (ไม่มี global scope ให้ใช้)
    import pandas
    print(f"pandas version ใน venv แยก: {pandas.__version__}")
    return pandas.__version__


with DAG(
    dag_id="dag_01_action",
    description="Action: PythonOperator / PythonVirtualenvOperator / SQLExecuteQueryOperator",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lab", "action"],
    params={"name": "porto"},
) as dag:

    # --- 1. PythonOperator พื้นฐาน ---
    say_hello = PythonOperator(
        task_id="say_hello",
        python_callable=greet,
    )

    # --- 2. PythonVirtualenvOperator: venv แยกต่างหาก (ต้องลง virtualenv + cloudpickle ก่อน) ---
    check_pandas = PythonVirtualenvOperator(
        task_id="check_pandas_in_venv",
        python_callable=check_pandas_version,
        requirements=["pandas==2.2.3"],
        system_site_packages=False,
    )

    # --- 3. SQLExecuteQueryOperator: ต้องมี connection "pg_lab" + ตาราง source_data ก่อน ---
    count_source = SQLExecuteQueryOperator(
        task_id="count_source_rows",
        conn_id="pg_lab",
        sql="SELECT COUNT(*) FROM source_data;",
    )

    # --- 4. BashOperator ปกติ (ไม่มีกับดัก template_ext) ---
    safe_echo = BashOperator(
        task_id="safe_echo_no_trap",
        bash_command='echo "ข้อความปกติ ไม่มีปัญหา"',
    )
