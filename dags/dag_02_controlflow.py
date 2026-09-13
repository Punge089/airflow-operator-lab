from datetime import datetime

import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, ShortCircuitOperator
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


def choose_path(**context):
    choice = context["params"]["path"]
    return f"path_{choice}"


def check_data_condition(**context):
    return context["params"]["proceed"]


# ============================================================
# DAG 1: dag_02_controlflow — Branch / trigger_rule / ShortCircuit / TriggerDagRun
# ============================================================
with DAG(
    dag_id="dag_02_controlflow",
    description="Control-flow: Branch / trigger_rule / ShortCircuit / TriggerDagRun",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lab", "controlflow"],
    params={"path": "a", "proceed": True},
) as dag:

    # --- ส่วนที่ 1: BranchPythonOperator + trigger_rule trap ---
    start = EmptyOperator(task_id="start")

    choose = BranchPythonOperator(
        task_id="choose_path",
        python_callable=choose_path,
    )

    path_a = EmptyOperator(task_id="path_a")
    path_b = EmptyOperator(task_id="path_b")

    # กิ่งซ้าย: ปล่อย default trigger_rule (all_success) ไว้ตั้งใจ ให้เห็น skip cascade สด ๆ
    join_broken = EmptyOperator(task_id="join_broken")

    # กิ่งขวา: แก้ trigger_rule ให้รอดจาก branch ได้ถูกต้อง
    join_fixed = EmptyOperator(
        task_id="join_fixed",
        trigger_rule="none_failed_min_one_success",
    )

    start >> choose >> [path_a, path_b]
    [path_a, path_b] >> join_broken
    [path_a, path_b] >> join_fixed

    # --- ส่วนที่ 2: ShortCircuitOperator (เส้นอิสระ) ---
    check_data = ShortCircuitOperator(
        task_id="check_data",
        python_callable=check_data_condition,
    )

    after_gate = EmptyOperator(task_id="after_gate")

    check_data >> after_gate

    # --- ส่วนที่ 3: TriggerDagRunOperator (สั่ง dag_02_target ให้รัน) ---
    trigger_target = TriggerDagRunOperator(
        task_id="trigger_target",
        trigger_dag_id="dag_02_target",
        conf={"triggered_by": "dag_02_controlflow"},
    )


# ============================================================
# DAG 2: dag_02_target — ปลายทางที่ถูก TriggerDagRunOperator สั่งรัน
# ============================================================
with DAG(
    dag_id="dag_02_target",
    description="DAG ปลายทาง ถูกสั่งรันโดย dag_02_controlflow",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lab", "controlflow"],
) as target_dag:

    received = EmptyOperator(task_id="received")


# ============================================================
# DAG 3: dag_02_latestonly — ต้องมี schedule จริงถึงเห็นผล (สาธิตด้วย airflow backfill create)
# ============================================================
with DAG(
    dag_id="dag_02_latestonly",
    description="LatestOnlyOperator - skip ถ้าไม่ใช่ run ล่าสุด (สาธิตด้วย airflow backfill create)",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,   # ปิด catchup ไว้เป็นค่าเริ่มต้น กันรันย้อนหลังพรวดพราดตอนเปิด DAG
    tags=["lab", "controlflow"],
) as latestonly_dag:

    latest_only = LatestOnlyOperator(task_id="latest_only")
    after_latest = EmptyOperator(task_id="after_latest")

    latest_only >> after_latest
