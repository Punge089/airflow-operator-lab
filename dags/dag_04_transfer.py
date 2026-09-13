from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.common.sql.operators.generic_transfer import GenericTransfer

# ต้องมี connection "pg_lab" (conn_type=postgres) และตาราง source_data อยู่ก่อน
# ดูวิธีสร้างใน README.md

with DAG(
    dag_id="dag_04_transfer",
    description="Transfer: GenericTransfer (Postgres -> Postgres)",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lab", "transfer"],
) as dag:

    transfer = GenericTransfer(
        task_id="transfer_source_to_dest",
        sql="SELECT id, name, created_at FROM source_data;",
        destination_table="dest_data",
        source_conn_id="pg_lab",
        destination_conn_id="pg_lab",
        preoperator=[
            "DROP TABLE IF EXISTS dest_data",
            "CREATE TABLE dest_data (id INT, name TEXT, created_at TIMESTAMP)",
        ],
    )
