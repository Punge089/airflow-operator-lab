from datetime import datetime
from typing import Sequence

from airflow.sdk import DAG, BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class RowCountOperator(BaseOperator):
    """Custom operator: นับจำนวนแถวในตาราง Postgres แล้ว log + ส่งค่ากลับผ่าน XCom"""

    template_fields: Sequence[str] = ("table_name",)

    def __init__(self, table_name: str, postgres_conn_id: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name
        self.postgres_conn_id = postgres_conn_id

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        result = hook.get_first(f"SELECT COUNT(*) FROM {self.table_name}")
        count = result[0]
        self.log.info("ตาราง %s มี %s แถว", self.table_name, count)
        return count


# ต้องมี connection "pg_lab" และตาราง source_data อยู่ก่อน (ดู README.md)

with DAG(
    dag_id="dag_06_custom",
    description="Custom Operator: RowCountOperator เขียนเอง",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lab", "custom"],
    params={"table": "source_data"},
) as dag:

    count_rows = RowCountOperator(
        task_id="count_rows",
        table_name="{{ params.table }}",   # ต้องอยู่ใน template_fields ถึงจะ render
        postgres_conn_id="pg_lab",
    )
