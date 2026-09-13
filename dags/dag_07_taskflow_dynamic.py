from datetime import datetime

from airflow.sdk import DAG, task

with DAG(
    dag_id="dag_07_taskflow_dynamic",
    description="TaskFlow API + Dynamic Task Mapping",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["lab", "taskflow"],
) as dag:

    # --- ส่วนที่ 1: TaskFlow ธรรมดา (XCom ไหลอัตโนมัติ ไม่ต้อง .set_upstream/xcom_pull เอง) ---
    @task
    def get_names():
        return ["alice", "bob", "charlie"]

    @task
    def greet(name: str):
        message = f"Hello, {name}!"
        print(message)
        return message

    @task
    def summarize(messages: list[str]):
        print(f"ทักทายไปทั้งหมด {len(messages)} คน")
        return len(messages)

    # --- ส่วนที่ 2: Dynamic Task Mapping (.expand สร้าง task ตาม runtime data) ---
    names = get_names()
    greetings = greet.expand(name=names)
    summarize(greetings)
