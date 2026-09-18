# Airflow 3.3 Operator Lab

Repo สำหรับลองทำ lab Airflow 3 ด้วยตัวเอง — DAG ตัวอย่าง 6 ไฟล์ แบ่งตามกลไก 5 กลุ่ม
(Action, Control-flow, Sensor, Transfer, Deferrable) + Intro เขียนขึ้นเพื่อเรียนรู้ Airflow 3
อย่างละเอียด ทุกไฟล์ทดสอบรันจริงแล้วบน Airflow **3.3.1** / Python **3.14** (WSL2 Ubuntu)

เอกสารอธิบายละเอียด (concept, evidence, กับดักที่เจอ) อยู่แยกเป็นไฟล์ `.md` ต่างหาก
ไม่ได้รวมอยู่ใน repo นี้ — ถามคนที่ดูแล repo นี้ถ้าต้องการ

## โครงสร้าง

```
dags/
├── dag_00_hello.py              Intro — DAG object + task dependency (BashOperator, EmptyOperator)
├── dag_01_action.py             Action — PythonOperator / PythonVirtualenvOperator / SQLExecuteQueryOperator
├── dag_02_controlflow.py        Control-flow — Branch / trigger_rule / ShortCircuit / TriggerDagRun / LatestOnly (3 DAG ในไฟล์เดียว)
├── dag_03_sensor.py             Sensor — PythonSensor (poke vs reschedule) + FileSensor
├── dag_04_transfer.py           Transfer — GenericTransfer (Postgres → Postgres)
└── dag_05_deferrable.py         Deferrable — FileSensor(deferrable=True)
```

## Requirements

- Airflow **3.3.1**, Python **3.10–3.14**
- ลงด้วย constraint file เสมอ:
  ```bash
  pip install "apache-airflow==3.3.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.14.txt"
  pip install -r requirements.txt --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.14.txt"
  ```
- Metadata DB เป็น **PostgreSQL** (ไม่ใช่ SQLite default) + `executor = LocalExecutor` ใน `airflow.cfg`

## Setup ก่อนรัน (จำเป็นสำหรับบาง DAG)

### 1. โฟลเดอร์ signal (สำหรับ `dag_03_sensor.py`, `dag_05_deferrable.py`)
```bash
mkdir -p $AIRFLOW_HOME/signals
```
ทดสอบโดยสร้างไฟล์เปล่า ๆ ด้วย `touch $AIRFLOW_HOME/signals/<ชื่อไฟล์ที่ sensor รอ>`

### 2. Connection `fs_default` (สำหรับ `FileSensor` ใน dag_03/dag_05)
```bash
airflow connections add fs_default --conn-type fs --conn-extra '{"path": "/"}'
```

### 3. Connection `pg_lab` + ตาราง `source_data` (สำหรับ dag_01/dag_04)
```bash
airflow connections add pg_lab \
  --conn-type postgres \
  --conn-host <POSTGRES_HOST> \
  --conn-login <USER> \
  --conn-password <PASSWORD> \
  --conn-schema <DATABASE> \
  --conn-port 5432
```
```sql
CREATE TABLE IF NOT EXISTS source_data (
    id SERIAL PRIMARY KEY,
    name TEXT,
    created_at TIMESTAMP DEFAULT now()
);
INSERT INTO source_data (name) VALUES ('alice'), ('bob'), ('charlie');
```

### 4. `dag_02_controlflow.py` — `dag_02_target` ต้องไม่ paused ด้วย
`TriggerDagRunOperator` ใน `dag_02_controlflow` สั่ง `dag_02_target` ให้รัน — ต้องเปิด toggle
ทั้งสอง DAG ใน UI (ไม่ใช่แค่ตัวหลัก) ถึงจะเห็นผลสำเร็จ

### 5. `dag_02_latestonly` — ต้อง backfill ถึงจะเห็นผล
DAG นี้ตั้งใจปล่อย `paused` ไว้ (schedule จริงทุกชั่วโมง) สาธิตผลจริงด้วย:
```bash
airflow backfill create --dag-id dag_02_latestonly --from-date "<ISO8601>" --to-date "<ISO8601>"
```

### 6. `dag_01_action.py` — `PythonVirtualenvOperator` ต้องมี `virtualenv` + `cloudpickle`
ไม่ใช้ extras syntax (`apache-airflow[virtualenv]` ไม่มีผลจริงบน 3.3.1) — อยู่ใน
`requirements.txt` แล้ว ลงตรง ๆ ได้เลย

## ⚠️ ข้อควรระวังเรื่อง secrets

`airflow.cfg` (ไม่ได้อยู่ใน repo นี้ — ดู `.gitignore`) จะมี connection string ของ metadata DB
รวม password อยู่ข้างใน **ห้าม commit ไฟล์นี้เด็ดขาด** เช่นเดียวกับ
`simple_auth_manager_passwords.json.generated`

## หมายเหตุสำคัญ

- ลง provider package ใหม่ (เช่น `apache-airflow-providers-postgres`) แล้ว **ต้อง restart
  ทั้ง 4 process** (`api-server`, `scheduler`, `dag-processor`, `triggerer`) — provider ไม่
  hot-reload เหมือนไฟล์ DAG
- Filter ด้วย tag `lab` ใน UI เพื่อดูเฉพาะ DAG ชุดนี้ ไม่ปนกับ DAG ตัวอย่างที่ Airflow แถมมาเอง
  (`load_examples` ยังเป็นค่า default `True`)
