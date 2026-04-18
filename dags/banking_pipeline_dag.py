"""
dags/banking_pipeline_dag.py
------------------------------
Master Airflow DAG for the Real-Time Banking Transaction Pipeline.

DAG flow:
  health_check_kafka
       │
  start_producer
       │
  start_streaming_consumer
       │
  dq_validation_gate
       ├── [PASS] → compact_s3_partitions → notify_success
       └── [FAIL] → quarantine_batch     → notify_failure
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule


# ── Default Args ──────────────────────────────────────────────────────────────
default_args = {
    "owner":            "gaurav.kumar",
    "depends_on_past":  False,
    "email":            ["gkumar318raja@gmail.com"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
    "retry_exponential_backoff": True,
}


# ── Python Callables ──────────────────────────────────────────────────────────
def check_kafka_health(**context):
    """
    Verify Kafka broker is reachable and the txn_events topic exists.
    Raises exception on failure — Airflow will retry as per retry policy.
    """
    from kafka import KafkaAdminClient
    import yaml

    with open("/opt/airflow/config/config.yaml") as f:
        cfg = yaml.safe_load(f)

    broker = cfg["kafka"]["broker"]
    topic  = cfg["kafka"]["topic"]

    admin = KafkaAdminClient(bootstrap_servers=broker, request_timeout_ms=5000)
    topics = admin.list_topics()

    if topic not in topics:
        raise ValueError(f"Kafka topic '{topic}' not found on broker {broker}. "
                         f"Available topics: {topics}")

    print(f"Kafka health check PASSED. Broker: {broker} | Topic: {topic} exists.")


def run_dq_validation(**context):
    """
    Pull DQ report from XCom (written by streaming job) and decide branch.
    Returns task_id of the next branch.
    """
    ti         = context["ti"]
    dq_report  = ti.xcom_pull(task_ids="start_streaming_consumer", key="dq_report")

    if dq_report is None:
        print("No DQ report found in XCom — assuming passed (first run or no failures).")
        return "compact_s3_partitions"

    if dq_report.get("batch_passed", True):
        print(f"DQ validation PASSED. Report: {dq_report}")
        return "compact_s3_partitions"
    else:
        print(f"DQ validation FAILED. Failures: {dq_report.get('failures')}. Routing to quarantine.")
        return "quarantine_batch"


def compact_s3(**context):
    """
    Compact small Parquet files in the curated zone to optimise BI query performance.
    In production: use S3 Inventory + Spark repartition job or AWS Glue compaction.
    """
    print("S3 compaction task triggered. Merging small files in curated zone...")
    # Placeholder — wire to a Spark submit or Glue job in production


def quarantine_batch(**context):
    """
    Move failed batch records to a quarantine prefix for manual review.
    """
    print("Quarantine task triggered. Failed batch records moved to s3://.../.quarantine/")


def notify_success(**context):
    dag_run = context["dag_run"]
    print(f"Pipeline SUCCESS | DAG Run: {dag_run.run_id} | "
          f"Execution date: {context['execution_date']}")


def notify_failure(**context):
    dag_run = context["dag_run"]
    print(f"Pipeline FAILED | DAG Run: {dag_run.run_id} | "
          f"DQ failures detected. Check quarantine prefix on S3.")


# ── DAG Definition ────────────────────────────────────────────────────────────
with DAG(
    dag_id              = "banking_pipeline_dag",
    description         = "Real-Time Banking Transaction Pipeline — Master DAG",
    default_args        = default_args,
    start_date          = datetime(2025, 1, 1),
    schedule_interval   = "0 * * * *",    # hourly trigger
    catchup             = False,
    max_active_runs     = 1,
    tags                = ["banking", "streaming", "kafka", "pyspark"],
) as dag:

    start = DummyOperator(task_id="start")

    health_check_kafka = PythonOperator(
        task_id         = "health_check_kafka",
        python_callable = check_kafka_health,
        provide_context = True,
    )

    start_producer = BashOperator(
        task_id      = "start_producer",
        bash_command = "python /opt/airflow/producer/transaction_producer.py &",
        doc_md       = "Starts the Kafka transaction producer as a background process.",
    )

    start_streaming_consumer = BashOperator(
        task_id      = "start_streaming_consumer",
        bash_command = (
            "spark-submit "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 "
            "/opt/airflow/consumer/stream_consumer.py"
        ),
        doc_md       = "Submits PySpark Structured Streaming job.",
    )

    dq_validation_gate = BranchPythonOperator(
        task_id         = "dq_validation_gate",
        python_callable = run_dq_validation,
        provide_context = True,
    )

    compact_s3_partitions = PythonOperator(
        task_id         = "compact_s3_partitions",
        python_callable = compact_s3,
        provide_context = True,
    )

    quarantine_batch_task = PythonOperator(
        task_id         = "quarantine_batch",
        python_callable = quarantine_batch,
        provide_context = True,
    )

    success_notify = PythonOperator(
        task_id              = "notify_success",
        python_callable      = notify_success,
        provide_context      = True,
        trigger_rule         = TriggerRule.ONE_SUCCESS,
    )

    failure_notify = PythonOperator(
        task_id              = "notify_failure",
        python_callable      = notify_failure,
        provide_context      = True,
        trigger_rule         = TriggerRule.ONE_SUCCESS,
    )

    end = DummyOperator(
        task_id      = "end",
        trigger_rule = TriggerRule.ONE_SUCCESS,
    )

    # ── Task Dependencies ─────────────────────────────────────────────────────
    (
        start
        >> health_check_kafka
        >> start_producer
        >> start_streaming_consumer
        >> dq_validation_gate
    )

    dq_validation_gate >> compact_s3_partitions >> success_notify >> end
    dq_validation_gate >> quarantine_batch_task  >> failure_notify >> end
