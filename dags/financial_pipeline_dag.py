# ============================================================
# final_capstone_orchestrator.py
# Cloud Native Financial Risk & Performance Pipeline
# UPDATED WITH ENVIRONMENT PARAMETER FOR CI/CD
# ============================================================

from airflow import DAG
from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import os

# ============================================================
# SETTINGS
# ============================================================

AZ_RESOURCE_GROUP = "rg-capstone2"
AZ_FACTORY_NAME   = "adfcapstone123"
# UPDATE THIS TO YOUR NEW PIPELINE NAME!
AZ_PIPELINE_NAME  = "pl_incremental_transactions_copy1"  # ← CHANGE THIS to your new pipeline name
RECIPIENT_EMAIL   = "sec22am032@sairamtap.edu.in"

AUDIT_LOG_PATH    = os.path.expanduser("~/airflow/logs/capstone_audit.json")
METADATA_LOG_PATH = os.path.expanduser("~/airflow/logs/capstone_metadata.json")

# ============================================================
# HELPER — WRITE AUDIT LOG
# ============================================================

def write_audit_log(dag_id, task_id, status, message=""):
    entry = {
        "timestamp" : datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "dag_id"    : dag_id,
        "task_id"   : task_id,
        "status"    : status,
        "message"   : message
    }
    logs = []
    if os.path.exists(AUDIT_LOG_PATH):
        with open(AUDIT_LOG_PATH, "r") as f:
            try:
                logs = json.load(f)
            except:
                logs = []
    logs.append(entry)
    os.makedirs(os.path.dirname(AUDIT_LOG_PATH), exist_ok=True)
    with open(AUDIT_LOG_PATH, "w") as f:
        json.dump(logs, f, indent=2)
    print(f"📋 AUDIT LOG → {task_id} : {status}")

# ============================================================
# HELPER — WRITE METADATA LOG
# ============================================================

def write_metadata_log(run_id, status, details={}):
    entry = {
        "run_id"          : str(run_id),
        "dag_id"          : "financial_risk_pipeline_final12",
        "run_timestamp"   : datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        "pipeline_status" : status,
        "details"         : details
    }
    logs = []
    if os.path.exists(METADATA_LOG_PATH):
        with open(METADATA_LOG_PATH, "r") as f:
            try:
                logs = json.load(f)
            except:
                logs = []
    logs.append(entry)
    os.makedirs(os.path.dirname(METADATA_LOG_PATH), exist_ok=True)
    with open(METADATA_LOG_PATH, "w") as f:
        json.dump(logs, f, indent=2)
    print(f"📊 METADATA LOG → {status}")

# ============================================================
# CALLBACK — FAILURE + RETRY EXHAUSTION ESCALATION
# ============================================================

def on_failure_alert(context):
    task_instance = context["task_instance"]
    dag_id        = context["dag"].dag_id
    task_id       = task_instance.task_id
    exec_date     = str(context["execution_date"])
    try_number    = task_instance.try_number
    max_tries     = task_instance.max_tries + 1

    print("=" * 60)
    print(f"  ❌ FAILURE ALERT!")
    print(f"  DAG     : {dag_id}")
    print(f"  Task    : {task_id}")
    print(f"  Time    : {exec_date}")
    print(f"  Attempt : {try_number} of {max_tries}")
    print("=" * 60)

    write_audit_log(
        dag_id  = dag_id,
        task_id = task_id,
        status  = "FAILED",
        message = f"Attempt {try_number} of {max_tries} failed"
    )

    if try_number >= max_tries:
        print("=" * 60)
        print("  🚨 RETRY EXHAUSTION ESCALATION!")
        print(f"  ALL {max_tries} RETRIES EXHAUSTED!")
        print(f"  Task   : {task_id}")
        print("  Action : Manual intervention needed!")
        print("=" * 60)

        write_audit_log(
            dag_id  = dag_id,
            task_id = task_id,
            status  = "RETRY_EXHAUSTED",
            message = f"ALL {max_tries} retries failed! Manual fix needed!"
        )

        write_metadata_log(
            run_id  = exec_date,
            status  = "RETRY_EXHAUSTED",
            details = {
                "failed_task"    : task_id,
                "total_retries"  : max_tries,
                "escalation_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
                "action"         : "Manual intervention required"
            }
        )

# ============================================================
# CALLBACK — SLA MISS
# ============================================================

def on_sla_miss(dag, task_list, blocking_task_list,
                slas, blocking_tis):
    print("=" * 60)
    print(f"  ⚠️  SLA BREACH ALERT!")
    print(f"  DAG   : {dag.dag_id}")
    print(f"  Tasks : {task_list}")
    print(f"  Time  : {datetime.utcnow()}")
    print("=" * 60)

    write_audit_log(
        dag_id  = dag.dag_id,
        task_id = str(task_list),
        status  = "SLA_BREACHED",
        message = "Pipeline SLA of 2 hours breached!"
    )

    write_metadata_log(
        run_id  = str(datetime.utcnow().date()),
        status  = "SLA_BREACHED",
        details = {
            "breached_tasks": str(task_list),
            "sla_limit"     : "2 hours"
        }
    )

# ============================================================
# TASK — PRE PIPELINE AUDIT
# ============================================================

def pre_pipeline_fn(**context):
    run_id    = str(context.get("run_id", "manual"))
    exec_date = str(context["execution_date"])

    # Get environment from Airflow variable or default
    from airflow.models import Variable
    try:
        env = Variable.get("environment", default_var="DEV")
    except:
        env = "DEV"

    print("=" * 60)
    print("  📋 PRE-PIPELINE AUDIT LOG")
    print(f"  Run ID     : {run_id}")
    print(f"  Exec Date  : {exec_date}")
    print(f"  Environment: {env}")
    print(f"  ADF Factory: {AZ_FACTORY_NAME}")
    print(f"  Pipeline   : {AZ_PIPELINE_NAME}")
    print("=" * 60)

    write_audit_log(
        dag_id  = "financial_risk_pipeline_final43",
        task_id = "pre_pipeline_audit",
        status  = "STARTED",
        message = f"Pipeline started for {exec_date} in {env}"
    )

    write_metadata_log(
        run_id  = run_id,
        status  = "PIPELINE_STARTED",
        details = {
            "execution_date": exec_date,
            "environment"   : env,
            "adf_factory"   : AZ_FACTORY_NAME,
            "adf_pipeline"  : AZ_PIPELINE_NAME,
            "resource_group": AZ_RESOURCE_GROUP,
            "start_time"    : datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        }
    )

    print("  ✅ Pre-pipeline audit complete!")

# ============================================================
# TASK — POST PIPELINE AUDIT
# ============================================================

def post_pipeline_fn(**context):
    run_id    = str(context.get("run_id", "manual"))
    exec_date = str(context["execution_date"])

    from airflow.models import Variable
    try:
        env = Variable.get("environment", default_var="DEV")
    except:
        env = "DEV"

    print("=" * 60)
    print("  📋 POST-PIPELINE AUDIT LOG")
    print(f"  Run ID     : {run_id}")
    print(f"  Exec Date  : {exec_date}")
    print(f"  Environment: {env}")
    print(f"  Status     : FULL PIPELINE SUCCESS ✅")
    print("=" * 60)

    write_audit_log(
        dag_id  = "financial_risk_pipeline_final",
        task_id = "post_pipeline_audit",
        status  = "COMPLETED",
        message = f"Full pipeline completed for {exec_date} in {env}"
    )

    write_metadata_log(
        run_id  = run_id,
        status  = "PIPELINE_COMPLETED",
        details = {
            "execution_date" : exec_date,
            "environment"    : env,
            "end_time"       : datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "adf_status"     : "SUCCESS",
            "bronze_status"  : "SUCCESS",
            "silver_status"  : "SUCCESS",
            "gold_status"    : "SUCCESS",
            "email_sent_to"  : RECIPIENT_EMAIL,
            "overall"        : "ALL LAYERS COMPLETE"
        }
    )

    print("  ✅ Audit log written for compliance!")
    print("  ✅ Metadata log updated!")

# ============================================================
# DEFAULT ARGS
# ============================================================

default_args = {
    "owner"              : "capstone_team",
    "depends_on_past"    : False,
    "start_date"         : datetime(2024, 1, 1),
    "retries"            : 3,
    "retry_delay"        : timedelta(minutes=5),
    "email_on_failure"   : True,
    "email"              : [RECIPIENT_EMAIL],
    "execution_timeout"  : timedelta(hours=2),
    "on_failure_callback": on_failure_alert,
}

# ============================================================
# DAG
# ============================================================

with DAG(
    dag_id            = "financial_risk_pipeline_final",
    default_args      = default_args,
    schedule_interval = "@daily",
    catchup           = False,
    sla_miss_callback = on_sla_miss,
    tags              = ["finance", "azure", "capstone", "ci-cd"]
) as dag:

    # Task 1 — Pre pipeline audit
    t0_pre_audit = PythonOperator(
        task_id         = "pre_pipeline_audit",
        python_callable = pre_pipeline_fn,
        sla             = timedelta(hours=2),
    )

    # Task 2 — ADF pipeline with environment parameter
    run_pipeline = AzureDataFactoryRunPipelineOperator(
        task_id                    = "ingest_and_transform_adf",
        pipeline_name              = AZ_PIPELINE_NAME,
        resource_group_name        = AZ_RESOURCE_GROUP,
        factory_name               = AZ_FACTORY_NAME,
        azure_data_factory_conn_id = "azure_data_factory_default",
        wait_for_termination       = True,
        check_interval             = 60,
        sla                        = timedelta(hours=2),
        # Pass environment parameter to ADF pipeline
        pipeline_parameters={
            "environment": "{{ var.value.get('environment', 'DEV') }}"
        }
    )

    # Task 3 — Success email
    send_success_notification = EmailOperator(
        task_id      = "send_success_email",
        to           = RECIPIENT_EMAIL,
        subject      = "🚀 Capstone Pipeline Success: {{ ds }}",
        html_content = """
            <h3>Capstone Financial Pipeline Completed</h3>
            <p>Status: <b>SUCCESS ✅</b></p>
            <p>Execution date: <b>{{ ds }}</b></p>
            <hr>
            <h4>Pipeline Summary:</h4>
            <ul>
                <li>✅ ADF Incremental Load : Complete</li>
                <li>✅ Bronze Layer         : Validated</li>
                <li>✅ Silver Layer         : Transformed</li>
                <li>✅ Gold Layer           : Aggregated</li>
                <li>✅ Audit Logs           : Written</li>
                <li>✅ Metadata Logs        : Updated</li>
            </ul>
            <p><i>Automated notification from Airflow</i></p>
        """
    )

    # Task 4 — Post pipeline audit
    t3_post_audit = PythonOperator(
        task_id         = "post_pipeline_audit",
        python_callable = post_pipeline_fn,
        sla             = timedelta(hours=2),
    )

    # ============================================================
    # TASK CHAIN
    # ============================================================

    t0_pre_audit >> run_pipeline >> send_success_notification >> t3_post_audit