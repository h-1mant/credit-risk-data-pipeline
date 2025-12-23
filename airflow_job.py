from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.trigger_rule import TriggerRule
import uuid

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_interval": timedelta(seconds=5),
    "schedule_interval": None
}

with DAG(
    dag_id = "credit-risk-data-pipeline",
    default_args = default_args,
    start_date = datetime(2025,12,23), 
    catchup=False
) as dag:


    #Task 1: Sense Files in GCS Bucket to trigger spark job
    gcs_bucket = "credit-card-data-analysis-hrm2934"
    file_pattern = "transactions/transactions_*.json"

    file_sensor_task = GCSObjectsWithPrefixExistenceSensor(
        task_id = "check_transaction_file_arrival",
        bucket = gcs_bucket,
        prefix = file_pattern,
        timeout = 600, #timeout in 10mins if no file arrives
        poke_interval = 20,
        mode = "poke"
    )

    #Task 2: Trigger Spark Job
    batch_id = f"credit-card-batch-{str(uuid.uuid4())[:8]}"  # Shortened UUID

    batch_details = {
        "pyspark_batch": {
            "main_python_file_uri": "gs://credit-card-data-analysis-hrm2934/spark_job/spark_job.py"
        },
        "runtime_config": {
            "version": "2.2",
        },
        "environment_config": {
            "execution_config": {
                "service_account": "533474688810-compute@developer.gserviceaccount.com",
                "network_uri": "projects/airflow-hrm293/global/networks/default",
                "subnetwork_uri": "projects/airflow-hrm293/regions/us-central1/subnetworks/default",
            }
        },
    }

    pyspark_task = DataprocCreateBatchOperator(
        task_id = "credit_risk_processing_pyspark_job",
        batch = batch_details,
        project_id = "airflow-hrm293",
        region = "us-central1",
        gcp_conn_id = "google_cloud_default"
    )

    #Task 3: Post-processing, move transaction files to archive
    source_prefix = "transactions/"
    archive_prefix = "archive/"

    archive_files_task = GCSToGCSOperator(
        task_id = "archive_transaction_files",
        source_bucket = gcs_bucket,
        source_object = source_prefix,
        destination_bucket = gcs_bucket,
        destination_object = archive_prefix,
        move_object = True,
        trigger_rule = TriggerRule.ALL_SUCCESS
    )


    file_sensor_task >> pyspark_task >> archive_files_task

