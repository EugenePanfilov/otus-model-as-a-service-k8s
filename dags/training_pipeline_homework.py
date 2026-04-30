"""
DAG: training_pipeline_homework
Description: Train fraud model (baseline/tuned) + compare challenger vs champion on Dataproc.
"""

import uuid
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Connection, Variable
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.yandex.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocCreatePysparkJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.settings import Session

# Общие переменные для облака
YC_ZONE = Variable.get("YC_ZONE")
YC_FOLDER_ID = Variable.get("YC_FOLDER_ID")
YC_SUBNET_ID = Variable.get("YC_SUBNET_ID")
YC_SSH_PUBLIC_KEY = Variable.get("YC_SSH_PUBLIC_KEY")

# Object Storage
S3_ENDPOINT_URL = Variable.get("S3_ENDPOINT_URL")
S3_ACCESS_KEY = Variable.get("S3_ACCESS_KEY")
S3_SECRET_KEY = Variable.get("S3_SECRET_KEY")
S3_BUCKET_NAME = Variable.get("S3_BUCKET_NAME")

S3_SRC_BUCKET = f"s3a://{S3_BUCKET_NAME}/src"

# Dataproc / MLflow
DP_SA_AUTH_KEY_PUBLIC_KEY = Variable.get("DP_SA_AUTH_KEY_PUBLIC_KEY")
DP_SA_JSON = Variable.get("DP_SA_JSON")
DP_SA_ID = Variable.get("DP_SA_ID")
MLFLOW_TRACKING_URI = Variable.get("MLFLOW_TRACKING_URI")

# Runtime paths
INPUT_DATA_PATH = f"s3a://{S3_BUCKET_NAME}/input_data/2019-08-22-sample-100k.txt"
TRAIN_MODEL_OUTPUT_PATH = f"s3a://{S3_BUCKET_NAME}/models/fraud_candidate_latest"

# Режим обучения: baseline или tuned
TRAIN_MODE = Variable.get("FRAUD_TRAIN_MODE", default_var="baseline")
OPTUNA_N_TRIALS = Variable.get("FRAUD_OPTUNA_N_TRIALS", default_var="20")

# Создание подключения для Object Storage
YC_S3_CONNECTION = Connection(
    conn_id="yc-s3",
    conn_type="s3",
    host=S3_ENDPOINT_URL,
    extra={
        "aws_access_key_id": S3_ACCESS_KEY,
        "aws_secret_access_key": S3_SECRET_KEY,
        "host": S3_ENDPOINT_URL,
    },
)

# Создание подключения для Dataproc
YC_SA_CONNECTION = Connection(
    conn_id="yc-sa",
    conn_type="yandexcloud",
    extra={
        "extra__yandexcloud__public_ssh_key": DP_SA_AUTH_KEY_PUBLIC_KEY,
        "extra__yandexcloud__service_account_json": DP_SA_JSON,
    },
)


def setup_airflow_connections(*connections: Connection) -> None:
    session = Session()
    try:
        for conn in connections:
            print("Checking connection:", conn.conn_id)
            existing = session.query(Connection).filter(
                Connection.conn_id == conn.conn_id
            ).first()
            if not existing:
                session.add(conn)
                print("Added connection:", conn.conn_id)
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def run_setup_connections(**kwargs):
    setup_airflow_connections(YC_S3_CONNECTION, YC_SA_CONNECTION)
    return True


default_args = {
    "owner": "evgeniy",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="training_pipeline_homework",
    default_args=default_args,
    description="Periodic training and A/B testing of fraud detection model",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["mlops", "fraud", "spark", "ab-test"],
) as dag:

    setup_connections = PythonOperator(
        task_id="setup_connections",
        python_callable=run_setup_connections,
    )

    create_spark_cluster = DataprocCreateClusterOperator(
        task_id="spark-cluster-create-task",
        folder_id=YC_FOLDER_ID,
        cluster_name=f"tmp-dp-fraud-{uuid.uuid4()}",
        cluster_description="YC Temporary cluster for fraud model training and A/B testing",
        subnet_id=YC_SUBNET_ID,
        s3_bucket=S3_BUCKET_NAME,
        service_account_id=DP_SA_ID,
        ssh_public_keys=YC_SSH_PUBLIC_KEY,
        zone=YC_ZONE,
        cluster_image_version="2.0",

        # masternode
        masternode_resource_preset="s3-c2-m8",
        masternode_disk_type="network-ssd",
        masternode_disk_size=50,

        # datanodes
        datanode_resource_preset="s3-c4-m16",
        datanode_disk_type="network-ssd",
        datanode_disk_size=50,
        datanode_count=2,

        # computenodes
        computenode_count=0,

        # software
        services=["YARN", "SPARK", "HDFS", "MAPREDUCE"],

        connection_id=YC_SA_CONNECTION.conn_id,
    )

    train_model = DataprocCreatePysparkJobOperator(
        task_id="train",
        cluster_id="{{ ti.xcom_pull(task_ids='spark-cluster-create-task') }}",
        connection_id=YC_SA_CONNECTION.conn_id,
        name="train-fraud-model",
        main_python_file_uri=f"{S3_SRC_BUCKET}/train_fraud_detection.py",
        python_file_uris=[
            f"{S3_SRC_BUCKET}/common_fraud.py",
        ],
        properties={
            "spark.submit.deployMode": "cluster",
            "spark.yarn.dist.archives": f"s3a://{S3_BUCKET_NAME}/venvs/venv.tar.gz#environment",
            "spark.pyspark.python": "./environment/bin/python",
            "spark.pyspark.driver.python": "./environment/bin/python",
        },
        args=[
            "--input", INPUT_DATA_PATH,
            "--output", TRAIN_MODEL_OUTPUT_PATH,
            "--input-format", "txt",
            "--tracking-uri", MLFLOW_TRACKING_URI,
            "--experiment-name", "fraud_detection",
            "--train-mode", TRAIN_MODE,
            "--n-trials", OPTUNA_N_TRIALS,
            "--auto-register",
            "--s3-endpoint-url", S3_ENDPOINT_URL,
            "--s3-access-key", S3_ACCESS_KEY,
            "--s3-secret-key", S3_SECRET_KEY,
        ],
    )

    ab_test_fraud_model = DataprocCreatePysparkJobOperator(
        task_id="ab_test_fraud",
        cluster_id="{{ ti.xcom_pull(task_ids='spark-cluster-create-task') }}",
        connection_id=YC_SA_CONNECTION.conn_id,
        name="ab-test-fraud-model",
        main_python_file_uri=f"{S3_SRC_BUCKET}/ab_test_fraud.py",
        python_file_uris=[
            f"{S3_SRC_BUCKET}/common_fraud.py",
        ],
        properties={
            "spark.submit.deployMode": "cluster",
            "spark.yarn.dist.archives": f"s3a://{S3_BUCKET_NAME}/venvs/venv.tar.gz#environment",
            "spark.pyspark.python": "./environment/bin/python",
            "spark.pyspark.driver.python": "./environment/bin/python",
        },
        args=[
            "--input", INPUT_DATA_PATH,
            "--input-format", "txt",
            "--tracking-uri", MLFLOW_TRACKING_URI,
            "--experiment-name", "fraud_detection",
            "--bootstrap-iterations", "100",
            "--alpha", "0.01",
            "--auto-deploy",
            "--s3-endpoint-url", S3_ENDPOINT_URL,
            "--s3-access-key", S3_ACCESS_KEY,
            "--s3-secret-key", S3_SECRET_KEY,
        ],
    )

    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id="spark-cluster-delete-task",
        cluster_id="{{ ti.xcom_pull(task_ids='spark-cluster-create-task') }}",
        connection_id=YC_SA_CONNECTION.conn_id,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    setup_connections >> create_spark_cluster >> train_model >> ab_test_fraud_model >> delete_spark_cluster