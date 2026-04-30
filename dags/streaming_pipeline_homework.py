"""
DAG: streaming_pipeline_homework
Description: Run Spark Structured Streaming fraud scoring on Dataproc.
"""

import time
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

# Kafka / streaming
KAFKA_BOOTSTRAP_SERVERS = Variable.get("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_SCORER_USERNAME = Variable.get("KAFKA_SCORER_USERNAME")
KAFKA_SCORER_PASSWORD = Variable.get("KAFKA_SCORER_PASSWORD")
KAFKA_PRODUCER_USERNAME = Variable.get("KAFKA_PRODUCER_USERNAME")
KAFKA_PRODUCER_PASSWORD = Variable.get("KAFKA_PRODUCER_PASSWORD")

INPUT_TOPIC = "fraud.input.transactions"
OUTPUT_TOPIC = "fraud.output.predictions"
DEAD_LETTER_TOPIC = "fraud.dead_letter"

MODEL_NAME = "fraud_detection_model"
MODEL_ALIAS = "champion"

CHECKPOINT_LOCATION = f"s3a://{S3_BUCKET_NAME}/checkpoints/fraud-score-stream"
SCORE_STREAM_SCRIPT_PATH = f"{S3_SRC_BUCKET}/streaming/score_stream.py"
REPLAY_PRODUCER_SCRIPT_PATH = f"{S3_SRC_BUCKET}/streaming/replay_to_kafka_spark.py"
REPLAY_INPUT_PATH = f"s3a://{S3_BUCKET_NAME}/input_data/2019-08-22-sample-100k.txt"

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

            existing = (
                session.query(Connection)
                .filter(Connection.conn_id == conn.conn_id)
                .first()
            )

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

def wait_before_replay():
    print("Waiting before replay producer starts...")
    time.sleep(90)
    print("Replay producer can start now.")

default_args = {
    "owner": "evgeniy",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="streaming_pipeline_homework",
    default_args=default_args,
    description="Streaming fraud scoring with Kafka + Spark Structured Streaming",
    schedule_interval=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["mlops", "fraud", "spark", "streaming", "kafka"],
) as dag:

    setup_connections = PythonOperator(
        task_id="setup_connections",
        python_callable=run_setup_connections,
    )
    
    wait_before_replay_producer = PythonOperator(
        task_id="wait_before_replay_producer",
        python_callable=wait_before_replay,
    )

    create_spark_cluster = DataprocCreateClusterOperator(
        task_id="spark-cluster-create-task",
        folder_id=YC_FOLDER_ID,
        cluster_name=f"tmp-dp-stream-{uuid.uuid4()}",
        cluster_description="YC Temporary cluster for fraud streaming scoring",
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

    start_score_stream = DataprocCreatePysparkJobOperator(
        task_id="start_score_stream",
        cluster_id="{{ ti.xcom_pull(task_ids='spark-cluster-create-task') }}",
        connection_id=YC_SA_CONNECTION.conn_id,
        name="fraud-score-stream",
        main_python_file_uri=SCORE_STREAM_SCRIPT_PATH,
        properties={
            "spark.submit.deployMode": "cluster",
            "spark.yarn.dist.archives": f"s3a://{S3_BUCKET_NAME}/venvs/venv.tar.gz#environment",
            "spark.pyspark.python": "./environment/bin/python",
            "spark.pyspark.driver.python": "./environment/bin/python",

            "spark.yarn.appMasterEnv.MLFLOW_S3_ENDPOINT_URL": S3_ENDPOINT_URL,
            "spark.yarn.appMasterEnv.AWS_ACCESS_KEY_ID": S3_ACCESS_KEY,
            "spark.yarn.appMasterEnv.AWS_SECRET_ACCESS_KEY": S3_SECRET_KEY,
            
            "spark.executorEnv.MLFLOW_S3_ENDPOINT_URL": S3_ENDPOINT_URL,
            "spark.executorEnv.AWS_ACCESS_KEY_ID": S3_ACCESS_KEY,
            "spark.executorEnv.AWS_SECRET_ACCESS_KEY": S3_SECRET_KEY,
        },
        args=[
            "--bootstrap-servers", KAFKA_BOOTSTRAP_SERVERS,
            "--kafka-username", KAFKA_SCORER_USERNAME,
            "--kafka-password", KAFKA_SCORER_PASSWORD,
            "--input-topic", INPUT_TOPIC,
            "--output-topic", OUTPUT_TOPIC,
            "--dead-letter-topic", DEAD_LETTER_TOPIC,
            "--checkpoint-location", CHECKPOINT_LOCATION,
            "--trigger-seconds", "15",
            "--stop-after-seconds", "300",
            "--mlflow-tracking-uri", MLFLOW_TRACKING_URI,
            "--model-name", MODEL_NAME,
            "--model-alias", MODEL_ALIAS,
            "--feature-cols", "tx_amount,tx_time_seconds,tx_time_days,tx_hour,tx_day_of_week,tx_month,is_weekend",
        ],
    )

    run_replay_producer = DataprocCreatePysparkJobOperator(
        task_id="run_replay_producer",
        cluster_id="{{ ti.xcom_pull(task_ids='spark-cluster-create-task') }}",
        connection_id=YC_SA_CONNECTION.conn_id,
        name="fraud-replay-producer",
        main_python_file_uri=REPLAY_PRODUCER_SCRIPT_PATH,
        properties={
            "spark.submit.deployMode": "cluster",
            "spark.yarn.dist.archives": f"s3a://{S3_BUCKET_NAME}/venvs/venv.tar.gz#environment",
            "spark.pyspark.python": "./environment/bin/python",
            "spark.pyspark.driver.python": "./environment/bin/python",
            
            # producer job is small, reduce resources so it can run in parallel
            "spark.driver.memory": "1g",
            "spark.driver.cores": "1",
            "spark.yarn.driver.memoryOverhead": "384",
            
            "spark.executor.instances": "1",
            "spark.executor.memory": "1g",
            "spark.executor.cores": "1",
            "spark.yarn.executor.memoryOverhead": "384",
        },
        args=[
            "--input", REPLAY_INPUT_PATH,
            "--bootstrap-servers", KAFKA_BOOTSTRAP_SERVERS,
            "--kafka-username", KAFKA_PRODUCER_USERNAME,
            "--kafka-password", KAFKA_PRODUCER_PASSWORD,
            "--topic", INPUT_TOPIC,
            "--key-column", "transaction_id",
            "--max-messages", "5000",
        ],
    )

    delete_spark_cluster = DataprocDeleteClusterOperator(
        task_id="spark-cluster-delete-task",
        cluster_id="{{ ti.xcom_pull(task_ids='spark-cluster-create-task') }}",
        connection_id=YC_SA_CONNECTION.conn_id,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    setup_connections >> create_spark_cluster
    
    create_spark_cluster >> start_score_stream
    create_spark_cluster >> wait_before_replay_producer >> run_replay_producer
    
    [start_score_stream, run_replay_producer] >> delete_spark_cluster
