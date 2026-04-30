import argparse
import os
import traceback

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.ml.functions import vector_to_array

import mlflow
import mlflow.spark


# ---------- CLI ----------
def parse_args():
    parser = argparse.ArgumentParser(
        description="Kafka -> Spark Structured Streaming -> MLflow Spark model scoring -> Kafka"
    )

    parser.add_argument(
        "--bootstrap-servers",
        required=True,
        help="Kafka bootstrap servers, e.g. host1:9091,host2:9091",
    )
    parser.add_argument("--kafka-username", required=True, help="Kafka username")
    parser.add_argument("--kafka-password", required=True, help="Kafka password")

    parser.add_argument("--input-topic", default="fraud.input.transactions")
    parser.add_argument("--output-topic", default="fraud.output.predictions")
    parser.add_argument("--dead-letter-topic", default="fraud.dead_letter")

    parser.add_argument(
        "--checkpoint-location",
        required=True,
        help="S3 checkpoint path, e.g. s3a://bucket/checkpoints/score_stream",
    )
    parser.add_argument("--trigger-seconds", type=int, default=15)
    parser.add_argument("--stop-after-seconds", type=int, default=300)

    parser.add_argument("--mlflow-tracking-uri", required=True)
    parser.add_argument("--model-name", required=True, help="Registered model name in MLflow")
    parser.add_argument("--model-alias", default="champion", help="MLflow model alias")

    parser.add_argument(
        "--feature-cols",
        required=True,
        help=(
            "Comma-separated feature columns, e.g. "
            "tx_amount,tx_time_seconds,tx_time_days,tx_hour,tx_day_of_week,tx_month,is_weekend"
        ),
    )

    return parser.parse_args()


# ---------- Schema of incoming Kafka JSON ----------
def build_input_schema() -> T.StructType:
    return T.StructType(
        [
            T.StructField("transaction_id", T.LongType(), True),
            T.StructField("tx_datetime", T.StringType(), True),
            T.StructField("customer_id", T.LongType(), True),
            T.StructField("terminal_id", T.LongType(), True),
            T.StructField("tx_amount", T.DoubleType(), True),
            T.StructField("tx_time_seconds", T.LongType(), True),
            T.StructField("tx_time_days", T.IntegerType(), True),
            T.StructField("tx_fraud", T.IntegerType(), True),
            T.StructField("tx_fraud_scenario", T.IntegerType(), True),
        ]
    )


def add_derived_features(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "tx_timestamp",
        F.to_timestamp(F.col("tx_datetime"), "yyyy-MM-dd HH:mm:ss"),
    )

    # dayofweek: 1 = Sunday, 7 = Saturday
    df = (
        df.withColumn("tx_hour", F.hour("tx_timestamp"))
        .withColumn("tx_day_of_week", F.dayofweek("tx_timestamp"))
        .withColumn("tx_month", F.month("tx_timestamp"))
        .withColumn(
            "is_weekend",
            F.when(F.col("tx_day_of_week").isin([1, 7]), F.lit(1)).otherwise(F.lit(0)),
        )
    )

    numeric_fill = {
        "tx_amount": 0.0,
        "tx_time_seconds": 0,
        "tx_time_days": 0,
        "tx_hour": 0,
        "tx_day_of_week": 0,
        "tx_month": 0,
        "is_weekend": 0,
    }

    return df.fillna(numeric_fill)


def kafka_read_stream(spark: SparkSession, args) -> DataFrame:
    print("DEBUG: creating Kafka read stream", flush=True)
    print(f"DEBUG: bootstrap_servers={args.bootstrap_servers}", flush=True)
    print(f"DEBUG: input_topic={args.input_topic}", flush=True)

    jaas = (
        "org.apache.kafka.common.security.scram.ScramLoginModule required "
        f'username="{args.kafka_username}" password="{args.kafka_password}";'
    )

    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.input_topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
        .option("kafka.sasl.jaas.config", jaas)
        .load()
    )

    print("DEBUG: Kafka read stream created", flush=True)
    return df


def kafka_write_stream(df: DataFrame, args, topic: str, checkpoint_suffix: str):
    checkpoint_path = f"{args.checkpoint_location.rstrip('/')}/{checkpoint_suffix}"

    print(f"DEBUG: creating Kafka write stream to topic={topic}", flush=True)
    print(f"DEBUG: checkpoint_path={checkpoint_path}", flush=True)

    jaas = (
        "org.apache.kafka.common.security.scram.ScramLoginModule required "
        f'username="{args.kafka_username}" password="{args.kafka_password}";'
    )

    query = (
        df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("topic", topic)
        .option("checkpointLocation", checkpoint_path)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
        .option("kafka.sasl.jaas.config", jaas)
        .outputMode("append")
        .trigger(processingTime=f"{args.trigger_seconds} seconds")
        .start()
    )

    print(f"DEBUG: Kafka write stream started for topic={topic}", flush=True)
    return query


def build_scored_dataframe(
    featured: DataFrame,
    model_uri: str,
    mlflow_tracking_uri: str,
) -> DataFrame:
    print(f"DEBUG: setting MLflow tracking uri on driver: {mlflow_tracking_uri}", flush=True)
    mlflow.set_tracking_uri(mlflow_tracking_uri)

    print(f"DEBUG: loading Spark MLflow model on driver: {model_uri}", flush=True)
    model = mlflow.spark.load_model(model_uri)
    print("DEBUG: Spark MLflow model loaded on driver", flush=True)

    scored_raw = model.transform(featured)
    print(f"DEBUG: model.transform output columns={scored_raw.columns}", flush=True)

    if "probability" in scored_raw.columns:
        print("DEBUG: using probability[1] as score", flush=True)
        scored_raw = scored_raw.withColumn(
            "score",
            vector_to_array(F.col("probability")).getItem(1).cast("double"),
        )
    elif "rawPrediction" in scored_raw.columns:
        print("DEBUG: probability column not found, using prediction as score", flush=True)
        scored_raw = scored_raw.withColumn(
            "score",
            F.col("prediction").cast("double"),
        )
    elif "prediction" in scored_raw.columns:
        print("DEBUG: using prediction as score", flush=True)
        scored_raw = scored_raw.withColumn(
            "score",
            F.col("prediction").cast("double"),
        )
    else:
        raise ValueError(
            "Model output has neither probability nor prediction column. "
            f"Columns: {scored_raw.columns}"
        )

    scored = (
        scored_raw
        .withColumn(
            "prediction",
            F.when(F.col("score") >= F.lit(0.5), F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn("processed_at", F.current_timestamp())
        .withColumn(
            "latency_seconds",
            F.col("processed_at").cast("long") - F.col("kafka_timestamp").cast("long"),
        )
    )

    return scored


def main():
    print("DEBUG: score_stream.py started", flush=True)

    args = parse_args()

    print("DEBUG: args parsed", flush=True)
    print(f"DEBUG: input_topic={args.input_topic}", flush=True)
    print(f"DEBUG: output_topic={args.output_topic}", flush=True)
    print(f"DEBUG: dead_letter_topic={args.dead_letter_topic}", flush=True)
    print(f"DEBUG: checkpoint_location={args.checkpoint_location}", flush=True)
    print(f"DEBUG: stop_after_seconds={args.stop_after_seconds}", flush=True)
    print(f"DEBUG: model_name={args.model_name}", flush=True)
    print(f"DEBUG: model_alias={args.model_alias}", flush=True)

    feature_cols = [x.strip() for x in args.feature_cols.split(",") if x.strip()]
    model_uri = f"models:/{args.model_name}@{args.model_alias}"

    print(f"DEBUG: feature_cols={feature_cols}", flush=True)
    print(f"DEBUG: model_uri={model_uri}", flush=True)
    print(f"DEBUG: MLFLOW_TRACKING_URI={args.mlflow_tracking_uri}", flush=True)

    os.environ["MLFLOW_TRACKING_URI"] = args.mlflow_tracking_uri

    spark = (
        SparkSession.builder
        .appName("fraud-score-stream")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("DEBUG: SparkSession created", flush=True)

    raw = kafka_read_stream(spark, args)
    print("DEBUG: raw Kafka dataframe created", flush=True)

    parsed = (
        raw.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_json"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("json_data", F.from_json(F.col("raw_json"), build_input_schema()))
        .select(
            "kafka_key",
            "raw_json",
            "kafka_timestamp",
            "json_data.*",
        )
    )

    print("DEBUG: JSON parsing dataframe created", flush=True)
    print(f"DEBUG: parsed columns={parsed.columns}", flush=True)

    valid = parsed.filter(F.col("transaction_id").isNotNull())
    invalid = parsed.filter(F.col("transaction_id").isNull())

    print("DEBUG: valid/invalid dataframes created", flush=True)

    featured = add_derived_features(valid)

    print("DEBUG: derived features added", flush=True)
    print(f"DEBUG: featured columns={featured.columns}", flush=True)

    missing = [c for c in feature_cols if c not in featured.columns]
    if missing:
        print(f"ERROR: missing feature columns: {missing}", flush=True)
        raise ValueError(f"Missing feature columns in stream: {missing}")

    print("DEBUG: all feature columns exist", flush=True)

    scored = build_scored_dataframe(
        featured=featured,
        model_uri=model_uri,
        mlflow_tracking_uri=args.mlflow_tracking_uri,
    )

    print("DEBUG: scored dataframe created", flush=True)

    predictions_json = (
        scored.select(
            F.col("transaction_id").cast("string").alias("key"),
            F.to_json(
                F.struct(
                    "transaction_id",
                    "tx_datetime",
                    "customer_id",
                    "terminal_id",
                    "tx_amount",
                    "tx_time_seconds",
                    "tx_time_days",
                    "tx_hour",
                    "tx_day_of_week",
                    "tx_month",
                    "is_weekend",
                    "score",
                    "prediction",
                    "tx_fraud",
                    "processed_at",
                    "kafka_timestamp",
                    "latency_seconds",
                )
            ).alias("value"),
        )
    )

    print("DEBUG: predictions_json dataframe created", flush=True)

    dlq_json = (
        invalid.select(
            F.lit(None).cast("string").alias("key"),
            F.to_json(
                F.struct(
                    F.col("raw_json").alias("raw_payload"),
                    F.lit("transaction_id is null or JSON parse failed").alias("error_message"),
                    F.current_timestamp().alias("failed_at"),
                )
            ).alias("value"),
        )
    )

    print("DEBUG: dlq_json dataframe created", flush=True)

    q1 = kafka_write_stream(
        predictions_json,
        args,
        args.output_topic,
        "predictions",
    )

    q2 = kafka_write_stream(
        dlq_json,
        args,
        args.dead_letter_topic,
        "dead_letter",
    )

    print("DEBUG: streaming queries started", flush=True)
    print("DEBUG: awaiting q1 termination", flush=True)

    try:
        q1.awaitTermination(args.stop_after_seconds)
        print("DEBUG: q1 awaitTermination finished", flush=True)

        if q1.exception() is not None:
            print(f"ERROR: q1 exception: {q1.exception()}", flush=True)
            raise q1.exception()

        if q2.exception() is not None:
            print(f"ERROR: q2 exception: {q2.exception()}", flush=True)
            raise q2.exception()

    finally:
        if q1.isActive:
            print("DEBUG: stopping q1", flush=True)
            q1.stop()

        if q2.isActive:
            print("DEBUG: stopping q2", flush=True)
            q2.stop()

    print("DEBUG: score_stream.py finished successfully", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print("ERROR: score_stream.py failed with exception", flush=True)
        traceback.print_exc()
        raise