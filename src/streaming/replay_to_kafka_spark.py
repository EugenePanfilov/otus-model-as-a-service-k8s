import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


COLUMNS = [
    "transaction_id",
    "tx_datetime",
    "customer_id",
    "terminal_id",
    "tx_amount",
    "tx_time_seconds",
    "tx_time_days",
    "tx_fraud",
    "tx_fraud_scenario",
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Spark batch replay producer: S3 retrospective file -> Kafka topic"
    )

    parser.add_argument("--input", required=True)
    parser.add_argument("--bootstrap-servers", required=True)
    parser.add_argument("--kafka-username", required=True)
    parser.add_argument("--kafka-password", required=True)
    parser.add_argument("--topic", required=True)

    parser.add_argument("--key-column", default="transaction_id")
    parser.add_argument("--max-messages", type=int, default=5000)

    return parser.parse_args()


def read_input_dataframe(spark: SparkSession, path: str):
    lower_path = path.lower()

    if lower_path.endswith(".parquet") or lower_path.endswith(".pq"):
        df = spark.read.parquet(path)
        return df

    if lower_path.endswith(".csv"):
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "false")
            .csv(path)
        )
        return df

    if lower_path.endswith(".txt"):
        raw = (
            spark.read
            .option("header", "false")
            .option("comment", "#")
            .option("inferSchema", "false")
            .csv(path)
        )

        df = raw.toDF(*COLUMNS)

        for col_name in COLUMNS:
            df = df.withColumn(col_name, F.trim(F.col(col_name).cast("string")))

        df = (
            df
            .withColumn("transaction_id", F.col("transaction_id").cast(T.LongType()))
            .withColumn("customer_id", F.col("customer_id").cast(T.LongType()))
            .withColumn("terminal_id", F.col("terminal_id").cast(T.LongType()))
            .withColumn("tx_amount", F.col("tx_amount").cast(T.DoubleType()))
            .withColumn("tx_time_seconds", F.col("tx_time_seconds").cast(T.LongType()))
            .withColumn("tx_time_days", F.col("tx_time_days").cast(T.IntegerType()))
            .withColumn("tx_fraud", F.col("tx_fraud").cast(T.IntegerType()))
            .withColumn("tx_fraud_scenario", F.col("tx_fraud_scenario").cast(T.IntegerType()))
            .withColumn(
                "tx_datetime",
                F.date_format(
                    F.to_timestamp(F.col("tx_datetime"), "yyyy-MM-dd HH:mm:ss"),
                    "yyyy-MM-dd HH:mm:ss",
                ),
            )
        )

        df = df.dropna(
            subset=[
                "transaction_id",
                "tx_datetime",
                "tx_amount",
                "tx_time_seconds",
                "tx_time_days",
                "tx_fraud",
            ]
        )

        return df

    raise ValueError(f"Unsupported file format: {path}")


def main():
    args = parse_args()

    print("DEBUG: replay_to_kafka_spark.py started", flush=True)
    print(f"DEBUG: input={args.input}", flush=True)
    print(f"DEBUG: topic={args.topic}", flush=True)
    print(f"DEBUG: max_messages={args.max_messages}", flush=True)

    spark = (
        SparkSession.builder
        .appName("fraud-replay-producer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("DEBUG: SparkSession created", flush=True)

    df = read_input_dataframe(spark, args.input)

    print(f"DEBUG: input columns={df.columns}", flush=True)

    if args.key_column not in df.columns:
        raise ValueError(
            f"Key column not found: {args.key_column}. Columns: {df.columns}"
        )

    df = df.limit(args.max_messages)

    print("DEBUG: dataframe prepared", flush=True)

    kafka_df = (
        df.select(
            F.col(args.key_column).cast("string").alias("key"),
            F.to_json(
                F.struct(*[F.col(c) for c in df.columns])
            ).alias("value"),
        )
    )

    jaas = (
        "org.apache.kafka.common.security.scram.ScramLoginModule required "
        f'username="{args.kafka_username}" password="{args.kafka_password}";'
    )

    print("DEBUG: writing to Kafka", flush=True)

    (
        kafka_df.write
        .format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("topic", args.topic)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512")
        .option("kafka.sasl.jaas.config", jaas)
        .save()
    )

    print("DEBUG: replay_to_kafka_spark.py finished successfully", flush=True)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        import traceback
        print("ERROR: replay_to_kafka_spark.py failed with exception", flush=True)
        traceback.print_exc()
        raise
