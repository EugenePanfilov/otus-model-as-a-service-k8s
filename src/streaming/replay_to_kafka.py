import argparse
import json
import time
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer


def parse_args():
    parser = argparse.ArgumentParser(
        description="Replay historical transactions to Kafka with a controlled rate."
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Path to CSV, TXT or Parquet file",
    )
    parser.add_argument(
        "--topic",
        required=True,
        help="Kafka topic name",
    )
    parser.add_argument(
        "--bootstrap-servers",
        required=True,
        help="Kafka bootstrap servers, e.g. host:9091",
    )
    parser.add_argument(
        "--username",
        required=True,
        help="Kafka username",
    )
    parser.add_argument(
        "--password",
        required=True,
        help="Kafka password",
    )
    parser.add_argument(
        "--ca-file",
        required=True,
        help="Path to CA certificate",
    )
    parser.add_argument(
        "--rate-per-sec",
        type=float,
        default=50.0,
        help="Messages per second",
    )
    parser.add_argument(
        "--timestamp-column",
        default="tx_datetime",
        help="Event timestamp column",
    )
    parser.add_argument(
        "--key-column",
        default="transaction_id",
        help="Kafka message key column",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=None,
        help="Optional limit",
    )

    return parser.parse_args()


def load_dataframe(path: str) -> pd.DataFrame:
    p = Path(path)
    suffix = p.suffix.lower()

    if suffix == ".csv":
        return pd.read_csv(p)

    if suffix in {".parquet", ".pq"}:
        return pd.read_parquet(p)

    if suffix == ".txt":
        columns = [
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

        df = pd.read_csv(
            p,
            sep=",",
            header=None,
            names=columns,
            comment="#",
            dtype=str,
        )

        for col in columns:
            df[col] = df[col].astype(str).str.strip()

        df["transaction_id"] = pd.to_numeric(df["transaction_id"], errors="coerce")
        df["customer_id"] = pd.to_numeric(df["customer_id"], errors="coerce")
        df["terminal_id"] = pd.to_numeric(df["terminal_id"], errors="coerce")
        df["tx_amount"] = pd.to_numeric(df["tx_amount"], errors="coerce")
        df["tx_time_seconds"] = pd.to_numeric(df["tx_time_seconds"], errors="coerce")
        df["tx_time_days"] = pd.to_numeric(df["tx_time_days"], errors="coerce")
        df["tx_fraud"] = pd.to_numeric(df["tx_fraud"], errors="coerce")
        df["tx_fraud_scenario"] = pd.to_numeric(df["tx_fraud_scenario"], errors="coerce")

        df["tx_datetime"] = pd.to_datetime(
            df["tx_datetime"],
            format="%Y-%m-%d %H:%M:%S",
            errors="coerce",
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

        df["transaction_id"] = df["transaction_id"].astype("int64")
        df["customer_id"] = df["customer_id"].astype("int64")
        df["terminal_id"] = df["terminal_id"].astype("int64")
        df["tx_time_seconds"] = df["tx_time_seconds"].astype("int64")
        df["tx_time_days"] = df["tx_time_days"].astype("int64")
        df["tx_fraud"] = df["tx_fraud"].astype("int64")
        df["tx_fraud_scenario"] = df["tx_fraud_scenario"].astype("int64")

        return df

    raise ValueError(f"Unsupported file format: {p.suffix}")


def build_producer(args) -> Producer:
    conf = {
        "bootstrap.servers": args.bootstrap_servers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": args.username,
        "sasl.password": args.password,
        "ssl.ca.location": args.ca_file,
        "client.id": "fraud-replay-producer",
        "acks": "1",
        "compression.type": "none",
        "linger.ms": 0,
        "batch.num.messages": 100,
        "message.timeout.ms": 60000,
        "request.timeout.ms": 30000,
    }

    return Producer(conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        key = msg.key().decode("utf-8") if msg.key() else None
        print(
            f"Delivered topic={msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()} "
            f"key={key}"
        )


def main():
    args = parse_args()

    df = load_dataframe(args.input).copy()

    if args.timestamp_column in df.columns:
        df[args.timestamp_column] = pd.to_datetime(
            df[args.timestamp_column],
            errors="coerce",
        )
        df = df.sort_values(args.timestamp_column)

    if args.max_messages is not None:
        df = df.head(args.max_messages)

    if df.empty:
        raise ValueError("Input dataframe is empty after loading and cleaning")

    print(f"Loaded rows: {len(df)}")
    print(f"Columns: {list(df.columns)}")

    producer = build_producer(args)

    interval = 1.0 / args.rate_per_sec if args.rate_per_sec > 0 else 0.0
    sent = 0
    start_ts = time.time()

    for row in df.to_dict(orient="records"):
        key = None

        if args.key_column in row and row[args.key_column] is not None:
            key = str(row[args.key_column])

        payload = json.dumps(
            row,
            ensure_ascii=False,
            default=str,
        )

        producer.produce(
            topic=args.topic,
            key=key.encode("utf-8") if key else None,
            value=payload.encode("utf-8"),
            callback=delivery_report,
        )

        producer.poll(0)

        sent += 1

        if interval > 0:
            time.sleep(interval)

        if sent % 1000 == 0:
            elapsed = max(time.time() - start_ts, 1e-9)
            print(f"Sent {sent} messages, avg_rate={sent / elapsed:.2f} msg/s")

    producer.flush()

    elapsed = max(time.time() - start_ts, 1e-9)
    print(f"Finished. Total sent={sent}, avg_rate={sent / elapsed:.2f} msg/s")


if __name__ == "__main__":
    main()