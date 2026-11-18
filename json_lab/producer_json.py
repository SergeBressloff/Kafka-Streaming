"""JSON producer: converts each CSV row into a JSON event and sends it to Kafka.

Schema v1 (default):
    {"schema_version": 1, "transaction_id": 1, "user_id": 103, "amount": 171.07, "timestamp": "2024-01-01T10:00:20"}
Schema v2 (--schema-version 2): 'amount' is replaced by 'amount_cents' and 'currency' is added:
    {"schema_version": 2, "transaction_id": 1, "user_id": 103, "amount_cents": 17107, "currency": "EUR", "timestamp": "2024-01-01T10:00:20"}

Rows that cannot be converted (wrong number of fields, missing or invalid values, a
transaction_id already sent) are not sent. The producer prints the reason and moves on.

Examples:
    python producer_json.py
    python producer_json.py --schema-version 2 --topic transactions-json-v2
    python producer_json.py --file ../csv_lab/data/transactions_dirty.csv --topic transactions-dirty-json
"""
import argparse
import csv
import json
import math
import time
from datetime import datetime
from pathlib import Path

from kafka import KafkaProducer

DEFAULT_FILE = Path(__file__).resolve().parent.parent / "csv_lab" / "data" / "transactions.csv"
# The first format is the standard one. The second appears in the dirty file and is converted.
TIMESTAMP_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S")


def parse_args():
    p = argparse.ArgumentParser(description="Convert CSV rows to JSON events and stream them into Kafka.")
    p.add_argument("--file", type=Path, default=DEFAULT_FILE, help="CSV file to stream")
    p.add_argument("--topic", default="transactions-json", help="Kafka topic (default: transactions-json)")
    p.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap server")
    p.add_argument("--delay", type=float, default=0.5, help="seconds to wait between rows (default: 0.5)")
    p.add_argument("--schema-version", type=int, choices=(1, 2), default=1, help="event format (default: 1)")
    return p.parse_args()


def to_event(row, version):
    """Turn one CSV row (a dict from DictReader) into an event. Raises ValueError if the row is malformed."""
    if None in row or None in row.values():  # DictReader marks extra fields with key None, missing ones with value None
        raise ValueError("wrong number of fields")
    values = {name: text.strip() for name, text in row.items()}
    for name, text in values.items():
        if not text:
            raise ValueError(f"{name} is missing")

    for name in ("transaction_id", "user_id"):
        if not values[name].isdigit():
            raise ValueError(f"{name} is not a whole number: {values[name]!r}")
    try:
        amount = float(values["amount"])
    except ValueError:
        raise ValueError(f"amount is not a number: {values['amount']!r}") from None
    if not math.isfinite(amount):  # float() accepts 'nan' and 'inf', but JSON cannot represent them
        raise ValueError(f"amount is not a finite number: {values['amount']!r}")

    for fmt in TIMESTAMP_FORMATS:
        try:
            when = datetime.strptime(values["timestamp"], fmt)
            break
        except ValueError:
            continue
    else:
        raise ValueError(f"timestamp is not a valid date: {values['timestamp']!r}")

    event = {"schema_version": version,
             "transaction_id": int(values["transaction_id"]),
             "user_id": int(values["user_id"])}
    if version == 1:
        event["amount"] = amount
    else:
        event["amount_cents"] = round(amount * 100)  # round(), not int(): int(278.65 * 100) gives 27864
        event["currency"] = "EUR"  # the source has no currency column; all transactions are assumed to be in EUR
    event["timestamp"] = when.isoformat()  # ISO 8601, e.g. 2024-01-01T10:00:20
    return event


def main():
    args = parse_args()
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        acks="all",
        key_serializer=lambda key: str(key).encode("utf-8"),
        # allow_nan=False: never write NaN/Infinity, which are not valid JSON
        value_serializer=lambda event: json.dumps(event, allow_nan=False).encode("utf-8"),
    )

    sent, rejected, seen_ids = 0, 0, set()
    try:
        with args.file.open(encoding="utf-8", newline="") as f:
            for line_no, row in enumerate(csv.DictReader(f), start=2):
                try:
                    event = to_event(row, args.schema_version)
                    if event["transaction_id"] in seen_ids:
                        raise ValueError(f"duplicate transaction_id {event['transaction_id']}")
                except ValueError as e:
                    rejected += 1
                    print(f"line {line_no:>4} -> REJECTED: {e}", flush=True)
                    continue
                seen_ids.add(event["transaction_id"])

                # The key is user_id, so each user's events stay in one partition, in order.
                meta = producer.send(args.topic, key=event["user_id"], value=event).get(timeout=10)
                sent += 1
                print(f"line {line_no:>4} -> partition {meta.partition} offset {meta.offset:>4} | "
                      f"{json.dumps(event)}", flush=True)
                time.sleep(args.delay)  # stream gradually, not all at once
    except KeyboardInterrupt:
        print("\ninterrupted")
    finally:
        producer.flush()
        producer.close()
        print(f"done: {sent} events sent to '{args.topic}', {rejected} rows rejected")


if __name__ == "__main__":
    main()
