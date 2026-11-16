"""CSV producer: streams a CSV file into Kafka, one row = one message.

The value of each message is the raw CSV line (bytes). Kafka does not parse it.
The key is taken from one CSV column (user_id by default), so all rows of the
same user land in the same partition and keep their order.

Examples:
    python producer_csv.py                                   # transactions.csv -> topic 'transactions'
    python producer_csv.py --delay 0.1 --topic transactions-p1
    python producer_csv.py --key none                        # no key: rows spread over partitions
"""
import argparse
import csv
import time
from pathlib import Path

from kafka import KafkaProducer

DEFAULT_FILE = Path(__file__).parent / "data" / "transactions.csv"


def parse_args():
    p = argparse.ArgumentParser(description="Stream a CSV file into Kafka, one row per message.")
    p.add_argument("--file", type=Path, default=DEFAULT_FILE, help="CSV file to stream")
    p.add_argument("--topic", default="transactions", help="Kafka topic (default: transactions)")
    p.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap server")
    p.add_argument("--delay", type=float, default=0.5, help="seconds to wait between rows (default: 0.5)")
    p.add_argument("--key", default="user_id", help="CSV column used as message key, or 'none' (default: user_id)")
    return p.parse_args()


def main():
    args = parse_args()
    producer = KafkaProducer(bootstrap_servers=args.bootstrap, acks="all")

    sent = 0
    try:
        with args.file.open(encoding="utf-8", newline="") as f:
            # The header describes the columns; it is not a transaction, so it is not sent.
            header = next(csv.reader([f.readline()]))
            key_index = None if args.key == "none" else header.index(args.key)

            for line_no, line in enumerate(f, start=2):
                row = line.rstrip("\r\n")
                if not row.strip():
                    continue  # an empty line is not a record

                key = None
                if key_index is not None:
                    fields = next(csv.reader([row]))
                    if key_index < len(fields) and fields[key_index]:
                        key = fields[key_index].encode("utf-8")

                # .get() waits for the broker's acknowledgement and returns where the row was stored.
                meta = producer.send(args.topic, key=key, value=row.encode("utf-8")).get(timeout=10)
                sent += 1
                key_text = key.decode() if key else "-"
                print(f"line {line_no:>4} -> partition {meta.partition} offset {meta.offset:>4} "
                      f"key={key_text:<4} | {row}", flush=True)

                time.sleep(args.delay)  # stream gradually, not all at once
    except KeyboardInterrupt:
        print("\ninterrupted")
    finally:
        producer.flush()
        producer.close()
        print(f"done: {sent} rows sent to topic '{args.topic}'")


if __name__ == "__main__":
    main()
