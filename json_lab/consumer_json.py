"""JSON consumer: parses each event, extracts its fields and detects malformed events.

Both schema versions are understood (see producer_json.py). Each valid event is converted to
the latest format (v2), so the rest of the code does not care which version it was written in.
Fields that are not required are ignored, so producers can add fields without breaking it.

A malformed event (not JSON, not an object, unknown version, missing field, wrong type) is
reported with its partition and offset, and the consumer carries on.

With --route, valid events are written to '<topic>-filtered' in the v2 format, and malformed
ones are copied unchanged to '<topic>-invalid'. The original topic is left untouched.

The consumer stops after 10 seconds without new events.

Examples:
    python consumer_json.py --from-beginning
    python consumer_json.py --group json-router --from-beginning --route
"""
import argparse
import json
import math
from datetime import datetime

from kafka import KafkaConsumer, KafkaProducer

REQUIRED_FIELDS = {  # per schema_version: field -> allowed type(s)
    1: {"transaction_id": int, "user_id": int, "amount": (int, float), "timestamp": str},
    2: {"transaction_id": int, "user_id": int, "amount_cents": int, "currency": str, "timestamp": str},
}


def parse_args():
    p = argparse.ArgumentParser(description="Read JSON events from Kafka and check them.")
    p.add_argument("--topic", default="transactions-json", help="Kafka topic (default: transactions-json)")
    p.add_argument("--group", default="json-consumers", help="consumer group id (default: json-consumers)")
    p.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap server")
    p.add_argument("--from-beginning", action="store_true",
                   help="if the group has no saved offset, start at the oldest event instead of only new ones")
    p.add_argument("--route", action="store_true",
                   help="write valid events (as v2) to '<topic>-filtered' and malformed ones to '<topic>-invalid'")
    return p.parse_args()


def parse_event(raw):
    """Return (transaction, None) for a valid event, or (None, reason) for a malformed one."""
    try:
        event = json.loads(raw)
    except (TypeError, ValueError) as e:  # ValueError also covers bytes that are not UTF-8
        return None, f"not valid JSON: {e}"
    if not isinstance(event, dict):
        return None, "not a JSON object"

    version = event.get("schema_version")
    if type(version) is not int or version not in REQUIRED_FIELDS:  # type check: True == 1 in Python
        return None, f"unknown schema_version {version!r}"

    for field, expected_type in REQUIRED_FIELDS[version].items():
        if field not in event:
            return None, f"missing field '{field}' (v{version})"
        value = event[field]
        if isinstance(value, bool) or not isinstance(value, expected_type):  # in Python, True is an int
            return None, f"'{field}' has the wrong type ({type(value).__name__})"
    try:
        datetime.fromisoformat(event["timestamp"])
    except ValueError:
        return None, "timestamp is not an ISO 8601 date"

    if version == 1:
        if not math.isfinite(event["amount"]):  # Python's json module accepts NaN and Infinity
            return None, "amount is not a finite number"
        cents, currency = round(event["amount"] * 100), "EUR"  # v1 had no currency: everything was in EUR
    else:
        cents, currency = event["amount_cents"], event["currency"]
    # Return the event in the latest format (v2), whatever version it was written in.
    return {"schema_version": 2, "transaction_id": event["transaction_id"], "user_id": event["user_id"],
            "amount_cents": cents, "currency": currency, "timestamp": event["timestamp"]}, None


def main():
    args = parse_args()
    consumer = KafkaConsumer(
        args.topic,
        bootstrap_servers=args.bootstrap,
        group_id=args.group,
        auto_offset_reset="earliest" if args.from_beginning else "latest",
        consumer_timeout_ms=10_000,  # stop after 10 s without new events
    )
    router = KafkaProducer(bootstrap_servers=args.bootstrap, acks="all") if args.route else None

    valid, malformed, total_cents = 0, 0, 0
    for msg in consumer:
        where = f"partition {msg.partition} offset {msg.offset:>4}"
        tx, problem = parse_event(msg.value)
        if problem:
            malformed += 1
            text = msg.value.decode("utf-8", errors="replace") if msg.value else ""
            print(f"{where} | MALFORMED ({problem}): {text[:70]}", flush=True)
            if router:
                router.send(f"{args.topic}-invalid", key=msg.key, value=msg.value)
        else:
            valid += 1
            total_cents += tx["amount_cents"]
            print(f"{where} | tx {tx['transaction_id']:>3}  user {tx['user_id']}  "
                  f"{tx['amount_cents'] / 100:>7.2f} {tx['currency']}  at {tx['timestamp']}", flush=True)
            if router:
                router.send(f"{args.topic}-filtered", key=msg.key, value=json.dumps(tx).encode("utf-8"))

    consumer.close()  # also commits the offsets of the events read
    if router:
        router.flush()
        router.close()
    print(f"read {valid + malformed} events from '{args.topic}' (group '{args.group}'): "
          f"{valid} valid, {malformed} malformed; total of valid amounts: {total_cents / 100:.2f} EUR")
    if router:
        print(f"routed {valid} to '{args.topic}-filtered' and {malformed} to '{args.topic}-invalid'")


if __name__ == "__main__":
    main()
