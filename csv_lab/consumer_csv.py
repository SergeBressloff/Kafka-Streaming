"""CSV consumer: reads raw CSV rows from Kafka and tracks offsets.

Each message is printed as-is with its partition and offset. After each row is
processed, the consumer commits offset + 1 (the next row to read) for that
partition, so a restarted consumer in the same group resumes where it stopped.

With --validate (phase 2, dirty data) every row is also checked. Usable rows are
marked OK, rows that can be repaired safely are marked FIXED, and broken or
duplicate rows are marked REJECTED. With --dlq, rejected rows are copied to a
dead-letter topic together with the reason, so nothing is lost. Rejected rows
are still committed, so one bad row can never block the consumer.

Examples:
    python consumer_csv.py --from-beginning                   # group 'csv-consumers'
    python consumer_csv.py --group other-group --from-beginning   # new group = replay
    python consumer_csv.py --name c1 --group shared           # run several with the same group
    python consumer_csv.py --limit 50                          # stop after 50 rows
    python consumer_csv.py --check-order                       # report ordering at the end
    python consumer_csv.py --topic transactions-dirty --from-beginning --validate --dlq transactions-dirty-dlq
"""
import argparse
import csv
import math
from collections import Counter, defaultdict
from datetime import datetime

from kafka import ConsumerRebalanceListener, KafkaConsumer, KafkaProducer, TopicPartition
from kafka.structs import OffsetAndMetadata

EXPECTED_FIELDS = 4  # transaction_id,user_id,amount,timestamp
# The first format is the standard one. Rows in the second format are repaired, not rejected.
TIMESTAMP_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S")


def parse_args():
    p = argparse.ArgumentParser(description="Read raw CSV rows from Kafka and track offsets.")
    p.add_argument("--topic", default="transactions", help="Kafka topic (default: transactions)")
    p.add_argument("--group", default="csv-consumers", help="consumer group id (default: csv-consumers)")
    p.add_argument("--name", default="consumer", help="label printed on each line")
    p.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap server")
    p.add_argument("--from-beginning", action="store_true",
                   help="if the group has no saved offset, start at the oldest row instead of only new rows")
    p.add_argument("--limit", type=int, default=0, help="stop after this many rows (0 = no limit)")
    p.add_argument("--idle-timeout", type=float, default=0,
                   help="stop after this many seconds without new rows (0 = run until Ctrl+C)")
    p.add_argument("--check-order", action="store_true",
                   help="at the end, check whether transaction_id values arrived in increasing order")
    p.add_argument("--validate", action="store_true",
                   help="check each row and mark it OK, FIXED or REJECTED (with the reason)")
    p.add_argument("--dlq", metavar="TOPIC",
                   help="with --validate: copy rejected rows to this dead-letter topic")
    return p.parse_args()


class ShowAssignment(ConsumerRebalanceListener):
    """Prints which partitions this consumer owns whenever the group rebalances."""

    def __init__(self, name):
        self.name = name

    def on_partitions_revoked(self, revoked):
        pass

    def on_partitions_assigned(self, assigned):
        parts = sorted(tp.partition for tp in assigned)
        print(f"[{self.name}] assigned partitions: {parts if parts else 'none (idle)'}", flush=True)


def check_row(row):
    """Validate one CSV row. Returns (problem, clean_row); problem is None when the row is usable."""
    fields = next(csv.reader([row]), [])
    if len(fields) != EXPECTED_FIELDS:
        return f"expected {EXPECTED_FIELDS} fields, got {len(fields)}", None
    tid, uid, amount, ts = (f.strip() for f in fields)

    if not tid.isdigit():
        return "transaction_id is not a whole number", None
    if not uid.isdigit():
        return "user_id is not a number", None
    if not amount:
        return "amount is missing", None
    try:
        value = float(amount)
    except ValueError:
        value = math.nan
    if not math.isfinite(value):
        return "amount is not a number", None
    if not ts:
        return "timestamp is missing", None
    for fmt in TIMESTAMP_FORMATS:
        try:
            when = datetime.strptime(ts, fmt)
            break
        except ValueError:
            continue
    else:
        return "timestamp is not a valid date", None

    return None, f"{tid},{uid},{amount},{when:%Y-%m-%d %H:%M:%S}"


def order_report(rows):
    """rows: list of (partition, key, transaction_id) in the order they were read."""
    def late(ids):
        # rows that arrived after a row with a higher transaction_id
        count, highest = 0, float("-inf")
        for tid in ids:
            if tid < highest:
                count += 1
            else:
                highest = tid
        return count

    by_partition, by_key = defaultdict(list), defaultdict(list)
    for partition, key, tid in rows:
        by_partition[partition].append(tid)
        by_key[key].append(tid)

    print("ordering check (count of rows that arrived after a row with a higher transaction_id):")
    print(f"  overall, in arrival order : {late([tid for _, _, tid in rows])} of {len(rows)}")
    print(f"  inside each partition     : {sum(late(ids) for ids in by_partition.values())}")
    print(f"  inside each key (user_id) : {sum(late(ids) for ids in by_key.values())}")


def main():
    args = parse_args()
    consumer = KafkaConsumer(
        bootstrap_servers=args.bootstrap,
        group_id=args.group,
        # Only used when the group has no committed offset for a partition.
        auto_offset_reset="earliest" if args.from_beginning else "latest",
        enable_auto_commit=False,  # we commit ourselves, after each row is handled
        consumer_timeout_ms=int(args.idle_timeout * 1000) if args.idle_timeout else float("inf"),
    )
    consumer.subscribe([args.topic], listener=ShowAssignment(args.name))
    dlq = KafkaProducer(bootstrap_servers=args.bootstrap, acks="all") if args.validate and args.dlq else None

    count = 0
    per_partition = defaultdict(lambda: {"rows": 0, "first": None, "last": None})
    seen = []  # (partition, key, transaction_id) for the ordering check
    results = Counter()  # OK / FIXED / REJECTED
    problems = Counter()  # reason -> number of rejected rows
    accepted_ids = set()  # transaction_ids already accepted, to drop duplicates (kept in memory only)

    try:
        for msg in consumer:
            row = msg.value.decode("utf-8", errors="replace")
            key = msg.key.decode("utf-8", errors="replace") if msg.key else "-"
            line = f"[{args.name}] partition {msg.partition} offset {msg.offset:>4} key={key:<4} | {row}"
            if msg.headers:  # e.g. the reason attached to a dead-letter row
                line += "   [" + ", ".join(f"{k}={v.decode('utf-8', 'replace')}" for k, v in msg.headers) + "]"

            if args.validate:
                problem, clean = check_row(row)
                tid = clean.split(",", 1)[0] if clean else None
                if problem is None and tid in accepted_ids:
                    problem = f"duplicate of transaction_id {tid}"

                if problem:
                    results["REJECTED"] += 1
                    problems[problem] += 1
                    line += f"   -> REJECTED: {problem}"
                    if dlq:
                        source = f"{msg.topic}/{msg.partition}/{msg.offset}"
                        headers = [("error", problem.encode()), ("source", source.encode())]
                        # Wait until the DLQ has the row before committing, so it cannot get lost.
                        dlq.send(args.dlq, key=msg.key, value=msg.value, headers=headers).get(timeout=10)
                else:
                    accepted_ids.add(tid)
                    if clean == row:
                        results["OK"] += 1
                        line += "   -> OK"
                    else:
                        results["FIXED"] += 1
                        line += f"   -> FIXED: {clean}"
            print(line, flush=True)

            stats = per_partition[msg.partition]
            stats["rows"] += 1
            stats["first"] = msg.offset if stats["first"] is None else stats["first"]
            stats["last"] = msg.offset

            if args.check_order:
                first_field = row.split(",", 1)[0]
                if first_field.isdigit():
                    seen.append((msg.partition, key, int(first_field)))

            # Save progress: the committed offset is the NEXT row to read in this partition.
            # This also happens for rejected rows, so a bad row is never read again by this group.
            tp = TopicPartition(msg.topic, msg.partition)
            consumer.commit({tp: OffsetAndMetadata(msg.offset + 1, "", -1)})

            count += 1
            if args.limit and count >= args.limit:
                break
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        if dlq:
            dlq.flush()
            dlq.close()

    print(f"[{args.name}] read {count} rows from '{args.topic}' (group '{args.group}')")
    for partition in sorted(per_partition):
        s = per_partition[partition]
        print(f"[{args.name}]   partition {partition}: {s['rows']:>3} rows, offsets {s['first']}..{s['last']}, "
              f"committed {s['last'] + 1}")
    if args.validate:
        sent = f", sent to '{args.dlq}'" if dlq and results["REJECTED"] else ""
        print(f"[{args.name}] validation: {results['OK']} ok, {results['FIXED']} fixed, "
              f"{results['REJECTED']} rejected{sent}")
        for problem, n in problems.most_common():
            print(f"[{args.name}]   {n} x {problem}")
    if args.check_order and seen:
        order_report(seen)


if __name__ == "__main__":
    main()
