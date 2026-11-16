"""CSV consumer: reads raw CSV rows from Kafka and tracks offsets.

Each message is printed as-is with its partition and offset. After each row is
processed, the consumer commits offset + 1 (the next row to read) for that
partition, so a restarted consumer in the same group resumes where it stopped.

Examples:
    python consumer_csv.py --from-beginning                   # group 'csv-consumers'
    python consumer_csv.py --group other-group --from-beginning   # new group = replay
    python consumer_csv.py --name c1 --group shared           # run several with the same group
    python consumer_csv.py --limit 50                          # stop after 50 rows
    python consumer_csv.py --check-order                       # report ordering at the end
"""
import argparse
from collections import defaultdict

from kafka import ConsumerRebalanceListener, KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata


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

    count = 0
    per_partition = defaultdict(lambda: {"rows": 0, "first": None, "last": None})
    seen = []  # (partition, key, transaction_id) for the ordering check

    try:
        for msg in consumer:
            row = msg.value.decode("utf-8", errors="replace")
            key = msg.key.decode("utf-8", errors="replace") if msg.key else "-"
            print(f"[{args.name}] partition {msg.partition} offset {msg.offset:>4} key={key:<4} | {row}",
                  flush=True)

            stats = per_partition[msg.partition]
            stats["rows"] += 1
            stats["first"] = msg.offset if stats["first"] is None else stats["first"]
            stats["last"] = msg.offset

            if args.check_order:
                first_field = row.split(",", 1)[0]
                if first_field.isdigit():
                    seen.append((msg.partition, key, int(first_field)))

            # Save progress: the committed offset is the NEXT row to read in this partition.
            tp = TopicPartition(msg.topic, msg.partition)
            consumer.commit({tp: OffsetAndMetadata(msg.offset + 1, "", -1)})

            count += 1
            if args.limit and count >= args.limit:
                break
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

    print(f"[{args.name}] read {count} rows from '{args.topic}' (group '{args.group}')")
    for partition in sorted(per_partition):
        s = per_partition[partition]
        print(f"[{args.name}]   partition {partition}: {s['rows']:>3} rows, offsets {s['first']}..{s['last']}, "
              f"committed {s['last'] + 1}")
    if args.check_order and seen:
        order_report(seen)


if __name__ == "__main__":
    main()
