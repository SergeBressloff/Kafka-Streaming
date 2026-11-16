# Part I — CSV Streaming: Observations

Setup: one Kafka broker plus ZooKeeper in Docker (Confluent 7.6.15), and the Python client kafka-python 2.3.3. Data: `data/transactions.csv` (200 rows, 11 users).

How to run (from `csv_lab/`):

```bash
python consumer_csv.py --from-beginning   # terminal 1
python producer_csv.py                    # terminal 2: sends one row every 0.5 s
```

Other flags. Producer: `--topic`, `--delay`, `--key none`. Consumer: `--topic`, `--group`, `--name`, `--limit N`, `--idle-timeout S`, `--check-order`.

## Dataset

- The file is clean: a header line plus 200 rows with 4 columns, no quotes and no empty values.
- How it maps to Kafka: each data line becomes one message. The value is the raw line, and the key is `user_id`. The header is not a transaction, so the producer does not send it.
- Things to watch out for when streaming CSV:
	- Every value is plain text, so nothing tells a consumer that `amount` is a number.
	- The column names only exist in the header, so a consumer has no way to know what each field means.
	- A comma or line break inside a field would break the rule "one line = one row".
	- Users are uneven: user 105 has 29 rows, while users 102 and 110 have 12 each.

## Task 1 — Kafka setup

`docker compose ps` shows `kafka` and `zookeeper` running. `kafka-topics --list` answers without an error.

## Task 2 — Topic design

- **Name: `transactions`.** It says what the events are, and it is the name the lab and the install guide use. The JSON part can use its own topic so the two formats don't mix.
- **Partitions: 2 to start.** The data is small and there is only one broker on a laptop, but 2 partitions are enough to see parallel reading and ordering effects. Task 6 raises this to 4.
- **Replication factor: 1.** There is only one broker to hold copies.
- **Key: `user_id`.** All transactions of one user go to the same partition, so each user's history stays in order. That matters for something like per-customer fraud checks.

## Tasks 3 & 4 — Producer and consumer

- **Producer** (`producer_csv.py`): reads the file line by line and sends each row with `user_id` as the key. It waits for Kafka to confirm each row, prints the partition and offset, then pauses 0.5 s before the next row.
- **Consumer** (`consumer_csv.py`): prints each raw row with its partition and offset. After each row it saves (commits) the next offset to read. When it stops, it prints a summary for each partition.
- **Result:** all 200 rows arrived. Partition 0 got 68 rows and partition 1 got 132, because 7 of the 11 users hash to partition 1. Each partition counts its own offsets (0–67 and 0–131).

## Task 5 — Offsets & replay

| What I did | Rows read | Why |
|---|---|---|
| Restarted the same group (`csv-consumers`), even with `--from-beginning` | 0 | The group had saved offsets (68 and 132) and carried on from there. `--from-beginning` only applies when a group has no saved offset. |
| New group `csv-replay` with `--from-beginning` | 200 | A new group has no saved offsets, so it starts at the oldest row. Reading a message does not delete it. |
| New group `csv-latest` without `--from-beginning` | 0 | A new group starts at the end and only waits for new rows. |
| Stopped a group after 50 rows, then restarted it | 50 + 150 | It picked up exactly where it stopped: no row was read twice and none were missed. |
| Reset `csv-consumers` to the start with `kafka-consumer-groups --reset-offsets --to-earliest`, then restarted | 200 | Moving the saved offset back makes the same group replay everything. |

A catch I noticed: after the 50-row stop, partition 0 had a saved offset of 50 (lag 18), but partition 1 had no saved offset at all. That's because the consumer had read only partition 0 so far. Restarting without `--from-beginning` would have jumped to the end of partition 1 and skipped its 132 rows.

## Task 6 — Consumer groups

I ran three consumers in the same group (`csv-group`) and started them before the producer.

| Partitions | c1 | c2 | c3 |
|---|---|---|---|
| 2 | partition 1: 132 rows | partition 0: 68 rows | nothing (idle) |
| 4 | partitions 0 + 1: 86 rows | partition 2: 52 rows | partition 3: 62 rows |

- Within one group, each partition is read by only one consumer. If there are more consumers than partitions, the extra ones sit idle. So the number of partitions caps how many consumers can work in parallel.
- With 4 partitions, all three consumers had work. The load was still uneven, because it follows how the keys are spread, not how many consumers there are.
- Side effect: going from 2 to 4 partitions moved 6 of the 11 users to a different partition (for example, user 104 went from partition 1 to partition 3). Their older rows stay in the old partition, so their order is no longer guaranteed across the change. It's better to pick the partition count up front.

## Task 7 — Ordering guarantees

I sent the same 200 rows into three new topics, then read each one from the beginning with a single consumer. `--check-order` counts rows that arrived after a row with a higher `transaction_id`.

| Topic | Out of order overall | Inside a partition | Inside a user |
|---|---|---|---|
| `transactions-p1`: 1 partition, key `user_id` | 0 / 200 | 0 | 0 |
| `transactions-p4`: 4 partitions, key `user_id` | 173 / 200 | 0 | 0 |
| `transactions-p4-nokey`: 4 partitions, no key | 147 / 200 | 0 | 147 |

- Inside a partition, order is always kept, because offsets only go up.
- Across partitions there is no order. The consumer read partition 0 first (ids 1, 7, 19, 20, …) and then the other partitions.
- With `user_id` as the key, each user's rows still came back in order. Without a key, rows are spread at random (user 105's rows landed in all 4 partitions), so even one user's history comes back shuffled.
- In the live run (Tasks 3 & 4), rows happened to arrive in order. That was only because they were read as fast as they were written, so it isn't a guarantee. Replaying from the start shows the real behaviour.

## Reflection questions

- **Why does Kafka not care about CSV structure?** Kafka only stores and moves bytes (a key and a value). It never opens the message, which keeps it fast and works for any format. Making sense of the data is left to producers and consumers.
- **What problems does CSV cause in streaming?**
	- There is no schema inside each message, so column names and types have to be guessed.
	- Adding or reordering columns breaks consumers without any warning.
	- Commas or line breaks inside a field break the line-by-line splitting.
	- Bad rows are only found by the consumer, after they are already stored in the topic.
- **Why are offsets critical?** An offset is a bookmark. It lets a consumer restart without losing or repeating rows, lets different groups read the same data on their own, and lets you replay history on purpose by moving the bookmark back.
