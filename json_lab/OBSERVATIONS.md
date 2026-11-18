# Part II — JSON Streaming: Observations

Setup: the same as Part I (one Kafka broker plus ZooKeeper in Docker, Confluent 7.6.15, Python client kafka-python 2.3.3). Data: the CSV files from Part I, `../csv_lab/data/transactions.csv` (200 rows) and `transactions_dirty.csv` (14 rows).

How to run (from `json_lab/`):

```bash
python consumer_json.py --from-beginning   # terminal 1
python producer_json.py                    # terminal 2: sends one event every 0.5 s
```

Other flags. Producer: `--file`, `--topic`, `--delay`, `--schema-version 2`. Consumer: `--topic`, `--group`, `--from-beginning`, `--route`. The consumer stops after 10 s without new events.

Topics (replication factor 1, created with `kafka-topics --create`):

| Topic | Partitions | Content |
|---|---|---|
| `transactions-json` | 2 | raw events from the producer, key `user_id` |
| `transactions-json-filtered` | 2 | valid events only, in the v2 format (Task 5) |
| `transactions-json-invalid` | 1 | malformed events, unchanged (Task 5) |
| `transactions-json-v2` | 2 | the same 200 rows in the v2 format (Task 4) |
| `transactions-dirty-json` | 2 | Phase 2 |

## Task 1 — From CSV to JSON: schema decisions

Each CSV row becomes one flat JSON object (schema v1):

```json
{"schema_version": 1, "transaction_id": 1, "user_id": 103, "amount": 171.07, "timestamp": "2024-01-01T10:00:20"}
```

| Field | JSON type | Why |
|---|---|---|
| `schema_version` | integer | Tells the consumer which layout it is reading. This is what makes Task 4 manageable. |
| `transaction_id` | integer | It is a whole number in the source. A number type lets the producer reject values like `abc`. |
| `user_id` | integer | Same reason. It is also the message key, so each user's events stay in one partition, in order. |
| `amount` | number | It is a quantity, so it should not be text the consumer has to guess about. (Changed in v2, see Task 4.) |
| `timestamp` | string, ISO 8601 | JSON has no date type. ISO 8601 is the standard text form and sorts correctly. The source has no time zone, so none is added. |

- Field names are the CSV column names, so nothing has to be renamed between Part I and Part II.
- The header row is not sent, because it is not a transaction.
- Serialization uses `json.dumps(..., allow_nan=False)`. By default Python writes `NaN`, which is not valid JSON and is refused by most other languages.
- The cost of JSON is size. An event is 104 bytes on average, against 33 bytes for the CSV row, because every message repeats the field names.

## Task 2 — JSON producer

- `producer_json.py` reads the CSV, converts each row into an event, and sends it with `user_id` as key. It waits for Kafka to confirm, prints the partition and offset, then pauses 0.5 s.
- **Malformed rows are not sent.** The producer prints the line number and the reason, then carries on. A row is rejected if it has the wrong number of fields, an empty value, a non-numeric id, an amount that is not a finite number, an unreadable date, or a `transaction_id` already sent in this run.
- Result on the clean file: 200 events sent, 0 rejected. 68 went to partition 0 and 132 to partition 1, the same split as Part I because the keys are the same.

## Task 3 — JSON consumer

- `consumer_json.py` parses each message with `json.loads`. It checks that the message is a JSON object, that every required field is present and has the right type, and that the timestamp is a valid ISO date. It then prints the extracted fields.
- A malformed event is printed with its partition, offset and reason, and the consumer moves on. A bad message can't block it (no "poison pill" as in Part I).
- Result: 200 valid, 0 malformed, total 52,761.57 EUR, which is the sum of the CSV.

Our producer only sends valid events. To test detection, I wrote 5 bad messages straight into the topic with Kafka's own console producer (`data/bad_events.txt`, format `key|value`):

```bash
docker exec -i kafka-installation-kafka-1 kafka-console-producer --bootstrap-server localhost:9092 \
  --topic transactions-json --property parse.key=true --property 'key.separator=|' < data/bad_events.txt
```

| Offset | Message | Detected as |
|---|---|---|
| 68 | a CSV line `3,101,19.99,...` | not valid JSON |
| 69 | JSON cut off before the end | not valid JSON |
| 70 | `"amount": NaN` | amount is not a finite number |
| 71 | no `amount` field | missing field `amount` |
| 72 | `"amount": "19.99"` (a string) | `amount` has the wrong type |

- Kafka accepted all 5 without complaint, because it does not look inside messages. Any client that can reach the topic can write anything to it. So the consumer has to check events even when our own producer is careful.
- The restarted group read only these 5 new messages, because it resumed from its saved offsets.
- Two Python details were caught by testing. `json.loads` accepts `NaN`, so it needs an explicit check. `True` counts as an integer in Python, so `"user_id": true` would pass without a separate check.

## Task 4 — Schema evolution

Schema v2 removes one field and adds one:

```json
{"schema_version": 2, "transaction_id": 1, "user_id": 103, "amount_cents": 17107, "currency": "EUR", "timestamp": "2024-01-01T10:00:20"}
```

- **Removed `amount`, added `amount_cents` (integer).** A float cannot store most cent values exactly. For example, `int(278.65 * 100)` gives 27864, and this naive conversion is wrong for 7 of the 200 amounts. Whole cents are exact. The producer uses `round()`.
- **Added `currency`.** The source has no currency column, so all events get `"EUR"`. This is an assumption.

I sent the 200 rows in v2 format to `transactions-json-v2`, so that the raw topic keeps one copy of each transaction. Then I read them with two consumers:

| Consumer | Result on the 200 v2 events |
|---|---|
| Task 3 consumer, unchanged (group `json-old`) | 200 malformed: `missing field 'amount'`. The new `currency` field caused no problem, it was simply ignored. |
| Updated consumer (group `json-new`) | 200 valid, total 52,761.57 EUR, the same as v1 |

- **Adding a field is safe** if consumers ignore fields they don't know. The updated consumer does this too: a test event with an extra `channel` field was accepted.
- **Removing or renaming a field breaks every consumer that uses it.** Here it showed up as "malformed". A consumer that simply read `event["amount"]` would have crashed on every event.
- The old consumer still saved its offsets, so group `json-old` moved past all 200 events. They are not lost, since they are still in Kafka, but this group will not read them again unless its offsets are reset (Task 6).
- **The updated consumer** reads `schema_version` first, checks the fields of that version, and converts v1 events to the v2 shape (cents = `round(amount * 100)`, currency EUR). It must keep accepting v1 for as long as v1 events are kept in Kafka.
- **Deployment order matters:** update the consumers first, then the producer.

## Task 5 — Topic derivation

The router is the consumer run with `--route` (group `json-router`, `--from-beginning`). It reads the raw topic and:

- writes each valid event to `transactions-json-filtered`, converted to v2
- copies each malformed event, unchanged, to `transactions-json-invalid`

| Topic | Events |
|---|---|
| `transactions-json` (raw) | 205 (200 from the producer + 5 bad ones) |
| `transactions-json-filtered` | 200 |
| `transactions-json-invalid` | 5 |

- Reading the filtered topic back with the same consumer gives 200 valid, 0 malformed, and the same total. Consumers of the filtered topic see one clean format and never have to deal with v1 or broken events.
- The raw topic is never modified, so the filtered topic can be rebuilt from it at any time.
- Here "filtered" means "valid". A business rule (for example, only amounts ≥ 300 for fraud review) could be added in the same place.
- Limit: offsets are committed automatically, every 5 s and on close. If the router crashed after writing to the filtered topic but before committing, those events would be written again after a restart, creating duplicates. Preventing this would need Kafka transactions.

## Task 6 — Replay & debugging

| What I did | Result |
|---|---|
| New group `json-replay` with `--from-beginning` on the raw topic | The same 205 events, 200 valid / 5 malformed, the same total. Reading had removed nothing. |
| Reset group `json-old` (it had rejected all v2 events in Task 4) to the start, then ran the **updated** consumer with that group | Before the reset, offsets 68 and 132 were committed (lag 0). After the reset, all 200 events were read and valid. The bug was fixed in code, and the replay recovered the data. |
| Read one malformed event again by its partition and offset | See below |

Commands:

```bash
# move a group back to the start (the group must be stopped)
docker exec kafka-installation-kafka-1 kafka-consumer-groups --bootstrap-server localhost:9092 \
  --reset-offsets --to-earliest --group json-old --topic transactions-json-v2 --execute

# read one exact event, using the partition/offset the consumer printed
docker exec kafka-installation-kafka-1 kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic transactions-json --partition 0 --offset 70 --max-messages 1 --property print.key=true
```

- **Debugging.** The consumer prints the partition and offset of each malformed event, which is enough to fetch it again at any time. Offset 70 holds `"amount": NaN`. Our producer can't write `NaN` (`allow_nan=False`), so the event came from another producer. The fix belongs there, not in the consumer.
- **Kafka as an event log.** The raw topic holds the full history. Derived topics and results can be rebuilt by replaying it with corrected code, as group `json-old` showed. This only works while the events are retained, which is 7 days by default. A raw topic used as a source of truth should have a longer retention, or `retention.ms=-1` to keep everything.

---

## Phase 2 — Dirty data

Same producer and consumer, unchanged, with the topic `transactions-dirty-json`:

```bash
python producer_json.py --file ../csv_lab/data/transactions_dirty.csv --topic transactions-dirty-json
python consumer_json.py --topic transactions-dirty-json --group json-dirty --from-beginning
```

| Line | Row | Producer |
|---|---|---|
| 4 | `3,101,,2024-01-01 10:01:10` | rejected: amount is missing |
| 5 | `4,abc,89.9,…` | rejected: user_id is not a whole number |
| 7 | `6,102,15.2,not-a-date` | rejected: timestamp is not a valid date |
| 10 | `9,106,seven,…` | rejected: amount is not a number |
| 12 | `2,102,250.5,…` (second copy) | rejected: duplicate transaction_id 2 |
| 13 | `11,107,55.0,` | rejected: timestamp is missing |
| 15 | `13,109` | rejected: wrong number of fields |
| 14 | `12,108,20.0,2024/01/01 10:10:00` | sent, date converted to `2024-01-01T10:10:00` |

- Producer: 7 events sent, 7 rows rejected. Consumer: 7 valid, 0 malformed, total 856.22 EUR.
- These are the same 7 good rows as Part I's validating consumer. **The difference is where the problems are caught:**
	- In Part I, all 14 rows went into Kafka, and problems were found only by the consumer, after the rows were stored. Every consumer had to repeat the same checks.
	- In Part II, converting to JSON forces you to decide each value's type, so the checks happen naturally in the producer. Bad rows never reach the topic, and every consumer receives clean, typed events.
- The consumer still checks everything, because other clients can write to the topic (Task 3).

Limits:

- Rejected rows are only printed, not stored. In a real pipeline they should go to a dead-letter topic, as in Part I, so that someone can fix and re-send them.
- Duplicate detection only works within one producer run, in memory. If the same file is sent twice, or a duplicate arrives in a later file, it is not caught. This is the same limit as in Part I.

## Reflection questions

- **Why is JSON better than CSV for streaming?**
	- Each message describes itself. Field names travel with the values, so a consumer does not need a header that only exists in the original file.
	- Values have types. `amount` is a number, `user_id` is an integer, and missing data can be an explicit `null`.
	- Adding a field does not shift the other columns. Consumers that ignore unknown fields keep working (Task 4).
	- Every language has a standard parser, and commas, quotes and line breaks inside values are escaped properly.
	- Broken messages are easy to detect: a parse error, a missing field or a wrong type (Task 3).
	- The costs: messages are about 3 times larger (104 vs 33 bytes), JSON has no date or decimal type, and Kafka still does not enforce any schema.
- **What problems does schema evolution introduce?**
	- Old and new versions sit side by side in the same topic for as long as they are retained. A consumer replaying history must handle all of them.
	- Producers and consumers are deployed separately. If the producer changes first, consumers reject or crash on the new events. In Task 4, 200 events were rejected.
	- Removing or renaming a field, or changing its type or unit (euros to cents), breaks consumers. Only adding a field is safe, and only if consumers ignore what they don't know.
	- Nothing in Kafka checks the format. Safety depends on discipline (version numbers, tolerant consumers) or on a schema registry (Avro, Protobuf, JSON Schema) that refuses incompatible changes.
- **Why is Kafka often used as a source of truth?**
	- It is an ordered, append-only log. Reading does not delete events, and offsets let any consumer go back and read history again (Task 6).
	- Everything derived from it, such as the filtered topic, totals or a database, can be rebuilt by replaying after a bug fix or a new requirement. Group `json-old` recovered 200 events this way.
	- Many independent consumers can read the same events, each at its own pace.
	- This holds only if raw events are kept unchanged and retained long enough.
