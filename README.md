# Custom In-Memory DBMS 

A systems-style, CLI-driven **in-memory database engine** implemented in **one C++ file** (`main.cpp`).

This project focuses on:

- **Manual storage + memory management** (custom dynamic arrays, explicit frees)
- **Primary-key indexing** (custom hash index with open addressing + tombstones)
- **Query optimization** (indexed lookup for `WHERE id = ...`, scan otherwise)
- **Concurrency** (parallel `SELECT` readers; exclusive `INSERT`/`DELETE` writers)
- **Latency measurement + benchmarking** (microsecond timings via `std::chrono`)
- **Background maintenance** (periodic index rebuild / slot compaction per table)

## Build

### Windows (g++)

From the project folder:

```bash
g++ main.cpp -std=c++17 -O2 -o dbms.exe
```

Then run:

```bash
dbms.exe
```

## Quick start

```text
DBMS > CREATE people id:INT name:TEXT age:INT
DBMS > INSERT people 1 Alice 30
DBMS > INSERT people 2 Bob 25
DBMS > SELECT people WHERE id = 2
DBMS > SELECT people WHERE name = Bob
DBMS > DELETE people WHERE age = 25
DBMS > SELECT people
DBMS > BENCHMARK people
DBMS > EXIT
```

Values can be quoted (whitespace inside values):

```text
DBMS > INSERT people 3 "Alice Jones" 31
DBMS > SELECT people WHERE name = "Alice Jones"
```

## Command reference (SQL-like)

### `CREATE`

**Typed schema (recommended):**

```text
CREATE <table> id:INT <col:type> ...
```

Types:
- `INT`
- `TEXT`

Notes:
- **The first column must be `id:INT`** (primary key used by the hash index).
- Schema is stored per table and used to type-check inserts and WHERE comparisons.

**Legacy mode (still supported):**

```text
CREATE <table>
```

In this mode the schema is **inferred on first INSERT** as:

- `id:INT`
- `c1:TEXT`, `c2:TEXT`, ...

The inferred schema becomes fixed after the first insert.

### `INSERT`

```text
INSERT <table> <v1> <v2> ...
```

Rules:
- `<v1>` is the `id` value.
- The number of values must match the table’s schema.
- If a row with the same `id` already exists, the record is **replaced** (upsert-like behavior).

Prints:
- `[PERF] INSERT latency: X us`

### `SELECT`

Select all rows:

```text
SELECT <table>
```

Select with equality predicate:

```text
SELECT <table> WHERE <col> = <value>
```

Prints:
- matching rows as `id=<id> <col2> <col3> ...`
- `[PERF] SELECT latency: X us`

### `DELETE`

Delete by equality predicate:

```text
DELETE <table> WHERE <col> = <value>
```

Prints:
- `[PERF] DELETE latency: X us`

### `BENCHMARK`

```text
BENCHMARK <table>
```

Runs:
- concurrent indexed `SELECT` workload (multiple threads)
- serialized `INSERT` workload

Outputs:

```text
Benchmark Results:
SELECT avg latency: X us (threads=4, wall=Y us)
INSERT avg latency: X us
```

### `EXIT`

```text
EXIT
```

Frees all memory and terminates.

## Architecture overview

### Storage model

- A **Table** owns a manually-managed dynamic array of `Record*` slots.
- Deletes create **holes** (slot becomes `nullptr`).
- A **freelist** reuses holes for future inserts (reduces churn / fragmentation).

### Record representation (typed)

- Each record stores a heap array of typed `Value` entries.
- `TEXT` values are heap-allocated C-strings.
- Memory is released on `DELETE` and on `EXIT`.

### Indexing (mandatory)

Primary key index:

- `id (int) -> slot index`
- custom open-addressing hash table with **tombstones**

**Index fast-path**

- `SELECT ... WHERE id = X` uses the hash index (no scan).

**Scan fallback**

- `SELECT ... WHERE <non-id-col> = value` performs a linear scan.
- `SELECT <table>` scans all live slots.

### Concurrency + locking

Per-table reader/writer lock:

- `SELECT` uses a **shared/read** lock → multiple selects run concurrently.
- `INSERT` and `DELETE` use an **exclusive/write** lock.
- Table registry (`CREATE`) has a small mutex; data operations do not take a global DB lock.

### Performance measurement

Each `INSERT`, `SELECT`, and `DELETE` prints microsecond latency:

```text
[PERF] SELECT latency: X us
```

The benchmark mode prints averages (and also wall-clock time for the threaded SELECT run).

### Background maintenance

Each table runs a low-frequency background thread that can:

- **Rebuild the hash index** when tombstones/used factor grows (reduces probe chains).
- **Compact storage** when hole ratio becomes high (densify slots + rebuild index).

Maintenance acquires the **table’s exclusive lock** only when work is triggered.

## Design trade-offs / limitations

- **In-memory only**: no persistence, WAL, recovery, or durability.
- **No transactions/MVCC**: writers block readers; correctness is simple but not “real DB” isolation.
- **Limited query language**: single predicate `WHERE col = value` only.
- **Indexing only on primary key (`id`)**: other columns are scanned.
- **Benchmarking is lightweight**: measures microsecond timings, but does not report percentiles.

## Tips

- Use a typed schema for better realism:

```text
CREATE trades id:INT symbol:TEXT qty:INT price:INT
```

- For fastest lookups, query by primary key:

```text
SELECT trades WHERE id = 123
```

