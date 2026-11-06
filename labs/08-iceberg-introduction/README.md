# Module 08: Iceberg introduction

Lab: `08.iceberg-introduction/lab.md`. Done on the Onyxia `vscode-pyspark` service right after the Spark DataFrame lab,
in a new PySpark shell started with
`pyspark --master local[*] --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`: Spark 4.1.1, Scala
2.13, Iceberg 1.11.0, `hadoop` catalog `lab` in `s3a://user-b-assouline-ece/warehouse`.

## Files

| File | Content |
|---|---|
| [`iceberg_lab.py`](iceberg_lab.py) | the cells of the lab in order, then the exercises 1 to 5, runnable from the root of the repository after `silver_lab.py` of the lab 07, with `spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 labs/08-iceberg-introduction/iceberg_lab.py` |

`iceberg_lab.py` was run once after the lab, in 50.5 s, with the same results as the shell: only the snapshot ids and
the times differ, the ids quoted below come from the shell.

The lab has no commit step. The warehouse was removed from the bucket by the cleanup of the lab: only the metadata of
the tables written by the lab 07 remain, in `warehouse/silver/` at the root of the repository.

## Results

| Step | Result |
|---|---|
| `aws s3 ls warehouse/` | the tables of the lab 07: `data/`, `metadata/` with `v1.metadata.json`, a manifest `…-m0.avro`, a manifest list `snap-….avro` and `version-hint.text` |
| `SHOW NAMESPACES IN lab` | `silver` |
| Create an Iceberg table | skipped, as the lab says: `lab.silver.orders` exists, written by the lab 07 |
| `count(*)`, `sum(quantity)` | **2,829 orders, 8,615 items**; the lab shows 5,000 and 14,976 with a larger dataset, see the lab 07 |
| Files | 1 data file of 84,055 bytes, 1 manifest of 7,349 bytes, 1 manifest list of 4,457 bytes, `v1.metadata.json` of 1,933 bytes, `version-hint.text` of 1 byte |
| `snapshots` | 1 snapshot, `1809013845608729676`, `overwrite`, no parent, its manifest list and a summary |
| `files` | 1 data file, `PARQUET`, 2,829 records, 84,055 bytes |
| Second snapshot | 3 orders appended with new `uuid()`: 2 snapshots, `overwrite` then `append`, 2,832 rows, 2 data files |
| Time travel | 2,829 rows `VERSION AS OF` the first snapshot, 2,832 now |
| Schema evolution | `source STRING` added, `DESCRIBE` shows 6 columns; the `INSERT` adds 1 row with `source = 'spark'`: 3 snapshots, 2,833 rows, 2,832 with `source` `NULL` |
| Compare with Parquet | `silver/spark/orders.parquet/`: a part-file of 139,556 bytes and `_SUCCESS`; `warehouse/silver/orders/`: 3 data files, 3 manifests, 3 manifest lists, 4 `vN.metadata.json`, `version-hint.text` |

### What a snapshot contains

`SELECT * FROM lab.silver.orders.snapshots` returns `committed_at`, `snapshot_id`, `parent_id`, `operation`,
`manifest_list` and `summary`. The summary of the first snapshot: `added-data-files 1`, `added-records 2829`,
`added-files-size 84055`, `total-records 2829`, `total-data-files 1`, `total-delete-files 0`, `manifests-created 1`,
`engine-name spark`, `engine-version 4.1.1`, `iceberg-version Apache Iceberg 1.11.0`, `app-id`, `app-name PySparkShell`.

`v1.metadata.json` holds `format-version 2`, the `location`, the schemas with an id per column, `order_id` 1 to `product`
5, `ordered_at` stored as `timestamptz`, an empty partition spec, the properties, among them
`write.parquet.compression-codec zstd`, the snapshots and the logs of snapshots and of metadata files.

### Files of `lab.silver.orders` along the lab

| Version | Written by | Snapshot | Data files | Rows |
|---|---|---|---|---|
| `v1.metadata.json` | `writeTo().createOrReplace()`, lab 07 | `overwrite` 2,829 | 84,055 B | 2,829 |
| `v2.metadata.json` | `append()` of 3 orders | `append` +3 | + 1,954 B | 2,832 |
| `v3.metadata.json` | `ALTER TABLE ... ADD COLUMN source` | none | | 2,832 |
| `v4.metadata.json` | `INSERT INTO ... LIMIT 1` | `append` +1 | + 2,201 B | 2,833 |

`metadata_log_entries` lists the 4 files, and `version-hint.text` contains `4`. The `ALTER TABLE` wrote a new metadata
file without a snapshot: the data did not change. The data file of 84,055 bytes is never rewritten: read directly with
`spark.read.parquet()`, it still has 5 columns, without `source`.

## Exercises

### 1. `lab.silver.users` as an Iceberg table

The table already existed, written by the lab 07 with `writeTo().createOrReplace()`. It was dropped and created again
with SQL, from the silver Parquet of the lab 07:

```sql
DROP TABLE IF EXISTS lab.silver.users PURGE;
CREATE TABLE lab.silver.users USING iceberg
AS SELECT * FROM parquet.`s3a://user-b-assouline-ece/silver/spark/users.parquet`;
```

50 users, 1 snapshot. The `CREATE TABLE AS SELECT` records an `append` snapshot, where `createOrReplace()` records an
`overwrite`.

### 2. A new batch of users

5 users copied from the first 5 by `username`, with a new `uuid()` and the suffix `_batch2`, appended with
`writeTo("lab.silver.users").append()`:

| Snapshot | Parent | Operation | Added | Total |
|---|---|---|---|---|
| 8119307246846011173 | | `append` | 50 | 50 |
| 7077899770934632896 | 8119307246846011173 | `append` | 5 | 55 |

### 3. Before and after the append

| Query | Rows |
|---|---|
| `VERSION AS OF 8119307246846011173` | 50 |
| `VERSION AS OF 7077899770934632896` | 55 |
| `TIMESTAMP AS OF` the commit time of the first snapshot | 50 |
| current table | 55 |

The difference between the two versions, `EXCEPT` on `username`, is the 5 users of the batch: `ahancock_batch2`,
`atkinsbarbara_batch2`, `blairamanda_batch2`, `bruce43_batch2`, `calebrussell_batch2`.

### 4. A `country` column

```sql
ALTER TABLE lab.silver.users ADD COLUMN country STRING
```

The first write failed: `AnalysisException [INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH]`, "Detected
incompatible changes to table `lab`.`silver`.`users` after DataFrame/Dataset has been resolved and analyzed",
`country STRING has been added`. The new rows were built from the `users` DataFrame of exercise 2, analyzed before the
`ALTER TABLE`: Spark 4.1 refuses to write with a plan which no longer matches the table. Reading the table again with
`spark.table()` fixed it. 2 users, suffix `_batch3`, `country = 'US'`, appended: 57 users, 55 with `country` `NULL`, 2
`US`, a third snapshot `append` +2. Read `VERSION AS OF` the first snapshot, the table has the schema of that snapshot,
without `country`: time travel also travels in the schema.

### 5. S3 layout of the two tables

| | `users` | `orders` |
|---|---|---|
| Data files | 3, 14,956 bytes: 7,435 + 3,580 + 3,941 | 3, 88,210 bytes: 84,055 + 1,954 + 2,201 |
| Manifests `…-m0.avro` | 3 | 3 |
| Manifest lists `snap-….avro` | 3 | 3 |
| `vN.metadata.json` | 4 | 4 |
| `version-hint.text` | `4` | `4` |
| Total | 66,447 bytes | 136,892 bytes |

The two tables have the same layout, `data/` and `metadata/`, and the same history shape: a first write, an append, a
schema change without snapshot, an append. Each write adds 1 data file, 1 manifest, 1 manifest list and 1 metadata
file: none is modified. For `users`, the metadata weigh 51.5 KB for 15 KB of data, more than the data. For `orders`,
48.7 KB for 88.2 KB. The metadata cost is fixed per commit, a few kilobytes, whatever the size of the data: negligible
for files of hundreds of megabytes, dominant for small tables or many small commits, which is why streaming writes to
Iceberg need compaction and expiration of snapshots.

### 6. Parquet, Iceberg and an Iceberg catalog

- **Parquet** is a file format: one file stores rows in columnar form, with its own schema, row groups and statistics
  in its footer. A directory of Parquet files is only a set of files: the readers list the directory, and nothing says
  which files form a consistent version.
- **Iceberg** is a table format: a specification of metadata files, stored next to the data, which describe a table
  made of data files, most often Parquet. The metadata file holds the schema with column ids, the partition spec and the
  snapshots; each snapshot points to a manifest list, which points to manifests, which list the data files with their
  statistics. This gives atomic commits, snapshot isolation, time travel, schema and partition evolution, and file
  pruning without listing directories.
- **An Iceberg catalog** stores, for each table name, the pointer to its current metadata file, and swaps it atomically
  at each commit. Here the `hadoop` catalog keeps the pointer in the bucket itself, `version-hint.text`, safe only on a
  file system with atomic rename. A catalog service, a REST catalog such as Polaris, Nessie or Glue, makes the swap a
  compare-and-swap on a server, lists the namespaces and tables for every engine, and can control the access and vend
  credentials per table.

## Answers to the questions

### Inspect the files

**Which files contain the actual rows?**
The Parquet files of `data/`: 1 file of 84,055 bytes, 2,829 rows, at the start of the lab.

**Which files describe the table?**
The files of `metadata/`. `vN.metadata.json` describes the table: schema, partition spec, properties, list of
snapshots, current snapshot. `snap-<id>-….avro` is the manifest list of a snapshot, the list of its manifests with the
range of partition values of each. `<uuid>-m0.avro` is a manifest, the list of data files with their path, format,
record count, size and per-column statistics. `version-hint.text` holds the number of the current metadata file for
the `hadoop` catalog.

**Why does an Iceberg table need more than the Parquet files?**
Because the Parquet files alone do not say which files belong to the table, and in which version. The metadata list the
files of each version explicitly: a reader does not list the directory, does not see files being written or files of an
old version, and can prune files by their statistics before opening them. They hold the history, which gives time
travel and rollback, the schema with column ids, which allows renaming or adding a column without rewriting files,
and the partition spec. A commit is a single atomic switch to a new metadata file, which a set of files cannot offer.

### Explore Iceberg metadata

**How many snapshots does the table currently have?**
1: `1809013845608729676`, `overwrite`, written by `writeTo().createOrReplace()`.

**How many data files belong to the current table?**
1: `00000-372-…-00001.parquet`, 2,829 records, 84,055 bytes.

**What information does the `snapshots` table contain that the Parquet files do not?**
The history of the table: an id per version, its parent, the time of the commit, the operation (`overwrite`, `append`),
the manifest list which gives the files of that version, and a summary of the change: files and records added or
deleted, totals, and the engine, its version, the Iceberg version and the application which committed it. A Parquet file
only knows its own content: its schema, its row groups and their statistics. It does not know which other files form
the table, when it was added, by which job, or what the table looked like before.

### Create a second snapshot

**How many snapshots are there now?**
2: the `overwrite` of 2,829 rows, then the `append` of 3 rows, whose parent is the first. The table has 2,832 rows.

**Did the original Parquet files disappear?**
No. The data file of 84,055 bytes is still in `data/` and in the `files` table, next to a new file of 1,954 bytes with
the 3 new rows. The old metadata files are kept too: `v1.metadata.json` next to `v2.metadata.json`.

**Why does Iceberg create a new snapshot instead of simply replacing the table?**
Because the files are immutable on object storage, and readers may be using the current version. A new snapshot is
built next to the old one, new data files and new metadata, then the commit switches the pointer in one step. The
readers of the old snapshot finish on a consistent state, the next readers see the new one, and a failed write leaves
the table unchanged. It also costs only the new data: the 3 rows are written, not the 2,832. And the old snapshot
remains readable, for time travel, audit or rollback.

### Time travel

**What does time travel read?**
The metadata of the snapshot requested: its manifest list, then its manifests, then the data files they list, with the
schema of that snapshot. `VERSION AS OF 1809013845608729676` reads only the file of 84,055 bytes, 2,829 rows. The
current table reads the 2 files, 2,832 rows.

**Why can Iceberg access an older version without making a complete copy of the table?**
Because a snapshot is a list of files, not a copy of the data. The append wrote one new file and new metadata; the files
of the previous version were neither modified nor deleted, and the new snapshot lists them again. The two snapshots
share the file of 84,055 bytes: keeping the old version costs only its metadata files, a few kilobytes, and the files it
alone references, none here.

**What would happen if the old snapshot's metadata and data files were physically deleted?**
Time travel to that snapshot would fail: reading its manifest list or a data file would raise a file not found error.
Deleting a data file shared with the current snapshot would break the current table too. Deleting files by hand
corrupts the table. The safe way is `expire_snapshots`: it removes the snapshot from the metadata and then deletes only
the files that no remaining snapshot references. After that, the old version is gone for good, and time travel to it is
refused cleanly.

### Schema evolution

**Did the existing Parquet files have to be rewritten?**
No. The `ALTER TABLE` wrote only `v3.metadata.json`. The file of 84,055 bytes is unchanged: read directly, it still has 5
columns. When the table is read, Iceberg returns `NULL` for `source` in the files which do not have it, matching the
columns by their id, not by their name or position. Only the row inserted after the change is in a file with the 6
columns.

**Where is the new schema recorded?**
In the metadata file `v3.metadata.json`: a new schema, with id 1, is added to the list `schemas`, the column `source`
gets the next free field id, 6, and `current-schema-id` points to it. Each snapshot records the id of the schema it was
written with, which is why time travel returns the schema of the snapshot.

**Why is schema evolution a table-level operation rather than simply changing a Parquet file?**
Because the table is made of many files, written at different times with different schemas, and each Parquet file has
its own schema, fixed when it is written. The schema of the table must be defined once, above the files, so that every
reader applies the same one to all of them, old and new. Rewriting each file to change its schema would cost a full
rewrite of the table and would not be atomic. The column ids also make a rename or a drop safe: a new column never gets
the values of an old column of the same name.

The `INSERT` of the lab copies an existing order with `source = 'spark'`: 2,833 rows for 2,832 distinct `order_id`. The
table now has a duplicate order, which the `unique` test of the dbt lab would reject. Iceberg does not enforce keys.

### Compare Iceberg with Parquet

**Which one is simply a collection of Parquet files?**
`silver/spark/orders.parquet/`: a part-file and `_SUCCESS`, a marker of the end of the job which no reader uses. The
table is whatever the directory contains when it is listed.

**Which one has table metadata and snapshots?**
`warehouse/silver/orders/`: `metadata/` holds 4 metadata files, 3 manifest lists and 3 manifests, for 3 snapshots and
3 data files.

**How would you safely replace a Parquet directory while another query is reading it?**
Never overwrite it in place. Write the new version into a new directory, for example `orders/v=2026-11-06/` or with a
job id, check it, then switch the readers to it in one step: a view or a table of a metastore whose location is
updated, or a pointer file read by the consumers. Delete the old directory only once no query can still read it, after
a delay longer than the longest query. This is, done by hand, what a table format does.

**How does Iceberg make the table state visible atomically?**
A commit writes all the new data files first, then the new manifests, manifest list and metadata file, which are not
visible yet. The only visible step is the swap of the pointer of the catalog from the old metadata file to the new one,
a single atomic operation, which succeeds only if the pointer has not changed since the writer read it. A reader
resolves the pointer once, and then reads only the files of that snapshot. A concurrent commit makes the swap fail, and
the writer retries on top of the new version.

**Why is a catalog needed once several engines need to share the same Iceberg table?**
Each engine must find the same current metadata file and commit with the same atomic swap. With the `hadoop` catalog,
the pointer is `version-hint.text`, and the commit relies on an atomic rename which S3 does not provide: two engines
writing at the same time can lose a commit. Each engine must also be configured with the path of the warehouse. A
catalog service is a single place, reached through the same API by Spark, Trino, DuckDB or Flink, which maps the names
of the tables to their current metadata, serializes the commits, and controls who can read or write each table. The
next module of the course, Polaris, provides such a REST catalog.

## Cleanup

```bash
aws s3 --profile default rm "s3://$LAB_BUCKET_NAME/warehouse/" --recursive
```

The cleanup was run after the two scripts were verified. The bucket keeps `bronze/`, `silver/dbt/` and `silver/spark/`.

## Platform notes

- The PySpark shell of the lab 07 was stopped with `spark.stop()`, and a new shell was started for this lab.
- The lab asks to pick the kernel **Python (lab-pyspark uv)** in a VS Code notebook, but runs every cell in the PySpark
  shell: the shell was used, as in the lab 07.
- As in the lab 07, the cell `SparkSession.builder....getOrCreate()` returns the session created by `pyspark`: the `lab`
  catalog is set, the Iceberg SQL extensions are not, and no command of the lab needs them. `iceberg_lab.py`, run with
  `spark-submit`, loads them.
- To start this lab from the table written by the lab 07, with 1 snapshot, `lab.silver.orders` was dropped and written
  again at the end of the exercise 4 of the lab 07, which had appended an order.
