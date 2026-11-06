# Module 07: Spark SQL and DataFrames

Lab: `07.sparksql-and-dataframes/lab-pyspark-silver.md`. Done on the Onyxia `vscode-pyspark` service, in the PySpark
shell started with `pyspark --master local[*] --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`:
Spark 4.1.1, Scala 2.13.17, Java 21.0.12, Python 3.13.15, `hadoop-aws-3.4.2.jar` and AWS SDK `bundle-2.29.52.jar` in
`$SPARK_HOME/jars`, Iceberg 1.11.0 downloaded from Maven Central.

## Files

| File | Content |
|---|---|
| [`silver_lab.py`](silver_lab.py) | the cells of the lab in order, the data quality checks, then the 4 exercises, runnable from the root of the repository with `spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 labs/07-sparksql-and-dataframes/silver_lab.py` |
| [`../../warehouse/silver/`](../../warehouse/silver) | the metadata of the `lab.silver.users` and `lab.silver.orders` Iceberg tables, committed by the lab (`feat(spark-lab): add Iceberg table metadata`), without the data files |

## Results

| Step | Result |
|---|---|
| Bronze | `users.csv` 7,351 bytes, 50 users; `orders.csv` 331,568 bytes, 2,830 lines, **2,829 orders**, 8,615 items |
| `users_bronze.printSchema()` | the schema of the lab, `birthdate: date` inferred, every other column `string` |
| `users_bronze.show(3)` | the 3 users of the lab, same UUIDs, names and addresses; the birthdates differ, as in the previous labs |
| `stg_users.show(5)` | identical to the lab: `Robinsonshire, KY 01352`, the military address `DPO AP 09617` with a `NULL` city |
| Orders | 2,829 typed orders, 2,832 with the 3 duplicates, 2,829 after the window deduplication, no `order_id` left twice |
| Spark SQL version | same 2,829 rows and same columns, both `assert` pass |
| Users per state | 37 states, AE 3 users and 3 without city, FM 3, then 2 users for NJ, VT, DE, VI, NM, PA, TX and AP; 6 users without city in all |
| Parquet | `silver/spark/users.parquet/`: 1 part-file of 9,581 bytes; `silver/spark/orders.parquet/`: 1 part-file of 139,556 bytes |
| Read back | 50 users, 2,829 orders, both `assert` pass |
| Iceberg | `lab.silver.users` and `lab.silver.orders`, 1 snapshot `overwrite` each, 1 data file of 7,435 and 84,055 bytes |
| Commit | `ddfb9d1 feat(spark-lab): add Iceberg table metadata`, the 8 files of the 2 `metadata/` directories |
| dbt against Spark, DuckDB | 2,829 orders and 8,615 items on both sides, 50 users on both sides |

The lab shows 5,000 orders, 5,003 with the duplicates and a sum of 14,976 in the Iceberg lab. Its dataset is not ours:
5,000 orders would not fit in the 331,568 bytes of the `bronze/orders.csv` listed by the same lab, which is the size of
our file. Our 2,829 orders are the orders of the dbt and RDD labs.

### Comparison with the dbt lab

The dbt lab exported `silver/dbt/users.parquet` and `silver/dbt/orders.parquet`, not `stg_*.parquet`, and the Spark
outputs are in `silver/spark/`: the paths of the DuckDB commands were adapted. The DuckDB secret of the service was
created from the credentials of its startup, expired: it was regenerated as the DuckDB lab describes, after updating
`AWS_*` from the profile.

| Measure | dbt | Spark |
|---|---|---|
| Orders, items | 2,829, 8,615 | 2,829, 8,615 |
| Users | 50 | 50 |
| `order_id`, `user_id` | `UUID` | `VARCHAR` |
| `ordered_at` | `TIMESTAMP WITH TIME ZONE` | `TIMESTAMP` (Spark writes `INT96` by default) |
| `orders` file | 1 object, 72,466 bytes | 1 directory, 1 part-file of 139,556 bytes and a `_SUCCESS` of 11,803 bytes |
| Content, `EXCEPT ALL` both ways, UUIDs cast to `VARCHAR`, times in UTC | | 0 and 0 rows for orders, 0 and 0 for users |

The content is identical: only the types differ.

| Build of the silver layer | Time |
|---|---|
| `uv run dbt build` in `lab_medallion/`: the silver views, the seeds and the tests, PASS=27 WARN=1 TOTAL=28 | 15.5 s, process included |
| Spark, interactive: the 2 Parquet writes | 4.3 s |
| `silver_lab.py`, from the bronze read to the 2 Iceberg writes, with the `show()` and `count()` of the cells | 31.0 s |
| `silver_lab.py` with `spark-submit`, whole script with its exercises, JVM start included | 1 min 46 s |

## Data quality checks

The lab validates with `assert` on counts and a `groupBy().count()` on `order_id`. `silver_lab.py` adds the checks of
the dbt tests, written with the DataFrame API: each one counts the failing rows.

| dbt test | DataFrame check | Failures |
|---|---|---|
| `unique` `order_id`, `user_id`, `email` | `groupBy(column).count().filter("count > 1")` | 0 |
| `not_null` on the 5 columns of `stg_orders` | `filter(col.isNull())` | 0 |
| `accepted_values` `quantity`, `product`, `sex` | `filter(~col.isin(...))` | 0 |
| `relationships` `stg_orders.user_id` → `stg_users` | `join(stg_users, "user_id", "left_anti")` | 0 |

## Exercises

### 1. `birthdate_is_valid`

```python
first_orders = stg_orders.groupBy("user_id").agg(F.min("ordered_at").alias("first_order_at"))
stg_users_valid_df = (
    stg_users.join(first_orders, "user_id", "left")
    .withColumn("birthdate_is_valid",
                F.coalesce(F.col("birthdate") <= F.to_date("first_order_at"), F.lit(True)))
    .drop("first_order_at")
)
```

```sql
SELECT u.*,
       coalesce(u.birthdate <= to_date(o.first_order_at), true) AS birthdate_is_valid
FROM stg_users u
LEFT JOIN (SELECT user_id, min(ordered_at) AS first_order_at FROM stg_orders GROUP BY user_id) o
  USING (user_id)
```

`exceptAll()` both ways returns 0 rows: the two versions agree. **45 valid, 5 invalid**:

| username | birthdate | first order |
|---|---|---|
| cgrimes | 2024-07-21 | 2020-03-10 15:24:50 |
| charleslandry | 2025-12-06 | 2020-03-17 18:29:17 |
| dayderek | 2026-10-08 | 2020-02-03 12:36:43 |
| nicolejohnson | 2022-12-09 | 2020-01-20 04:25:18 |
| ronaldstephens | 2022-05-12 | 2020-02-26 09:46:06 |

The fix of the dbt lab finds the same 5 users. It goes further: `stg_users` keeps `birthdate_is_valid` and replaces the
invalid birthdate with `NULL`, and a `not_null` and `accepted_values` test guards the new column. In dbt the join is a
CTE on `ref('stg_orders')`, which also tells dbt to build `stg_orders` first. In the notebook nothing enforces the order:
`stg_orders` must simply exist when the cell runs.

### 2. Python UDF and `pandas_udf`

The UDF splits the address, applies the regular expression to the second line, and returns `NULL` without a second
line and an empty string without a match, like `regexp_extract()`. The `pandas_udf` does the same on a `pd.Series`
with `str.split()` and `str.extract()`. Both return the same 50 values as `regexp_extract()`, 0 difference.

The plain PySpark shell has no `%%timeit`, which is an IPython magic: the cells were timed with `timeit.repeat()`, 3
runs of 3, best run kept, each run forcing the computation with a `noop` write.

| Dataset | `regexp_extract()` | Python UDF | `pandas_udf` |
|---|---|---|---|
| `users_bronze`, 50 rows, read from S3 each time | 107.1 ms | 128.4 ms | 130.7 ms |
| 200,000 addresses, cached, 4 partitions | 282.0 ms | 675.0 ms | 589.9 ms |
| `silver_lab.py`, 50 rows | 112.6 ms | 118.5 ms | 124.0 ms |
| `silver_lab.py`, 200,000 addresses | 227.0 ms | 708.2 ms | 653.4 ms |

On 50 rows, the job overhead and the S3 read dominate: the 3 versions are within 25 ms. On 200,000 rows, the built-in
function is 2.4 to 3.1 times faster than the Python UDF: it runs in the JVM, on Spark's internal rows, and Catalyst can
optimize around it. The Python UDF serializes every row to a Python worker and back, and calls a Python function per
row. The `pandas_udf` exchanges Arrow batches and calls the function once per batch, so it saves the per-row overhead,
but `str.split()` and `str.extract()` still loop in Python over the strings: only 8 to 13 % faster here. A UDF is a black
box for Catalyst. A built-in function is the first choice, a `pandas_udf` the fallback when no function exists.

### 3. `.cache()` and the Spark UI

`localhost:4040` is not reachable from the browser: the Spark UI was opened through the `sparkui` ingress of the
service, and the bytes come from its REST API, `/api/v1/applications/<app-id>/stages`. The query is
`stg_orders.join(stg_users, "user_id").count()`, timed twice before and twice after `.cache()` on `users_bronze` and
`orders_bronze`, followed by a `count()` to fill the cache.

| Run | Time | Stages | Input |
|---|---|---|---|
| before cache, 1st | 1.33 s | 5 | 670,487 bytes, from S3 |
| before cache, 2nd | 0.61 s | 5 | 670,487 bytes, from S3 |
| after cache, 1st | 0.59 s | 5 | 305,128 bytes, from memory |
| after cache, 2nd | 0.58 s | 5 | 305,128 bytes, from memory |
| `silver_lab.py`, before cache, 1st and 2nd | 0.53 s, 0.49 s | 5 | 670,487 bytes each |
| `silver_lab.py`, after cache, 1st and 2nd | 0.41 s, 0.35 s | 5 | 305,128 bytes each |

- Without cache, every query reads the CSV files again from S3: 670,487 bytes = `orders.csv` twice, because of the
  `unionByName(duplicates)` which scans it a second time for the 3 duplicates, plus `users.csv`.
- **Storage tab**: 2 cached relations, `FileScan csv` of `users.csv` and of `orders.csv`, `Disk Memory Deserialized 1x
  Replicated`, 1 partition each, 100 % cached, 9.2 KiB and 144.4 KiB in memory. The cache is columnar and compressed:
  144.4 KiB for a CSV of 323.8 KiB.
- After the cache, the stages read the `InMemoryRelation` instead of the files: 305,128 bytes = 2 × 147,856 + 9,416,
  the in-memory size of the cached batches. No more S3 request and no more CSV parsing.
- The gain in time is small: in the shell, 1.33 s to 0.6 s, most of it already reached by the 2nd run without cache,
  the JVM being warm; in the script, 0.5 s to 0.35-0.41 s. The files are small: the cost of a query is mostly
  scheduling. The cache pays when the source is slow or large and reused by several queries, which is the case of the
  bronze DataFrames in this notebook, read by every section.
- The DataFrames built before the `.cache()` call, `stg_users` and `stg_orders`, use it too: Spark matches the cached
  plans when it plans a query, not when the DataFrame is defined.

### 4. Append and time travel

One order, `00000000-0000-4000-8000-000000000001`, 2 croissants for the first user, is appended with
`writeTo("lab.silver.orders").append()`.

| Snapshot | Parent | Operation | Added | Total |
|---|---|---|---|---|
| 3671467887114644904 | | `overwrite` | 2,829 | 2,829 |
| 3230829632580908145 | 3671467887114644904 | `append` | 1 | 2,830 |

`SELECT count(*) FROM lab.silver.orders VERSION AS OF 3671467887114644904` returns 2,829, the current table 2,830.

The same order appended to the Parquet directory with `write.mode("append")`: 2,830 rows, new part-files written next to
the old one. ``SELECT count(*) FROM parquet.`…` VERSION AS OF 1`` fails with `UNSUPPORTED_FEATURE.TIME_TRAVEL`, and
`.option("versionAsOf", 0)` is silently ignored, 2,830 rows. A Parquet directory has no history: its state is the list
of files present when it is read. The append can only be undone by deleting the right part-file by hand, and an
`overwrite` deletes the previous files: the previous version is lost. Iceberg keeps the previous snapshot, its manifest
list and its data files, until the snapshot is expired.

To start the Iceberg lab from the table the lab writes, the Parquet directory was overwritten with `stg_orders` and
`lab.silver.orders` dropped and written again: 2,829 rows, 1 snapshot.

## Answers to the questions

### SparkSession with S3 access

**The credentials are temporary. What happens to a running Spark job if the service is restarted mid-lab, and why?**
The job is lost. In `local[*]` mode the driver and the executors are threads of one JVM in the container of the
service: the restart kills the JVM, with the running tasks, the cached data and the shuffle files. Nothing is
checkpointed: the job must be run again from the bronze files. An `overwrite` of a Parquet directory interrupted
midway can leave it empty or half written. An Iceberg write interrupted before its commit leaves orphan data files,
but the table still points to its previous snapshot.
Running it again is not enough either. The session reads the keys when it is created, and after a restart the
container gets back the `AWS_*` variables written when the service was deployed: on this service, restarted during the
lab, they were weeks old and expired, `403 InvalidAccessKeyId`. The new credentials of the Onyxia profile must be
exported before the session is created. Even without restart, a job running longer than the lifetime of the token
fails with `ExpiredToken`: `TemporaryAWSCredentialsProvider` holds fixed values and does not renew them.

**Why does a plain `SimpleAWSCredentialsProvider` fail here, while it worked in setups without a session token?**
It sends only `fs.s3a.access.key` and `fs.s3a.secret.key`. The keys of Onyxia are temporary STS credentials: the access
key is only valid together with its session token, which proves that the key was issued by the token service and is
not expired. Without the token, the server rejects the request. Tested on the service with the same keys, on a new S3A
file system: `SimpleAWSCredentialsProvider` gets `403 InvalidAccessKeyId`, "The access key ID you provided does not
exist in our records", and `TemporaryAWSCredentialsProvider` lists the 2 objects of `bronze/`. With long-lived keys of
an IAM user or of a MinIO account there is no token, and the simple provider is enough.

### Silver layer

**Explain the output of the code that was just run.**
The query groups the 50 users of `stg_users` by `state` and counts, per state, the users and those whose `city` is
`NULL`, the 10 largest groups first. The users spread over 37 codes, so most groups have 1 or 2 users. AE comes first
with 3 users and 3 missing cities, AP has 2 users and 2 missing cities: AE and AP are military regions, whose addresses,
`DPO AP 09617`, have no comma and no city. The `regexp_extract()` of the city does not match them, and the
`F.when(... == "", None)` turns the empty string into `NULL`: the query checks that this conversion worked. FM, the
Federated States of Micronesia, also has 3 users, but civil addresses with a city. The groups of 2 users come in an
arbitrary order: `ORDER BY users DESC` does not order the ties.

**Compare the DataFrame and SQL cells above. Which one would you choose for a one-off exploration, and which for a model
reused by several colleagues? Why?**
Both compile to the same plan, so performance is not the criterion. For a one-off exploration, SQL: it is short, every
analyst reads it, it is easy to paste into DuckDB or a notebook, and the window clause states the intent directly. For
a model reused by colleagues, the DataFrame API: it is Python code, so it can be split into functions, parameterized,
imported, unit tested on small DataFrames and checked by a linter, and a wrong column name fails when the DataFrame is
defined rather than inside a string. SQL kept for a shared model belongs in a tool which brings these properties to
SQL, such as dbt.

**`regexp_extract()` returned an empty string instead of `NULL` on no match. Where else in this notebook could that same
behavior silently produce a wrong result if left unhandled?**
In `state` and `zip_code`, extracted with the same function and left as they are: only `city` is converted. An address
whose second line does not end with `XX 12345`, a 9-digit ZIP code or a lowercase state, would get `state = ''` and
`zip_code = ''`. The users-per-state query would count them as a group `''`, `count(state)` would count them, a
`not_null` test would pass, and a join with a table of states would drop them silently. Measured: 0 such rows in this
dataset, but nothing guarantees it for the next file. The `F.trim()` calls behave the same way: an empty or blank
`username` becomes `''`, not `NULL`. The casts have the opposite defect: `cast("int")` or `cast("timestamp")` on a
malformed value returns `NULL` without error, and the deduplication then orders `NULL` dates.

### Write the silver layer as Parquet

**List the objects written under `silver/users.parquet/`. How many part-files are there, and why?**
The directory is `silver/spark/users.parquet/` in this repository's layout. It contains 1 part-file,
`part-00000-…-c000.snappy.parquet`, 9,581 bytes, and a `_SUCCESS` file of 11,787 bytes, the JSON report written by the
S3A committer, enabled in `spark-defaults.conf`. Spark writes one file per partition of the DataFrame. `users.csv`, 7 KB,
is read as 1 partition: a file smaller than `spark.sql.files.maxPartitionBytes`, 128 MB, is one split, and with
`multiLine=True` a CSV file is never split. No shuffle follows. `orders.parquet/` also has 1 part-file: the window
shuffles into `spark.sql.shuffle.partitions` = 200 partitions, but adaptive query execution coalesces them into 1, the
data being far below the 64 MB target of a partition. Measured: `getNumPartitions()` returns 1 for both.

**`coalesce(1)` before `.write` would produce a single file. What do you lose by doing that on a larger dataset?**
The parallelism of the last stage: one task, on one core of one executor, computes and writes the whole dataset while
the others wait, and `coalesce()` has no shuffle, so the reduction propagates upstream and the transformations before
the write run on 1 partition too. That task needs memory and disk for everything, and one failure restarts all of it.
The single file then cannot be read in parallel if it is large, unless it has several row groups, and later appends
cannot add files without breaking the single-file property. A single file is only reasonable for a small output meant
for a tool which wants one object. Otherwise, write files of 128 MB to 1 GB.

### Write the silver layer as Iceberg tables

**Compare `silver/orders.parquet/` and `warehouse/silver/orders/`. Which files hold the data, and which ones describe the
table?**
The rows are in Parquet files in both: the part-file of `silver/spark/orders.parquet/` and the file of
`warehouse/silver/orders/data/`. The Parquet directory has nothing else but `_SUCCESS`, a marker of the end of the job
which no reader uses. The Iceberg table is described by its `metadata/` directory: `v1.metadata.json`, the schema with
column ids, the partition spec, the properties and the list of snapshots; `snap-….avro`, the manifest list of a
snapshot; `…-m0.avro`, a manifest, the list of data files with their record count, size and column statistics; and
`version-hint.text`, the number of the current version for the `hadoop` catalog. The two data files differ: 139,556
bytes compressed with Snappy for Spark, 84,055 bytes for Iceberg, whose default codec is `zstd`
(`write.parquet.compression-codec`).

**A second job overwrites the Parquet directory while a query is reading it. What does the reader see? What changes
with an Iceberg table?**
With Parquet, it depends on timing. The reader lists the directory at planning time, then opens the files. The
`overwrite` deletes the old files, then writes the new ones: the reader can fail with `FileNotFoundException` on a file
deleted after the listing, read an empty directory, or mix old and new files, possibly with duplicates or a different
schema. Nothing isolates the reader from the writer. With Iceberg, the reader resolves the current snapshot when it
starts and reads only the files listed by its manifests. The writer adds new files and commits a new metadata file: the
old files stay in place until the snapshot is expired. The reader sees the complete old version, the next query the
complete new version, never a mix.

**With this catalog, the pointer to the current version of a table is a file in the bucket. What can go wrong if two
jobs commit at the same time, and what would a catalog service add?**
Each job writes `v<n+1>.metadata.json` from the version `n` it read, then updates `version-hint.text`. The commit is
only atomic if the creation of `v<n+1>.metadata.json` fails when the file already exists, a rename without overwrite
that HDFS provides but S3 does not. On S3 the second writer can overwrite the file of the first: the first commit is
lost, its data files become orphans, or a reader follows a `version-hint.text` which points to a version being
replaced. The Iceberg documentation states that the `hadoop` catalog is not safe for concurrent writes on object
storage. A catalog service, a REST catalog such as Polaris, Nessie or Glue, makes the commit an atomic compare-and-swap
of the pointer: the commit of the second job fails, and Iceberg retries it on top of the new version. It also gives a
single list of tables to every engine, with access control and credentials vended per table.

### Compare with the dbt lab

**What differs between the two schemas? Spark has no `UUID` type: what did you do about it, and what is the consequence
for the consumers of the silver layer?**
`order_id` and `user_id` are `UUID` in the dbt output and `VARCHAR` in the Spark output. `ordered_at` is
`TIMESTAMP WITH TIME ZONE` for dbt, a plain `TIMESTAMP` for Spark, which writes the legacy `INT96` type by default:
the instants are the same, read in UTC. The UUIDs were kept as strings with `cast("string")`. For the consumers: 36
bytes per value instead of 16, which explains part of the 139,556 bytes against 72,466; no validation of the format,
any string is accepted; comparisons and joins with a `UUID` column need a cast, and a case or format difference makes
two equal UUIDs different strings. A consumer which mixes both outputs must cast one side. Iceberg has a `uuid` type,
but Spark has no type to map it to: the table created from the DataFrame has `string` columns too.

**Compare the output layout: one object per model with dbt, one directory of part-files with Spark. Which one is easier
for a consumer to read? Which one scales?**
One object is easier: any tool reads `orders.parquet` directly, DuckDB, pandas or a download, without a glob or a
directory listing, and without `_SUCCESS` or the temporary directories of the committer. The directory scales: each
task writes its file in parallel, the write is not limited by one process, and the readers split the work by file. A
single object is written by one process, and a large one must be read through its row groups. Above some size the
directory, or better a table format which lists the files, is the only option.

**The dbt lab validates the silver layer with the `unique`, `not_null`, `accepted_values` and `relationships` tests.
What did this lab use instead, and what would you need to write to get the same guarantees?**
The lab uses `assert` on counts, bronze against silver and DataFrame against SQL, and a `groupBy("order_id").count()`
filtered on `count > 1`, read by eye. It checks that no row was lost, not that the values are valid. To get the same
guarantees, each test becomes a query counting the failing rows, run after the build, which fails the job above 0:
`groupBy().count().filter("count > 1")` for `unique`, `filter(col.isNull())` for `not_null`, `filter(~col.isin(...))`
for `accepted_values`, a `left_anti` join for `relationships`. They are written in `silver_lab.py`, all pass. dbt also
provides what this code lacks: the declaration next to the model, the severity, `warn` or `error`, the report, and the
build stopped before the downstream models. Libraries such as Great Expectations, Soda or Deequ do it for Spark, or dbt
itself with the Spark adapter.

**`ref()` and `source()` give dbt the dependency graph of the models. What plays this role in the notebook?**
The Python variables and the order of the cells. `stg_orders` is built from `orders_bronze` because the cell names the
variable, and the cell must run after the one which defines it. The dependency is implicit: no tool knows the graph,
can build it in the right order, run one model and its children, or draw the lineage. Running the cells out of order
uses a stale variable or fails with a `NameError`. Within a query, Spark's logical plan is a dependency graph, but
only for one DataFrame and only during the session. The paths `s3a://…/bronze/…` play the role of `source()`, written
by hand.

**Compare the time to build the silver layer with `dbt build` and with the cells of this lab. Why are these numbers not
directly comparable?**
`dbt build` took 15.5 s, process included. In Spark, the 2 Parquet writes of the shell took 4.3 s, and `silver_lab.py`
needed 31.0 s from the read of bronze to the Iceberg writes, 1 min 46 s for the whole script with its exercises.
They do not measure the same work. `dbt build` creates views, which compute nothing, loads the seeds, and spends its time
in the tests, each a query reading the CSV files from S3 through the views, plus the startup of Python and dbt and the
parsing of the project. It does not write the Parquet files, exported by hand in the dbt lab. The Spark cells read the
CSV files once, infer their schema, and write Parquet and Iceberg files, in a JVM already started: the session, the
download of Iceberg and the warm-up are outside the measure. At 7 KB and 324 KB, the fixed costs dominate both: startup,
scheduling, S3 latency. These numbers say nothing about how each one scales with the data.

**For each of these cases, would you choose dbt with DuckDB or Spark: a daily batch of 10 GB, a daily batch of 10 TB, a
team of analysts who only know SQL?**
- **10 GB per day**: dbt with DuckDB. One machine with a few cores reads 10 GB of Parquet in seconds to minutes, without
  a cluster to run, and dbt brings the tests, the graph and the documentation.
- **10 TB per day**: Spark. The data exceed the memory and the disk of one machine and the time window of a single
  process: the work must be split over a cluster, with recovery of failed tasks. dbt can still be the layer of models
  and tests, with the Spark adapter, Spark doing the compute.
- **Analysts who only know SQL**: dbt, on the engine which fits the volume: DuckDB for small data, Spark SQL, Trino or a
  warehouse for large data. The models are `SELECT` statements, `ref()` and the tests need no Python.

## Platform notes

- **Resources**: the lab asks for a service limited to `cpu: 4000m` and `memory: 4000Mi`. The existing service had
  2 CPUs and 2,000 MiB. The namespace quota allows 4 CPUs and 4 GiB: the StatefulSet was patched with the `kubectl
  patch` command given by the Polaris lab, with the values of this lab. The pod restarted, `/home/onyxia/work` intact,
  the home directory reset. `local[*]` then gets 4 threads, and the driver keeps the 2 GB of `spark-defaults.conf`
  without being killed as in the RDD lab.
- **Credentials**: the `AWS_*` variables of the container date from the creation of the service and were expired,
  `InvalidAccessKeyId`. The Onyxia S3 profile was pasted again, and `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and
  `AWS_SESSION_TOKEN` were exported from `aws configure get … --profile default` before starting `pyspark`, in each
  terminal used by Spark or DuckDB.
- **`getOrCreate()` in the shell**: `pyspark` creates the session before the first cell, so the cell of the lab
  returns the existing session. The runtime settings apply, such as the `lab` catalog. The static ones do not:
  `spark.sql.extensions` stays unset, so the Iceberg SQL extensions, needed for the `CALL` procedures, are not loaded.
  None of the commands of the lab needs them. S3A works anyway, because Spark copies the `AWS_*` variables into the
  Hadoop configuration when the context starts: `fs.s3a.endpoint` is `AWS_ENDPOINT_URL`, with its trailing slash, and
  the provider is already `TemporaryAWSCredentialsProvider`. With `spark-submit`, `silver_lab.py` creates the session
  itself and every setting applies.
- `PYTHON_BASIC_REPL=1` was exported before `pyspark`, to type the cells without the automatic indentation of the Python
  3.13 REPL.
