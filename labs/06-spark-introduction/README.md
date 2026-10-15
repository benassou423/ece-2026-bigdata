# Module 06: Spark introduction

Lab: `06.spark-introduction/lab-pyspark-rdd.md`. Done on the Onyxia `vscode-pyspark` service, in the PySpark shell
started with `pyspark --master local[*]`: Spark 4.1.1, Python 3.13.15.

## Files

| File | Content |
|---|---|
| [`rdd_lab.py`](rdd_lab.py) | the cells of the lab in order, then the 4 exercises, runnable from the root of the repository with `spark-submit --driver-memory 1g labs/06-spark-introduction/rdd_lab.py` |

`users.csv` and `orders.csv` are downloaded from the bronze layer at the start of the lab and deleted at the end. They
are not committed (`*.csv` in `.gitignore`).

## Results

| Step | Result |
|---|---|
| Bronze files | `users.csv` 7,351 bytes, 101 lines; `orders.csv` 331,568 bytes, 2,830 lines |
| `getNumPartitions()` | 2 for `users_lines`, 2 for `orders_lines`, 8 after `repartition(8)` |
| `sc.defaultParallelism` | 2: `local[*]` uses the 2 CPUs of the container limit, not the 16 CPUs of the node |
| Lineage of `orders_data` | `PythonRDD` ← `MapPartitionsRDD` ← `HadoopRDD`, printed without reading the file |
| `orders_data.count()` twice | 2,829 twice: 2 jobs, 2 stages, 417.9 KiB read from the file each time |
| `users_lines.take(6)` | the header, then each user split over 2 elements: the address breaks the record after the street |
| `users_lines.count()` | 101 elements, 50 after the UUID filter: the 50 users of the DataFrame lab |
| `orders_rdd.take(3)` | the 3 first orders of the dbt lab: drink x4, bread x5, donut x2 |
| `reduceByKey` / `groupByKey` | same 50 totals, shuffle write of 2,513 bytes against 6,230 bytes |
| Deduplication | 2,829 orders before and after; with 3 replayed orders: 2,832 then 2,829, the 2099 version kept |
| `join()` | 2,829 rows in 4 partitions; `leftOuterJoin()` 2,829 rows, 0 without user |
| Co-partitioned `join()` | 2,829 rows, no new shuffle: 2 of its 3 stages skipped |

### Spark UI

The UI is exposed by the `sparkui` ingress of the service, `https://<user>-<service-id>-spark.lab.adm.adaltas.cloud`
(`kubectl get ingress`), and its REST API (`/api/v1/applications/<app-id>/stages`) gives the exact byte counts.

| Stage | Tasks | Input | Shuffle write | Shuffle read |
|---|---|---|---|---|
| `orders_lines.first()` | 1 | 256.0 KiB | | |
| `orders_data.count()`, 1st | 2 | 417.9 KiB | | |
| `orders_data.count()`, 2nd | 2 | 417.9 KiB | | |
| `count()` on a cached copy of `orders_data`, 2nd | 2 | 162.2 KiB, from memory | | |
| `reduceByKey(add)` | 2 | | 2,513 B | |
| `groupByKey().mapValues(sum)` | 2 | | 6,230 B | |
| `collect()` after `reduceByKey` | 2 | | | 2,513 B |
| `collect()` after `groupByKey` | 2 | | | 6,230 B |
| `join()` of exercise 2, map side | 6 | | 183,477 B | |
| DataFrame `groupBy().sum()`, exercise 3 | 1 | 331,568 B | 5,098 B, 50 rows | |

The `collect()` jobs re-ran only the result stage: the map stage of the shuffle was skipped, its output files being
still available.

## Exercises

### 1. `aggregateByKey`

```python
count_quantity = (
    orders_rdd
    .map(lambda o: (o["user_id"], o["quantity"]))
    .aggregateByKey(
        (0, 0),
        lambda acc, quantity: (acc[0] + 1, acc[1] + quantity),
        lambda a, b: (a[0] + b[0], a[1] + b[1]),
    )
)
average_quantity = count_quantity.mapValues(lambda acc: acc[1] / acc[0])
```

The zero value `(0, 0)` is the accumulator of each key on each partition. The first function adds one order to it, on
the partition, before the shuffle. The second merges the accumulators of the partitions. One pass, one shuffle, and
a fixed-size accumulator per key.

| Measure | Value |
|---|---|
| Users | 50 |
| Orders, total quantity | 2,829 orders, 8,615 items |
| Average quantity per order | 3.05, as in the DuckDB lab |
| Orders per user | 56.58, as in the DuckDB lab |
| Average quantity per user | from 1.5 to 3.57 (`f94d6204…`, 175 items in 49 orders) |

The totals are identical to `user_quantity`, computed with `reduceByKey`.

### 2. Orders without user

A copy of the first order, with a new `order_id` and the `user_id` `00000000-0000-4000-8000-000000000000`, is added
with `union()`. After `leftOuterJoin(users_kv)` and a filter on `None`: **1 order without user**, out of 2,830. The
inner `join()` returns 2,829 rows: it drops the orphan order silently. The left outer join followed by a filter is the
way to detect the broken foreign keys, like the `relationships` test of the dbt lab.

### 3. DataFrame API

```python
orders_df = spark.read.csv("orders.csv", header=True, inferSchema=True).withColumnRenamed("user_uuid", "user_id")
user_quantity_df = orders_df.groupBy("user_id").sum("quantity")
```

The bronze column is `user_uuid`: it is renamed to keep the `groupBy("user_id")` of the exercise. The result is
identical to the RDD version.

- **Line count**: 2 lines against about 13 for the RDD version (`textFile`, header filter, `parse_order` and its 9
  lines, `map` and `reduceByKey`).
- **`.explain()`**: the plan is readable and optimized. `FileScan csv` with
  `ReadSchema: struct<user_uuid:string,quantity:int>` reads only 2 columns of 5, `HashAggregate(partial_sum)`
  aggregates on each partition before the `Exchange hashpartitioning(user_id, 200)`, then `HashAggregate(sum)`. With
  adaptive query execution, the `AQEShuffleRead coalesced` step merges the 200 shuffle partitions, nearly empty for 50
  keys.
- **`.toDebugString()`**: a chain of RDDs, `HadoopRDD` → `PythonRDD` (cached) → `PairwiseRDD` → `ShuffledRDD` →
  `PythonRDD`. Spark only sees opaque Python functions: it cannot prune the unused columns, all 5 fields are parsed in
  Python, and each record crosses the JVM-Python boundary.
- `inferSchema=True` cost 2 extra jobs before the query: one to read the header, one full pass over the 331,568 bytes.

### 4. With and without `.cache()`

`orders_rdd.count()` is timed on the same RDD, while `user_quantity` is rebuilt for each run: a second `count()` of the
same RDD would skip its map stage and reuse the shuffle files, whatever the cache.

| Run | Median of `orders_rdd.count()` | Median of `user_quantity.count()` |
|---|---|---|
| Shell, 5 runs, with cache / without | 0.111 s / 0.151 s | 0.384 s / 0.310 s |
| Shell after restart, 10 runs, with cache / without | 0.238 s / 0.201 s | 0.401 s / 0.404 s |
| `rdd_lab.py`, 5 runs, with cache / without | 0.196 s / 0.179 s | 0.304 s / 0.360 s |

- **Storage tab, with cache**: 1 RDD, `PythonRDD`, `Memory Serialized 1x Replicated`, 2 cached partitions out of 2,
  100 %, 168.0 KiB in memory, 0 on disk. **Without cache**: empty.
- The first count with the cache is the most expensive: it reads the file, parses it and stores the partitions
  (0.204 s to 1.238 s for the first run, depending on the JVM warm-up).
- On this dataset the cache brings **no measurable gain**: the differences change sign from one series to another and
  stay within the run-to-run variation, about 0.1 s. A job here costs mostly its scheduling and the round trip with the
  Python workers. Reading 324 KiB from the local disk and parsing 2,829 lines is negligible. The cache also keeps the
  data in serialized form, so the Python workers still deserialize it.
- The cache pays when the lineage is expensive and reused: a file read from S3, a heavy parsing, a shuffle, and several
  actions on the same RDD, as `orders_rdd` reused by every section of the lab.

## Answers to the questions

### RDD creation and partitions

**What decides the number of partitions `textFile()` picks by default, for a file this size?**
The `minPartitions` argument, which defaults to `sc.defaultMinPartitions = min(sc.defaultParallelism, 2)`, 2 here.
Hadoop's input format aims at `file size / minPartitions` bytes per split, capped by the block size of the file
system. For a file of 324 KiB, smaller than a block, the minimum wins: 2 partitions for both files. Measured with
`sc.textFile("orders.csv", n)`: 1, 2 and 5 partitions for `n` = 1, 2 and 5. For a large file, the block size wins:
1 partition per block, 128 MB on HDFS.

**`repartition()` and `coalesce()` both change the partition count. Which one shuffles data, and which one doesn't?**
`repartition(n)` always shuffles: it is `coalesce(n, shuffle=True)`, and its lineage contains a `ShuffledRDD`. It
redistributes the records evenly and can increase or decrease the partition count. `coalesce(n)` merges existing
partitions without a shuffle, a narrow `CoalescedRDD`: it can only decrease the count. Measured: `coalesce(8)` on
`orders_lines` keeps 2 partitions, `coalesce(1)` gives 1.

### Lazy evaluation and lineage

**Call `orders_data.count()` a second time. Does the Spark UI show a new stage? Why?**
Yes: a new job and a new stage, 2 tasks, which read the 417.9 KiB of the file again. Every action submits a job, and
an RDD stores its lineage, not its data: without persistence, Spark recomputes `orders_data` from the file. Only the
map stages of a shuffle whose output files still exist are skipped, which does not apply to this job without shuffle.

**At which point in this notebook would `.cache()` change that answer?**
On `orders_data`, in the cell of the `filter()`, before the first `count()`. The cache is lazy: the first action fills
it, and the next ones read it. The second `count()` still creates a job and a stage, but its tasks read the cached
partitions instead of the file: on a cached copy of `orders_data`, the second stage read 162.2 KiB from memory against
417.9 KiB from the file. Placed after the first `count()`, `.cache()` only helps from the following action on. The
lab caches one step later, on `orders_rdd`, because the next sections reuse the parsed orders.

### The cost of no schema: the multiline address

**Why does `spark.read.csv(multiLine=True)` not have this problem, while `sc.textFile()` does?**
`textFile()` cuts the file on every line break, without knowing the CSV format: the line break inside the quoted
address ends the element. The CSV reader parses the file with a CSV parser, which respects the quotes: with
`multiLine=True`, a line break between quotes stays in the value of the field. The price is that a file read with
`multiLine=True` cannot be split, since a split could start in the middle of a quoted value.

**In general terms, what has to be true of a file format for a line-oriented reader like `textFile()` to be safe to
use on it?**
One record per line: the line break must never appear inside a value, either forbidden or escaped as `\n`. Every line
break is then a record boundary, and the file can be split anywhere: a reader starting at any offset only has to skip
to the next line break. JSON Lines and CSV without embedded line breaks satisfy it.

### Manual parsing of orders

**What would happen to this cell if one line in `orders.csv` had an extra comma inside `product`?**
Nothing at first: `map()` is lazy. At the first action touching the partition, `line.split(",")` returns 6 values,
and the unpacking raises `ValueError: too many values to unpack (expected 5)` in the Python worker. The task fails,
then the job: tested on a parallelized line, `collect()` ends with a `PythonException` wrapping this error. Even
quoted, `"cookie, extra"` would be split, since `split()` ignores the quotes.

**Where does the DataFrame API perform the equivalent check, and when?**
In the CSV reader, when the rows are parsed against the schema, so during the execution of an action. Only the
schema itself, the names and types of the columns, is known when the DataFrame is defined, and a wrong column name
fails then, before any job. `inferSchema` reads the data before any action. The behavior on a malformed row depends on
`mode`, tested on a file with a 6th field `cookie, extra` and a quantity `abc`:

| `mode` | Row with an extra field | Row with `quantity = abc` |
|---|---|---|
| `PERMISSIVE`, the default | kept, extra field dropped silently | kept, `quantity` is `NULL` |
| `DROPMALFORMED` | dropped | dropped |
| `FAILFAST` | the DataFrame is created, `collect()` fails with `MALFORMED_CSV_RECORD` | not reached, the job has failed |

### Key-value RDDs: total quantity per user

**`reduceByKey` combines values on each partition before shuffling. What does `groupByKey` shuffle instead, and why is
that more expensive?**
Every value: the 2,829 quantities cross the shuffle, grouped by key, while `reduceByKey` sends one partial sum per
key and per partition, at most 100 values for 50 users and 2 partitions. Measured: 6,230 bytes against 2,513 bytes of
shuffle write, 2.5 times more for the same result. The gap grows with the number of values per key. All the values of
a key must also fit in the memory of the reducer, which fails on a skewed key.

**For which aggregations would `groupByKey` be unavoidable, `reduceByKey` not being able to express them?**
When the result needs all the values of a key together, and cannot be built by merging partial results of bounded
size with an associative and commutative function: an exact median or percentile, the time between consecutive orders
of a user, which needs them sorted, or the complete list of orders of a user, to write one file per user. Averages,
counts, minimums or the top N per key do not need it: `reduceByKey` or `aggregateByKey` express them.

### Manual deduplication

**`partitionBy()` in the window function is described as a wide transformation, shuffling rows so every row of a
given `order_id` lands on the same partition. What plays that role here?**
`reduceByKey(most_recent)`, on the pairs `(order_id, order)` built by the `map()`. Its shuffle hash-partitions the
pairs on `order_id`, so every version of an order reaches the same partition, where `most_recent` keeps one. PySpark
implements it with a `partitionBy()`, visible in the lineage: `ShuffledRDD at partitionBy`. The function is also
applied on each partition before the shuffle.

**Which version states the intent more directly: the window function, or the `reduceByKey` with a custom combiner?**
The window function. `row_number()` over `PARTITION BY order_id ORDER BY ordered_at DESC` then `row_number = 1` says
what is kept: the most recent row per order, on a typed timestamp. The `reduceByKey` hides this rule in a function,
and compares the dates as strings, correct only because they are ISO 8601 with the same offset. It works, verified
with 3 replayed orders dated 2099: 2,832 orders, 2,829 after deduplication, the 2099 versions kept.

### Joining orders with users

**`join()` is a wide transformation on both RDDs. What would need to be true of how `orders_kv` and `users_kv` are
partitioned for Spark to skip the shuffle?**
Both RDDs must be partitioned by key with the same partitioner: same hash function and same number of partitions.
Partition `i` of the orders then only joins partition `i` of the users, a narrow dependency. Tested with
`partitionBy(4)` and `.cache()` on both: the lineage of the join shows a `PartitionerAwareUnionRDD` and no new
`ShuffledRDD`, and its job ran 1 stage, the 2 others being skipped. The `partitionBy()` shuffles happen once: the gain
comes when the partitioned RDDs are reused by several joins.

**Replace `join()` with `leftOuterJoin()`. What changes in the result if a `user_id` in `orders` has no matching
user?**
The order is kept, as `(user_id, (order, None))`, where `join()` drops it. On the real data nothing changes, 2,829
rows and 0 `None`: every order has a user, consistent with the `relationships` test of the dbt lab. With the fake
order of exercise 2, `leftOuterJoin()` returns 1 row with `None`, and `join()` loses it.

**`enriched` is not yet filtered or aggregated. What is the risk of calling `.collect()` on it directly, on a dataset
much larger than this one?**
`collect()` brings every element to the driver, into the memory of a single process, as Python objects: here a
dictionary per order plus the username, the whole dataset. On a large dataset the driver runs out of memory and the
application fails. Here the driver shares a container limited to 2,000 MiB, which was killed by the out-of-memory
killer during exercise 4, see the platform notes. Filter and aggregate first, use `take(n)`, or write the result to
storage.

## Platform notes

- `local[*]` gives 2 threads: the pod is limited to 2 CPUs, and the JVM reports 2 processors while `nproc` shows the
  16 CPUs of the node.
- `PYTHON_BASIC_REPL=1` was exported before `pyspark`, to type the cells line by line without the automatic
  indentation of the Python 3.13 REPL.
- `http://localhost:4040` is not reachable from the browser: the Spark UI was opened through the `sparkui` ingress of
  the service.
- During exercise 4, a series of 20 timed runs ended with the container killed by the out-of-memory killer, exit code
  137, `OOMKilled`. The pod is limited to 2,000 MiB, while `spark-defaults.conf` sets `spark.driver.memory 2g`: the
  heap of the driver alone may grow beyond the limit of the whole container, which also runs code-server and the
  Python workers. The container restarted: `/home/onyxia/work` was intact, but the home directory, `~/.aws` included,
  was reset. PySpark was then started with `--driver-memory 1g`, the only change to the commands of the lab, and the
  cells needed by exercise 4 were run again.
