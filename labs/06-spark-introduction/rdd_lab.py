"""Lab 06: introduction to Spark's RDD API.

The cells of lab-pyspark-rdd.md in order, then the 4 exercises. Run it from the root of the repository, once the
bronze files are downloaded:

    aws s3 --profile default cp "s3://$LAB_BUCKET_NAME/bronze/users.csv" users.csv
    aws s3 --profile default cp "s3://$LAB_BUCKET_NAME/bronze/orders.csv" orders.csv
    spark-submit --driver-memory 1g labs/06-spark-introduction/rdd_lab.py
"""

import re
import statistics
import time
from operator import add

from pyspark.sql import SparkSession


def section(title):
    print(f"\n=== {title}")


# SparkContext
spark = SparkSession.builder.appName("lab-rdd").master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

section("RDD creation and partitions")
users_lines = sc.textFile("users.csv")
orders_lines = sc.textFile("orders.csv")
print(users_lines.getNumPartitions())
print(orders_lines.getNumPartitions())
orders_repartitioned = orders_lines.repartition(8)
print(orders_repartitioned.getNumPartitions())
print("defaultParallelism:", sc.defaultParallelism, "defaultMinPartitions:", sc.defaultMinPartitions)

section("Lazy evaluation and lineage")
header = orders_lines.first()
orders_data = orders_lines.filter(lambda line: line != header and line.strip() != "")
print(orders_data.toDebugString().decode())
print(orders_data.count())
print(orders_data.count())

section("The cost of no schema: the multiline address")
for line in users_lines.take(6):
    print(line)
print(users_lines.count())

UUID_AT_START = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")

users_records = users_lines.filter(lambda line: UUID_AT_START.match(line) is not None)
print(users_records.count())

section("Manual parsing of orders")


def parse_order(line):
    order_id, user_id, date, quantity, product = line.split(",")
    return {
        "order_id": order_id,
        "user_id": user_id,
        "date": date,
        "quantity": int(quantity),
        "product": product.strip().lower(),
    }


orders_rdd = orders_data.map(parse_order).cache()
print(orders_rdd.take(3))

section("Key-value RDDs: total quantity per user")
user_quantity = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
print(user_quantity.take(5))

user_quantity_grouped = (
    orders_rdd
    .map(lambda o: (o["user_id"], o["quantity"]))
    .groupByKey()
    .mapValues(sum)
)
print(user_quantity_grouped.take(5))
print("same result:", sorted(user_quantity.collect()) == sorted(user_quantity_grouped.collect()))

section("Manual deduplication")


def most_recent(order_a, order_b):
    return order_a if order_a["date"] > order_b["date"] else order_b


orders_dedup = (
    orders_rdd
    .map(lambda o: (o["order_id"], o))
    .reduceByKey(most_recent)
    .values()
)
print(orders_rdd.count(), orders_dedup.count())

# Check: 3 replayed orders with a later date, only the most recent version must be kept
replayed = [dict(order, date="2099-01-01 00:00:00+00:00") for order in orders_rdd.take(3)]
replayed_ids = {order["order_id"] for order in replayed}
orders_replayed_dedup = (
    orders_rdd.union(sc.parallelize(replayed))
    .map(lambda o: (o["order_id"], o))
    .reduceByKey(most_recent)
    .values()
)
print(
    "with 3 replayed orders:", orders_rdd.count() + len(replayed),
    "after deduplication:", orders_replayed_dedup.count(),
    "kept dates:",
    sorted({o["date"] for o in orders_replayed_dedup.filter(lambda o: o["order_id"] in replayed_ids).collect()}),
)

section("Joining orders with users")


def parse_user_for_join(line):
    fields = line.split(",")
    return fields[0], fields[1]  # uuid, username


users_kv = users_records.map(parse_user_for_join)
orders_kv = orders_rdd.map(lambda o: (o["user_id"], o))

enriched = orders_kv.join(users_kv)
print(enriched.take(3))
print("join:", enriched.count(), "rows,", enriched.getNumPartitions(), "partitions")
print(enriched.toDebugString().decode())

enriched_left = orders_kv.leftOuterJoin(users_kv)
print(
    "leftOuterJoin:", enriched_left.count(), "rows,",
    enriched_left.filter(lambda kv: kv[1][1] is None).count(), "without user",
)

# Question: co-partitioned RDDs, the join reuses the partitioning and adds no shuffle
orders_kv_partitioned = orders_kv.partitionBy(4).cache()
users_kv_partitioned = users_kv.partitionBy(4).cache()
enriched_partitioned = orders_kv_partitioned.join(users_kv_partitioned)
print("co-partitioned join:", enriched_partitioned.count(), "rows")
print(enriched_partitioned.toDebugString().decode())

section("Exercise 1: aggregateByKey")
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
print(count_quantity.take(3))
print(sorted(average_quantity.collect(), key=lambda kv: kv[1], reverse=True)[:3])
order_count, total_quantity = count_quantity.values().reduce(lambda a, b: (a[0] + b[0], a[1] + b[1]))
print(
    "users:", count_quantity.count(), "orders:", order_count, "quantity:", total_quantity,
    "quantity per order:", round(total_quantity / order_count, 2),
    "orders per user:", round(order_count / count_quantity.count(), 2),
)
totals = count_quantity.mapValues(lambda acc: acc[1])
print("same totals as reduceByKey:", sorted(totals.collect()) == sorted(user_quantity.collect()))

section("Exercise 2: orders without user")
FAKE_USER_ID = "00000000-0000-4000-8000-000000000000"
fake_order = dict(orders_rdd.first(), order_id="ffffffff-ffff-4fff-8fff-ffffffffffff", user_id=FAKE_USER_ID)
orders_with_fake = orders_rdd.union(sc.parallelize([fake_order]))

orphans = (
    orders_with_fake
    .map(lambda o: (o["user_id"], o))
    .leftOuterJoin(users_kv)
    .filter(lambda kv: kv[1][1] is None)
)
print("orders:", orders_with_fake.count(), "orders without user:", orphans.count())
print(orphans.collect())
print("inner join:", orders_with_fake.map(lambda o: (o["user_id"], o)).join(users_kv).count(), "rows")

section("Exercise 3: DataFrame API")
# The bronze column is user_uuid: renamed to user_id to keep the groupBy() of the exercise
orders_df = spark.read.csv("orders.csv", header=True, inferSchema=True).withColumnRenamed("user_uuid", "user_id")
user_quantity_df = orders_df.groupBy("user_id").sum("quantity")
user_quantity_df.show(5, truncate=False)
rows = [(row["user_id"], row["sum(quantity)"]) for row in user_quantity_df.collect()]
print("same result as the RDD:", sorted(rows) == sorted(user_quantity.collect()))
user_quantity_df.explain()
print(user_quantity.toDebugString().decode())

section("Exercise 4: with and without cache")


def timed(label, action, runs=5):
    durations = []
    for _ in range(runs):
        start = time.perf_counter()
        result = action()
        durations.append(time.perf_counter() - start)
    print(f"{label}: {result}, 1st run {durations[0]:.3f} s, median {statistics.median(durations):.3f} s")


def total_quantity_per_user(orders):
    return orders.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)


def storage():
    return [(rdd_id, rdd.getStorageLevel().description()) for rdd_id, rdd in sc._jsc.getPersistentRDDs().items()]


for rdd in (orders_rdd, orders_kv_partitioned, users_kv_partitioned):
    rdd.unpersist()

# user_quantity is rebuilt for every run: a second count() of the same RDD would reuse its shuffle files
orders_rdd = orders_data.map(parse_order).cache()
timed("cache    orders_rdd.count()", orders_rdd.count)
timed("cache    user_quantity.count()", lambda: total_quantity_per_user(orders_rdd).count())
print("persisted RDDs:", storage())

orders_rdd.unpersist()
orders_rdd = orders_data.map(parse_order)
timed("no cache orders_rdd.count()", orders_rdd.count)
timed("no cache user_quantity.count()", lambda: total_quantity_per_user(orders_rdd).count())
print("persisted RDDs:", storage())

spark.stop()
