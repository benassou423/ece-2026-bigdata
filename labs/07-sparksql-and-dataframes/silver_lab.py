"""Lab 07: bronze to silver with Spark DataFrames.

The cells of lab-pyspark-silver.md in order, then the 4 exercises. Run it from the root of the repository, with
AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_SESSION_TOKEN set to valid credentials:

    spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
        labs/07-sparksql-and-dataframes/silver_lab.py

It rewrites silver/spark/ and the lab.silver Iceberg tables, and leaves lab.silver.orders with a single snapshot, as
written by the lab.
"""

import json
import os
import re
import time
import timeit
import urllib.request

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def section(title):
    print(f"\n=== {title}")


# SparkSession with S3 access
os.environ["LAB_BUCKET_NAME"] = os.environ["KUBERNETES_NAMESPACE"]
BUCKET = os.environ["LAB_BUCKET_NAME"]

# Adapt to the Spark and Scala versions of your environment
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

spark = (
    SparkSession.builder
    .appName("lab-silver-pyspark")
    .master("local[*]")
    # S3 access
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # Iceberg
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
print(spark)
print("spark.sql.extensions:", spark.conf.get("spark.sql.extensions", "unset"))
print("fs.s3a.endpoint:", spark._jsc.hadoopConfiguration().get("fs.s3a.endpoint"))

section("Credentials provider")


def try_provider(provider):
    """List bronze/ with a new S3A file system using the given credentials provider."""
    jvm = spark._jvm
    conf = jvm.org.apache.hadoop.conf.Configuration(spark._jsc.hadoopConfiguration())
    conf.set("fs.s3a.aws.credentials.provider", provider)
    conf.set("fs.s3a.impl.disable.cache", "true")
    conf.set("fs.s3a.attempts.maximum", "1")
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI(f"s3a://{BUCKET}/"), conf)
    name = provider.split(".")[-1]
    try:
        n = len(fs.listStatus(jvm.org.apache.hadoop.fs.Path(f"s3a://{BUCKET}/bronze/")))
        print(f"{name}: OK, {n} objects")
    except Exception as e:  # noqa: BLE001
        print(f"{name}: {e.java_exception.toString()[:200]}")


try_provider("org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
try_provider("org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")

section("Bronze layer")
t_build = time.time()
users_bronze = spark.read.csv(
    f"s3a://{BUCKET}/bronze/users.csv", header=True, inferSchema=True, samplingRatio=0.01, multiLine=True
)
orders_bronze = spark.read.csv(
    f"s3a://{BUCKET}/bronze/orders.csv", header=True, inferSchema=True, samplingRatio=0.01, multiLine=True
)
users_bronze.printSchema()
users_bronze.show(3, truncate=False)
users_bronze.select("address").show(3, truncate=False)

section("Silver layer: users, DataFrame API")
users_typed = (
    users_bronze
    .select(
        F.col("uuid").cast("string").alias("user_id"),
        F.trim("username").alias("username"),
        F.trim("name").alias("name"),
        F.upper(F.trim("sex")).alias("sex"),
        F.lower(F.trim("mail")).alias("email"),
        F.col("birthdate").cast("date").alias("birthdate"),
        F.split(F.col("address"), "\n").alias("address_lines"),
    )
    .withColumn("street", F.element_at("address_lines", 1))
    .withColumn("address_line_2", F.element_at("address_lines", 2))
    .drop("address_lines")
)
stg_users = users_typed.select(
    "user_id", "username", "name", "sex", "email", "birthdate", "street",
    F.regexp_extract("address_line_2", "^(.+), [A-Z]{2} [0-9]{5}$", 1).alias("city"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 1).alias("state"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 2).alias("zip_code"),
)
# regexp_extract() returns an empty string, not NULL, when there is no match
stg_users = stg_users.withColumn(
    "city", F.when(F.col("city") == "", None).otherwise(F.col("city"))
)
stg_users.show(5, truncate=False)

section("Silver layer: orders, window deduplication")
orders_typed = orders_bronze.select(
    F.col("uuid").cast("string").alias("order_id"),
    F.col("user_uuid").cast("string").alias("user_id"),
    F.col("date").cast("timestamp").alias("ordered_at"),
    F.col("quantity").cast("int").alias("quantity"),
    F.lower(F.trim("product")).alias("product"),
)
print("Original:", orders_typed.count())

duplicates = orders_typed.limit(3)
orders_with_duplicates = orders_typed.unionByName(duplicates)
print("With duplicates:", orders_with_duplicates.count())
orders_with_duplicates.groupBy("order_id").count().filter(F.col("count") > 1).show()

dedup_window = Window.partitionBy("order_id").orderBy(F.col("ordered_at").desc())
stg_orders = (
    orders_with_duplicates
    .withColumn("row_number", F.row_number().over(dedup_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)
print("After deduplication:", stg_orders.count())
stg_orders.groupBy("order_id").count().filter(F.col("count") > 1).show()

section("The same transformation with Spark SQL")
orders_typed.createOrReplaceTempView("orders_typed")
stg_orders_sql = spark.sql("""
    SELECT order_id, user_id, ordered_at, quantity, product
    FROM (
        SELECT *,
               row_number() OVER (PARTITION BY order_id ORDER BY ordered_at DESC) AS rn
        FROM orders_typed
    )
    WHERE rn = 1
""")
stg_orders_sql.show(5)
assert stg_orders.count() == stg_orders_sql.count()
assert set(stg_orders.columns) == set(stg_orders_sql.columns)

stg_users.createOrReplaceTempView("stg_users")
spark.sql("""
    SELECT state,
           count(*) AS users,
           sum(CASE WHEN city IS NULL THEN 1 ELSE 0 END) AS missing_city
    FROM stg_users
    GROUP BY state
    ORDER BY users DESC
""").show(10)
print("states:", stg_users.select("state").distinct().count(),
      "| empty state or zip_code:", stg_users.filter("state = '' OR zip_code = ''").count(),
      "| NULL city:", stg_users.filter("city IS NULL").count())

section("Write the silver layer as Parquet")
stg_users.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
stg_orders.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
print("partitions users / orders:", stg_users.rdd.getNumPartitions(), "/", stg_orders.rdd.getNumPartitions(),
      "| AQE:", spark.conf.get("spark.sql.adaptive.enabled"),
      "| shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"),
      "| parquet timestamp type:", spark.conf.get("spark.sql.parquet.outputTimestampType"))

users_silver = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
orders_silver = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
assert users_silver.count() == users_bronze.count()
assert orders_silver.count() == stg_orders.count()

section("Write the silver layer as Iceberg tables")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")
stg_users.writeTo("lab.silver.users").createOrReplace()
stg_orders.writeTo("lab.silver.orders").createOrReplace()
print(f"silver build, bronze read to Iceberg write: {time.time() - t_build:.1f} s")

spark.sql("""
    SELECT snapshot_id, operation, committed_at
    FROM lab.silver.orders.snapshots
""").show(truncate=False)
spark.sql("""
    SELECT file_path, file_format, record_count, file_size_in_bytes
    FROM lab.silver.orders.files
""").show(truncate=False)

users_iceberg = spark.table("lab.silver.users")
orders_iceberg = spark.table("lab.silver.orders")
assert users_iceberg.count() == users_bronze.count()
assert orders_iceberg.count() == stg_orders.count()

section("Data quality checks, the equivalent of the dbt tests")


def check(name, failures):
    print(f"{'PASS' if failures == 0 else 'FAIL'} {name}: {failures}")


for column in ["order_id"]:
    check(f"unique stg_orders.{column}", stg_orders.groupBy(column).count().filter("count > 1").count())
for column in ["order_id", "user_id", "ordered_at", "quantity", "product"]:
    check(f"not_null stg_orders.{column}", stg_orders.filter(F.col(column).isNull()).count())
check("accepted_values stg_orders.quantity",
      stg_orders.filter(~F.col("quantity").isin(1, 2, 3, 4, 5)).count())
check("accepted_values stg_orders.product",
      stg_orders.filter(~F.col("product").isin("bread", "brioche", "cookie", "croissant", "donut", "drink")).count())
check("relationships stg_orders.user_id -> stg_users.user_id",
      stg_orders.join(stg_users, "user_id", "left_anti").count())
for column in ["user_id", "email"]:
    check(f"unique stg_users.{column}", stg_users.groupBy(column).count().filter("count > 1").count())
check("accepted_values stg_users.sex", stg_users.filter(~F.col("sex").isin("F", "M")).count())

section("Exercise 1: birthdate_is_valid")
first_orders = stg_orders.groupBy("user_id").agg(F.min("ordered_at").alias("first_order_at"))
stg_users_valid_df = (
    stg_users.join(first_orders, "user_id", "left")
    # A user without order is kept valid
    .withColumn("birthdate_is_valid",
                F.coalesce(F.col("birthdate") <= F.to_date("first_order_at"), F.lit(True)))
    .drop("first_order_at")
)
stg_orders.createOrReplaceTempView("stg_orders")
stg_users_valid_sql = spark.sql("""
    SELECT u.*,
           coalesce(u.birthdate <= to_date(o.first_order_at), true) AS birthdate_is_valid
    FROM stg_users u
    LEFT JOIN (SELECT user_id, min(ordered_at) AS first_order_at FROM stg_orders GROUP BY user_id) o
      USING (user_id)
""")
print("DataFrame minus SQL:", stg_users_valid_df.exceptAll(stg_users_valid_sql).count(),
      "| SQL minus DataFrame:", stg_users_valid_sql.exceptAll(stg_users_valid_df).count())
stg_users_valid_df.groupBy("birthdate_is_valid").count().show()
(stg_users_valid_df.join(first_orders, "user_id")
    .filter(~F.col("birthdate_is_valid"))
    .select("username", "birthdate", "first_order_at")
    .orderBy("username")
    .show(truncate=False))

section("Exercise 2: zip_code with a Python UDF and a pandas_udf")
ZIP_RE = re.compile(r"([A-Z]{2}) ([0-9]{5})$")


@F.udf("string")
def zip_udf(address):
    lines = address.split("\n") if address is not None else []
    if len(lines) < 2:
        return None
    match = ZIP_RE.search(lines[1])
    return match.group(2) if match else ""


@F.pandas_udf("string")
def zip_pandas_udf(address: pd.Series) -> pd.Series:
    line2 = address.str.split("\n").str[1]
    # NULL without a second line, empty string without a match, like regexp_extract()
    return line2.str.extract(r"([A-Z]{2}) ([0-9]{5})$")[1].where(line2.isna(), lambda s: s.fillna(""))


zip_native = F.regexp_extract(F.element_at(F.split("address", "\n"), 2), "([A-Z]{2}) ([0-9]{5})$", 2)
reference = users_bronze.select("uuid", zip_native.alias("zip"))
for name, expr in [("udf", zip_udf("address")), ("pandas_udf", zip_pandas_udf("address"))]:
    other = users_bronze.select("uuid", expr.alias("zip"))
    print(name, "differences with regexp_extract():",
          reference.exceptAll(other).count() + other.exceptAll(reference).count())


def bench(df, label, number=3, repeat=3):
    for name, expr in [("regexp_extract", zip_native), ("python udf", zip_udf("address")),
                       ("pandas_udf", zip_pandas_udf("address"))]:
        runs = timeit.repeat(
            lambda: df.select(expr.alias("zip")).write.format("noop").mode("overwrite").save(),
            number=number, repeat=repeat,
        )
        print(f"{label:>10} {name:>15}: {min(runs) / number * 1000:7.1f} ms per run")


bench(users_bronze, "50 rows")
big = users_bronze.select("address").crossJoin(spark.range(4000)).repartition(4).cache()
print("rows:", big.count())
bench(big, "200k rows")
big.unpersist()

section("Exercise 3: cache the bronze DataFrames")
API = spark.sparkContext.uiWebUrl + f"/api/v1/applications/{spark.sparkContext.applicationId}"


def api(path):
    return json.load(urllib.request.urlopen(API + path))


def run(label):
    spark.sparkContext.setJobGroup(label, label)
    t0 = time.time()
    rows = stg_orders.join(stg_users, "user_id").count()
    elapsed = time.time() - t0
    jobs = [j for j in api("/jobs") if j.get("jobGroup") == label]
    stage_ids = {s for j in jobs for s in j["stageIds"]}
    stages = [s for s in api("/stages?status=complete") if s["stageId"] in stage_ids]
    print(f"{label}: {rows} rows, {elapsed:.2f} s, {len(stages)} stages, "
          f"input {sum(s['inputBytes'] for s in stages)} bytes")


run("before-cache-1")
run("before-cache-2")
users_bronze.cache()
orders_bronze.cache()
print(users_bronze.count(), orders_bronze.count())
run("after-cache-1")
run("after-cache-2")
for rdd in api("/storage/rdd"):
    print(rdd["name"][:40], rdd["storageLevel"], rdd["numCachedPartitions"], "partition,",
          rdd["memoryUsed"], "bytes in memory")
users_bronze.unpersist()
orders_bronze.unpersist()

section("Exercise 4: append one order, then time travel")
new_order = spark.createDataFrame(
    [("00000000-0000-4000-8000-000000000001", "bdd640fb-0667-4ad1-9c80-317fa3b1799d", "croissant", 2)],
    "order_id string, user_id string, product string, quantity int",
).select("order_id", "user_id", F.current_timestamp().alias("ordered_at"), "quantity", "product")

new_order.writeTo("lab.silver.orders").append()
spark.sql("""
    SELECT snapshot_id, parent_id, operation, committed_at,
           summary['added-records'] AS added, summary['total-records'] AS total
    FROM lab.silver.orders.snapshots
    ORDER BY committed_at
""").show(truncate=False)
first_snapshot = spark.sql(
    "SELECT snapshot_id FROM lab.silver.orders.snapshots ORDER BY committed_at"
).first()[0]
spark.sql(f"SELECT count(*) AS before_append FROM lab.silver.orders VERSION AS OF {first_snapshot}").show()
spark.sql("SELECT count(*) AS current FROM lab.silver.orders").show()

PARQUET_ORDERS = f"s3a://{BUCKET}/silver/spark/orders.parquet"
new_order.write.mode("append").parquet(PARQUET_ORDERS)
print("Parquet directory after append:", spark.read.parquet(PARQUET_ORDERS).count(), "rows in",
      len(spark.read.parquet(PARQUET_ORDERS).inputFiles()), "files")
try:
    spark.sql(f"SELECT count(*) FROM parquet.`{PARQUET_ORDERS}` VERSION AS OF 1").show()
except Exception as e:  # noqa: BLE001
    print(type(e).__name__, str(e).split("\n")[0][:120])
print("option versionAsOf is ignored:", spark.read.option("versionAsOf", 0).parquet(PARQUET_ORDERS).count())

# Back to the state written by the lab: the silver Parquet and an Iceberg table with one snapshot
stg_orders.write.mode("overwrite").parquet(PARQUET_ORDERS)
spark.sql("DROP TABLE lab.silver.orders PURGE")
stg_orders.writeTo("lab.silver.orders").createOrReplace()
print("restored:", spark.read.parquet(PARQUET_ORDERS).count(), "Parquet rows,",
      spark.sql("SELECT count(*) FROM lab.silver.orders.snapshots").first()[0], "Iceberg snapshot")

spark.stop()
