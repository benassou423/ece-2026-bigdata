"""Lab 08: explore Iceberg tables with Spark.

The cells of lab.md in order, then the exercises 1 to 5. Run it from the root of the repository, after silver_lab.py
of the lab 07, with AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_SESSION_TOKEN set to valid credentials:

    spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0 \
        labs/08-iceberg-introduction/iceberg_lab.py

The tables are created again from the silver Parquet files at the start, so that the script can be run twice. The
cleanup of the lab, which removes the warehouse, stays a separate command.
"""

import os
from collections import Counter

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


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
spark.sql("SHOW NAMESPACES IN lab").show()


def list_table(table):
    """Print the objects of an Iceberg table directory, with their size."""
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(f"s3a://{BUCKET}/warehouse/silver/{table}")
    fs = path.getFileSystem(spark._jsc.hadoopConfiguration())
    files = fs.listFiles(path, True)
    kinds = Counter()
    while files.hasNext():
        status = files.next()
        name = status.getPath().toString().split(f"/silver/{table}/")[1]
        print(f"{status.getLen():>8} {name}")
        kind = ("data" if name.startswith("data/") else "metadata.json" if name.endswith(".metadata.json")
                else "manifest list" if "/snap-" in name else "manifest" if name.endswith(".avro") else name)
        kinds[kind] += 1
    print(dict(kinds))


section("Create an Iceberg table")
# Created again from the silver Parquet of the lab 07, so the script starts from a table with one snapshot
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")
spark.sql("DROP TABLE IF EXISTS lab.silver.orders PURGE")
stg_orders = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
stg_orders.writeTo("lab.silver.orders").createOrReplace()
spark.sql("""
    SELECT count(*) AS orders,
           sum(quantity) AS quantity
    FROM lab.silver.orders
""").show()

section("Inspect the files")
list_table("orders")

section("Explore Iceberg metadata")
spark.sql("""
    SELECT *
    FROM lab.silver.orders.snapshots
""").show(truncate=False)
spark.sql("""
    SELECT file_path,
           file_format,
           record_count,
           file_size_in_bytes
    FROM lab.silver.orders.files
""").show(truncate=False)

section("Create a second snapshot")
stg_orders = spark.table("lab.silver.orders")
stg_orders.show(5)
new_orders = stg_orders.limit(3).withColumn("order_id", F.expr("uuid()"))
new_orders.writeTo(
    "lab.silver.orders"
).append()
spark.sql("""
    SELECT snapshot_id, operation, committed_at
    FROM lab.silver.orders.snapshots
    ORDER BY committed_at
""").show(truncate=False)
spark.sql("""
    SELECT count(*) FROM lab.silver.orders
""").show()
spark.sql("SELECT file_path, record_count, file_size_in_bytes FROM lab.silver.orders.files").show(truncate=False)

section("Time travel")
snapshots = spark.sql("""
    SELECT snapshot_id
    FROM lab.silver.orders.snapshots
    ORDER BY committed_at
""").collect()
print(snapshots)
first_snapshot = snapshots[0]["snapshot_id"]
spark.sql(f"""
    SELECT count(*)
    FROM lab.silver.orders
    VERSION AS OF {first_snapshot}
""").show()
spark.sql("""
    SELECT count(*)
    FROM lab.silver.orders
""").show()

section("Schema evolution")
spark.sql("""
    ALTER TABLE lab.silver.orders
    ADD COLUMN source STRING
""")
spark.sql("""
    DESCRIBE lab.silver.orders
""").show()
spark.sql("""
    INSERT INTO lab.silver.orders
    SELECT order_id, user_id, ordered_at, quantity, product, 'spark'
    FROM lab.silver.orders
    LIMIT 1
""")
spark.sql("SELECT source, count(*) AS orders FROM lab.silver.orders GROUP BY source").show()
original_file = spark.sql(
    "SELECT file_path FROM lab.silver.orders.files ORDER BY record_count DESC"
).first()[0]
print("columns of the original data file:", spark.read.parquet(original_file).columns)
spark.sql("SELECT count(*) AS orders, count(DISTINCT order_id) AS distinct_orders FROM lab.silver.orders").show()
spark.sql("SELECT timestamp, file FROM lab.silver.orders.metadata_log_entries ORDER BY timestamp").show(truncate=False)

section("Compare Iceberg with Parquet")
print("silver/spark/orders.parquet:", spark.read.parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet").inputFiles())
list_table("orders")

section("Exercise 1: lab.silver.users as an Iceberg table")
spark.sql("DROP TABLE IF EXISTS lab.silver.users PURGE")
spark.sql(f"""
    CREATE TABLE lab.silver.users USING iceberg
    AS SELECT * FROM parquet.`s3a://{BUCKET}/silver/spark/users.parquet`
""")
spark.sql("SELECT count(*) AS users FROM lab.silver.users").show()

section("Exercise 2: append a new batch of users")
users = spark.table("lab.silver.users")
new_users = (
    users.orderBy("username").limit(5)
    .withColumn("user_id", F.expr("uuid()"))
    .withColumn("username", F.concat("username", F.lit("_batch2")))
)
new_users.writeTo("lab.silver.users").append()
spark.sql("""
    SELECT snapshot_id, parent_id, operation, committed_at,
           summary['added-records'] AS added, summary['total-records'] AS total
    FROM lab.silver.users.snapshots
    ORDER BY committed_at
""").show(truncate=False)

section("Exercise 3: before and after the append")
before, after = spark.sql(
    "SELECT snapshot_id, committed_at FROM lab.silver.users.snapshots ORDER BY committed_at"
).collect()
spark.sql(f"SELECT count(*) AS before_append FROM lab.silver.users VERSION AS OF {before['snapshot_id']}").show()
spark.sql(f"SELECT count(*) AS after_append FROM lab.silver.users VERSION AS OF {after['snapshot_id']}").show()
spark.sql(f"SELECT count(*) AS by_timestamp FROM lab.silver.users TIMESTAMP AS OF '{before['committed_at']}'").show()
spark.sql(f"""
    SELECT username FROM lab.silver.users VERSION AS OF {after['snapshot_id']}
    EXCEPT
    SELECT username FROM lab.silver.users VERSION AS OF {before['snapshot_id']}
    ORDER BY username
""").show()

section("Exercise 4: add a country column")
spark.sql("ALTER TABLE lab.silver.users ADD COLUMN country STRING")
try:
    # users was analyzed before the ALTER TABLE: Spark 4.1 refuses to write with a stale plan
    users.orderBy("username").limit(2).withColumn("country", F.lit("US")).writeTo("lab.silver.users").append()
except Exception as e:  # noqa: BLE001
    print(type(e).__name__, str(e).split("\n")[0][:120])
users = spark.table("lab.silver.users")
new_users_v2 = (
    users.orderBy("username").limit(2)
    .withColumn("user_id", F.expr("uuid()"))
    .withColumn("username", F.concat("username", F.lit("_batch3")))
    .withColumn("country", F.lit("US"))
)
new_users_v2.writeTo("lab.silver.users").append()
spark.sql("SELECT country, count(*) AS users FROM lab.silver.users GROUP BY country ORDER BY country").show()
print("columns at the first snapshot:",
      spark.sql(f"SELECT * FROM lab.silver.users VERSION AS OF {before['snapshot_id']}").columns)

section("Exercise 5: S3 layout of the users and orders tables")
list_table("users")
list_table("orders")

spark.stop()
