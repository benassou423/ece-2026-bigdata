# Module 04: SQL analytics with DuckDB

Lab: `04.sql-analytics/lab-duckdb.md`. Done on the Onyxia `vscode-pyspark` service with the DuckDB CLI v1.5.5.

## Files

- `exercises.sql`: the queries answering the 5 exercises
- `src/ece_2026_bigdata/orders_report.py`: the `orders-report` command of the Python API section
- `init.sql` and `analytics.duckdb` are generated locally and ignored by Git, as requested by the lab

## Results

| Step | Result |
|---|---|
| Tables loaded from the bronze layer | 50 users, 2829 orders, from 2020-01-01 to 2020-04-27 |
| Data quality checks | 0 orphan orders, 0 inactive users, 0 duplicated identifiers |
| Best-selling product | croissant, 1523 items (17.7 %) |
| Parquet export of `orders` | 130 KB against 332 KB for the CSV file |
| Hive partitioning, filter `product = 'cookie'` | 1 file scanned out of 6, 466 rows |
| `orders-report` | 744, 696, 744 and 645 orders from January to April 2020 |

CSV vs. Parquet at scale, with 252,416 orders generated for 5,000 users:

| Query | Data received | GET requests | Total time |
|---|---|---|---|
| CSV, quantity per product | 28.2 MiB | 1 | 1.21 s |
| Parquet, quantity per product | 203.5 KiB | 4 | 0.35 s |
| Parquet, `date >= '2100-01-01'` | 16.0 KiB | 1 | 0.25 s |

The CSV object weighs 29.6 MB and the Parquet object 11.5 MB.

## Exercises

1. Average number of orders per user and quantity per order: 2829 orders / 50 users = **56.58** orders per user, **3.05** items per order.
2. First and last order of each user: one row per user. The gaps range from **0 to 5 days**: the generator places the orders of a user one hour apart and a user has at most 100 orders, about 4 days, while `date_diff('day', ...)` counts the midnights crossed.
3. Hour of the day with the highest quantity: **6 AM**, 385 items, ahead of 3 PM (382) and midnight (379). Every hour has about 118 orders since the generator creates one order per hour, only the quantities differ.
4. Month-over-month variation per product: 24 rows, `NULL` for January which has no previous month. It ranges from **-28.3 %** (donut, February) to **+14.9 %** (bread, February). April is often lower because the data stops on April 27.
5. Users who ordered every product: **45 users out of 50**.

## Answers to the questions

### S3 configuration

**The persistent secret is stored in a file of your home directory. What are the risks, and why are they limited on Onyxia?**
The file `~/.duckdb/stored_secrets` contains the access key, the secret key and the session token in clear text. Any process running as the user can read it, and it can leak through a backup, a shared home directory or an accidental commit. On Onyxia the risk is limited: the credentials are temporary STS tokens which expire after a few days, the file lives in the home directory of the user's own pod, which other users cannot access, and it is not persistent: Onyxia recreates it when the service starts.

**The secret is visible to any process running as your user. How would you restrict the access to S3 for a Kubernetes Job?**
Give the Job its own credentials instead of the user's: a dedicated Kubernetes Secret, referenced only by the Job, with RBAC preventing other users from reading it. The credentials follow the principle of least privilege, for example read-only on `bronze/` and write on `silver/`. In production, the Job uses a dedicated service account and obtains short-lived credentials through workload identity (STS `AssumeRoleWithWebIdentity`), so that no static key is stored at all.

### Query the bronze layer

**The dataset contains 50 users and 2829 orders with unique identifiers. Why does `approx_unique` return 40 and 3167?**
`SUMMARIZE` estimates the number of distinct values with HyperLogLog, an approximate algorithm which uses a small, fixed amount of memory whatever the size of the column. The estimate has an error margin, large here in relative terms. `count(DISTINCT ...)` returns the exact values, 50 and 2829, at the cost of keeping every value in memory.

**The types are inferred from a sample of the file. Which issue may occur with a large file whose first rows are not representative?**
DuckDB detects the types on a sample, the first 20,480 rows by default. If a column only contains integers in the sample and text further down, it is typed `BIGINT` and the read fails with a conversion error in the middle of the file. The same happens with dates in another format, or with a column which is empty in the sample. The fix is to scan the whole file (`sample_size = -1`) or, better, to declare the schema explicitly with the `columns` or `types` parameters.

### Parquet export

**Why is the `uuid` column barely compressed?**
Every UUID is unique and random: there is no repeated value for the dictionary encoding, so the column is written with the `PLAIN` encoding, and no repeated sequence for Snappy. Only the hexadecimal alphabet gives a small gain, 113 KB down to 102 KB. Storing the column with the `UUID` type, 16 bytes instead of 36 characters, would reduce it much more than compression.

**Why is the compressed size of some columns larger than their uncompressed size?**
The `date`, `quantity` and `product` columns are already encoded compactly before compression: `PLAIN_DICTIONARY` for `product` and `quantity`, `PLAIN` 8-byte binary timestamps with no repetition for `date`. Snappy finds nothing more to remove and adds its own framing bytes, hence a few extra bytes (22,667 against 22,661 for `date`).

### Hive partitioning

**Why is partitioning by `uuid` a bad idea?**
It creates one directory and one tiny file per order, which hits two limitations of object storage seen in the previous module. The small files problem: every object costs a request and metadata operations, so reading the dataset turns into thousands of `GET` requests on files of a few hundred bytes, where 100 MB to 1 GB files are recommended. Listing is slow and costly: listing the prefix requires one paginated request per 1,000 keys. Only a query on a single `uuid` would benefit from the partitioning.

**Which partition column would you choose for a dataset of orders growing every day?**
The order date, at a granularity producing files large enough, ideally 100 MB to 1 GB, for example `order_date=YYYY-MM-DD`, or the month if a day contains too little data. Each day only adds a new partition without rewriting the previous ones, and most analytical queries filter on a time range, which skips the other partitions.

### CSV vs. Parquet at scale

**How many row groups does the file contain, and how many are skipped by the filter `date >= '2100-01-01'`?**
3 row groups: 123,573, 124,276 and 4,567 rows. The highest date in the statistics is 2048-10-17, so the 3 row groups are skipped: only the footer is read, 16 KiB in a single `GET`.

**The orders are sorted by date. What would happen to the efficiency of the filter if they were shuffled?**
Each row group would contain dates spread over the whole range, so the min and max statistics of every row group would overlap the filter. No row group could be skipped and the whole `date` column would be downloaded and read. Sorting, or clustering, the data on the filtered column is what makes the statistics useful.

**Compare the execution time of the CSV and Parquet queries. Which part of the difference is due to the network, which part to the parsing of the CSV file?**
The CSV query takes about 1.2 s and the Parquet query 0.35 s. The two parts of the CSV query were measured separately, 3 runs each, on the same 29.6 MB file:

| Measure | Time |
|---|---|
| CSV query on S3 (download and parsing) | 1.13 to 1.33 s |
| Same query on a local copy of the file (parsing only) | 0.22 s |
| Download of the object alone, `s5cmd cat` | 0.65 to 0.77 s |

The network is the largest part, about 0.7 s for 28.2 MiB, against about 0.2 s for the parsing, the rest being the overhead of the request. The Parquet query avoids both: it downloads 203.5 KiB, about 140 times less, and decodes binary column chunks of the 2 needed columns instead of converting every field from text. Its 0.35 s include the latency of its 4 `GET` requests.

## Platform notes

- The DuckDB secret `s3_onyxia_connection` is created from the credentials of the service creation. After a few days it returned `403 InvalidAccessKeyId`, and suspending and resuming the service did not renew it. It was recreated from the AWS profile copied from the Onyxia "S3 Profile Detail" page:
  `CREATE OR REPLACE PERSISTENT SECRET s3_onyxia_connection (TYPE s3, PROVIDER credential_chain, CHAIN 'config', PROFILE 'default', REGION 'us-east-1', ENDPOINT '<s3 endpoint>', URL_STYLE 'path');`
- `s5cmd`, installed in `~/.local/bin` during the S3 lab, is lost when the service restarts and was installed again.
- The cleanup of the lab removed `analytics/` and `large/`: the bucket only contains `bronze/orders.csv` and `bronze/users.csv`.
