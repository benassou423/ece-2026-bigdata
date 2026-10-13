# Module 05: ETL/ELT and the medallion architecture

Lab: `05.etl-elt-and-medallion-architecture/lab-dbt.md`. Done on the Onyxia `vscode-pyspark` service with dbt Core
1.12.5 and the dbt-duckdb adapter 1.11.0.

## Files

The dbt project is in [`lab_medallion/`](../../lab_medallion), at the root of the repository as requested by the lab.

| File | Content |
|---|---|
| `models/bronze/sources.yml` | `bronze.users` and `bronze.orders` sources, read from the S3 CSV files |
| `models/silver/stg_users.sql`, `stg_orders.sql` | silver models, typed, cleaned and deduplicated |
| `models/silver/silver.yml` | documentation and generic tests |
| `seeds/states.csv` | states, territories and military regions, reference of the `relationships` test |
| `seeds/zip_prefixes.csv` | ZIP code prefixes per state, exercise 4 |
| `tests/generic/is_positive.sql` | custom generic test, exercise 2 |
| `tests/assert_*.sql` | singular tests |
| `profiles.yml` | connection profile, without credential |

## Results

| Step | Result |
|---|---|
| `dbt debug` | all checks passed, `lab_medallion.duckdb` database |
| `dbt run --select silver` | 2 views created in the `main_silver` schema |
| Failing `relationships` test on `state` | 6 rows: `AE` x3, `AP` x2, `AA` x1, the military regions missing from the reference data |
| After adding the military regions to the seed | 14 tests passed |
| `assert_orders_after_user_birth` | 288 orders placed before the birth of their user, turned into a warning |
| `dbt build --select states silver` | 1 seed, 2 models, 15 tests: PASS=17, WARN=1, ERROR=0 |
| Lineage of `stg_orders` | `source:lab_medallion.bronze.orders` then `lab_medallion.silver.stg_orders` |
| `dbt docs generate --static` | `target/static_index.html`, 2.5 MB |
| Parquet export of `stg_users` | `silver/dbt/users.parquet`, 7,018 bytes, 20 M and 30 F |
| Parquet export of `stg_orders` | `silver/dbt/orders.parquet`, 2,829 rows like the view, `EXCEPT ALL` empty in both directions, same types |

## Exercises

### 1. Cleaned column

`stg_users` has a `username_normalized` column, `lower(trim(username))`, documented in `silver.yml` and tested with
`unique` and `not_null`. The 50 usernames of the dataset are already in lowercase without whitespace: the column
protects the comparisons against future sources, and `unique` guarantees that two usernames differing only by case or
spaces are detected.

### 2. Order validation

| Rule | Test | Type |
|---|---|---|
| `quantity` is positive | `is_positive`, written in `tests/generic/is_positive.sql` | custom generic test, reusable on any column |
| `quantity` is between 1 and 5 | `accepted_values` | generic |
| `product` belongs to the expected list | `accepted_values` | generic |
| every `user_id` exists in `stg_users` | `relationships` | generic |
| every order has an existing user | `assert_orders_have_users` | singular, same rule written in SQL |
| no missing value | `not_null` on `ordered_at`, `quantity`, `product` | generic |

dbt Core has no built-in test for a comparison such as `quantity > 0`: a generic test written once is preferable to a
singular test, because it can be applied to any column in YAML.

### 3. Invalid birthdates

`stg_users` computes the first order of each user from `stg_orders`. When the birthdate is later than the first order,
it is replaced with `NULL` and `birthdate_is_valid` is `false`. Users without any order keep their birthdate.

| `birthdate_is_valid` | Users | With a birthdate |
|---|---|---|
| `false` | 5 | 0 |
| `true` | 45 | 45 |

The `severity='warn'` configuration was removed from `assert_orders_after_user_birth`: the test passes as an error-level
test, so a new invalid birthdate would break the build. `birthdate_is_valid` is tested with `not_null` and
`accepted_values`.

### 4. Invalid ZIP codes

The 3 first digits of a ZIP code identify a sectional center of the USPS network, located in one state. The
`zip_prefixes` seed contains the prefix ranges of each state, territory and military region, and the singular test
`assert_users_zip_code_matches_state` returns the users whose ZIP code is outside the ranges of their state.

Result: **49 users out of 50** have an inconsistent ZIP code. The only consistent one is `PA 18641`, prefix 186 in the
150 to 196 range of Pennsylvania, by chance: Faker draws the state and the ZIP code independently. The test is
configured with `severity='warn'`, since the source data is wrong and the bronze layer cannot be corrected.

- **Where should this reference data come from?** From the authority which assigns the codes, the USPS, which
  publishes the list of the 3-digit prefixes, or from a public dataset derived from it. The seed of this lab was
  written by hand from the main prefix ranges of each state: it is simplified, a few prefixes are shared between
  states, and it should be replaced with the official list in production.
- **Should it be stored as a dbt seed?** Yes for the 69 rows of prefix ranges: small, versioned with the code, rarely
  updated. A complete list of the 5-digit ZIP codes, about 40,000 rows updated regularly, should instead be ingested
  into the bronze layer and declared as a source.
- **Should the validation happen in the silver layer?** Yes. Bronze keeps the raw data untouched, and silver is the
  layer where records are validated. The test can be complemented with a `zip_code_is_valid` column, in the same way as
  `birthdate_is_valid`, so that the consumers can filter the addresses.
- **What happens when the reference data changes?** The seed is updated and loaded again with `dbt seed`, and the next
  `dbt build` evaluates the tests with the new ranges. Git keeps the history of the seed, which tells which version of
  the reference data was used at any time.

### 5. Materialization experiment

The silver models were built as tables, measured, then built as views again.

| Measure | `view` | `table` |
|---|---|---|
| `information_schema.tables` | `VIEW` | `BASE TABLE` |
| Size of `lab_medallion.duckdb` | 1,060,864 bytes | 1,323,008 bytes |
| Quantity per product on `stg_orders` | 323.7 KiB downloaded from S3, 1 `GET`, 0.33 to 0.38 s | no S3 request, 0.005 s |

- **Where is the data stored?** View: only the query is stored in the catalog of DuckDB, the data stays in the CSV
  files on S3. Table: the transformed rows are copied into the `lab_medallion.duckdb` file.
- **When is the transformation executed?** View: every time the view is queried. Table: once, during `dbt run` or
  `dbt build`.
- **What happens when the underlying CSV changes?** The view returns the new data at the next query. The table keeps
  the data of the last build until the model is built again.
- **Which approach requires more storage?** The table: the file grows by 262,144 bytes, one block of 256 KiB, for the 2,879 rows of the 2 models.
- **Which approach avoids re-reading the source files for every query?** The table. This matches the course: views
  for light silver models, tables for models which are read frequently.

## Answers to the questions

### Bronze layer

**The bronze CSV files are left untouched. What is the benefit when a bug is found in a transformation, six months
later?**
The raw data is still available, as it was extracted. The bug is fixed in the SQL of the model and the silver layer is
rebuilt from bronze with `dbt build`: the past period is reprocessed, a backfill, without extracting the data from the
source system again, which may no longer hold it. It is the advantage of ELT over ETL presented in the course: raw data
preserved, reprocessing by replaying it. It also allows comparing the old and the new results to measure the impact of
the bug.

**The source reads the whole file on every query. Which other format and layout would you choose if the orders were
ingested every hour for years?**
Parquet files partitioned by date, for example `bronze/orders/date=2026-10-13/`. A query on a time range only reads the
partitions concerned, and Parquet only reads the needed columns, compressed. Ingesting every hour produces small files:
they are compacted periodically into files of 100 MB to 1 GB, or the data is written to an open table format such as
Apache Iceberg, which tracks the files in its metadata. The silver model then becomes incremental and only processes the
new partitions.

### Silver layer

**The silver models are views. What happens when a view is queried, and when is it preferable to a table?**
A view only stores a query: when it is read, DuckDB executes the query, here a `read_csv` of the S3 object, the casts and
the deduplication. Exercise 5 measured it: every query on the `stg_orders` view downloads the CSV file, 323.7 KiB, while
the table answers without any S3 request. A view is preferable when the transformation is light, the data is small,
the model is rarely read, or the consumers need the latest data without waiting for a rebuild. It also costs no
storage.

**`UUID` values are stored in 16 bytes. How many bytes does the `VARCHAR` representation use?**
36 bytes, measured with `strlen(cast(user_id AS varchar))`: 32 hexadecimal digits and 4 dashes, one byte per ASCII
character. It is more than twice the 16 bytes of the `UUID` type, without the overhead of the string representation,
and comparisons and joins on strings are slower than on 128-bit integers.

### Data tests

**Which strategy would you choose for a dashboard displaying the age of the customers? For a dashboard displaying the
sales per region?**
For the age of the customers, the invalid birthdates must not be displayed: they are replaced with `NULL`, as done in
exercise 3, and the 5 users are excluded from the age statistics while their other attributes stay usable. Rejecting
the users would also remove their orders. For the sales per region, the birthdate is not used: the records are kept,
and the issue is only monitored with a warning. Rejecting the 5 users would remove all their orders, including the 288 placed before their
birthdate, and underestimate the sales.

**The addresses associate states with random zip codes, `KY 01352` for example. Which test would detect it, and which
reference data would it require?**
A singular test comparing the ZIP code with the state, or a `relationships` test on the pair (state, ZIP prefix). It
requires reference data mapping the ZIP codes, or their 3-digit prefixes, to the states. It is implemented in exercise
4: 49 users out of 50 fail.

## Platform notes

- The `relationships` test of the lab references `ref('states')`, but the current version of the lab never creates this
  seed: it was removed with the gold layer. The `seeds/states.csv` file was restored from the previous version of the
  lab, identical byte for byte, and the military regions were added as in that version. Without it, dbt only prints a
  warning and silently skips the test.
- The `*.csv` rule added to the `.gitignore` file during the S3 lab also excludes the seeds:
  `!lab_medallion/seeds/*.csv` was added.
- `aws s3 ls` without `--profile default` fails with `InvalidAccessKeyId`: the `AWS_*` environment variables still hold
  the expired credentials of the service creation. The commands use the `default` profile.
- The DuckDB views read the S3 objects through the `s3_onyxia_connection` secret, recreated from the `default` profile
  during the DuckDB lab.
- The cleanup of the lab removes `gold/`, which does not exist anymore. The `silver/dbt/` exports are kept: the Spark
  lab of module 07 compares its output with them.
