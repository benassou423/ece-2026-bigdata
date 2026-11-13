# Module 09: Polaris

Labs: `09.polaris/lab-1-deployment.md` and `09.polaris/lab-2.md`. Done on the Onyxia `vscode-pyspark` service on
2026-11-13, the deployment from the terminal of VS Code and the second lab in the PySpark shell started with
`pyspark --master local[*] --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`: Spark 4.1.1,
Python 3.13.15, Iceberg 1.11.0, Polaris 1.8.0 (Helm chart 1.8.0), PostgreSQL chart 18.12.15, Helm v3.21.4. The bucket,
the Kubernetes namespace and the Polaris catalog share one name, `user-b-assouline-ece`.

## Files

| File | Content |
|---|---|
| `labs/09-polaris/README.md` | this file |
| [`exercise_2_silver_reader.sh`](exercise_2_silver_reader.sh) | the REST calls of the exercise 2: `setup` creates `silver_user`, `silver_analyst` and `silver_reader` with the 5 grants on `silver` and the two assignments, `cleanup` deletes them. Needs `curl` and `jq`, the variables `POLARIS_URI` and `POLARIS_CATALOG`, and `POLARIS_ROOT_SECRET` for the root secret of the lab |
| [`../../polaris/deployment/`](../../polaris/deployment) | the Helm chart of the deployment lab, created by the heredocs of the lab at the path it uses: `Chart.yaml`, `Chart.lock` (added by `helm dependency update`), `values.yaml`, `templates/bootstrap-job.yaml`, `templates/polaris-db-secret.yaml`. The `charts/` directory, with the two downloaded sub-charts, is ignored by git |
| [`../../warehouse/silver/{orders,users}/metadata/`](../../warehouse/silver) | the Iceberg metadata of the two tables, committed by the lab (`feat(polaris-lab): add Polaris table metadata`), without the data files |

## Lab 1: deploy Polaris

| Step | Result |
|---|---|
| Resources of the service | before: `limits cpu 4 / memory 4000Mi`. The `kubectl patch statefulset` of the lab sets requests `50m / 200Mi` and limits `2000m / 2500Mi`; the pod restarts |
| Quota of the namespace | `onyxia-quota`: `limits.cpu 4`, `limits.memory 4Gi`, `requests.cpu 1`, `requests.memory 1Gi`, `count/pods 8` |
| Files of the chart | `Chart.yaml` 10 lines, `values.yaml` 130, `templates/bootstrap-job.yaml` 445, `templates/polaris-db-secret.yaml` 10 |
| Environment | `echo "$LAB_BUCKET_NAME"` gives `user-b-assouline-ece`; `bronze/` holds `orders.csv` (331,568 bytes) and `users.csv` (7,351 bytes) |
| ConfigMap | `polaris-config`: `POLARIS_BASE_LOCATION=s3://user-b-assouline-ece/warehouse/`, `POLARIS_CATALOG=user-b-assouline-ece`, `POLARIS_CLIENT_ID=root`, `POLARIS_MGMT_URI=http://polaris-mgmt:8182`, `POLARIS_REALM=POLARIS`, `POLARIS_URI=http://polaris:8181`, `S3_ENDPOINT=https://s3.seaweedfs.adm.adaltas.cloud/` |
| Secrets | `polaris-credential` (1 key), `polaris-s3` (3 keys) |
| `helm upgrade --install polaris polaris/deployment --wait` | failed several times because of a cluster incident, see below; deployed at the fifth run with `--timeout 15m`: `STATUS: deployed`, `REVISION: 6`, after 8m35s |
| Final state | `pod/polaris-69c7966454-4lclj 1/1 Running`, `pod/polaris-postgresql-0 1/1 Running`, `job.batch/polaris-bootstrap Complete 1/1` |
| `curl -s http://polaris-mgmt:8182/q/health` | `"status": "UP"`, with `Database connections health check` `UP` |
| Credentials | `/home/onyxia/.polaris-credentials.env`, 2 lines: `POLARIS_DEV_CREDENTIAL` and `POLARIS_ANALYST_CREDENTIAL`, 49 characters each, `clientId:clientSecret` |

### Incident during the deployment

The 4 commands of the lab were run as written. The cluster had an incident, visible in the Events panel of Onyxia and in
the terminal. All times are UTC.

| Time | What happened |
|---|---|
| 12:36 | `helm upgrade --install polaris polaris/deployment --wait`: `Release "polaris" does not exist. Installing it now.` |
| 12:36-12:44 | `polaris-...` and `polaris-postgresql-0` stay `Pending`: `0/9 nodes are available: 3 Insufficient cpu, 6 node(s) had untolerated taint(s)` |
| 12:41 | `Error: context deadline exceeded` after 5m02s, the default timeout of `--wait`. Release `failed`, pods still `Pending` |
| 12:44 | both pods scheduled on `compute-02`, images pulled, containers started |
| 12:45-12:48 | node events: `no space left on device`, `The node was low on resource: ephemeral-storage`. The Polaris pod is evicted and replaced |
| 12:45 | second run: `UPGRADE FAILED: services "polaris-mgmt" is forbidden: ... cannot get resource "services"`. Release left in `pending-upgrade` |
| 12:27-12:59 | `kubectl` alternates between normal answers, `TLS handshake timeout` and `You must be logged in to the server` |
| 12:58 | `curl http://polaris-mgmt:8182/q/health` answers `"status": "UP"` |
| 13:00 | `helm rollback polaris 1 --wait`: `Rollback was a success!`, revision 3 `deployed`. The evicted pod, in `Init:ContainerStatusUnknown`, is deleted |
| 13:01 | third run: the hook Job is created but its pod starts about 7 minutes later, `post-upgrade hooks failed: ... timed out waiting for the condition` after 5m02s |
| 13:08 | the pod of the Job runs and completes, everything is created in Polaris |
| 13:12 | fourth run, same timeout on the hook; the Job completes after 6m04s |
| 13:33 | fifth run with `--timeout 15m`: `Release "polaris" has been upgraded. Happy Helming!`, revision 6 |

`helm history polaris`: 1 `failed`, 2 `pending-upgrade`, 3 `deployed` (rollback to 1), 4 and 5 `failed` (post-upgrade
hooks failed), 6 `deployed`. The explanation and the three corrections are in the platform notes.

### Bootstrap Job

The log of the first completed run has 62 lines. It creates, in this order, the principals `dev` and `analyst`, the
principal roles `data_engineer` and `data_analyst`, the catalog `user-b-assouline-ece`, the catalog roles
`catalog_contributor` and `catalog_reader`, then the grants, the assignments, and ends with the summary:

```
Polaris RBAC bootstrap completed.
Catalog: user-b-assouline-ece
dev      -> data_engineer -> catalog_contributor
analyst  -> data_analyst -> catalog_reader
```

The grants are `CATALOG_MANAGE_ACCESS`, `CATALOG_MANAGE_METADATA`, `CATALOG_MANAGE_CONTENT` to `catalog_contributor`,
and `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES`, `CATALOG_READ_PROPERTIES` to `catalog_reader`. Two lines of the log
contain `"credentials"`, one per principal, printed only when the principal is created.

### Credentials of the principals

- The credentials of `dev` and `analyst` exist only in the log of the Job pod which created the principals. The log of
  the first run was saved to a file before running Helm again. The pipeline of the lab
  (`grep '"credentials"' | jq -r ... > /home/onyxia/.polaris-credentials.env`) was applied to that file instead of
  `kubectl logs job/polaris-bootstrap`.
- The file was checked without printing the secrets: 2 lines, 49 characters per credential.
- The home directory is reset when the pod restarts. A copy of the file is kept in `/home/onyxia/work/.cache/`, on the
  persistent volume and outside the git repository. The credentials are not committed.

### Advanced checks

With the token of the `root` principal:

| Call | Result |
|---|---|
| `GET $API/catalogs` | one catalog, `"type":"INTERNAL"`, `"name":"user-b-assouline-ece"`, `"default-base-location":"s3://user-b-assouline-ece/warehouse/"`; `storageConfigInfo`: `"storageType":"S3"`, `"allowedLocations":["s3://user-b-assouline-ece/warehouse/"]`, `"endpoint":"https://s3.seaweedfs.adm.adaltas.cloud"`, `"pathStyleAccess":true`, `"region":"us-east-1"`, `"stsUnavailable":true` |
| `GET $API/principals` | `analyst`, `dev`, `root` |
| `GET $API/principal-roles` | `service_admin`, `data_engineer`, `data_analyst` |
| `GET $API/catalogs/$POLARIS_CATALOG/catalog-roles` | `catalog_contributor`, `catalog_admin`, `catalog_reader` |
| `GET .../catalog-roles/catalog_contributor/grants` | `CATALOG_MANAGE_ACCESS`, `CATALOG_MANAGE_METADATA`, `CATALOG_MANAGE_CONTENT` |
| `GET .../catalog-roles/catalog_reader/grants` | `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES`, `CATALOG_READ_PROPERTIES` |
| `GET $API/principals/dev/principal-roles`, then `analyst` | `data_engineer`, then `data_analyst` |

The expected output of the lab lists a principal role `data_scientist`; the Job creates `data_analyst`, which is the
role used by the RBAC table of the second lab. The reset of credentials and the teardown were not run: Polaris is kept
for the Trino module.

## Lab 2: govern Iceberg tables with Polaris

| Section | Result |
|---|---|
| Prerequisite | `lab.silver.users` and `lab.silver.orders` written again in the Hadoop catalog with the statements of the lab 07 (see the platform notes): 50 users, 2,829 orders, snapshot `3618245128506716403`, `overwrite`, `2026-11-13 12:44:40.299` |
| Environment | `GIT_REPO_NAME=ece-2026-bigdata`, `LAB_BUCKET_NAME=user-b-assouline-ece`, `AWS_S3_ENDPOINT=s3.seaweedfs.adm.adaltas.cloud` |
| SparkSession | session cell prints `Polaris principal: dev`; `spark.hadoop.fs.s3.impl` is `org.apache.hadoop.fs.s3a.S3AFileSystem` (set by `--conf`), so the session was not recreated. `SHOW NAMESPACES IN lab` gives `silver`; `SHOW NAMESPACES IN polaris` gives an empty result |
| Register | `version-hint.text` is `1`, one `v1.metadata.json` of 1,933 bytes. `register_table` returns `current_snapshot_id 3618245128506716403`, `total_records_count 2829`, `total_data_files_count 1`. `count(*)` gives **2829**. `warehouse/silver/orders/` holds 5 objects before and after |
| Create tables as `dev` | `SHOW TABLES IN polaris.silver` gives `orders`; one snapshot, `3618245128506716403`, `overwrite`. `polaris.silver.users` created with `createOrReplace()`: 50 users, snapshot `2527518887479098568`, `overwrite`, `2026-11-13 13:19:28.759`. The two tables share `warehouse/silver/users/` with the Hadoop table |
| Schema change | `ALTER TABLE polaris.silver.users ADD COLUMN country STRING`: `DESCRIBE` shows 11 columns. `metadata_log_entries` has 2 files, `00000-e1caee5a-...` and `00001-2a91fc01-9411-404a-bd64-4b2bb294cbcc.metadata.json` (3,090 bytes, 13:20:02); still the single snapshot `2527518887479098568` |
| Read as the analyst | `Polaris principal: analyst`. `SHOW TABLES` gives `users`, `orders`. The aggregation works: donut 462, cookie 466, croissant 491, bread 458, brioche 468, drink 484 (2,829 in all). The `INSERT` fails with `ForbiddenException ... not authorized for op ADD_TABLE_SNAPSHOT` and the `ALTER TABLE` with `... not authorized for op ADD_TABLE_SCHEMA` |
| After the refusals, as `dev` | `polaris.silver.orders` still has one snapshot, `3618245128506716403`, 2,829 rows, and only `v1.metadata.json` in its metadata log; `polaris.silver.users` has no `phone` column; `warehouse/silver/orders/` still holds its 5 objects |
| Commit | `aws s3 sync` downloads the metadata; commit `feat(polaris-lab): add Polaris table metadata`, `10 files changed, 4 insertions(+), 2 deletions(-)` |

The exact error of the `INSERT`, as recorded:

```
Py4JJavaError: An error occurred while calling o49.sql.
: org.apache.iceberg.exceptions.ForbiddenException: Forbidden: Principal 'analyst' with activated PrincipalRoles
'[data_analyst]' and activated grants via '[data_analyst, catalog_reader]' is not authorized for op ADD_TABLE_SNAPSHOT
```

The stack trace goes through `ErrorHandlers$CommitErrorHandler` and `RESTTableOperations.commit`: the refusal comes from
the commit request sent to Polaris.

### Layout in the bucket

`warehouse/silver/` along the lab, after the `ALTER TABLE`: 15 objects, 5 for `orders` and 10 for `users`:

| Table | Written by | Objects |
|---|---|---|
| `orders` (registered) | Hadoop catalog `lab`, 12:44 | `data/00000-9-8227e36f-...-00001.parquet` (84,055 B), manifest `7284ad80-...-m0.avro` (7,346 B), manifest list `snap-3618245128506716403-...avro` (4,457 B), `v1.metadata.json` (1,933 B), `version-hint.text` (1 B) |
| `users` (Hadoop) | Hadoop catalog `lab`, 12:44 | `data/00000-7-ff7d61b5-...parquet` (7,435 B), manifest `62d03ed7-...-m0.avro` (7,742 B), manifest list `snap-429491410339859286-...avro` (4,455 B), `v1.metadata.json` (2,197 B), `version-hint.text` (1 B) |
| `users` (Polaris) | Polaris, 13:19 | `data/00000-3-0f27fdeb-...parquet` (7,435 B), manifest `07fe3a55-...-m0.avro` (7,738 B), manifest list `snap-2527518887479098568-...avro` (4,457 B), `00000-e1caee5a-....metadata.json` (2,246 B), then `00001-2a91fc01-...metadata.json` after the `ALTER TABLE` |

The Hadoop table `lab.silver.users` and the Polaris table `polaris.silver.users` are two different tables which share
one directory. The exercise 1 adds a 16th object, `00002-7bf2a015-....metadata.json`.

### Answers to the questions

#### SparkSession with the Polaris catalog

**Which component answered each of the two `SHOW NAMESPACES` commands? How can you tell?**
The first was answered by Spark itself, through the Iceberg `hadoop` catalog `lab`: the catalog lists the prefixes of
`s3a://user-b-assouline-ece/warehouse` through the S3A connector and returned `silver`. The second was answered by the
Polaris server, which received a request on the Iceberg REST API (`spark.sql.catalog.polaris.type` is `rest`, URI
`http://polaris:8181/api/catalog`) and returned an empty result. The difference proves it: the directory `silver`
exists in the bucket, but no namespace was created in Polaris yet, so a catalog service which keeps its own entities
in its database answers with nothing.

**Where is the credential used: in the Spark session, in the bucket, or in a request to Polaris?**
In a request to Polaris. The credential is stored in the configuration of the `polaris` catalog of the session
(`spark.sql.catalog.polaris.credential`), but the REST client sends it to the OAuth endpoint of Polaris with the
`client_credentials` grant and the scope `PRINCIPAL_ROLE:ALL`, receives a bearer token, and sends that token with
every catalog request. It is never used against the bucket: the data and metadata files are read and written with the
S3A keys of the student (`AWS_ACCESS_KEY_ID` and the other variables), through `HadoopFileIO`, because the catalog is
created with `stsUnavailable: true` and Polaris vends no credential.

#### Register the Silver table in Polaris

**What did `register_table` write, in Polaris and in the bucket? Compare the number of objects under
`warehouse/silver/orders/` before and after.**
In Polaris, it recorded the table `silver.orders` in the namespace `silver`, with a reference to the metadata file
`s3a://user-b-assouline-ece/warehouse/silver/orders/metadata/v1.metadata.json`: `metadata_log_entries` returns that one
file, written by the Hadoop catalog. In the bucket, it wrote nothing: 5 objects before (1 data file, 1 manifest, 1
manifest list, `v1.metadata.json`, `version-hint.text`) and the same 5 after, with the same sizes and dates (12:44:40),
and still 5 at the end of the lab. The call returned the current snapshot `3618245128506716403`, 2,829 records and 1
data file: Polaris read the metadata, it did not copy the data.

**Why is it dangerous to keep writing to this table through the `lab` catalog once it is registered in `polaris`?**
Because the two catalogs keep their own pointer to the current metadata, and neither knows about the other. A commit
through `lab` writes a new metadata file and moves `version-hint.text`, while Polaris still points to
`v1.metadata.json`: the readers of Polaris keep seeing the old table, and the next commit through Polaris starts from
the old version, so the history forks and one of the two branches is lost. The Hadoop catalog has no server to serialize the commits (it
relies on an atomic rename, which S3 does not offer), and a write through `lab` also goes around the access control
of Polaris. A single catalog must own the table.

#### Create tables as `dev`

**Where did Polaris store the files for `silver.users`, and why?**
In `s3://user-b-assouline-ece/warehouse/silver/users/`, with `data/` and `metadata/`. The client did not choose that
path: Polaris builds the location of a new table from the `default-base-location` of the catalog,
`s3://user-b-assouline-ece/warehouse/`, followed by the namespace and the table name, and checks that it is inside the
allowed locations of the catalog. It happens to be the same prefix as the warehouse of the Hadoop catalog, so the
directory already held the files of `lab.silver.users` written at 12:44. The two tables coexist there with different
file names, for example `v1.metadata.json` for the Hadoop table and `00000-e1caee5a-....metadata.json` for the table
of Polaris.

**Both tables are in your bucket. What is the difference between "registered" and "created through Polaris" for the
data files?**
For the data files, none is moved or copied in either case: they stay in the bucket, written by Spark with the S3 keys
of the student. The difference is in who decided where they are and who holds the pointer. A registered table keeps the
files and the location chosen by the Hadoop catalog (`s3a://` URI, `v1.metadata.json`, `version-hint.text` still
present), and Polaris only references its current metadata file. A table created through Polaris gets its location
from the catalog, `s3://` URIs, metadata files named `<5-digit version>-<uuid>.metadata.json`, and no
`version-hint.text`: the pointer to the current version is in the PostgreSQL database of Polaris, and each commit
goes through the server.

#### Schema change as `dev`

**Which metadata file records the change? Is it the same file that `register_table` pointed to?**
`warehouse/silver/users/metadata/00001-2a91fc01-9411-404a-bd64-4b2bb294cbcc.metadata.json` (3,090 bytes, written at
13:20:02) records the new schema, with `country`. It is not the file that `register_table` pointed to: that one is
`v1.metadata.json` of `silver/orders`, which is untouched. The `ALTER TABLE` did not create a snapshot (the table still
has `2527518887479098568` only) and did not touch the data file: Iceberg wrote a new metadata file next to
`00000-e1caee5a-...` and Polaris moved its pointer to it.

**Which privilege does Polaris check for this statement?**
The statement is a commit of a metadata update to the table, which Polaris authorizes as an operation on the table:
the error the `analyst` receives names it `ADD_TABLE_SCHEMA`. For `dev` the check passes through the catalog role
`catalog_contributor`, which holds `CATALOG_MANAGE_CONTENT`, the privilege which covers the management of all the
content of the catalog and therefore the write privileges on tables such as `TABLE_WRITE_PROPERTIES` and
`TABLE_WRITE_DATA`.
The log does not print which of the table privileges is looked up; only the super-privilege held by `dev` and the
operation refused to the `analyst` are observed.

#### Read as the analyst

**Which privilege is missing for the `INSERT`? Which privileges does `catalog_reader` provide?**
The `INSERT` is refused at the commit, for the operation `ADD_TABLE_SNAPSHOT`: the missing privilege is a table write
privilege (the data write privilege `TABLE_WRITE_DATA` of the lecture, which `CATALOG_MANAGE_CONTENT` includes for
`catalog_contributor`). The error says which grants were active: `[data_analyst, catalog_reader]`. `catalog_reader`
provides only `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES` and `CATALOG_READ_PROPERTIES`: it allows loading the tables
and their metadata and reading the data, which is why the aggregation by product worked, but nothing that adds a
snapshot or changes a schema.

**Check the snapshots of `silver.orders` as `dev`, after restarting the kernel with `POLARIS_PRINCIPAL = "dev"`. Did
the failed `INSERT` create one?**
No. `polaris.silver.orders.snapshots` still has the single snapshot `3618245128506716403`, the count is still 2,829 and
`metadata_log_entries` still lists only `v1.metadata.json`. Spark had written a data file for the `INSERT`, but Polaris
refused the commit before updating its pointer, so no snapshot exists, and the data file was removed when the commit
failed: `warehouse/silver/orders/` still has its 5 objects. A write becomes visible only when the catalog accepts the
commit, which is the reason a refused writer leaves the table unchanged.

## Exercises

### 1. Remove the `country` column

```python
spark.sql("ALTER TABLE polaris.silver.users DROP COLUMN country")
spark.sql("DESCRIBE polaris.silver.users").show(20)
spark.sql("SELECT snapshot_id, parent_id, operation, committed_at FROM polaris.silver.users.snapshots").show(truncate=False)
spark.sql("SELECT timestamp, file, latest_snapshot_id, latest_schema_id FROM polaris.silver.users.metadata_log_entries").show(truncate=False)
spark.sql("SELECT file_path, record_count, file_size_in_bytes FROM polaris.silver.users.files").show(truncate=False)
spark.sql("SELECT count(*) AS users FROM polaris.silver.users").show()
```

| Check | Result |
|---|---|
| `DESCRIBE` | 10 columns, `country` is gone |
| `snapshots` | still one row, `2527518887479098568`, no parent, `overwrite`, `2026-11-13 13:19:28.759` |
| `metadata_log_entries` | 3 rows, all with `latest_snapshot_id 2527518887479098568`: `00000-e1caee5a-...` at 13:19:32.728, `00001-2a91fc01-...` at 13:20:02.498, `00002-7bf2a015-9301-4a98-b51b-ec152eca17c8.metadata.json` at 13:28:05.738 (3,254 bytes in the bucket) |
| `files` | the same single data file, `data/00000-3-0f27fdeb-6ddb-475f-90e8-40b49cbbe11a-0-00001.parquet`, 50 records, 7,435 bytes, dated 13:19:28 in the bucket |
| `count(*)` | 50 |

Dropping a column is a metadata operation because the schema of an Iceberg table is a property of the table, recorded in
the metadata file, not of the data files. The statement wrote only `00002-7bf2a015-....metadata.json`, in which the
current schema no longer lists `country`. No snapshot was created (a snapshot is a change of the set of data files),
the data file was not rewritten (same name, same 7,435 bytes, same time of 13:19:28), and the 50 rows are unchanged.
This file was in any case written before the column was added at 13:20, so it never contained `country`. Iceberg
identifies columns by id and not by name or position, which makes the drop safe: the id of `country` is not reused
by a later column of the same name. The previous metadata files stay in the log, and a query on an earlier version
still reads the schema of that version.

### 2. A restricted catalog role `silver_reader`

Names chosen: principal `silver_user`, principal role `silver_analyst`, catalog role `silver_reader`. The calls of
[`exercise_2_silver_reader.sh`](exercise_2_silver_reader.sh) (`setup`) are the ones run, with the root token of the
Advanced section, `C=$API/catalogs/$POLARIS_CATALOG` and the headers `Authorization: Bearer $TOKEN` and
`Content-Type: application/json`:

| Call | Body | Status |
|---|---|---|
| `POST $API/principals` | `{"principal":{"name":"silver_user","properties":{}},"credentialRotationRequired":false}` | 201 |
| `POST $API/principal-roles` | `{"principalRole":{"name":"silver_analyst","properties":{}}}` | 201 |
| `PUT $API/principals/silver_user/principal-roles` | `{"principalRole":{"name":"silver_analyst"}}` | 201 |
| `POST $C/catalog-roles` | `{"catalogRole":{"name":"silver_reader","properties":{}}}` | 201 |
| `PUT $C/catalog-roles/silver_reader/grants`, 5 times | `{"grant":{"type":"namespace","namespace":["silver"],"privilege":"<P>"}}` for `NAMESPACE_LIST`, `NAMESPACE_READ_PROPERTIES`, `TABLE_LIST`, `TABLE_READ_PROPERTIES`, `TABLE_READ_DATA` | 201 each |
| `PUT $API/principal-roles/silver_analyst/catalog-roles/$POLARIS_CATALOG` | `{"catalogRole":{"name":"silver_reader"}}` | 201 |

The credential returned by the first call was appended to `/home/onyxia/.polaris-credentials.env` as
`POLARIS_SILVER_USER_CREDENTIAL`, without being printed, so that the session cell works with
`POLARIS_PRINCIPAL = "silver_user"`. Checks: `GET $C/catalog-roles/silver_reader/grants` returns the 5 grants, all
`{"namespace":["silver"],"privilege":"...","type":"namespace"}`; `GET $API/principals/silver_user/principal-roles`
returns `silver_analyst`; `GET $API/principal-roles/silver_analyst/catalog-roles/$POLARIS_CATALOG` returns
`silver_reader`.

As `dev`, the `gold` namespace and a table:

```python
spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.gold")
spark.sql("CREATE TABLE polaris.gold.orders_by_product AS SELECT product, count(*) AS orders, sum(quantity) AS units FROM polaris.silver.orders GROUP BY product")
spark.sql("SELECT * FROM polaris.gold.orders_by_product ORDER BY units DESC").show()
```

The table has 6 rows: croissant 491 orders / 1,523 units, drink 484 / 1,493, brioche 468 / 1,424, bread 458 / 1,420,
donut 462 / 1,390, cookie 466 / 1,365. `SHOW NAMESPACES IN polaris` gives `silver` and `gold`.

As `silver_user` (shell restarted, session cell prints `Polaris principal: silver_user`):

| Statement | Result |
|---|---|
| `SHOW TABLES IN polaris.silver` | `users`, `orders` |
| `SELECT count(*) AS orders FROM polaris.silver.orders` | 2829 |
| `SELECT count(*) AS users FROM polaris.silver.users` | 50 |
| `SHOW NAMESPACES IN polaris` | `ForbiddenException: Forbidden: Principal 'silver_user' with activated PrincipalRoles '[silver_analyst]' and activated grants via '[silver_analyst, silver_reader]' is not authorized for op LIST_NAMESPACES` |
| `SHOW TABLES IN polaris.gold` | same message, `... is not authorized for op LIST_TABLES` |
| `SELECT * FROM polaris.gold.orders_by_product` | same message, `... is not authorized for op LOAD_TABLE` |
| `INSERT INTO polaris.silver.orders SELECT * FROM polaris.silver.orders LIMIT 1` | same message, `... is not authorized for op ADD_TABLE_SNAPSHOT` |

A principal assigned `silver_reader` therefore reads `silver` and not `gold`. The grants are on the namespace `silver`
only, and Polaris applies them to the tables below it: listing and loading the tables of `silver` is allowed, while
listing the namespaces of the catalog, listing the tables of `gold` and loading `gold.orders_by_product` are refused,
each with the name of the operation. The role is also read-only: the `INSERT` is refused as it was for `analyst`. It is
more restrictive than `catalog_reader`, which grants `TABLE_READ_DATA` on the whole catalog, `gold` included.

#### What Polaris cannot enforce on this storage

In the same `silver_user` session:

```python
df = spark.read.parquet(f"s3a://{BUCKET}/warehouse/gold/orders_by_product/data/")
print("rows read directly from the bucket, without Polaris:", df.count())
df.orderBy("units", ascending=False).show(3)
```

It prints `rows read directly from the bucket, without Polaris: 6`, then croissant 491 / 1523, drink 484 / 1493,
brioche 468 / 1424: the gold table, which Polaris refused to load, is read in full. The reason is the storage
configuration. The catalog is created with `stsUnavailable: true` and the session uses
`io-impl=org.apache.iceberg.hadoop.HadoopFileIO` with the S3 keys of the student: there is no credential vending, which
the lecture describes as the mechanism by which Polaris limits access to the location of one table. Polaris vends no
credential, so every principal reads and writes the files with the same S3 keys, whatever its Polaris role. The roles
control the access through the catalog only: they stop a well-behaved engine, not somebody who holds the keys of the
bucket. To enforce the access for real, the object store needs an STS service so that the engines hold no static keys,
or the bucket policies must restrict the keys.

#### Cleanup of the exercise

- As `dev`: `DROP TABLE IF EXISTS polaris.gold.orders_by_product`. The namespace must be empty for the cleanup of the
  lab to drop it.
- `./exercise_2_silver_reader.sh cleanup` runs, all `204`: `DELETE` of the catalog role from the principal role,
  `DELETE $C/catalog-roles/silver_reader`, `DELETE` of `silver_analyst` from `silver_user`, `DELETE` of the principal
  `silver_user`, and `DELETE` of the principal role `silver_analyst`. The three lists come back to the state of the
  bootstrap: principals `root`, `dev`, `analyst`; principal roles `service_admin`, `data_engineer`, `data_analyst`;
  catalog roles `catalog_admin`, `catalog_contributor`, `catalog_reader`. The line `POLARIS_SILVER_USER_CREDENTIAL` is
  removed from the credentials file.
- The `DROP TABLE` without `PURGE` left the 4 files of the gold table in the bucket under
  `warehouse/gold/orders_by_product/`: a data file of 1,047 bytes, `00000-76a50f89-....metadata.json` of 1,852 bytes, a
  manifest of 7,140 bytes and a manifest list of 4,464 bytes. They were removed with
  `aws s3 rm "s3://$LAB_BUCKET_NAME/warehouse/gold/" --recursive`, so that the gold layer of the Trino lab starts empty.

## Cleanup

Run as `dev`, then the Spark session is stopped:

```python
spark.sql("DROP TABLE IF EXISTS polaris.silver.users")
spark.sql("DROP NAMESPACE IF EXISTS polaris.gold")
spark.stop()
exit()
```

State after the cleanup: `SHOW NAMESPACES IN polaris` gives `silver`, `SHOW TABLES IN polaris.silver` gives
`orders`, and the count of `polaris.silver.orders` is 2829. `silver.orders` was not unregistered, as the lab asks.

A `DROP TABLE` without `PURGE` removes the entry of the catalog, not the files. `warehouse/silver/users/` therefore
still holds its objects, including the three `0000N-...metadata.json` files and the data file written through Polaris;
the Hadoop table `lab.silver.users` is untouched. The Polaris deployment itself is kept, as the lab says.

Consequence for the next module: the Trino lab reads `lakehouse.silver.users`, which this cleanup removes from Polaris.
The table has to be registered again at the start of the Trino lab, see the README of the module 10.

## Platform notes

- **Resources and quota.** The service had `limits cpu 4 / memory 4000Mi` since the lab 07, and the namespace quota
  `onyxia-quota` allows `limits.cpu 4`, `limits.memory 4Gi` and 8 pods. With that pod nothing else could start, hence
  the `kubectl patch statefulset` of the lab (requests `50m / 200Mi`, limits `2000m / 2500Mi`), which frees room for the
  Polaris and PostgreSQL pods.
- **Credentials.** The patch restarts the pod: `/home/onyxia/work` is intact, but the home directory is reset (`~/.aws`
  is gone) and the `AWS_*` variables of the container had expired (`InvalidAccessKeyId`). The S3 profile script of
  Onyxia was pasted again, then `eval "$(aws configure export-credentials --profile default --format env)"`. The
  Polaris credentials file is kept outside the repository (a copy in `/home/onyxia/work/.cache/`) and never committed.
  The root secret of the lab is the demo value of the deployment lab.
- **Cluster incident and corrections.** During the first runs of `helm upgrade --install`, the cluster ran out of CPU
  for the new pods, then a node ran out of ephemeral storage (the Polaris pod was evicted), and the API server answered
  intermittently with timeouts and authentication errors. The lab was not at fault. Three corrections were needed: 1)
  wait for the pods to be scheduled and run the same `helm upgrade --install` again; 2) `helm rollback polaris 1 --wait`
  to leave the `pending-upgrade` state, in which Helm refuses any new upgrade; 3) `--timeout 15m` on the last run,
  because the pod of the hook Job `polaris-bootstrap` started several minutes late and the 5 minutes of the default
  timeout were not enough. The final state is the one the lab describes.
- **Log of the bootstrap Job.** Each `helm upgrade` deletes and recreates the hook Job
  (`helm.sh/hook-delete-policy: before-hook-creation`). The credentials of the principals are printed only when they are
  created, so the log of a later run says `Principal 'dev' already exists.` and does not contain them. The log of the
  first completed Job pod was saved before the next run, and the pipeline of the credentials was applied to that file.
- **`getOrCreate()` in the shell.** As in the labs 07 and 08, the session cell returns the session created by
  `pyspark`: the runtime settings (both catalogs) apply, the static ones do not. `spark.sql.extensions` is `None`
  (`spark.conf.get("spark.sql.extensions", None)`). The `CALL polaris.system.register_table` of the lab works anyway
  with Spark 4.1 and Iceberg 1.11. `PYTHON_BASIC_REPL=1` was exported before starting the shell, as in the previous
  labs.
- **`aws s3 sync` and `--exact-timestamps`.** The sync downloaded 11 files and skipped the two `v1.metadata.json`: the
  repository already had the files committed after the lab 07 with the same sizes (1,933 and 2,197 bytes) but another
  content, since the tables were written again, and `sync` compares sizes and dates. It was run again with
  `--exact-timestamps`, which downloaded `v1.metadata.json` and `version-hint.text` of both tables. `git status` showed
  the two `v1.metadata.json` modified and 8 new metadata files (for `orders` the manifest and the manifest list, for
  `users` the two `0000N-...metadata.json`, the two manifests and the two manifest lists); the `data/` directories are
  ignored by `.gitignore` (`warehouse/**/data/`). The commit has 10 files.
- **2,829 orders instead of 5,000.** The lab announces 5,000 orders. The dataset of this project has 2,829 distinct
  orders, see the lab 07; every count of this module is 2,829.
- **Prerequisite tables written again.** The cleanup of the lab 08 removed `warehouse/` from the bucket. Before the lab,
  `lab.silver.users` and `lab.silver.orders` were written again in the Hadoop catalog with the statements of the lab 07
  (`CREATE NAMESPACE IF NOT EXISTS lab.silver`, then `writeTo(...).createOrReplace()` from
  `silver/spark/users.parquet` and `silver/spark/orders.parquet`): one `overwrite` snapshot each, at 12:44:40.
- **Push.** `git push` asks for the GitHub sign-in of VS Code; it was done at the end of the session, with the commits
  of the module 10.
