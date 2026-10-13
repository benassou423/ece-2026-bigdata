# Module 03: object storage

Labs: `03.object-storage/lab-1-uv.md` and `03.object-storage/lab-2-s3.md`. Done on the Onyxia `vscode-pyspark` service.

## Lab 1: Python project with uv

The project of this repository, `ece-2026-bigdata`, is created with `uv init --package`. Its Python package is `ece_2026_bigdata`.

- `src/ece_2026_bigdata/serialize.py`: prints a dataset as CSV, JSON or JSON lines
- `src/ece_2026_bigdata/dataset_users.py`: `dataset-users` command, random users generated with Faker
- `src/ece_2026_bigdata/dataset_orders.py`: `dataset-orders` command, orders linked to the users

The lab declares the entry points as `<git-repo-name>.dataset_users:main`. A Python module name cannot contain dashes, so they point to the package name instead, `ece_2026_bigdata.dataset_users:main`.

```bash
uv run dataset-users -c 2 -o jsonline
uv run dataset-orders -u 2 -C 1 -c 2 -o json | jq '.[0]'
```

Both outputs match the lab: Faker is seeded, so the first user is always `garzaanthony` and the `user_uuid` of the first order is the `uuid` of this user.

## Lab 2: S3 object storage

### Files

- `job-upload-bronze.yaml`: Kubernetes Job uploading `users.csv` and `orders.csv` to the bronze layer

### Results

| Step | Result |
|---|---|
| Upload, list, download | `users.csv`, 7351 bytes, downloaded file identical |
| Metadata | ETag equal to the MD5 of the file, user metadata `source`, `version`, `rows` read back |
| Presigned URLs | `GET` URL readable with `curl`, `PUT` URL generated with boto3 |
| Multipart upload | 200 MB file, ETag ending with `-25`, i.e. 25 parts |
| Versioning | version 1 restored, delete marker created by `rm`, all versions kept |
| Lifecycle | 3 rules applied and read back |
| Bucket policy | deletion of `bronze/protected.csv` denied with `AccessDenied` |
| Kubernetes Job | `bronze/orders.csv` (331,568 bytes) and `bronze/users.csv` (7,351 bytes) uploaded |

### Platform notes

- The personal bucket `user-b-assouline-ece` did not exist. It was created from the Data Storage page of the Onyxia portal, as described in the lab.
- From 2026-10-09 to 2026-10-12, every `PutObject` request on the new bucket returned `500 InternalError` from the SeaweedFS server, with the AWS CLI, `s5cmd` and `s3api`. Reads worked. The issue was fixed on the server side.
- `aws s3 ls` lists the buckets of every user of the platform, not only the personal bucket.
- The bucket policy is enforced for authenticated users on SeaweedFS: the deletion is denied.
- The `onyxia-quota` ResourceQuota of the namespace rejects any Pod without a CPU limit (`must specify limits.cpu`). The Job of the lab only defines a memory limit, so no Pod was created and `kubectl wait` timed out. `cpu: 200m` was added to the `limits` of the container.
- The temporary credentials expire after a few days: the commands then fail with `InvalidAccessKeyId`, and the script of the Onyxia "S3 Profile Detail" page must be copied again.

### Answers to the questions

**Why are the credentials stored in a Secret and not in the ConfigMap?**
A Secret is the Kubernetes object dedicated to sensitive data. Its access is controlled by separate RBAC rules, so a user allowed to read the ConfigMaps of a namespace is not necessarily allowed to read its Secrets. Its values are not displayed by `kubectl describe`, they can be encrypted at rest in etcd, and they are mounted in a memory filesystem. A ConfigMap is plain configuration, readable by anyone with access to the namespace.

**The credentials of Onyxia are temporary. What happens if the Job is executed again tomorrow? How would a production platform provide credentials to a Job?**
The Secret still contains the session token of today: once it expires, the upload fails with an authentication error such as `ExpiredToken` or `InvalidAccessKeyId`, which happened on this platform after a few days. A production platform does not copy credentials into a Secret by hand. The Job runs with a dedicated service account and exchanges its Kubernetes token for short-lived S3 credentials (workload identity, STS `AssumeRoleWithWebIdentity`), or a secret manager such as Vault, through the External Secrets Operator, renews the Secret automatically.

**How would you turn this Job into a daily ingestion?**
Wrap the same Pod template in a CronJob, for example with `schedule: "0 2 * * *"`, `concurrencyPolicy: Forbid` and history limits. Each run reads the data of the day from the source system instead of a ConfigMap, and writes it to a date-based key such as `bronze/orders/date=2026-10-12/orders.csv`, so that a run never overwrites the previous days and a failed day can be replayed.
