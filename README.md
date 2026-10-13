# Dataset generator

This project generates a random dataset consisting of users and orders. Scripts are written in Python and the project
uses [uv](https://docs.astral.sh/uv/).

## Usage

```bash
uv run dataset-users -h
#> usage: dataset-users [-h] [-c COUNT] [-o {csv,json,jsonline}]
uv run dataset-orders -h
#> usage: dataset-orders [-h] [-C COUNT_MIN] [-c COUNT_MAX] [-d DATE_FROM] [-o {csv,json,jsonline}] [-u COUNT_USERS]
uv run orders-report -h
#> usage: orders-report [-h] [-p PRODUCT]
```

## Labs

This repository is the project of the labs of the Big Data Framework course, ECE 2026. Each lab extends it. The files
which a lab expects at a given path, such as `src/` and `pyproject.toml`, stay at that path. The other files of each
module, with the exercises and the answers to the questions, are grouped in `labs/`.

| Module | Labs | Folder |
|---|---|---|
| 03 Object storage | Python project with uv, S3 object storage | [labs/03-object-storage](labs/03-object-storage/README.md) |
| 04 SQL analytics | SQL analytics with DuckDB | [labs/04-sql-analytics](labs/04-sql-analytics/README.md) |
