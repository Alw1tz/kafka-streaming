# kafka-streaming

Streaming sandbox: Airflow orchestrating a Kafka produce/consume cycle. See [README.md](README.md) for architecture and how to run it.

## Conventions

- **DAG file format** (production-style, mirrors the user's work repo): docstring header with `DAG Name` / `Owner` / `Description`, a `# -------------------- Globals --------------------` section for constants, `with DAG(...)` context manager (not the `@dag` decorator), individual tasks as `@task`-decorated functions (TaskFlow), and a `# -------------------- Task Dependencies --------------------` section at the bottom wiring the graph. Follow `airflow/dags/kafka_streaming/lab_01_produce_consume_pipeline.py` as the reference example.
- **Shared helpers** live in `airflow/dags/utils/`: `settings.py` for connection/broker config + `get_default_args()`, `commons.py` for the reusable `create_producer_template()`.
- **Consumers must terminate**: any Kafka consumer task should use `poll(timeout=...)` and break when no message arrives, never an unbounded `while True` with no exit — Airflow tasks need to finish.
- **New labs**: number them (`lab_02_...`) under `airflow/dags/kafka_streaming/` if they're a variation on this pipeline, or a new subfolder if it's a genuinely different scenario (e.g. Avro + Schema Registry).

## Scope boundary

This repo is scoped to **Kafka + Airflow only**. If a change pulls in Snowflake, S3/Spark, or dbt, it belongs in a different sandbox under `~/Documents/dev/` — don't grow this one into a monorepo again.
