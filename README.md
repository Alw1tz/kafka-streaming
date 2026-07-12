# Kafka + Airflow Streaming Pipeline

A local, containerized event-streaming sandbox: **Airflow orchestrates a produce → consume cycle against a real Kafka cluster.**

## Why this project

This is the Streaming counterpart to [spark-aws-pipeline](../spark-aws-pipeline) and [dbt-snowflake-analytics](../dbt-snowflake-analytics): event-driven ingestion instead of batch/warehouse transforms. It shows the core skills a Streaming/Data Engineer role expects — a real Kafka broker (not mocked), a producer publishing JSON events, a consumer reading them back with proper offset/group handling, and Airflow as the orchestration layer around both.

## Architecture

```
Airflow DAG: lab_01_produce_consume_pipeline
   │
   ├─ produce_events ──────▶ Kafka topic "clickstream-events"
   │                              │
   └─ consume_and_aggregate ◀─────┘
        (poll with timeout, groups events by type, logs summary)
```

`consume_and_aggregate` uses `poll(timeout=5.0)` and exits once the topic is drained — a batch-friendly consumption pattern that fits an Airflow task (as opposed to a long-running streaming consumer service).

## Stack

- **Airflow 3.2.2** (SequentialExecutor + SQLite)
- **Apache Kafka** (Confluent Platform 7.5, via Zookeeper — no KRaft yet)
- **Kafka UI** for browsing topics/messages visually

## Running it

```bash
cp .env.example .env
docker compose up airflow-init
docker compose up -d
```

- Airflow UI → http://localhost:8080 (airflow / airflow)
- Kafka UI → http://localhost:8082

Trigger the pipeline:

```bash
docker compose exec airflow-apiserver airflow dags trigger lab_01_produce_consume_pipeline
```

Check the `consume_and_aggregate` task log for the event-count-by-type summary, or browse the `clickstream-events` topic directly in Kafka UI.

## Project layout

```
airflow/dags/kafka_streaming/   # the DAG (Airflow -> Kafka orchestration)
airflow/dags/utils/             # shared settings + Kafka producer helper
```

## Possible extensions

Not built yet, but natural next steps: Avro serialization + Schema Registry (already easy to add — swap the plaintext JSON producer/consumer for `AvroProducer`/`AvroConsumer` against a `schema-registry` service), or a Kafka Connect sink pushing straight into S3/Snowflake.
