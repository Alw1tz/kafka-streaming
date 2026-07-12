"""
Kafka produce -> consume streaming pipeline.

--------
* DAG Name:
    lab_01_produce_consume_pipeline
* Owner:
    alw1tz
* Description:
    Produces a batch of JSON clickstream events to a Kafka topic, then
    consumes them back (poll-with-timeout, so the task terminates once the
    topic is drained) and aggregates event counts by type. Demonstrates
    Airflow orchestrating a produce/consume cycle against a real Kafka
    cluster — the core "Streaming: Kafka + Airflow" pattern.
"""
from __future__ import annotations

import json

import pendulum
from airflow import DAG
from airflow.decorators import task

from utils.settings import get_default_args, KAFKA_BROKER

# -------------------- Globals --------------------
TOPIC = 'clickstream-events'
CONSUMER_GROUP = 'clickstream-consumer-group'

EVENTS = [
    {'id': 1, 'event': 'PAGE_VIEW', 'user': 'alice', 'page': '/home'},
    {'id': 2, 'event': 'CLICK', 'user': 'alice', 'page': '/checkout'},
    {'id': 3, 'event': 'PURCHASE', 'user': 'alice', 'amount': 99.99},
    {'id': 4, 'event': 'PAGE_VIEW', 'user': 'bob', 'page': '/home'},
    {'id': 5, 'event': 'PAGE_VIEW', 'user': 'bob', 'page': '/products'},
    {'id': 6, 'event': 'CLICK', 'user': 'bob', 'page': '/cart'},
    {'id': 7, 'event': 'LOGIN', 'user': 'carlos'},
    {'id': 8, 'event': 'PAGE_VIEW', 'user': 'carlos', 'page': '/home'},
    {'id': 9, 'event': 'PURCHASE', 'user': 'carlos', 'amount': 49.50},
    {'id': 10, 'event': 'LOGOUT', 'user': 'carlos'},
]

# -------------------- DAG --------------------
with DAG(
    dag_id='lab_01_produce_consume_pipeline',
    description='Produce clickstream events to Kafka, then consume and aggregate them.',
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz='UTC'),
    default_args=get_default_args('alw1tz'),
    catchup=False,
    tags=['kafka', 'lab'],
) as dag:

    @task
    def produce_events():
        from utils.commons import create_producer_template
        producer = create_producer_template(KAFKA_BROKER)
        for event in EVENTS:
            producer.produce(
                topic=TOPIC,
                key=str(event['id']).encode(),
                value=json.dumps(event).encode(),
            )
            print(f"Produced -> {event}")
        producer.flush()
        print(f"\nFlushed {len(EVENTS)} messages to topic '{TOPIC}'")

    @task
    def consume_and_aggregate():
        from confluent_kafka import Consumer, KafkaException
        consumer = Consumer({
            'bootstrap.servers': KAFKA_BROKER,
            'group.id': CONSUMER_GROUP,
            'auto.offset.reset': 'earliest',
        })
        consumer.subscribe([TOPIC])
        messages = []
        try:
            while True:
                msg = consumer.poll(timeout=5.0)
                if msg is None:
                    break
                if msg.error():
                    raise KafkaException(msg.error())
                data = json.loads(msg.value().decode('utf-8'))
                messages.append(data)
                print(f"Consumed <- {data}")
        finally:
            consumer.close()

        by_event = {}
        for m in messages:
            by_event[m['event']] = by_event.get(m['event'], 0) + 1

        print(f"\nTotal consumed: {len(messages)} messages")
        print("Events by type:", by_event)
        return {'total_messages': len(messages), 'by_event_type': by_event}

    # -------------------- Task Dependencies --------------------
    produce_events() >> consume_and_aggregate()
