"""Reusable utilities for all DAGs — mirrors commons.py pattern from work repo."""
from confluent_kafka import Producer

from utils.settings import KAFKA_BROKER


# ── Kafka ─────────────────────────────────────────────────────────────────────

def create_producer_template(broker: str = KAFKA_BROKER) -> Producer:
    """Create a Kafka producer. Mirrors work repo create_producer_template()."""
    return Producer({
        'bootstrap.servers': broker,
        'queue.buffering.max.messages': 1_000_000,
    })
