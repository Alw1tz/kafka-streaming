"""Global sandbox settings — mirrors production pattern from work repo."""
import os
from copy import deepcopy
from datetime import datetime, timedelta

# ── Environment ──────────────────────────────────────────────────────────────
AIRFLOW_ENVIRONMENT = os.getenv('AIRFLOW_ENVIRONMENT', 'local')
IS_LOCAL      = AIRFLOW_ENVIRONMENT == 'local'
IS_PRODUCTION = AIRFLOW_ENVIRONMENT == 'production'

# ── Kafka ─────────────────────────────────────────────────────────────────────
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:29092')


def get_default_args(owner: str = 'airflow') -> dict:
    """Default DAG args factory — same pattern as work repo."""
    return deepcopy({
        'owner': owner,
        'depends_on_past': False,
        'start_date': datetime(2025, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'email_on_failure': False,
        'email_on_retry': False,
    })
