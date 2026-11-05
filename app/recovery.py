"""Resume reconciliation of a blocked review without changing remote identity."""

import argparse
from uuid import UUID

from app.config import Settings
from app.persistence.storage import Store


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("review_id", type=UUID)
    args = parser.parse_args()
    config = Settings()
    store = Store(config.database_url, topic=config.kafka_topic, dlq_topic=config.kafka_dlq_topic)
    if not store.retry_recovery(str(args.review_id)):
        raise SystemExit("Review is not blocked for recovery; no state changed.")
    print("Reconciliation scheduled for the saved instance/request. No generation was started.")


if __name__ == "__main__":
    main()
