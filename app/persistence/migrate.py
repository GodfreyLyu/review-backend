"""Explicit migration command; API/worker processes never create schemas implicitly."""

from app.config import Settings
from app.persistence.storage import Store


def main() -> None:
    config = Settings()
    Store(config.database_url).initialize()
    print("PostgreSQL migrations applied and verified.")


if __name__ == "__main__":
    main()
