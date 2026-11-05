"""Container process heartbeat probe, separate from API/database readiness."""

import os
import sys
import time
from pathlib import Path


def main():
    path = Path(
        os.getenv(
            "PROCESS_HEARTBEAT_FILE",
            sys.argv[1] if len(sys.argv) > 1 else "/tmp/review-worker.heartbeat",
        )
    )
    raise SystemExit(0 if path.exists() and time.time() - path.stat().st_mtime < 30 else 1)


if __name__ == "__main__":
    main()
