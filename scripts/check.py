"""Project checks, optionally against isolated real PostgreSQL and Kafka."""

import argparse
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--functional", action="store_true")
    args = parser.parse_args()
    python = str(ROOT / ".venv/bin/python")
    env = dict(os.environ)
    if args.functional:
        env.setdefault(
            "BACKEND_TEST_DATABASE_URL",
            "postgresql://review:local-test-password@127.0.0.1:55432/review_backend_test",
        )
        env.setdefault("BACKEND_TEST_KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:59092")
    subprocess.run(
        [str(ROOT / ".venv/bin/ruff"), "check", "app", "tests", "scripts"], cwd=ROOT, check=True
    )
    subprocess.run(
        [str(ROOT / ".venv/bin/ruff"), "format", "--check", "app", "tests", "scripts"],
        cwd=ROOT,
        check=True,
    )
    subprocess.run(
        [
            python,
            "-m",
            "pytest",
            "tests",
            "-m",
            "functional" if args.functional else "not functional",
        ],
        cwd=ROOT,
        env=env,
        check=True,
    )
    subprocess.run([python, "-m", "build", "--wheel"], cwd=ROOT, check=True)


if __name__ == "__main__":
    main()
