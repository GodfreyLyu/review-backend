"""Create this project's venv and install locked dependencies + verified release wheel."""

import argparse
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dev", action="store_true")
    parser.add_argument("--ssh-key")
    args = parser.parse_args()
    python = ROOT / ".venv/bin/python"
    if not python.exists():
        subprocess.run(["uv", "venv", "--python", "3.12", ".venv"], cwd=ROOT, check=True)
    subprocess.run(
        [str(python), "-c", "import sys; assert sys.version_info[:2] == (3, 12)"], check=True
    )
    subprocess.run(
        [str(python), "scripts/fetch_contract.py"]
        + (["--ssh-key", args.ssh_key] if args.ssh_key else []),
        cwd=ROOT,
        check=True,
    )
    subprocess.run(
        [
            "uv",
            "pip",
            "install",
            "--python",
            str(python),
            "--require-hashes",
            "-r",
            "requirements-dev.lock" if args.dev else "requirements.lock",
        ],
        cwd=ROOT,
        check=True,
    )
    subprocess.run(
        [
            "uv",
            "pip",
            "install",
            "--python",
            str(python),
            "--no-deps",
            ".artifacts/local_llm_contract-1.0.0-py3-none-any.whl",
            ".",
        ],
        cwd=ROOT,
        check=True,
    )


if __name__ == "__main__":
    main()
