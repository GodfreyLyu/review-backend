"""Fetch an immutable published wheel; never install service source or a live branch."""

import argparse
import hashlib
import json
import os
import shlex
import subprocess
import tempfile
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ssh-key", type=Path)
    args = parser.parse_args()
    lock = json.loads((ROOT / "contracts.lock.json").read_text())
    destination = ROOT / ".artifacts"
    destination.mkdir(exist_ok=True)
    wheel = destination / lock["filename"]
    with tempfile.TemporaryDirectory(prefix="contracts-artifact-") as directory:
        if args.ssh_key:
            meta = json.load(urllib.request.urlopen("https://api.github.com/meta", timeout=20))
            known = Path(directory) / "known_hosts"
            known.write_text("".join("github.com " + key + "\n" for key in meta["ssh_keys"]))
            env = dict(os.environ)
            env["GIT_SSH_COMMAND"] = shlex.join(
                [
                    "ssh",
                    "-i",
                    str(args.ssh_key.resolve()),
                    "-o",
                    "IdentitiesOnly=yes",
                    "-o",
                    "StrictHostKeyChecking=yes",
                    "-o",
                    f"UserKnownHostsFile={known}",
                ]
            )
            subprocess.run(
                ["git", "init", "--bare", directory + "/package.git"],
                check=True,
                capture_output=True,
            )
            git = ["git", "--git-dir", directory + "/package.git"]
            subprocess.run(
                git
                + [
                    "fetch",
                    "--depth=1",
                    f"git@github.com:{lock['repository']}.git",
                    lock["commit"],
                ],
                env=env,
                check=True,
                capture_output=True,
            )
            data = subprocess.check_output(git + ["show", f"{lock['commit']}:{lock['filename']}"])
        else:
            subprocess.run(
                [
                    "gh",
                    "release",
                    "download",
                    lock["tag"],
                    "--repo",
                    lock["repository"],
                    "--pattern",
                    lock["filename"],
                    "--dir",
                    directory,
                ],
                check=True,
            )
            data = (Path(directory) / lock["filename"]).read_bytes()
    if hashlib.sha256(data).hexdigest() != lock["sha256"]:
        raise SystemExit("Contract artifact SHA256 mismatch; refusing installation.")
    wheel.write_bytes(data)
    print(f"Verified {lock['filename']} sha256:{lock['sha256']}")


if __name__ == "__main__":
    main()
