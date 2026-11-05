"""Publish this backend's wheel to a private, immutable GitHub Release.

The contract has a separate lifecycle. This script never publishes or replaces it.
"""

import argparse
import hashlib
import json
import subprocess
import sys
import tomllib
import zipfile
from email.parser import BytesParser
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def command(*args, capture=False):
    return subprocess.run(args, cwd=ROOT, check=True, text=True, capture_output=capture).stdout


def validate_wheel(path, name, version):
    if not path.is_file():
        raise ValueError("Backend wheel does not exist.")
    with zipfile.ZipFile(path) as archive:
        metadata_files = [
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        ]
        if len(metadata_files) != 1:
            raise ValueError("Expected exactly one wheel metadata record.")
        metadata = BytesParser().parsebytes(archive.read(metadata_files[0]))
        if metadata["Name"] != name or metadata["Version"] != version:
            raise ValueError("Wheel name/version does not match the backend project.")
        if not any(name.startswith("app/") for name in archive.namelist()):
            raise ValueError("Wheel does not contain the backend implementation.")
        if any(name.startswith("llm_contract/") for name in archive.namelist()):
            raise ValueError("Do not bundle generated contracts into the service wheel.")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", type=Path, help="Use an already built, validated service wheel")
    parser.add_argument("--tag", help="Must equal backend-v plus pyproject service version")
    parser.add_argument("--target", help="Must equal this checkout HEAD")
    parser.add_argument("--dry-run", action="store_true", help="Build/validate; do not call GitHub")
    args = parser.parse_args()
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    name, version = project["name"], project["version"]
    tag = args.tag or "backend-v" + version
    if tag != "backend-v" + version:
        parser.error("Release tag must match the service version in pyproject.toml.")
    target = command("git", "rev-parse", "HEAD", capture=True).strip()
    if args.target and args.target != target:
        parser.error("Release target must be the exact checkout HEAD.")
    if (
        not args.dry_run
        and command(
            "git", "status", "--porcelain=v1", "--untracked-files=normal", capture=True
        ).strip()
    ):
        parser.error("Commit the checkout changes before creating a backend release.")
    wheel = args.wheel
    if wheel is None:
        command(sys.executable, "-m", "build", "--wheel", "--outdir", "dist", ".")
        wheel = ROOT / "dist" / f"{name.replace('-', '_')}-{version}-py3-none-any.whl"
    else:
        wheel = wheel.resolve()
    digest = validate_wheel(wheel, name, version)
    checksum = wheel.parent / f"{wheel.name}.sha256"
    checksum.write_text(f"{digest}  {wheel.name}\n")
    if args.dry_run:
        print(f"Validated {tag} for {target}: {wheel.name} sha256={digest}; no publication.")
        return
    repo = json.loads(
        command("gh", "repo", "view", "--json", "nameWithOwner,isPrivate", capture=True)
    )
    if not repo["isPrivate"]:
        parser.error("Backend publication requires the private project repository.")
    repository = repo["nameWithOwner"]
    runs = json.loads(
        command(
            "gh",
            "run",
            "list",
            "--repo",
            repository,
            "--workflow",
            "ci.yml",
            "--commit",
            target,
            "--limit",
            "20",
            "--json",
            "conclusion,headSha",
            capture=True,
        )
    )
    if not any(run["headSha"] == target and run["conclusion"] == "success" for run in runs):
        parser.error("The independent ci.yml workflow must pass for this exact commit first.")
    # Creating the ref is atomic and fails if any tag already exists. Never move it.
    command(
        "gh",
        "api",
        "--method",
        "POST",
        f"repos/{repository}/git/refs",
        "-f",
        f"ref=refs/tags/{tag}",
        "-f",
        f"sha={target}",
    )
    protocol_dependencies = ", ".join(
        f"`{dependency}`"
        for dependency in project["dependencies"]
        if dependency.startswith(("local-llm-contract==", "thrift=="))
    )
    notes = wheel.parent / f"{tag}.notes.md"
    notes.write_text(
        f"Review backend {version}.\n\n"
        f"Source commit: `{target}`. Independent CI passed for this exact commit.\n\n"
        f"Wheel SHA256: `{digest}`.\n\n"
        f"Protocol dependencies: {protocol_dependencies}. "
        "The contract is not republished by this release.\n"
    )
    command(
        "gh",
        "release",
        "create",
        tag,
        str(wheel),
        str(checksum),
        "--repo",
        repository,
        "--verify-tag",
        "--title",
        f"Review backend {version}",
        "--notes-file",
        str(notes),
    )


if __name__ == "__main__":
    main()
