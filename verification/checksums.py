"""
MiniDB Checksum System
======================
Generates and validates SHA256 checksums for all module and test files.
Detects code changes and triggers re-verification.

Deterministic hashing guarantees:
- Line endings normalized to LF before hashing (CRLF → LF)
- File iteration order sorted alphabetically (os.walk + sorted)
- JSON output uses sorted keys
- No environment-dependent paths embedded in hashes
"""

import hashlib
import json
import os
from pathlib import Path
from typing import Dict, Optional


# Project root — resolved relative to this file's location
PROJECT_ROOT = Path(__file__).resolve().parent.parent

CHECKSUMS_FILE = PROJECT_ROOT / "build_integrity" / "checksums.json"


def compute_file_checksum(filepath: Path) -> str:
    """
    Compute SHA256 checksum of a single file.
    Text files (.py, .json, .md) are normalized to LF line endings
    before hashing to ensure cross-platform reproducibility.
    Binary files are hashed as-is.
    """
    text_extensions = {".py", ".json", ".md", ".txt", ".cfg", ".ini", ".toml"}
    suffix = filepath.suffix.lower()

    sha256 = hashlib.sha256()

    if suffix in text_extensions:
        # Normalize line endings: read as text, re-encode with LF
        with open(filepath, "r", encoding="utf-8", newline="") as f:
            content = f.read()
        # Normalize all line endings to LF
        normalized = content.replace("\r\n", "\n").replace("\r", "\n")
        sha256.update(normalized.encode("utf-8"))
    else:
        # Binary: hash raw bytes
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha256.update(chunk)

    return sha256.hexdigest()


def compute_all_checksums(root: Optional[Path] = None) -> Dict[str, str]:
    """
    Compute SHA256 checksums for all relevant project files.
    Scans .py, .json, .md files; excludes __pycache__, .git, data/, etc.
    File order is deterministic (sorted).
    """
    if root is None:
        root = PROJECT_ROOT

    checksums: Dict[str, str] = {}
    exclude_dirs = {"__pycache__", ".git", "data", ".pytest_cache", ".mypy_cache"}

    for dirpath, dirnames, filenames in os.walk(root):
        # Sort and filter directories for deterministic traversal
        dirnames[:] = sorted(d for d in dirnames if d not in exclude_dirs)

        for filename in sorted(filenames):
            if filename.endswith((".py", ".json", ".md")) and filename != "checksums.json":
                filepath = Path(dirpath) / filename
                rel_path = filepath.relative_to(root).as_posix()
                checksums[rel_path] = compute_file_checksum(filepath)

    return checksums


def load_stored_checksums() -> Dict[str, str]:
    """Load previously stored checksums from build_integrity/checksums.json."""
    if not CHECKSUMS_FILE.exists():
        return {}
    with open(CHECKSUMS_FILE, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data.get("checksums", {})


def save_checksums(checksums: Dict[str, str]) -> None:
    """Save checksums to build_integrity/checksums.json with sorted keys."""
    CHECKSUMS_FILE.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "generated_by": "MiniDB Checksum System",
        "description": "SHA256 checksums for all project files. Normalized to LF line endings for cross-platform reproducibility.",
        "checksums": dict(sorted(checksums.items()))
    }
    with open(CHECKSUMS_FILE, "w", encoding="utf-8", newline="\n") as f:
        json.dump(data, f, indent=2, sort_keys=False)
        f.write("\n")


def detect_changes(stored: Dict[str, str], current: Dict[str, str]) -> Dict[str, str]:
    """
    Compare stored vs current checksums.
    Returns a dict of changed files with their change type:
      'modified', 'added', 'deleted'
    """
    changes: Dict[str, str] = {}

    all_files = set(stored.keys()) | set(current.keys())
    for filepath in sorted(all_files):
        in_stored = filepath in stored
        in_current = filepath in current

        if in_stored and in_current:
            if stored[filepath] != current[filepath]:
                changes[filepath] = "modified"
        elif in_current and not in_stored:
            changes[filepath] = "added"
        elif in_stored and not in_current:
            changes[filepath] = "deleted"

    return changes


def get_feature_file_checksums(feature_files: list[str], all_checksums: Dict[str, str]) -> Dict[str, str]:
    """Extract checksums for files belonging to a specific feature."""
    return {f: all_checksums[f] for f in feature_files if f in all_checksums}
