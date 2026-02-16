"""
MiniDB Feature Registry Manager
================================
Reads, validates, and updates the feature registry.
Enforces dependency ordering — a feature cannot advance to COMPLETE
unless all its dependencies are COMPLETE or VERIFIED.
Includes circular dependency detection.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from verification.checksums import get_feature_file_checksums


# Project root — resolved relative to this file's location
PROJECT_ROOT = Path(__file__).resolve().parent.parent

REGISTRY_FILE = PROJECT_ROOT / "features" / "feature_registry.json"

# Valid status transitions (strict ordering)
VALID_STATUSES = ["NOT_STARTED", "IN_PROGRESS", "COMPLETE", "VERIFIED"]

STATUS_ORDER = {s: i for i, s in enumerate(VALID_STATUSES)}


def load_registry() -> Dict[str, Any]:
    """Load the feature registry from disk."""
    with open(REGISTRY_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def save_registry(registry: Dict[str, Any]) -> None:
    """Save the feature registry to disk."""
    registry["meta"]["last_updated"] = datetime.now(timezone.utc).isoformat()
    with open(REGISTRY_FILE, "w", encoding="utf-8") as f:
        json.dump(registry, f, indent=2)


def get_feature(registry: Dict[str, Any], feature_id: str) -> Optional[Dict[str, Any]]:
    """Get a single feature by its ID."""
    return registry.get("features", {}).get(feature_id)


def get_development_order(registry: Dict[str, Any]) -> List[str]:
    """Get the ordered list of feature IDs for development."""
    return registry.get("development_order", [])


# ─── Circular Dependency Detection ─────────────────────────────────────────

def detect_circular_dependencies(registry: Dict[str, Any]) -> List[List[str]]:
    """
    Detect circular dependencies in the feature graph using DFS.
    Returns a list of cycles found (each cycle is a list of feature IDs).
    Empty list = no cycles.
    """
    features = registry.get("features", {})
    visited: Set[str] = set()
    rec_stack: Set[str] = set()
    cycles: List[List[str]] = []

    def _dfs(node: str, path: List[str]) -> None:
        visited.add(node)
        rec_stack.add(node)
        path.append(node)

        feature = features.get(node, {})
        for dep_id in feature.get("dependencies", []):
            if dep_id not in visited:
                _dfs(dep_id, path)
            elif dep_id in rec_stack:
                # Found a cycle — extract it
                cycle_start = path.index(dep_id)
                cycle = path[cycle_start:] + [dep_id]
                cycles.append(cycle)

        path.pop()
        rec_stack.discard(node)

    for feature_id in features:
        if feature_id not in visited:
            _dfs(feature_id, [])

    return cycles


def validate_dependency_graph(registry: Dict[str, Any]) -> tuple[bool, list[str]]:
    """
    Full dependency graph validation:
    - No circular dependencies
    - All referenced dependencies exist
    - Development order covers all features
    Returns (valid, list_of_issues).
    """
    issues: list[str] = []
    features = registry.get("features", {})

    # Check for circular dependencies
    cycles = detect_circular_dependencies(registry)
    for cycle in cycles:
        issues.append(f"CIRCULAR DEPENDENCY: {' → '.join(cycle)}")

    # Check all dependency references exist
    for feature_id, feature in features.items():
        for dep_id in feature.get("dependencies", []):
            if dep_id not in features:
                issues.append(f"Feature '{feature_id}' depends on unknown '{dep_id}'")

    # Check development order completeness
    dev_order = set(get_development_order(registry))
    feature_ids = set(features.keys())
    missing_from_order = feature_ids - dev_order
    if missing_from_order:
        issues.append(f"Features not in development_order: {', '.join(sorted(missing_from_order))}")
    extra_in_order = dev_order - feature_ids
    if extra_in_order:
        issues.append(f"development_order references unknown features: {', '.join(sorted(extra_in_order))}")

    return len(issues) == 0, issues


# ─── Dependency Checking ───────────────────────────────────────────────────

def check_dependencies_met(
    registry: Dict[str, Any],
    feature_id: str,
    required_status: str = "COMPLETE"
) -> tuple[bool, list[str]]:
    """
    Check if all dependencies of a feature meet the required minimum status.
    Returns (all_met, list_of_unmet_dependency_ids).
    """
    feature = get_feature(registry, feature_id)
    if feature is None:
        return False, [f"Feature '{feature_id}' not found"]

    unmet: list[str] = []
    required_level = STATUS_ORDER.get(required_status, 0)

    for dep_id in feature.get("dependencies", []):
        dep = get_feature(registry, dep_id)
        if dep is None:
            unmet.append(f"{dep_id} (missing)")
            continue
        dep_level = STATUS_ORDER.get(dep["status"], 0)
        if dep_level < required_level:
            unmet.append(f"{dep_id} (status: {dep['status']}, required: {required_status})")

    return len(unmet) == 0, unmet


def validate_files_exist(feature: Dict[str, Any], root: Optional[Path] = None) -> tuple[bool, list[str]]:
    """
    Check that all implementation files for a feature exist on disk.
    Returns (all_exist, list_of_missing_files).
    """
    if root is None:
        root = PROJECT_ROOT

    missing: list[str] = []
    for filepath in feature.get("implementation_files", []):
        full_path = root / filepath
        if not full_path.exists():
            missing.append(filepath)

    return len(missing) == 0, missing


def can_advance_to(
    registry: Dict[str, Any],
    feature_id: str,
    target_status: str
) -> tuple[bool, list[str]]:
    """
    Check if a feature can be advanced to the target status.
    Enforces dependency ordering and completion rules.

    Rules:
    - COMPLETE: implementation exists, tests exist, dependencies met
    - VERIFIED: tests pass, checksum recorded, verification confirms
    """
    reasons: list[str] = []
    feature = get_feature(registry, feature_id)
    if feature is None:
        return False, [f"Feature '{feature_id}' not found"]

    current_level = STATUS_ORDER.get(feature["status"], 0)
    target_level = STATUS_ORDER.get(target_status, 0)

    if target_level <= current_level:
        reasons.append(f"Already at or beyond '{target_status}'")
        return False, reasons

    # Check dependencies
    deps_met, unmet = check_dependencies_met(registry, feature_id, "COMPLETE")
    if not deps_met:
        reasons.append(f"Unmet dependencies: {', '.join(unmet)}")

    # Check files exist
    files_ok, missing = validate_files_exist(feature)
    if not files_ok:
        reasons.append(f"Missing files: {', '.join(missing)}")

    if target_status in ("COMPLETE", "VERIFIED"):
        if not feature.get("unit_tests_present", False):
            reasons.append("Unit tests not present")

    if target_status == "VERIFIED":
        if not feature.get("last_verified_timestamp"):
            reasons.append("No verification timestamp — run verify_build.py first")

    return len(reasons) == 0, reasons


def update_feature_status(
    registry: Dict[str, Any],
    feature_id: str,
    new_status: str,
    checksums: Optional[Dict[str, str]] = None,
) -> tuple[bool, str]:
    """
    Attempt to update a feature's status. Enforces all rules.
    Returns (success, message).
    """
    feature = get_feature(registry, feature_id)
    if feature is None:
        return False, f"Feature '{feature_id}' not found"

    can, reasons = can_advance_to(registry, feature_id, new_status)
    # Allow IN_PROGRESS without full checks
    if new_status == "IN_PROGRESS":
        feature["status"] = new_status
        save_registry(registry)
        return True, f"Feature '{feature_id}' → IN_PROGRESS"

    if not can and new_status != "NOT_STARTED":
        return False, f"Cannot advance to {new_status}: {'; '.join(reasons)}"

    feature["status"] = new_status

    if new_status == "VERIFIED":
        feature["last_verified_timestamp"] = datetime.now(timezone.utc).isoformat()
        if checksums:
            feature["checksum_of_related_code"] = get_feature_file_checksums(
                feature["implementation_files"], checksums
            )

    save_registry(registry)
    return True, f"Feature '{feature_id}' → {new_status}"


def get_next_required_step(registry: Dict[str, Any]) -> Optional[str]:
    """
    Determine the next feature that should be worked on,
    based on development order and dependency completion.
    """
    order = get_development_order(registry)
    for feature_id in order:
        feature = get_feature(registry, feature_id)
        if feature and feature["status"] not in ("COMPLETE", "VERIFIED"):
            return feature_id
    return None  # All features complete


def get_status_summary(registry: Dict[str, Any]) -> Dict[str, list[str]]:
    """Group features by their current status for reporting."""
    summary: Dict[str, list[str]] = {s: [] for s in VALID_STATUSES}
    for feature_id, feature in registry.get("features", {}).items():
        status = feature.get("status", "NOT_STARTED")
        summary.setdefault(status, []).append(feature_id)
    return summary
