#!/usr/bin/env python3
"""
MiniDB Build Verification Engine
=================================
Reads the feature registry, validates files, recomputes checksums,
runs tests, and produces a structured BUILD STATUS REPORT.

Usage:
    python verify_build.py              # Full verification
    python verify_build.py --self-check # Verify only verification infrastructure
    python verify_build.py --report     # Report only (no test execution)
    python verify_build.py --update     # Recompute and store checksums
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ─── Project root ───────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent

# Add project root to sys.path so we can import verification modules
sys.path.insert(0, str(PROJECT_ROOT))

from verification.checksums import (
    compute_all_checksums,
    detect_changes,
    load_stored_checksums,
    save_checksums,
    get_feature_file_checksums,
)
from verification.registry import (
    load_registry,
    save_registry,
    get_feature,
    get_development_order,
    validate_files_exist,
    check_dependencies_met,
    validate_dependency_graph,
    get_next_required_step,
    get_status_summary,
    VALID_STATUSES,
)


# ─── ANSI color codes ──────────────────────────────────────────────────────
class Color:
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    CYAN = "\033[96m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    RESET = "\033[0m"


def status_icon(status: str) -> str:
    """Return a colored icon for a feature status."""
    icons = {
        "VERIFIED": f"{Color.GREEN}✔{Color.RESET}",
        "COMPLETE": f"{Color.GREEN}●{Color.RESET}",
        "IN_PROGRESS": f"{Color.YELLOW}⚠{Color.RESET}",
        "NOT_STARTED": f"{Color.RED}❌{Color.RESET}",
    }
    return icons.get(status, "?")


# ─── Test Runner ────────────────────────────────────────────────────────────

def run_feature_tests(feature: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Run tests for a specific feature using pytest.
    Returns (passed, output_summary).
    """
    verification_method = feature.get("verification_method", "")
    if not verification_method or not verification_method.startswith("pytest"):
        return False, "No test command defined"

    # Extract test file path from verification method
    parts = verification_method.split()
    if len(parts) < 2:
        return False, "Invalid verification method format"

    test_path = PROJECT_ROOT / parts[1]
    if not test_path.exists():
        return False, f"Test file not found: {parts[1]}"

    try:
        result = subprocess.run(
            [sys.executable, "-m", "pytest", str(test_path), "-v", "--tb=short", "-q"],
            capture_output=True,
            text=True,
            timeout=120,
            cwd=str(PROJECT_ROOT),
        )
        passed = result.returncode == 0
        output = result.stdout.strip()
        if result.stderr.strip():
            output += "\n" + result.stderr.strip()

        # Extract summary line
        lines = output.split("\n")
        summary_line = lines[-1] if lines else "No output"
        return passed, summary_line
    except subprocess.TimeoutExpired:
        return False, "Tests timed out (>120s)"
    except FileNotFoundError:
        return False, "pytest not found — install with: pip install pytest"
    except Exception as e:
        return False, f"Error running tests: {e}"


# ─── Verification Logic ────────────────────────────────────────────────────

def verify_feature(
    feature_id: str,
    feature: Dict[str, Any],
    registry: Dict[str, Any],
    checksums: Dict[str, str],
    run_tests: bool = True,
) -> Dict[str, Any]:
    """
    Perform full verification of a single feature.
    Returns a verification result dict.
    """
    result: Dict[str, Any] = {
        "feature_id": feature_id,
        "name": feature.get("name", feature_id),
        "declared_status": feature.get("status", "NOT_STARTED"),
        "files_exist": False,
        "missing_files": [],
        "tests_present": feature.get("unit_tests_present", False),
        "tests_passed": None,
        "test_output": None,
        "dependencies_met": False,
        "unmet_dependencies": [],
        "checksums_match": True,
        "verified_status": "NOT_STARTED",
        "issues": [],
    }

    # 1. Check files exist
    files_ok, missing = validate_files_exist(feature, PROJECT_ROOT)
    result["files_exist"] = files_ok
    result["missing_files"] = missing
    if not files_ok:
        result["issues"].append(f"Missing files: {', '.join(missing)}")

    # 2. Check dependencies
    deps_ok, unmet = check_dependencies_met(registry, feature_id, "COMPLETE")
    result["dependencies_met"] = deps_ok
    result["unmet_dependencies"] = unmet
    if not deps_ok:
        result["issues"].append(f"Unmet dependencies: {', '.join(unmet)}")

    # 3. Check checksums for changes since last verification
    stored_checksums = feature.get("checksum_of_related_code", {})
    if stored_checksums:
        current_feature_checksums = get_feature_file_checksums(
            feature.get("implementation_files", []), checksums
        )
        for filepath, stored_hash in stored_checksums.items():
            current_hash = current_feature_checksums.get(filepath)
            if current_hash and current_hash != stored_hash:
                result["checksums_match"] = False
                result["issues"].append(f"Code changed since verification: {filepath}")

    # 4. Run tests if applicable
    if run_tests and feature.get("unit_tests_present", False):
        passed, output = run_feature_tests(feature)
        result["tests_passed"] = passed
        result["test_output"] = output
        if not passed:
            result["issues"].append(f"Tests failed: {output}")

    # 5. Determine verified status
    declared = feature.get("status", "NOT_STARTED")

    if declared == "NOT_STARTED":
        # Check if skeleton files exist — that's fine, still NOT_STARTED
        result["verified_status"] = "NOT_STARTED"

    elif declared == "IN_PROGRESS":
        if files_ok:
            result["verified_status"] = "IN_PROGRESS"
        else:
            result["verified_status"] = "NOT_STARTED"
            result["issues"].append("Claimed IN_PROGRESS but files missing")

    elif declared == "COMPLETE":
        if files_ok and result["tests_present"]:
            result["verified_status"] = "COMPLETE"
        else:
            result["verified_status"] = "IN_PROGRESS"
            result["issues"].append("Claimed COMPLETE but requirements not met")

    elif declared == "VERIFIED":
        if files_ok and result["tests_present"] and result.get("tests_passed", False) and result["checksums_match"]:
            result["verified_status"] = "VERIFIED"
        elif files_ok and result["tests_present"] and result.get("tests_passed") is None:
            # Tests not run (--report mode)
            result["verified_status"] = "COMPLETE"
            result["issues"].append("Tests not executed — cannot confirm VERIFIED")
        else:
            result["verified_status"] = "IN_PROGRESS"
            result["issues"].append("Claimed VERIFIED but verification failed")

    return result


# ─── Report Generation ─────────────────────────────────────────────────────

def print_report(
    results: List[Dict[str, Any]],
    checksums: Dict[str, str],
    changes: Dict[str, str],
    registry: Dict[str, Any],
) -> None:
    """Print the BUILD STATUS REPORT to stdout."""
    print()
    print(f"{Color.BOLD}{'═' * 60}{Color.RESET}")
    print(f"{Color.BOLD}{Color.CYAN}           BUILD STATUS REPORT — MiniDB{Color.RESET}")
    print(f"{Color.BOLD}{'═' * 60}{Color.RESET}")
    print(f"  Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Files tracked: {len(checksums)}")
    if changes:
        print(f"  {Color.YELLOW}Files changed since last check: {len(changes)}{Color.RESET}")
    print(f"{Color.BOLD}{'─' * 60}{Color.RESET}")
    print()

    # Feature status table
    print(f"{Color.BOLD}  FEATURE STATUS{Color.RESET}")
    print(f"  {'─' * 56}")

    all_passed = True
    for r in results:
        icon = status_icon(r["verified_status"])
        name = r["name"]
        status = r["verified_status"]

        # Warn if declared != verified
        mismatch = ""
        if r["declared_status"] != r["verified_status"]:
            mismatch = f" {Color.YELLOW}(declared: {r['declared_status']}){Color.RESET}"
            all_passed = False

        print(f"  {icon} {name:<35} {status}{mismatch}")

        if r["issues"]:
            for issue in r["issues"]:
                print(f"    {Color.DIM}└─ {issue}{Color.RESET}")
            all_passed = False

    print()
    print(f"  {'─' * 56}")

    # Next step
    next_step = get_next_required_step(registry)
    if next_step:
        feature = get_feature(registry, next_step)
        name = feature["name"] if feature else next_step
        print(f"  {Color.CYAN}▶ Next required step:{Color.RESET} {name} ({next_step})")
    else:
        print(f"  {Color.GREEN}▶ All features complete!{Color.RESET}")

    print()

    # File changes
    if changes:
        print(f"{Color.BOLD}  FILE CHANGES SINCE LAST VERIFICATION{Color.RESET}")
        print(f"  {'─' * 56}")
        for filepath, change_type in sorted(changes.items()):
            type_color = {
                "modified": Color.YELLOW,
                "added": Color.GREEN,
                "deleted": Color.RED,
            }.get(change_type, "")
            print(f"  {type_color}{change_type:>10}{Color.RESET}  {filepath}")
        print()

    # Summary
    print(f"{Color.BOLD}{'═' * 60}{Color.RESET}")
    summary = get_status_summary(registry)
    verified = len(summary.get("VERIFIED", []))
    complete = len(summary.get("COMPLETE", []))
    in_progress = len(summary.get("IN_PROGRESS", []))
    not_started = len(summary.get("NOT_STARTED", []))
    total = verified + complete + in_progress + not_started

    print(f"  {Color.GREEN}✔ VERIFIED: {verified}{Color.RESET}  "
          f"{Color.GREEN}● COMPLETE: {complete}{Color.RESET}  "
          f"{Color.YELLOW}⚠ IN_PROGRESS: {in_progress}{Color.RESET}  "
          f"{Color.RED}❌ NOT_STARTED: {not_started}{Color.RESET}  "
          f"/ {total} total")

    if all_passed:
        print(f"\n  {Color.GREEN}{Color.BOLD}BUILD INTEGRITY: CONSISTENT{Color.RESET}")
    else:
        print(f"\n  {Color.YELLOW}{Color.BOLD}BUILD INTEGRITY: ISSUES DETECTED{Color.RESET}")
    print(f"{Color.BOLD}{'═' * 60}{Color.RESET}")
    print()


# ─── Main ───────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(
        description="MiniDB Build Verification Engine",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--self-check",
        action="store_true",
        help="Verify only the verification infrastructure itself",
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="Report only — skip test execution",
    )
    parser.add_argument(
        "--update",
        action="store_true",
        help="Recompute and store checksums without running tests",
    )
    parser.add_argument(
        "--feature",
        type=str,
        default=None,
        help="Verify a specific feature by ID",
    )
    args = parser.parse_args()

    # 1. Load registry
    try:
        registry = load_registry()
    except (FileNotFoundError, json.JSONDecodeError) as e:
        print(f"{Color.RED}ERROR: Cannot load feature registry: {e}{Color.RESET}")
        return 1

    # 1.5. Validate dependency graph integrity
    graph_ok, graph_issues = validate_dependency_graph(registry)
    if not graph_ok:
        print(f"{Color.RED}{Color.BOLD}DEPENDENCY GRAPH ERRORS:{Color.RESET}")
        for issue in graph_issues:
            print(f"  {Color.RED}✗ {issue}{Color.RESET}")
        print()

    # 2. Compute current checksums
    current_checksums = compute_all_checksums(PROJECT_ROOT)

    # 3. Load stored checksums and detect changes
    stored_checksums = load_stored_checksums()
    changes = detect_changes(stored_checksums, current_checksums)

    # 4. Update checksums if requested
    if args.update:
        save_checksums(current_checksums)
        print(f"{Color.GREEN}Checksums updated ({len(current_checksums)} files tracked).{Color.RESET}")
        if not args.report:
            return 0

    # 5. Determine which features to verify
    dev_order = get_development_order(registry)
    if args.self_check:
        feature_ids = ["verification_infrastructure"]
    elif args.feature:
        feature_ids = [args.feature]
    else:
        feature_ids = dev_order

    # 6. Run verification
    run_tests = not args.report
    results: List[Dict[str, Any]] = []

    for feature_id in feature_ids:
        feature = get_feature(registry, feature_id)
        if feature is None:
            print(f"{Color.YELLOW}WARNING: Feature '{feature_id}' not in registry{Color.RESET}")
            continue

        result = verify_feature(
            feature_id, feature, registry, current_checksums, run_tests=run_tests
        )
        results.append(result)

    # 7. Print report
    print_report(results, current_checksums, changes, registry)

    # 8. Save updated checksums
    save_checksums(current_checksums)

    # 9. Return exit code (0 = consistent, 1 = issues)
    has_issues = any(r["issues"] for r in results)
    return 1 if has_issues else 0


if __name__ == "__main__":
    sys.exit(main())
