#!/usr/bin/env python3
"""
Delete files under the uploads directory older than a configurable cutoff month.

Usage examples:
  # Dry run (default) - shows files that would be removed
  python scripts/cleanup_uploads.py --path static/uploads --before 2026-01

  # Actually delete files older than 2026-01-01
  python scripts/cleanup_uploads.py --path static/uploads --before 2026-01 --delete

  # Remove files older than N months (relative)
  python scripts/cleanup_uploads.py --path static/uploads --older-than-months 6 --delete

Notes:
 - By default the script does a dry-run and will not delete anything unless --delete is passed.
 - Cutoff month (--before) accepts YYYY-MM; files with mtime earlier than the first day of that month
   (00:00:00) will be removed.
 - The script walks the directory recursively and only removes regular files.
 - Use --remove-empty-dirs to remove directories that become empty after deletion.
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timezone, timedelta
from typing import Iterable, List, Tuple
from types import SimpleNamespace


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Clean up old uploaded files.")
    p.add_argument(
        "--path",
        "-p",
        default="static/uploads",
        help="Root uploads directory (relative to repo or absolute). Default: static/uploads",
    )
    p.add_argument(
        "--before",
        help="Cutoff month in YYYY-MM format. Files older than the first day of this month will be deleted.",
    )
    p.add_argument(
        "--older-than-months",
        type=int,
        help="Alternative cutoff by months relative to now (e.g. 6 means files older than 6 months).",
    )
    p.add_argument(
        "--delete",
        action="store_true",
        help="Actually delete files. Without this flag the script runs in dry-run mode.",
    )
    p.add_argument(
        "--remove-empty-dirs",
        action="store_true",
        help="Remove directories that become empty after deleting files.",
    )
    p.add_argument(
        "--extensions",
        nargs="*",
        default=[".jpg", ".jpeg", ".png", ".webp", ".gif"],
        help="File extensions to target (default common image types).",
    )
    return p.parse_args()


# -----------------------
# Editable configuration
# -----------------------
# If you prefer to run the script by editing values in the file instead of
# passing CLI arguments, change the values below and run:
#   python3 scripts/cleanup_uploads.py
# The script will use these settings when no CLI args are provided.
UPLOADS_PATH = "static/uploads"
# Specify cutoff month YYYY-MM (files older than first day of this month will be removed).
BEFORE_MONTH: str | None = "2025-12"
# Alternative: specify months relative to now (int), e.g. 6 -> older than 6 months.
OLDER_THAN_MONTHS_CFG: int | None = None
# Whether to actually delete files when running without CLI args (default False -> dry-run).
DELETE_MODE_CFG: bool = False
# Remove empty dirs after deletion
REMOVE_EMPTY_DIRS_CFG: bool = False
# Extensions to target
EXTENSIONS_CFG = [".jpg", ".jpeg", ".png", ".webp", ".gif"]



def cutoff_from_before(before: str) -> datetime:
    """Return cutoff datetime (UTC naive) for a YYYY-MM spec -> first day of month 00:00"""
    try:
        parts = before.split("-")
        year = int(parts[0])
        month = int(parts[1])
        return datetime(year, month, 1, 0, 0, 0)
    except Exception as e:
        raise ValueError("Invalid --before format. Use YYYY-MM") from e


def cutoff_from_months(months: int) -> datetime:
    """Return cutoff datetime months ago from now (approx, using 30-day months)."""
    now = datetime.utcnow()
    # use 30 days per month as approximation; acceptable for cleanup script
    return now - timedelta(days=30 * months)


def find_files(root: str, exts: Iterable[str]) -> Iterable[str]:
    for dirpath, _, filenames in os.walk(root):
        for fn in filenames:
            if not exts or os.path.splitext(fn)[1].lower() in exts:
                yield os.path.join(dirpath, fn)


def remove_empty_dirs(root: str) -> List[str]:
    removed = []
    # walk bottom-up so we can remove empty dirs
    for dirpath, dirnames, filenames in os.walk(root, topdown=False):
        if not dirnames and not filenames:
            try:
                os.rmdir(dirpath)
                removed.append(dirpath)
            except OSError:
                # ignore non-empty or permission errors
                pass
    return removed


def run_cleanup(root: str, cutoff: datetime, exts: List[str], do_delete: bool, remove_empty: bool) -> Tuple[int, int, List[str]]:
    """
    Walk root, find files older than cutoff (based on mtime) and optionally delete.
    Returns (found_count, deleted_count, sample_deleted_paths)
    """
    root = os.path.abspath(root)
    if not os.path.exists(root):
        raise FileNotFoundError(f"Uploads root not found: {root}")

    cutoff_ts = cutoff.replace(tzinfo=timezone.utc).timestamp() if cutoff.tzinfo is None else cutoff.timestamp()
    found = 0
    deleted = 0
    deleted_paths: List[str] = []

    for path in find_files(root, [e.lower() for e in exts]):
        try:
            mtime = os.path.getmtime(path)
        except OSError:
            continue
        if mtime < cutoff_ts:
            found += 1
            if do_delete:
                try:
                    os.remove(path)
                    deleted += 1
                    deleted_paths.append(path)
                except OSError:
                    # permission or other error - skip and continue
                    pass
            else:
                deleted_paths.append(path)  # in dry-run collect sample list

    if do_delete and remove_empty:
        removed_dirs = remove_empty_dirs(root)
    else:
        removed_dirs = []

    return found, deleted, deleted_paths[:100]  # return sample up to 100 paths


def main() -> int:
    # If the script is executed without CLI args, use the in-file configuration above.
    if len(sys.argv) == 1:
        args = SimpleNamespace(
            path=UPLOADS_PATH,
            before=BEFORE_MONTH,
            older_than_months=OLDER_THAN_MONTHS_CFG,
            delete=DELETE_MODE_CFG,
            remove_empty_dirs=REMOVE_EMPTY_DIRS_CFG,
            extensions=EXTENSIONS_CFG,
        )
        print("No CLI arguments detected — using in-file configuration.")
    else:
        args = parse_args()

    if args.before and args.older_than_months:
        print("Specify either --before or --older-than-months, not both.", file=sys.stderr)
        return 2

    if args.before:
        cutoff = cutoff_from_before(args.before)
    elif args.older_than_months:
        cutoff = cutoff_from_months(args.older_than_months)
    else:
        # default: remove files older than the first day of the current month (i.e., "1月份之前")
        now = datetime.utcnow()
        cutoff = datetime(now.year, now.month, 1, 0, 0, 0)

    print(f"Uploads root: {args.path}")
    print(f"Cutoff (UTC): {cutoff.isoformat()}  — files older than this will be removed")
    print(f"Extensions: {args.extensions}")
    print("DRY RUN (no files will be removed)." if not args.delete else "DELETE MODE: files will be removed")
    print()

    try:
        found, deleted, sample = run_cleanup(args.path, cutoff, args.extensions, args.delete, args.remove_empty_dirs)
    except FileNotFoundError as e:
        print(str(e), file=sys.stderr)
        return 3

    print(f"Matched files: {found}")
    if args.delete:
        print(f"Deleted files: {deleted}")
    else:
        print("Dry-run; no files deleted.")
    if sample:
        print()
        print("Sample paths (up to 100):")
        for p in sample:
            print("  " + p)

    print()
    print("Run with --delete to actually remove files.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


