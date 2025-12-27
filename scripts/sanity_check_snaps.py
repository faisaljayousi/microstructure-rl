"""
Sanity checker for `.snap` (L2 snapshot) artifacts produced by the C++ converter.

This script validates that each `.snap` file:
1) Contains a well-formed file header (magic/version/depth/record_size/endian/scales)
2) Has a file size consistent with header-declared record size
3) Optionally passes lightweight sampling checks on record timestamps

------------------------------------------------------------------------------
Expected `.snap` binary layout (v1)
------------------------------------------------------------------------------
File layout:
  [FileHeader][Record][Record]...

Header layout (40 bytes, little-endian) matching C++ `FileHeader`:
  uint32 magic
  uint16 version
  uint16 depth
  uint32 record_size
  uint32 endian_check
  int64  price_scale
  int64  qty_scale
  uint64 record_count

Record layout (we only sample the first 16 bytes for timestamp plausibility):
  int64 ts_event_ms
  int64 ts_recv_ns
  ... remaining fields not parsed by this checker ...

------------------------------------------------------------------------------
Configuration: .env + CLI
------------------------------------------------------------------------------
This script loads environment variables from a repo-root `.env` file using
python-dotenv. The repo root is inferred from this script's location.

Required env vars (in repo-root .env):
  DATA_PROCESSED_ROOT=<absolute path to processed root>

Optional:
  SNAP_EXPECTED_VERSION=1
  SNAP_EXPECTED_DEPTH=20
  SNAP_EXPECTED_RECORD_SIZE=656
  SNAP_SAMPLES=5

CLI arguments can override env defaults.

------------------------------------------------------------------------------
Usage examples
------------------------------------------------------------------------------
From repo root:

Check one date folder:
  python scripts\\sanity_check_snaps.py --date 2025-12-25

Check all dates (discover YYYY-MM-DD folders under processed root):
  python scripts\\sanity_check_snaps.py --all-dates

Check a range:
  python scripts\\sanity_check_snaps.py --range 2025-12-01 2025-12-31

Fail fast:
  python scripts\\sanity_check_snaps.py --all-dates --fail-fast

Header-only (no record sampling):
  python scripts\\sanity_check_snaps.py --all-dates --samples 0

Exit codes:
  0 = all checks passed (or no files found)
  1 = one or more files failed checks
  2 = configuration/usage error
"""

from __future__ import annotations

import argparse
import os
import struct
from dataclasses import dataclass
from datetime import datetime, timedelta, date as Date
from pathlib import Path
from typing import Iterable, Optional, List

from dotenv import load_dotenv


# ----------------------------
# Snap format constants (v1)
# ----------------------------

MAGIC = 0x4C32424F  # "L2BO" little-endian
ENDIAN_CHECK = 0x01020304

# Header layout (40 bytes):
# <IHHIIqqQ = uint32, uint16, uint16, uint32, uint32, int64, int64, uint64
HEADER_STRUCT = struct.Struct("<IHHIIqqQ")
HEADER_SIZE = HEADER_STRUCT.size  # 40

# Only sample the timestamps portion of Record (first 16 bytes): int64, int64
RECORD_TS_STRUCT = struct.Struct("<qq")


@dataclass(frozen=True)
class Header:
    """
    Parsed `.snap` file header.

    Attributes
    ----------
    magic:
        Magic constant identifying file type (expected MAGIC).
    version:
        Format version.
    depth:
        Order book depth (e.g., 20).
    record_size:
        Size of each record in bytes (e.g., 656).
    endian_check:
        Endianness marker (expected ENDIAN_CHECK).
    price_scale:
        Fixed-point scaling for price.
    qty_scale:
        Fixed-point scaling for quantity.
    record_count:
        Optional record count written by producer (may be 0 if unknown at write-time).
    """

    magic: int
    version: int
    depth: int
    record_size: int
    endian_check: int
    price_scale: int
    qty_scale: int
    record_count: int


@dataclass(frozen=True)
class FileCheckResult:
    """
    Outcome of checking a single file.

    Attributes
    ----------
    path:
        `.snap` file path.
    ok:
        True if all checks passed.
    msg:
        Diagnostic message for failures, "OK" on success.
    """

    path: Path
    ok: bool
    msg: str


# ----------------------------
# Repo root / env loading
# ----------------------------


def repo_root_from_script() -> Path:
    """
    Infer repo root from this script location.

    Assumes repository structure:
      repo_root/
        scripts/
          sanity_check_snaps.py

    Returns
    -------
    Path
        Absolute path to repo root.
    """
    return Path(__file__).resolve().parents[1]


def load_repo_env(root: Path) -> None:
    """
    Load environment variables from repo-root `.env`.

    Parameters
    ----------
    root:
        Repo root path.

    Notes
    -----
    This avoids reliance on the current working directory (cwd), which makes the
    script safe to run from IDEs, CI runners, or arbitrary shells.
    """
    load_dotenv(root / ".env")


def env_int(name: str, default: int) -> int:
    """
    Read an integer env var with a default.

    Parameters
    ----------
    name:
        Env var name.
    default:
        Default if missing or empty.

    Returns
    -------
    int
        Parsed integer.
    """
    v = os.getenv(name)
    if v is None or v.strip() == "":
        return default
    return int(v)


# ----------------------------
# Date selection utilities
# ----------------------------


def parse_date_str(s: str) -> str:
    """
    Validate YYYY-MM-DD string and return it.

    Parameters
    ----------
    s:
        Candidate date string.

    Returns
    -------
    str
        Same string if valid.

    Raises
    ------
    ValueError
        If not a valid YYYY-MM-DD.
    """
    datetime.strptime(s, "%Y-%m-%d")
    return s


def date_range(d0: str, d1: str) -> Iterable[str]:
    """
    Yield YYYY-MM-DD strings for inclusive date range.

    Parameters
    ----------
    d0:
        Start date YYYY-MM-DD.
    d1:
        End date YYYY-MM-DD.

    Yields
    ------
    str
        Date strings.
    """
    a: Date = datetime.strptime(d0, "%Y-%m-%d").date()
    b: Date = datetime.strptime(d1, "%Y-%m-%d").date()
    cur = a
    while cur <= b:
        yield cur.strftime("%Y-%m-%d")
        cur += timedelta(days=1)


def resolve_snap_files(
    processed_root: Path,
    date: Optional[str],
    dates: Optional[str],
    date_from: Optional[str],
    date_to: Optional[str],
    all_dates: bool,
) -> List[Path]:
    """
    Resolve `.snap` files to check based on CLI selection.

    Parameters
    ----------
    processed_root:
        Root processed directory (contains YYYY-MM-DD subfolders).
    date:
        Single date folder to check.
    dates:
        Comma-separated list of date folders to check.
    date_from:
        Range start date (inclusive).
    date_to:
        Range end date (inclusive).
    all_dates:
        If True, discover all YYYY-MM-DD folders under processed_root.

    Returns
    -------
    list[Path]
        List of `.snap` paths.

    Raises
    ------
    SystemExit
        If no selection mode is provided.
    """
    if date:
        return sorted((processed_root / date).glob("*.snap"))
    if dates:
        out: List[Path] = []
        for d in [x.strip() for x in dates.split(",") if x.strip()]:
            out.extend(sorted((processed_root / d).glob("*.snap")))
        return out
    if date_from and date_to:
        out: List[Path] = []
        for d in date_range(date_from, date_to):
            out.extend(sorted((processed_root / d).glob("*.snap")))
        return out
    if all_dates:
        out: List[Path] = []
        for p in sorted(processed_root.iterdir()):
            if not p.is_dir():
                continue
            try:
                parse_date_str(p.name)
            except ValueError:
                continue
            out.extend(sorted(p.glob("*.snap")))
        return out

    raise SystemExit("[FATAL] Provide one of: --date, --dates, --range, or --all-dates")


# ----------------------------
# Binary parsing / checks
# ----------------------------


def read_header(path: Path) -> Header:
    """
    Read and parse the `.snap` file header.

    Parameters
    ----------
    path:
        `.snap` file path.

    Returns
    -------
    Header
        Parsed header.

    Raises
    ------
    ValueError
        If the file is too small or header cannot be read.
    """
    with path.open("rb") as f:
        data = f.read(HEADER_SIZE)
    if len(data) != HEADER_SIZE:
        raise ValueError(f"File too small to contain header ({HEADER_SIZE} bytes).")
    return Header(*HEADER_STRUCT.unpack(data))


def check_header(h: Header) -> Optional[str]:
    """
    Validate header fields (magic/endian/scales/basic sanity).

    Parameters
    ----------
    h:
        Parsed header.

    Returns
    -------
    Optional[str]
        None if OK, else an error message.
    """
    if h.magic != MAGIC:
        return f"Bad magic: got=0x{h.magic:08X} expected=0x{MAGIC:08X}"
    if h.endian_check != ENDIAN_CHECK:
        return f"Bad endian_check: got=0x{h.endian_check:08X} expected=0x{ENDIAN_CHECK:08X}"
    if h.record_size <= 0:
        return f"Bad record_size: {h.record_size}"
    if h.depth <= 0:
        return f"Bad depth: {h.depth}"
    if h.version <= 0:
        return f"Bad version: {h.version}"
    if h.price_scale <= 0 or h.qty_scale <= 0:
        return f"Bad scales: price_scale={h.price_scale} qty_scale={h.qty_scale}"
    return None


def check_file_size(path: Path, record_size: int) -> Optional[str]:
    """
    Validate file size is consistent with record_size.

    Parameters
    ----------
    path:
        `.snap` file path.
    record_size:
        Record size in bytes (from header).

    Returns
    -------
    Optional[str]
        None if OK, else an error message.

    Checks
    ------
    - file_size >= HEADER_SIZE
    - (file_size - HEADER_SIZE) is divisible by record_size
    """
    sz = path.stat().st_size
    if sz < HEADER_SIZE:
        return f"File size {sz} < header size {HEADER_SIZE}"
    payload = sz - HEADER_SIZE
    if payload % record_size != 0:
        return f"Payload size {payload} not divisible by record_size {record_size}"
    return None


def sample_timestamp_checks(
    path: Path, record_size: int, samples: int
) -> Optional[str]:
    """
    Sample record timestamps for plausibility without parsing full records.

    Parameters
    ----------
    path:
        `.snap` file path.
    record_size:
        Record size in bytes (from header).
    samples:
        How many records to sample from the start of the file.

    Returns
    -------
    Optional[str]
        None if OK, else an error message.

    Notes
    -----
    This is a light check:
    - ts_recv_ns should be > 0
    - ts_event_ms should be >= 0 (it may be 0 if missing in feed)
    """
    if samples <= 0:
        return None

    sz = path.stat().st_size
    payload = sz - HEADER_SIZE
    n_records = payload // record_size
    if n_records == 0:
        return "No records in file."

    to_read = min(samples, n_records)

    with path.open("rb") as f:
        f.seek(HEADER_SIZE)
        for i in range(to_read):
            data = f.read(RECORD_TS_STRUCT.size)
            if len(data) != RECORD_TS_STRUCT.size:
                return f"Failed to read record timestamp for sample {i}."
            ts_event_ms, ts_recv_ns = RECORD_TS_STRUCT.unpack(data)

            if ts_recv_ns <= 0:
                return f"Sample {i}: ts_recv_ns <= 0 ({ts_recv_ns})"
            if ts_event_ms < 0:
                return f"Sample {i}: ts_event_ms < 0 ({ts_event_ms})"

            f.seek(HEADER_SIZE + (i + 1) * record_size)

    return None


def check_one_file(
    path: Path,
    expected_version: Optional[int],
    expected_depth: Optional[int],
    expected_record_size: Optional[int],
    samples: int,
) -> FileCheckResult:
    """
    Check a single `.snap` file.

    Parameters
    ----------
    path:
        `.snap` file path.
    expected_version:
        If not None, enforce header.version equals this.
    expected_depth:
        If not None, enforce header.depth equals this.
    expected_record_size:
        If not None, enforce header.record_size equals this.
    samples:
        Number of records to sample for timestamp plausibility.

    Returns
    -------
    FileCheckResult
        Pass/fail and diagnostic message.
    """
    try:
        h = read_header(path)

        err = check_header(h)
        if err:
            return FileCheckResult(path, False, err)

        if expected_version is not None and h.version != expected_version:
            return FileCheckResult(
                path,
                False,
                f"Unexpected version: got={h.version} expected={expected_version}",
            )
        if expected_depth is not None and h.depth != expected_depth:
            return FileCheckResult(
                path,
                False,
                f"Unexpected depth: got={h.depth} expected={expected_depth}",
            )
        if expected_record_size is not None and h.record_size != expected_record_size:
            return FileCheckResult(
                path,
                False,
                f"Unexpected record_size: got={h.record_size} expected={expected_record_size}",
            )

        err = check_file_size(path, h.record_size)
        if err:
            return FileCheckResult(path, False, err)

        err = sample_timestamp_checks(path, h.record_size, samples)
        if err:
            return FileCheckResult(path, False, err)

        return FileCheckResult(path, True, "OK")

    except Exception as e:
        return FileCheckResult(path, False, f"Exception: {repr(e)}")


# ----------------------------
# CLI
# ----------------------------


def parse_args() -> argparse.Namespace:
    """
    Parse CLI arguments for sanity checking.

    Returns
    -------
    argparse.Namespace
        Parsed args.
    """
    p = argparse.ArgumentParser(
        description="Sanity-check .snap files for header/layout integrity."
    )

    p.add_argument(
        "--processed-root",
        default=None,
        help="Override processed root. If omitted, uses DATA_PROCESSED_ROOT env var.",
    )

    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--date", type=parse_date_str, help="YYYY-MM-DD")
    g.add_argument("--dates", help="Comma-separated YYYY-MM-DD list")
    g.add_argument(
        "--range",
        nargs=2,
        metavar=("DATE_FROM", "DATE_TO"),
        help="Inclusive YYYY-MM-DD range",
    )
    g.add_argument(
        "--all-dates",
        action="store_true",
        help="Scan all YYYY-MM-DD folders under processed root",
    )

    p.add_argument(
        "--expected-version",
        type=int,
        default=None,
        help="If set, enforce this version. Default from env SNAP_EXPECTED_VERSION.",
    )
    p.add_argument(
        "--expected-depth",
        type=int,
        default=None,
        help="If set, enforce this depth. Default from env SNAP_EXPECTED_DEPTH.",
    )
    p.add_argument(
        "--expected-record-size",
        type=int,
        default=None,
        help="If set, enforce this record_size. Default from env SNAP_EXPECTED_RECORD_SIZE.",
    )

    p.add_argument(
        "--samples",
        type=int,
        default=None,
        help="Records per file to sample. Default from env SNAP_SAMPLES.",
    )
    p.add_argument(
        "--limit", type=int, default=None, help="Limit number of .snap files checked."
    )
    p.add_argument("--fail-fast", action="store_true", help="Stop at first failure.")
    return p.parse_args()


def main() -> int:
    """
    Main entrypoint.

    Returns
    -------
    int
        Exit code:
        - 0: all checks passed (or no files found)
        - 1: failures found
        - 2: configuration error
    """
    root = repo_root_from_script()
    load_repo_env(root)

    args = parse_args()

    processed_root = (
        Path(args.processed_root).expanduser().resolve()
        if args.processed_root
        else None
    )
    if processed_root is None:
        v = os.getenv("DATA_PROCESSED_ROOT")
        if not v:
            print(
                "[FATAL] DATA_PROCESSED_ROOT env var not set. Provide --processed-root or set it in repo-root .env."
            )
            return 2
        processed_root = Path(v).expanduser().resolve()

    # Defaults from env
    expected_version = (
        args.expected_version
        if args.expected_version is not None
        else env_int("SNAP_EXPECTED_VERSION", 1)
    )
    expected_depth = (
        args.expected_depth
        if args.expected_depth is not None
        else env_int("SNAP_EXPECTED_DEPTH", 20)
    )
    expected_record_size = (
        args.expected_record_size
        if args.expected_record_size is not None
        else env_int("SNAP_EXPECTED_RECORD_SIZE", 656)
    )
    samples = args.samples if args.samples is not None else env_int("SNAP_SAMPLES", 5)

    date_from = args.range[0] if args.range else None
    date_to = args.range[1] if args.range else None

    files = resolve_snap_files(
        processed_root=processed_root,
        date=args.date,
        dates=args.dates,
        date_from=date_from,
        date_to=date_to,
        all_dates=args.all_dates,
    )

    if args.limit is not None:
        files = files[: max(0, args.limit)]

    if not files:
        print("[INFO] No .snap files found for selection.")
        return 0

    print(
        f"[INFO] processed_root={processed_root}\n"
        f"[INFO] files={len(files)} samples={samples} "
        f"expected_version={expected_version} expected_depth={expected_depth} expected_record_size={expected_record_size}"
    )

    failures = 0
    for p in files:
        r = check_one_file(
            p,
            expected_version=expected_version,
            expected_depth=expected_depth,
            expected_record_size=expected_record_size,
            samples=samples,
        )
        if r.ok:
            print(f"OK   {r.path}")
        else:
            failures += 1
            print(f"FAIL {r.path} :: {r.msg}")
            if args.fail_fast:
                break

    print(f"\nSummary: checked={len(files)} failures={failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
