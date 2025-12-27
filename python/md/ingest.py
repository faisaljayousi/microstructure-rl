"""
This Python script converts hourly Binance L2 CSV.GZ files into a
memory-mappable binary snapshot format (.snap) using a C++ converter executable.

This script is designed for bulk ingestion and backfills across many date folders.
It parallelises conversion across CPU cores, supports retries/timeouts, is idempotent
(skip already-produced artifacts), and writes per-input logs for diagnostics.

------------------------------------------------------------------------------------
High-level behavior
------------------------------------------------------------------------------------
Given:
- A compiled converter executable (C++) that converts ONE input .csv.gz file -> ONE .snap file
- A raw data directory structure like:
    <DATA_RAW_ROOT>/<exchange>/<feed>/<symbol>/<YYYY-MM-DD>/*.csv.gz
- A processed output directory structure like:
    <DATA_PROCESSED_ROOT>/<YYYY-MM-DD>/*.snap

The script:
1) Loads machine-local settings from repo-root .env (paths, optional overrides)
2) Loads pipeline defaults from a committed YAML config (configs/ingest.yaml)
3) Resolves which dates to process based on CLI:
    --date, --dates, --range, or --all-dates (filesystem discovery)
4) Builds a flat job list (one job per hourly file)
5) Runs jobs in a ProcessPoolExecutor with bounded parallelism
6) Produces a global summary and returns non-zero exit code if any failures occur

------------------------------------------------------------------------------------
Configuration sources and precedence
------------------------------------------------------------------------------------
Settings come from:
1) CLI flags (highest precedence where applicable)
2) Environment variables from .env (machine-specific overrides)
3) YAML config (repo versioned defaults)

------------------------------------------------------------------------------------
Required .env keys (repo root)
------------------------------------------------------------------------------------
CONVERTER_PATH         Absolute path to converter.exe
DATA_RAW_ROOT          Root folder for raw data
DATA_PROCESSED_ROOT    Root folder for processed artifacts

Optional .env overrides:
MAX_PARALLEL_WORKERS
TASK_TIMEOUT_SECONDS
MAX_RETRIES_ON_FAILURE
SKIP_EXISTING_ARTIFACTS

------------------------------------------------------------------------------------
YAML config keys (configs/ingest.yaml)
------------------------------------------------------------------------------------
exchange: binance
feed: ws_depth20

pipeline:
  timeout_seconds: 1200
  max_retries: 2
  skip_existing: true
  max_parallel_workers: 4

layout:
  raw: "{exchange}/{feed}/{symbol}/{date}"
  processed: "{date}"

logging:
  dir: "logs/ingest"

------------------------------------------------------------------------------------
Examples
------------------------------------------------------------------------------------
Single date:
  python python\\md\\ingest.py --symbol BTCUSDT --date 2025-12-25 --config configs\\ingest.yaml

All dates discovered from raw folders:
  python python\\md\\ingest.py --symbol BTCUSDT --all-dates --config configs\\ingest.yaml

Date range (inclusive):
  python python\\md\\ingest.py --symbol BTCUSDT --range 2025-12-01 2025-12-31 --config configs\\ingest.yaml

Dry run:
  python python\\md\\ingest.py --symbol BTCUSDT --all-dates --dry-run
"""

from __future__ import annotations

import os
import time
import argparse
import subprocess
import multiprocessing
from dataclasses import dataclass
from datetime import datetime, timedelta, date as Date
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Optional, List, Dict, Any

import yaml
from dotenv import load_dotenv


# ----------------------------
# Models
# ----------------------------

@dataclass(frozen=True)
class Job:
    exe: Path
    src: Path
    dst: Path
    timeout_s: int
    retries: int
    skip_if_exists: bool
    log_dir: Optional[Path]
    tag: str  


@dataclass(frozen=True)
class Result:
    src: str
    ok: bool
    secs: float
    msg: str
    tag: str


# ----------------------------
# Config helpers
# ----------------------------

def load_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def require_env(name: str) -> str:
    v = os.getenv(name)
    if v is None or v.strip() == "":
        raise SystemExit(f"[FATAL] Missing required env var: {name}")
    return v


def require_env_path(name: str) -> Path:
    return Path(require_env(name)).expanduser().resolve()


def cfg_get(cfg: Dict[str, Any], dotted: str, default=None):
    cur: Any = cfg
    for part in dotted.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def resolve_layout(template: str, **kwargs) -> Path:
    try:
        return Path(template.format(**kwargs))
    except KeyError as e:
        raise SystemExit(f"[FATAL] Missing placeholder {e} for layout template: {template}")


# ----------------------------
# Date selection
# ----------------------------

def parse_date(s: str) -> Date:
    return datetime.strptime(s, "%Y-%m-%d").date()


def date_range(d0: Date, d1: Date):
    cur = d0
    step = timedelta(days=1)
    while cur <= d1:
        yield cur.strftime("%Y-%m-%d")
        cur += step


def discover_dates(raw_root: Path, exchange: str, feed: str, symbol: str) -> List[str]:
    symbol_root = raw_root / exchange / feed / symbol
    if not symbol_root.exists():
        raise SystemExit(f"[FATAL] Symbol folder not found for discovery: {symbol_root}")

    dates = sorted([p.name for p in symbol_root.iterdir() if p.is_dir()])

    out: List[str] = []
    for d in dates:
        try:
            parse_date(d)
            out.append(d)
        except ValueError:
            continue
    return out


def resolve_dates(args: argparse.Namespace, raw_root: Path, exchange: str, feed: str) -> List[str]:
    if args.date:
        return [args.date]
    if args.dates:
        return [d.strip() for d in args.dates.split(",") if d.strip()]
    if args.date_from and args.date_to:
        d0 = parse_date(args.date_from)
        d1 = parse_date(args.date_to)
        return list(date_range(d0, d1))
    if args.all_dates:
        return discover_dates(raw_root, exchange, feed, args.symbol)

    raise SystemExit("[FATAL] Provide one of: --date, --dates, --date-from/--date-to, or --all-dates")


# ----------------------------
# Conversion helpers
# ----------------------------

def is_valid_snap(path: Path, min_bytes: int = 1024) -> bool:
    try:
        return path.exists() and path.stat().st_size > min_bytes
    except OSError:
        return False


def write_log(log_path: Path, content: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_path.write_text(content, encoding="utf-8", errors="replace")


def convert_one(job: Job) -> Result:
    exe, inp, out = job.exe, job.src, job.dst

    if job.skip_if_exists and is_valid_snap(out):
        return Result(inp.name, True, 0.0, f"SKIP (exists) -> {out.name}", job.tag)

    out.parent.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    last_err: Optional[str] = None
    stderr_text_last: str = ""

    for attempt in range(job.retries + 1):
        try:
            cp = subprocess.run(
                [str(exe), str(inp), str(out)],
                text=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
                timeout=job.timeout_s,
            )

            stderr_text_last = (cp.stderr or "")
            if cp.returncode == 0 and is_valid_snap(out):
                if job.log_dir:
                    write_log(job.log_dir / f"{inp.stem}.log", stderr_text_last)
                return Result(inp.name, True, time.time() - t0, f"OK -> {out.name}", job.tag)

            tail = stderr_text_last.strip()
            last_err = tail[-2000:] if tail else f"returncode={cp.returncode}"

        except subprocess.TimeoutExpired:
            last_err = f"timeout after {job.timeout_s}s"
        except Exception as e:
            last_err = repr(e)

        # best-effort cleanup of invalid outputs
        try:
            if out.exists() and not is_valid_snap(out):
                out.unlink()
        except OSError:
            pass

        if attempt < job.retries:
            time.sleep(0.25 * (attempt + 1))

    if job.log_dir:
        payload = stderr_text_last.strip() or (last_err or "")
        write_log(job.log_dir / f"{inp.stem}.log", payload)

    return Result(inp.name, False, time.time() - t0, f"FAIL: {last_err}", job.tag)


# ----------------------------
# CLI
# ----------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Parallel CSV.GZ -> .snap ingestion runner (multi-date)")
    p.add_argument("--symbol", required=True, help="e.g. BTCUSDT")
    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--date", help="Single date YYYY-MM-DD")
    g.add_argument("--dates", help="Comma-separated dates YYYY-MM-DD,YYYY-MM-DD,...")
    g.add_argument("--all-dates", action="store_true", help="Discover all date folders under raw layout")
    g.add_argument("--range", nargs=2, metavar=("DATE_FROM", "DATE_TO"), help="Date range inclusive (YYYY-MM-DD YYYY-MM-DD)")

    p.add_argument("--config", default="configs/ingest_data.yaml", help="Path relative to repo root or absolute")
    p.add_argument("--workers", type=int, default=None, help="Override max workers")
    p.add_argument("--dry-run", action="store_true", help="Print planned work without running converter")
    p.add_argument("--limit", type=int, default=None, help="Limit number of files total (for testing)")
    return p.parse_args()


# ----------------------------
# Main
# ----------------------------

def main() -> int:
    args = parse_args()

    # Normalise range args into date_from/date_to for resolve_dates()
    args.date_from = None
    args.date_to = None
    if args.range:
        args.date_from, args.date_to = args.range[0], args.range[1]

    ROOT = Path(__file__).resolve().parents[2]

    # Load root-level .env (machine settings)
    load_dotenv(ROOT / ".env")

    converter_exe = require_env_path("CONVERTER_PATH")
    raw_root = require_env_path("DATA_RAW_ROOT")
    out_root = require_env_path("DATA_PROCESSED_ROOT")

    # Load YAML config (pipeline defaults)
    cfg_path = Path(args.config)
    if not cfg_path.is_absolute():
        cfg_path = (ROOT / cfg_path).resolve()
    if not cfg_path.exists():
        print(f"[FATAL] Config not found: {cfg_path}")
        return 2

    cfg = load_yaml(cfg_path)

    exchange = str(cfg.get("exchange", "binance"))
    feed = str(cfg.get("feed", "ws_depth20"))

    # Pipeline settings (env overrides allowed)
    timeout_s = int(os.getenv("TASK_TIMEOUT_SECONDS", cfg_get(cfg, "pipeline.timeout_seconds", 1200)))
    retries = int(os.getenv("MAX_RETRIES_ON_FAILURE", cfg_get(cfg, "pipeline.max_retries", 2)))
    skip_existing = str(os.getenv("SKIP_EXISTING_ARTIFACTS", cfg_get(cfg, "pipeline.skip_existing", "true"))).lower() in ("1", "true", "yes", "y")

    default_workers = max(1, min(4, multiprocessing.cpu_count() - 1))
    workers_env = os.getenv("MAX_PARALLEL_WORKERS")
    max_workers_cfg = int(cfg_get(cfg, "pipeline.max_parallel_workers", default_workers))
    max_workers = args.workers or (int(workers_env) if workers_env else max_workers_cfg)

    raw_tmpl = str(cfg_get(cfg, "layout.raw", "{exchange}/{feed}/{symbol}/{date}"))
    out_tmpl = str(cfg_get(cfg, "layout.processed", "{date}"))

    log_dir_rel = str(cfg_get(cfg, "logging.dir", "logs/ingest"))
    log_dir = (ROOT / log_dir_rel / args.symbol).resolve()

    # Validate inputs
    if not converter_exe.exists():
        print(f"[FATAL] converter.exe not found: {converter_exe}")
        return 2
    if not raw_root.exists():
        print(f"[FATAL] DATA_RAW_ROOT not found: {raw_root}")
        return 2

    # Resolve dates list
    dates = resolve_dates(args, raw_root, exchange, feed)

    # Build all jobs across all selected dates
    jobs: List[Job] = []
    for d in dates:
        raw_dir = (raw_root / resolve_layout(raw_tmpl, exchange=exchange, feed=feed, symbol=args.symbol, date=d)).resolve()
        out_dir = (out_root / resolve_layout(out_tmpl, date=d, symbol=args.symbol, exchange=exchange, feed=feed)).resolve()

        if not raw_dir.exists():
            print(f"[WARN] Missing raw_dir; skipping date={d}: {raw_dir}")
            continue

        out_dir.mkdir(parents=True, exist_ok=True)
        csv_files = sorted(raw_dir.glob("*.csv.gz"))

        for p in csv_files:
            dst = out_dir / p.name.replace(".csv.gz", ".snap")
            jobs.append(
                Job(
                    exe=converter_exe,
                    src=p,
                    dst=dst,
                    timeout_s=timeout_s,
                    retries=retries,
                    skip_if_exists=skip_existing,
                    log_dir=(log_dir / d),
                    tag=f"{args.symbol}/{d}",
                )
            )

    if args.limit is not None:
        jobs = jobs[: max(0, args.limit)]

    if not jobs:
        print("[INFO] No jobs to run.")
        return 0

    print(
        f"[INFO] config={cfg_path}\n"
        f"[INFO] converter={converter_exe}\n"
        f"[INFO] raw_root={raw_root}\n"
        f"[INFO] out_root={out_root}\n"
        f"[INFO] exchange={exchange} feed={feed} symbol={args.symbol}\n"
        f"[INFO] dates={len(dates)} jobs={len(jobs)} workers={max_workers} retries={retries} timeout_s={timeout_s} skip_existing={skip_existing}\n"
        f"[INFO] logs={log_dir}"
    )

    if args.dry_run:
        for j in jobs[:25]:
            print(f"[DRY] [{j.tag}] {j.src.name} -> {j.dst}")
        if len(jobs) > 25:
            print(f"[DRY] ... and {len(jobs) - 25} more")
        return 0

    results: List[Result] = []
    with ProcessPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(convert_one, j) for j in jobs]
        for fut in as_completed(futures):
            r = fut.result()
            results.append(r)
            print(f"{'SUCCESS' if r.ok else 'ERROR  '}: [{r.tag}] {r.src} ({r.secs:.2f}s) {r.msg}")

    fails = [r for r in results if not r.ok]
    print(f"\nSummary: ok={len(results) - len(fails)}/{len(results)} failed={len(fails)}")
    if fails:
        # group by tag
        by_tag: Dict[str, List[Result]] = {}
        for r in fails:
            by_tag.setdefault(r.tag, []).append(r)

        print("Failed inputs:")
        for tag, rs in sorted(by_tag.items()):
            print(f"  [{tag}] failed={len(rs)}")
            for rr in rs[:10]:
                print(f"    - {rr.src}: {rr.msg}")
            if len(rs) > 10:
                print(f"    ... {len(rs) - 10} more")

        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
