"""
Zero-copy reader for `.snap` files (C++-produced, mmappable, fixed-record format)
using numpy.memmap, with configuration loaded from ROOT/configs/.

Design goals
- Zero-copy access to records: no CSV parsing, no full-file load.
- Strict header validation: fail fast on schema mismatch/corruption.
- Config-driven defaults: symbol, depth, version, data roots.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union

import numpy as np
import yaml

# =============================================================================
# Constants must match the C++ schema
# =============================================================================

MAGIC = 0x4C32424F  # "L2BO" little-endian
ENDIAN_CHECK = 0x01020304
SUPPORTED_VERSIONS = {1}

# Sentinel contract (must match C++)
BID_NULL_PRICE_Q = 0
ASK_NULL_PRICE_Q = np.iinfo(np.int64).max


# =============================================================================
# Dtypes (must match the C++ binary layout; little-endian)
# =============================================================================

HEADER_DTYPE = np.dtype(
    [
        ("magic", "<u4"),
        ("version", "<u2"),
        ("depth", "<u2"),
        ("record_size", "<u4"),
        ("endian_check", "<u4"),
        ("price_scale", "<i8"),
        ("qty_scale", "<i8"),
        ("record_count", "<u8"),
    ],
    align=False,
)

LEVEL_DTYPE = np.dtype(
    [
        ("price_q", "<i8"),
        ("qty_q", "<i8"),
    ],
    align=False,
)


def record_dtype(depth: int) -> np.dtype:
    return np.dtype(
        [
            ("ts_event_ms", "<i8"),
            ("ts_recv_ns", "<i8"),
            ("bids", LEVEL_DTYPE, (depth,)),
            ("asks", LEVEL_DTYPE, (depth,)),
        ],
        align=False,
    )


# =============================================================================
# Config
# =============================================================================

@dataclass(frozen=True)
class SnapConfig:
    """
    Config loaded from ROOT/configs/<name>.yaml

    Required fields:
      data_root: path to project data directory (usually "data")
      processed_root: subdir under data_root where .snap live (e.g. "processed")

    Optional fields:
      venue: default venue name used in path building (e.g. "binance")
      dataset: dataset namespace (e.g. "l2_snap")
      version: dataset version tag (e.g. "v1")
      depth: default depth (e.g. 20)
      symbol: default symbol (e.g. "BTCUSDT")
      layout: how processed data is laid out ("flat" or "partitioned")
      snap_glob: optional glob pattern override (for flat layout)
    """
    project_root: Path
    data_root: Path
    processed_root: Path

    venue: str = "binance"
    dataset: str = "l2_snap"
    version: str = "v1"
    depth: int = 20
    symbol: str = "BTCUSDT"
    layout: str = "partitioned"
    snap_glob: Optional[str] = None

    @property
    def processed_dir(self) -> Path:
        return self.project_root / self.data_root / self.processed_root


def _find_project_root(start: Optional[Path] = None) -> Path:
    """
    Locate project root by searching upwards for a 'configs' directory.
    """
    cur = (start or Path.cwd()).resolve()
    for p in [cur, *cur.parents]:
        if (p / "configs").is_dir():
            return p
    raise FileNotFoundError("Could not locate project root (no ROOT/configs/ found).")


def load_config(config_name: str = "snap_reader.yaml", project_root: Optional[Path] = None) -> SnapConfig:
    """
    Load YAML config from ROOT/configs/<config_name>.
    If config_name has no suffix, '.yaml' is appended.
    """
    root = (project_root or _find_project_root()).resolve()
    name = config_name if config_name.endswith((".yaml", ".yml")) else f"{config_name}.yaml"
    path = root / "configs" / name
    if not path.exists():
        raise FileNotFoundError(f"Config not found: {path}")

    raw: Dict[str, Any]
    with path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    # Required
    if "data_root" not in raw or "processed_root" not in raw:
        raise ValueError("Config must define 'data_root' and 'processed_root'.")

    data_root = Path(raw["data_root"])
    processed_root = Path(raw["processed_root"])

    return SnapConfig(
        project_root=root,
        data_root=data_root,
        processed_root=processed_root,
        venue=str(raw.get("venue", "binance")),
        dataset=str(raw.get("dataset", "l2_snap")),
        version=str(raw.get("version", "v1")),
        depth=int(raw.get("depth", 20)),
        symbol=str(raw.get("symbol", "BTCUSDT")).upper(),
        layout=str(raw.get("layout", "partitioned")),
        snap_glob=raw.get("snap_glob"),
    )


# =============================================================================
# Reader
# =============================================================================

@dataclass(frozen=True)
class SnapHeader:
    path: Path
    magic: int
    version: int
    depth: int
    record_size: int
    endian_check: int
    price_scale: int
    qty_scale: int
    record_count: int


class SnapReader:
    """
    Memory-mapped `.snap` reader.

    - Validates header
    - Validates file size vs record size
    - Exposes records as numpy.memmap with structured dtype

    Usage:
      cfg = load_config("snap_reader.yaml")
      r = SnapReader.from_config(cfg, date="2025-12-25", hour="16")  # partitioned layout
      rec0 = r.records[0]
    """

    def __init__(self, path: Union[str, Path], mode: str = "r") -> None:
        self._path = Path(path)
        self._mode = mode

        if not self._path.exists():
            raise FileNotFoundError(str(self._path))

        # Read header (exactly one struct)
        hdr_arr = np.fromfile(self._path, dtype=HEADER_DTYPE, count=1)
        if hdr_arr.size != 1:
            raise ValueError(f"Failed to read header from {self._path}")

        h = hdr_arr[0]
        self._header = SnapHeader(
            path=self._path,
            magic=int(h["magic"]),
            version=int(h["version"]),
            depth=int(h["depth"]),
            record_size=int(h["record_size"]),
            endian_check=int(h["endian_check"]),
            price_scale=int(h["price_scale"]),
            qty_scale=int(h["qty_scale"]),
            record_count=int(h["record_count"]),
        )

        self._validate_header()

        self._record_dtype = record_dtype(self._header.depth)
        if self._header.record_size != self._record_dtype.itemsize:
            raise ValueError(
                f"Record size mismatch: header={self._header.record_size}, dtype={self._record_dtype.itemsize}"
            )

        # Infer count from file size
        file_size = self._path.stat().st_size
        payload = file_size - HEADER_DTYPE.itemsize
        if payload < 0 or payload % self._record_dtype.itemsize != 0:
            raise ValueError(
                f"Inconsistent file size: file={file_size}, header={HEADER_DTYPE.itemsize}, record={self._record_dtype.itemsize}"
            )
        inferred = payload // self._record_dtype.itemsize

        if self._header.record_count != 0 and self._header.record_count != inferred:
            raise ValueError(
                f"record_count mismatch: header={self._header.record_count}, inferred={inferred}"
            )

        self._count = int(inferred)

        self._records = np.memmap(
            self._path,
            dtype=self._record_dtype,
            mode=self._mode,
            offset=HEADER_DTYPE.itemsize,
            shape=(self._count,),
            order="C",
        )

    def _validate_header(self) -> None:
        h = self._header
        if h.magic != MAGIC:
            raise ValueError(f"Bad magic: {h.magic:#x} (expected {MAGIC:#x})")
        if h.version not in SUPPORTED_VERSIONS:
            raise ValueError(f"Unsupported version: {h.version} (supported: {sorted(SUPPORTED_VERSIONS)})")
        if h.depth <= 0:
            raise ValueError(f"Invalid depth: {h.depth}")
        if h.endian_check != ENDIAN_CHECK:
            raise ValueError(
                f"Endian check mismatch: {h.endian_check:#x} (expected {ENDIAN_CHECK:#x}). "
                "Wrong-endian file is not supported."
            )
        if h.price_scale <= 0 or h.qty_scale <= 0:
            raise ValueError(f"Invalid scales: price_scale={h.price_scale}, qty_scale={h.qty_scale}")

    @property
    def header(self) -> SnapHeader:
        return self._header

    @property
    def records(self) -> np.memmap:
        return self._records

    def __len__(self) -> int:
        return self._count

    def __getitem__(self, idx):
        return self._records[idx]

    # ---------------------------
    # Top-of-book helpers
    # ---------------------------

    def best_bid_ask_q(self, i: int) -> Tuple[int, int]:
        r = self._records[i]
        bid = int(r["bids"][0]["price_q"])
        ask = int(r["asks"][0]["price_q"])
        return bid, ask

    def best_bid_ask(self, i: int) -> Tuple[float, float]:
        bid_q, ask_q = self.best_bid_ask_q(i)
        ps = self._header.price_scale
        return bid_q / ps, ask_q / ps

    def mid_spread(self, i: int) -> Tuple[float, float]:
        bid_q, ask_q = self.best_bid_ask_q(i)
        if bid_q <= 0 or ask_q == ASK_NULL_PRICE_Q:
            return float("nan"), float("nan")
        ps = self._header.price_scale
        bid = bid_q / ps
        ask = ask_q / ps
        return 0.5 * (bid + ask), (ask - bid)

    def tob_valid(self, i: int) -> bool:
        bid_q, ask_q = self.best_bid_ask_q(i)
        return bid_q > 0 and ask_q != ASK_NULL_PRICE_Q and bid_q < ask_q

    # ---------------------------
    # Top-k access (structured views)
    # ---------------------------

    def topk_levels(self, i: int, k: int = 5):
        """
        Return (bids, asks) where each is a structured array view of shape (k,)
        with fields price_q, qty_q. Zero-copy.
        """
        if k < 1 or k > self._header.depth:
            raise ValueError(f"k must be in [1, {self._header.depth}]")
        r = self._records[i]
        return r["bids"][:k], r["asks"][:k]

    # ---------------------------
    # Config-driven constructors
    # ---------------------------

    @staticmethod
    def _resolve_snap_path(
        cfg: SnapConfig,
        *,
        symbol: Optional[str] = None,
        depth: Optional[int] = None,
        date: Optional[str] = None,
        hour: Optional[str] = None,
        filename: Optional[str] = None,
    ) -> Path:
        """
        Resolve a .snap path given config and optional partition keys.

        Supported layouts:
          - partitioned:
              <processed_dir>/<venue>/<dataset>/<version>/depth<depth>/<symbol>/<YYYY-MM-DD>/<...>.snap
            If filename not provided, it will glob for the hour.
          - flat:
              uses cfg.snap_glob relative to processed_dir, or requires filename
        """
        symbol_u = (symbol or cfg.symbol).upper()
        depth_v = int(depth or cfg.depth)

        base = cfg.processed_dir 

        if cfg.layout == "partitioned":
            if date is None:
                raise ValueError("partitioned layout requires 'date' (YYYY-MM-DD)")
            day_dir = base / date
            if filename is not None:
                return day_dir / filename

            if hour is None:
                raise ValueError("partitioned layout requires either filename or 'hour' (HH)")

            # Default naming convention (adjust if your converter uses a different one)
            # Example: BTCUSDT_depth20_2025-12-25_16.snap
            pattern = f"{symbol_u}_depth{depth_v}_{date}_{hour}.snap"
            candidate = day_dir / pattern
            if candidate.exists():
                return candidate

            # Fallback: glob any matching hour
            matches = sorted(day_dir.glob(f"*_{date}_{hour}.snap"))
            if not matches:
                raise FileNotFoundError(f"No snap found under {day_dir} for hour={hour}")
            if len(matches) > 1:
                raise RuntimeError(f"Ambiguous snaps for {date} {hour}: {matches}")
            return matches[0]

        if cfg.layout == "flat":
            # If snap_glob specified, use it to locate file(s)
            if filename is not None:
                return cfg.processed_dir / filename
            if cfg.snap_glob is None:
                raise ValueError("flat layout requires either filename or cfg.snap_glob")
            matches = sorted((cfg.processed_dir).glob(cfg.snap_glob))
            if not matches:
                raise FileNotFoundError(f"No snaps found with glob: {cfg.snap_glob}")
            if len(matches) > 1:
                raise RuntimeError(f"Glob matched multiple snaps; provide filename: {matches[:5]}...")
            return matches[0]

        raise ValueError(f"Unknown layout: {cfg.layout}")

    @classmethod
    def from_config(
        cls,
        cfg: SnapConfig,
        *,
        mode: str = "r",
        symbol: Optional[str] = None,
        depth: Optional[int] = None,
        date: Optional[str] = None,
        hour: Optional[str] = None,
        filename: Optional[str] = None,
    ) -> "SnapReader":
        path = cls._resolve_snap_path(
            cfg, symbol=symbol, depth=depth, date=date, hour=hour, filename=filename
        )
        return cls(path, mode=mode)
