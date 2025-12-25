"""
Capture Binance partial book depth snapshots (top 20 levels) to hourly gzipped CSV + SHA256.

Binance stream: <symbol>@depth20@100ms (top 20 levels, 100ms updates).
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import datetime as dt
import gzip
import hashlib
import signal
import time

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple, List

import yaml
import json

import websockets


def load_yaml(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def cfg_get(cfg: dict, path: str, default):
    cur = cfg
    for k in path.split("."):
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def floor_hour(ts: dt.datetime) -> dt.datetime:
    return ts.replace(minute=0, second=0, microsecond=0)


def sha256_file(path: Path, chunk: int = 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def atomic_rename(tmp: Path, final: Path) -> None:
    final.parent.mkdir(parents=True, exist_ok=True)
    tmp.replace(final)


def build_header(depth: int) -> List[str]:
    cols = ["ts_event_ms", "ts_recv_ns"]
    for i in range(1, depth + 1):
        cols += [f"bid_p{i}", f"bid_q{i}"]
    for i in range(1, depth + 1):
        cols += [f"ask_p{i}", f"ask_q{i}"]
    return cols


@dataclass
class RollingWriter:
    root: Path
    venue: str
    symbol: str
    depth: int

    cur_hour: Optional[dt.datetime] = None
    tmp_path: Optional[Path] = None
    final_path: Optional[Path] = None
    gz: Optional[gzip.GzipFile] = None
    csvw: Optional[csv.writer] = None
    rows_written: int = 0

    def _paths_for_hour(self, hour: dt.datetime) -> Tuple[Path, Path]:
        day = hour.strftime("%Y-%m-%d")
        hh = hour.strftime("%H")
        out_dir = (
            self.root / "raw" / self.venue / f"ws_depth{self.depth}" / self.symbol / day
        )
        final = out_dir / f"{self.symbol}_depth{self.depth}_{day}_{hh}.csv.gz"
        tmp = out_dir / (final.name + ".part")
        return tmp, final

    def _open_new(self, hour: dt.datetime) -> None:
        self.close()  # close previous if any

        tmp, final = self._paths_for_hour(hour)
        tmp.parent.mkdir(parents=True, exist_ok=True)

        self.cur_hour = hour
        self.tmp_path = tmp
        self.final_path = final

        self.gz = gzip.open(tmp, "wt", newline="", encoding="utf-8")
        self.csvw = csv.writer(self.gz)
        self.csvw.writerow(build_header(self.depth))
        self.rows_written = 0

    def maybe_roll(self, now: dt.datetime) -> None:
        h = floor_hour(now)
        if self.cur_hour is None:
            self._open_new(h)
        elif h != self.cur_hour:
            self._finalise_current()
            self._open_new(h)

    def write_row(self, row: List[str]) -> None:
        if not self.csvw:
            raise RuntimeError("writer not open")
        self.csvw.writerow(row)
        self.rows_written += 1

    def _finalise_current(self) -> None:
        if not (self.gz and self.tmp_path and self.final_path):
            return

        self.gz.flush()
        self.gz.close()
        self.gz = None
        self.csvw = None

        # Atomic finalise
        atomic_rename(self.tmp_path, self.final_path)

        # Write checksum file
        digest = sha256_file(self.final_path)
        checksum_path = self.final_path.with_suffix(self.final_path.suffix + ".sha256")
        checksum_path.write_text(
            f"{digest}  {self.final_path.name}\n", encoding="utf-8"
        )

    def close(self) -> None:
        # finalise if something is open
        if self.gz and self.tmp_path and self.final_path:
            self._finalise_current()
        self.cur_hour = None
        self.tmp_path = None
        self.final_path = None
        self.gz = None
        self.csvw = None
        self.rows_written = 0


def build_row(msg: dict, recv_ns: int, depth: int) -> list[str]:
    # Event time (not always present)
    ts_event_ms = str(
        msg.get("E")  # diff-depth / some streams
        or msg.get("eventTime")  # some variants
        or ""  # partial depth often has none
    )

    # Depth keys differ by stream
    bids_raw = msg.get("b") or msg.get("bids") or []
    asks_raw = msg.get("a") or msg.get("asks") or []

    def normalise(levels):
        out = []
        for i in range(depth):
            if i < len(levels):
                p, q = levels[i]
                out += [p, q]
            else:
                out += ["", ""]
        return out

    row = [ts_event_ms, str(recv_ns)]
    row += normalise(bids_raw)
    row += normalise(asks_raw)
    return row


async def capture_loop(
    cfg: dict, symbol: str, depth: int, interval_ms: int, out_root: Path
) -> None:
    ws_base = cfg_get(cfg, "ws.base_url", "wss://stream.binance.com:9443/ws")
    stream = f"{symbol.lower()}@depth{depth}@{interval_ms}ms"
    url = f"{ws_base}/{stream}"

    writer = RollingWriter(
        root=out_root, venue="binance", symbol=symbol.upper(), depth=depth
    )

    stop = asyncio.Event()

    def _handle_stop(*_: object) -> None:
        stop.set()

    signal.signal(signal.SIGINT, _handle_stop)
    signal.signal(signal.SIGTERM, _handle_stop)

    backoff = float(cfg_get(cfg, "ws.reconnect.initial_backoff_sec", 0.5))
    max_backoff = float(cfg_get(cfg, "ws.reconnect.max_backoff_sec", 10.0))
    flush_every = int(cfg_get(cfg, "output.flush_every_rows", 500))
    msg_count = 0

    # Ensure a file is opened immediately
    writer.maybe_roll(utc_now())
    print(f"[INFO] Output root: {out_root.resolve()}")
    print(f"[INFO] Connecting to: {url}")

    while not stop.is_set():
        try:
            print("[INFO] WS connect...")

            async with websockets.connect(
                url,
                open_timeout=cfg_get(cfg, "ws.open_timeout_sec", 10),
                ping_interval=cfg_get(cfg, "ws.ping_interval_sec", 20),
                ping_timeout=cfg_get(cfg, "ws.ping_timeout_sec", 20),
                close_timeout=cfg_get(cfg, "ws.close_timeout_sec", 5),
                max_queue=cfg_get(cfg, "ws.max_queue", 256),
            ) as ws:
                print("[INFO] WS connected.")

                while not stop.is_set():
                    # IMPORTANT: don't hang forever waiting for recv
                    raw = await asyncio.wait_for(
                        ws.recv(), timeout=cfg_get(cfg, "ws.recv_timeout_sec", 30)
                    )
                    recv_ns = time.time_ns()
                    now = utc_now()
                    writer.maybe_roll(now)

                    import json

                    msg = json.loads(raw)

                    row = build_row(msg, recv_ns, depth)
                    writer.write_row(row)
                    msg_count += 1

                    if (
                        writer.gz
                        and flush_every > 0
                        and (writer.rows_written % flush_every == 0)
                    ):
                        writer.gz.flush()

                    if msg_count == 1:
                        print("[INFO] First message received; writing data.")
                    if msg_count % 500 == 0:
                        print(
                            f"[INFO] messages={msg_count}, current_hour={writer.cur_hour.isoformat()}"
                        )

        except asyncio.TimeoutError:
            print("[WARN] Timeout (no messages). Reconnecting...")
        except Exception as e:
            print(f"[WARN] WS error: {type(e).__name__}: {e}. Reconnecting...")

        await asyncio.sleep(backoff)
        backoff = min(max_backoff, backoff * 2)

    writer.close()
    print("[INFO] Stopped cleanly.")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--config",
        type=Path,
        default=Path("configs") / "binance_data.yaml",
        help="Path to YAML config (default: configs/default.yaml)",
    )
    args = ap.parse_args()

    cfg = load_yaml(args.config)

    cfg_hash = hashlib.sha1(json.dumps(cfg, sort_keys=True).encode()).hexdigest()[:8]
    print(f"[INFO] Using config: {args.config} (hash={cfg_hash})")

    symbol = cfg["symbol"].upper()
    depth = int(cfg["depth"])
    interval_ms = int(cfg["interval_ms"])
    out_root = Path(cfg["output"]["root"])

    if depth not in (5, 10, 20):
        raise SystemExit("depth must be one of {5,10,20} for partial depth stream")

    try:
        asyncio.run(capture_loop(cfg, symbol, depth, interval_ms, out_root))
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
