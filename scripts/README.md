# Order Book Capture (Binance) - Python Collector

This repository includes a Python-based collector that captures Binance partial order book depth snapshots (top N levels, typically 20) from a WebSocket stream and persists them to rolled, gzipped CSV files with per-file SHA256 checksums.

---

## What this collector captures

The collector subscribes to Binance’s **partial book depth** WebSocket stream:

- `<symbol>@depth<levels>@<interval>` (e.g., `btcusdt@depth20@100ms`)

Each message contains the top-of-book levels for bids and asks (price, quantity pairs). This data represents the current visible resting liquidity for the top $N$ levels.

---

## Capture workflow overview

1. **Configuration selection**
   - The collector loads a YAML config (default: `configs/binance_data.yaml`).
   - The config defines the market (symbol), stream parameters, output location, and network timeouts.

2. **WebSocket subscription**
   - Connects to Binance WebSocket endpoint (configurable).
   - Subscribes by URL path (e.g., `.../ws/btcusdt@depth20@100ms`).

3. **Message parsing**
   - Parses incoming JSON messages.
   - Supports multiple Binance depth schemas (field names differ between endpoints/markets):
     - bids/asks may appear as `b`/`a` or `bids`/`asks`
     - event time may appear as `E` or may be absent
   - Always records:
     - `ts_recv_ns`: local receive timestamp (nanoseconds)
     - optional event timestamp if provided by the stream

4. **File rolling**
   - Writes rows continuously to a gzipped CSV “.part” file.
   - Rolls files on the hour (configurable conceptually; current implementation rolls hourly).
   - On roll (or shutdown), finalises the current file:
     - closes gzip stream
     - atomically renames `.part` → `.csv.gz`
     - computes SHA256 and writes `<file>.sha256`

5. **Reconnection and timeouts**
   - Uses:
     - a connection timeout (`open_timeout_sec`)
     - a receive timeout (`recv_timeout_sec`)
   - If no messages arrive within `recv_timeout_sec`, the collector reconnects.
   - Reconnection uses exponential backoff with configurable min/max.

---

## Output format

```
<data_root>/
raw/
binance/
ws_depth20/
BTCUSDT/
YYYY-MM-DD/
BTCUSDT_depth20_YYYY-MM-DD_HH.csv.gz
BTCUSDT_depth20_YYYY-MM-DD_HH.csv.gz.sha256
```

### CSV schema (wide format)
Header includes:
- `ts_event_ms` (string; may be empty if not provided by stream)
- `ts_recv_ns` (int64; always present)
- bids: `bid_p1,bid_q1,...,bid_pN,bid_qN`
- asks: `ask_p1,ask_q1,...,ask_pN,ask_qN`

This wide format is convenient for:
- fast ingestion into numpy/pandas/polars
- straightforward feature computation
- simulator pipelines expecting fixed-shape observations

---

## Configuration

The collector is configured via YAML. The YAML file is the source of truth for:
- symbol
- depth level count
- update interval
- output root path
- WebSocket endpoint and timeout policy
- reconnect/backoff policy
- flush cadence (optional)

### Example: `configs/binance_data.yaml`

```yaml
job:
  name: binance_spot_btc_depth20

venue: binance
market: spot

symbol: BTCUSDT
depth: 20
interval_ms: 100

ws:
  base_url: wss://stream.binance.com:9443/ws
  open_timeout_sec: 10
  recv_timeout_sec: 30
  ping_interval_sec: 20
  ping_timeout_sec: 20
  close_timeout_sec: 5
  max_queue: 256
  reconnect:
    initial_backoff_sec: 0.5
    max_backoff_sec: 10.0

output:
  root: ./data
  roll: hourly
  compression: gzip
  checksum: sha256
  flush_every_rows: 5000

validation:
  require_cross_free: true
  require_monotonic_levels: true
```

### Running with default config

```
python scripts/capture_order_book.py
```

### Running with custom config

```
python scripts/capture_order_book.py --config configs/binance_spot_btc_depth20.yaml
```