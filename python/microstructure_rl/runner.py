from __future__ import annotations

import json
import logging
import os
import platform
import subprocess
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union

from .invariants import FillConservation

from .artifacts import (
    append_jsonl,
    file_sha256,
    make_run_dir,
    sha256_text,
    write_csv,
    write_json,
)
from .fingerprint import fingerprint_file
from .invariants import InvariantChecker
from .markout import MarkoutTracker
from .spec import ScenarioSpec


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _git_info(repo_root: Path) -> Dict[str, object]:
    def _run(args: list[str]) -> Optional[str]:
        try:
            r = subprocess.run(
                args,
                cwd=str(repo_root),
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                text=True,
                check=True,
            )
            return r.stdout.strip()
        except Exception:
            return None

    sha = _run(["git", "rev-parse", "HEAD"])
    dirty = _run(["git", "status", "--porcelain"])
    return {"git_sha": sha, "git_dirty": bool(dirty) if dirty is not None else None}


def _mid_from_record(rec: Any) -> Optional[int]:
    try:
        bid = int(rec.best_bid_price_q)
        ask = int(rec.best_ask_price_q)
        if bid > 0 and ask > 0 and bid < ask:
            return (bid + ask) // 2
    except Exception:
        return None
    return None


def _build_params_and_ledger(mrl: Any, spec: ScenarioSpec) -> Tuple[Any, Any]:
    sim = mrl.sim
    p = sim.SimulatorParams()
    # required resource caps
    p.max_orders = int(spec.max_orders)
    p.max_events = int(spec.max_events)
    p.alpha_ppm = int(spec.alpha_ppm)

    # optional extended fields (present in your current bindings)
    if hasattr(p, "maker_fee_ppm"):
        p.maker_fee_ppm = int(spec.maker_fee_ppm)
    if hasattr(p, "taker_fee_ppm"):
        p.taker_fee_ppm = int(spec.taker_fee_ppm)
    if hasattr(p, "outbound_latency_ns"):
        p.outbound_latency_ns = int(spec.outbound_latency_ns)
    if hasattr(p, "observation_latency_ns"):
        p.observation_latency_ns = int(spec.observation_latency_ns)

    led = sim.Ledger()
    led.cash_q = int(spec.initial_cash_q)
    led.position_qty_q = int(spec.initial_position_qty_q)
    if hasattr(led, "locked_cash_q"):
        led.locked_cash_q = int(spec.initial_locked_cash_q)
    if hasattr(led, "locked_position_qty_q"):
        led.locked_position_qty_q = int(spec.initial_locked_position_qty_q)

    return p, led


def _place_demo_orders(
    mrl: Any, ex: Any, *, mid_q: int, qty_q: int, tick_q: int
) -> Tuple[int, int]:
    sim = mrl.sim
    bid_px = int(mid_q - tick_q)
    ask_px = int(mid_q + tick_q)

    b = sim.LimitOrderRequest()
    s = sim.LimitOrderRequest()

    b.side = sim.Side.Buy
    b.price_q = bid_px
    b.qty_q = int(qty_q)
    if hasattr(b, "tif"):
        b.tif = sim.Tif.GTC

    s.side = sim.Side.Sell
    s.price_q = ask_px
    s.qty_q = int(qty_q)
    if hasattr(s, "tif"):
        s.tif = sim.Tif.GTC

    idb = int(ex.place_limit(b))
    ida = int(ex.place_limit(s))
    return idb, ida


def run_scenario(
    *,
    spec: ScenarioSpec,
    out_root: Path,
    strict: bool,
    log_level: str,
) -> Path:
    import microstructure_rl._core as mrl  # local import to keep module load explicit

    logger = logging.getLogger("mrl.scenario")
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    h = logging.StreamHandler()
    h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.handlers[:] = [h]

    repo_root = Path.cwd()
    out_root = out_root.resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    # Run id is hash(spec canonical json + data fingerprint + git sha)
    dfp = fingerprint_file(Path(spec.snap_path))
    spec_json = spec.canonical_json()
    git = _git_info(repo_root)
    run_id_material = {
        "spec": json.loads(spec_json),
        "data": dfp.to_dict(),
        "git_sha": git.get("git_sha"),
    }
    run_id = sha256_text(
        json.dumps(run_id_material, sort_keys=True, separators=(",", ":"))
    )[0:16]
    ts = _utc_stamp()
    paths = make_run_dir(out_root, run_id, ts)

    # Persist spec + manifest + replay token
    spec.save(paths.spec_json)

    manifest: Dict[str, object] = {
        "run_id": run_id,
        "timestamp_utc": ts,
        "core_module_file": getattr(mrl, "__file__", None),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "data_fingerprint": dfp.to_dict(),
        **git,
    }
    write_json(paths.manifest_json, manifest)

    replay_token: Dict[str, object] = {
        "run_id": run_id,
        "timestamp_utc": ts,
        "spec_sha256": sha256_text(spec_json),
        "spec_path": str(paths.spec_json),
        "data_fingerprint": dfp.to_dict(),
        **git,
        "how_to_rerun": f'python -m microstructure_rl.scenario run --spec "{paths.spec_json}"',
    }
    write_json(paths.replay_token_json, replay_token)

    logger.info("core module: %s", getattr(mrl, "__file__", "<no file>"))
    logger.debug("exports(sim): %s", [x for x in dir(mrl.sim) if not x.startswith("_")])

    # Setup engine
    params, init_ledger = _build_params_and_ledger(mrl, spec)
    ex = mrl.sim.MarketSimulator(params)
    ex.reset(int(spec.start_ts_ns), init_ledger)

    # ---------------------------
    # Invariants: conservation in ledger units
    # ---------------------------
    init_tot = FillConservation.snapshot_ledger_total(ex)
    inv = FillConservation(
        initial_cash_total_q=init_tot.cash_total_q,
        initial_pos_total_q=init_tot.pos_total_q,
    )
    fills_cursor = 0  # only ingest new fills

    rk = mrl.md_l2.ReplayKernel(spec.snap_path)
    first = rk.next()
    if first is None:
        raise RuntimeError(f"empty snap: {spec.snap_path}")

    # Initialize trackers
    checker = InvariantChecker(
        initial_cash_q=int(spec.initial_cash_q),
        tolerance_q=int(spec.cash_residual_tolerance_q),
    )
    markout = (
        MarkoutTracker(list(spec.markout_horizons_steps))
        if spec.enable_markout
        else None
    )

    # Cursors for snapshot-based consumption
    last_fills_n = 0
    last_events_n = 0

    placed_orders = 0
    steps = 0
    failures = 0

    def checkpoint(step: int, rec: Any) -> None:
        nonlocal last_fills_n, last_events_n, failures

        # Snapshot deltas (expensive: copies) only at checkpoint cadence
        fills = ex.fills()
        events = ex.events()

        new_fills = fills[last_fills_n:]
        new_events = events[last_events_n:]
        last_fills_n = len(fills)
        last_events_n = len(events)

        # Update expected accounting from new fills/events and persist streams
        fill_rows = []
        for f in new_fills:
            checker.observe_fill(f)
            fill_rows.append(
                {
                    "ts": int(f.ts),
                    "order_id": int(f.order_id),
                    "liq": getattr(f.liq, "name", str(f.liq)),
                    "side": getattr(f.side, "name", str(f.side)),
                    "price_q": int(f.price_q),
                    "qty_q": int(f.qty_q),
                    "notional_cash_q": int(f.notional_cash_q),
                    "fee_cash_q": int(f.fee_cash_q),
                }
            )

        event_rows = []
        for e in new_events:
            checker.observe_event(e)
            event_rows.append(
                {
                    "ts": int(e.ts),
                    "order_id": int(e.order_id),
                    "type": getattr(e.type, "name", str(e.type)),
                    "state": getattr(e.state, "name", str(e.state)),
                    "reject_reason": getattr(
                        e.reject_reason, "name", str(e.reject_reason)
                    ),
                }
            )

        if fill_rows:
            append_jsonl(paths.fills_jsonl, fill_rows)
        if event_rows:
            append_jsonl(paths.events_jsonl, event_rows)

        # Mid (best-effort) for audit/wealth/markout
        mid_q = _mid_from_record(rec)

        # Markout updates: register new fills at the *step of checkpoint* (approx),
        # and update tracker at every step in the loop (see below)
        if markout is not None and mid_q is not None:
            for f in new_fills:
                markout.on_fill(f, step=step, mid_q=mid_q)

        # Contract checks that require orders snapshot: do it only at checkpoint
        orders = ex.orders()
        rej_msg = checker.check_reject_implies_terminal(orders=orders, strict=strict)
        if rej_msg:
            if strict and not rej_msg.startswith("WARN:"):
                failures += 1
                logger.error("contract violation: %s", rej_msg)
            else:
                logger.warning("%s", rej_msg)

        # Accounting residual check
        led = ex.ledger
        row, err = checker.check_accounting_residual(
            ledger=led,
            step=step,
            mid_q=mid_q,
            position_qty_q=int(getattr(led, "position_qty_q", 0)),
        )
        row["ts_ns"] = int(ex.now)
        append_jsonl(paths.audit_jsonl, [row])

        if err:
            failures += 1
            logger.error("invariant FAIL at step=%d: %s", step, err)

        cash = int(getattr(led, "cash_q", 0))
        locked_cash = int(getattr(led, "locked_cash_q", 0))
        pos = int(getattr(led, "position_qty_q", 0))
        locked_pos = int(getattr(led, "locked_position_qty_q", 0))
        logger.info(
            "progress | steps=%d | placed=%d | fills=%d | events=%d | cash=%d | locked_cash=%d | avail_cash=%d | pos=%d | locked_pos=%d | avail_pos=%d",
            step,
            placed_orders,
            last_fills_n,
            last_events_n,
            cash,
            locked_cash,
            cash - locked_cash,
            pos,
            locked_pos,
            pos - locked_pos,
        )

    # Main loop
    rec = first
    while True:
        ex.step(rec)
        steps += 1

        # Ingest new fills (avoid per-step full-vector accounting drift & Python GC)
        fills = ex.fills()
        if fills_cursor < len(fills):
            for f in fills[fills_cursor:]:
                inv.ingest_fill(f)
            fills_cursor = len(fills)

        # Check conservation every N steps
        check_every = 5000
        do_check = strict or (check_every > 0 and steps % check_every == 0)
        if do_check:
            # bounds are in ledger units; set sensible defaults:
            # - cash bound: 0 if integer accounting is exact, else 1..few units
            # - pos bound: 0 should hold
            cash_bound_q = int(getattr(spec, "cash_residual_tolerance_q", 1))
            pos_bound_q = int(
                getattr(
                    spec,
                    "pos_bound_q",
                    getattr(spec, "position_residual_tolerance_q", 0),
                )
            )
            err = inv.check(ex, cash_bound_q=cash_bound_q, pos_bound_q=pos_bound_q)
            if err:
                logger.error("invariant FAIL at step=%d: %s", steps, err)
                if strict:
                    raise RuntimeError(f"invariant FAIL at step={steps}: {err}")

        # per-step markout update is cheap (no snapshots)
        if markout is not None:
            mid_q = _mid_from_record(rec)
            if mid_q is not None:
                markout.update(step=steps, mid_q=mid_q)

        # order placement
        if (
            spec.order_every_steps > 0
            and steps >= spec.warmup_steps
            and (steps % spec.order_every_steps == 0)
        ):
            mid_q = _mid_from_record(rec)
            if mid_q is not None:
                idb, ida = _place_demo_orders(
                    mrl,
                    ex,
                    mid_q=mid_q,
                    qty_q=spec.qty_q,
                    tick_q=spec.tick_q,
                )
                if idb == 0 or ida == 0:
                    msg = f"order rejected (idb={idb}, ida={ida}); check max_events/max_orders"
                    if strict:
                        failures += 1
                        logger.error(msg)
                    else:
                        logger.warning(msg)
                else:
                    placed_orders += 2
                    logger.info(
                        "place | step=%d | mid_q=%d | bid id=%d px=%d qty=%d | ask id=%d px=%d qty=%d",
                        steps,
                        mid_q,
                        idb,
                        int(mid_q - spec.tick_q),
                        spec.qty_q,
                        ida,
                        int(mid_q + spec.tick_q),
                        spec.qty_q,
                    )

        # checkpoint cadence (logging + audit)
        if spec.check_every_steps > 0 and (steps % spec.check_every_steps == 0):
            checkpoint(steps, rec)

        if spec.max_steps > 0 and steps >= spec.max_steps:
            break

        rec = rk.next()
        if rec is None:
            break

    # Final checkpoint to flush tail deltas
    checkpoint(steps, rec if rec is not None else first)

    # Flush completed markouts
    if markout is not None:
        completed = markout.completed()
        if completed:
            # Write markout.csv
            header = list(completed[0].keys())
            rows = [[r.get(k) for k in header] for r in completed]
            write_csv(paths.markout_csv, header, rows)

    # Summaries + digests
    fills_digest = file_sha256(paths.fills_jsonl)
    events_digest = file_sha256(paths.events_jsonl)
    audit_digest = file_sha256(paths.audit_jsonl)

    metrics: Dict[str, object] = {
        "run_id": run_id,
        "timestamp_utc": ts,
        "steps": steps,
        "placed_orders": placed_orders,
        "fills": last_fills_n,
        "events": last_events_n,
        "failures": failures,
        "strict": bool(strict),
        "accounting": {
            "fills_seen": checker.acc.fills_seen,
            "expected_fee_cash_q": checker.acc.expected_fee_cash_q,
            "max_cash_residual_q": checker.acc.max_cash_residual_q,
            "max_cash_residual_bound_q": checker.acc.max_cash_bound_q,
            "inferred_price_scale": checker.acc.inferred_price_scale,
            "overflow_risk_flag": checker.acc.overflow_risk_flag,
        },
        "digests": {
            "fills_jsonl_sha256": fills_digest,
            "events_jsonl_sha256": events_digest,
            "audit_jsonl_sha256": audit_digest,
            "spec_json_sha256": sha256_text(spec_json),
        },
    }
    write_json(paths.metrics_json, metrics)

    # Update replay token with digests for byte-level reproducibility checks
    replay_token["digests"] = metrics["digests"]
    write_json(paths.replay_token_json, replay_token)

    if failures and strict:
        raise RuntimeError(
            f"scenario failed with {failures} invariant/contract violations; run_dir={paths.run_dir}"
        )

    logger.info("done | run_dir=%s", str(paths.run_dir))
    return paths.run_dir
