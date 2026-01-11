from __future__ import annotations

import dataclasses
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional


def _canonical_json(obj: Any) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


@dataclass(frozen=True)
class ScenarioSpec:
    """
    Pure-JSON specification (no nanobind objects). This is the auditable contract.

    Build SimulatorParams/Ledger from these primitives inside the runner.
    """

    snap_path: str

    # Runner controls
    max_steps: int = 0  # 0 => run to EOF
    warmup_steps: int = 1000
    order_every_steps: int = 5000  # 0 disables
    log_every_steps: int = 5000
    check_every_steps: int = 5000

    # Order placement
    qty_q: int = 1
    tick_q: int = 1

    # Simulator params (subset)
    max_orders: int = 200_000
    max_events: int = 200_000
    alpha_ppm: int = 0
    maker_fee_ppm: int = 0
    taker_fee_ppm: int = 0
    outbound_latency_ns: int = 0
    observation_latency_ns: int = 0
    start_ts_ns: int = 0

    # Ledger
    initial_cash_q: int = 10**18
    initial_position_qty_q: int = 10**9
    initial_locked_cash_q: int = 0
    initial_locked_position_qty_q: int = 0

    # Invariants
    cash_residual_tolerance_q: int = 1  # bounds integer rounding drift per checkpoint

    # Markout
    enable_markout: bool = True
    markout_horizons_steps: tuple[int, ...] = (100, 1000, 10000)

    def to_dict(self) -> Dict[str, Any]:
        d = dataclasses.asdict(self)
        # tuples -> lists for JSON
        d["markout_horizons_steps"] = list(self.markout_horizons_steps)
        return d

    def canonical_json(self) -> str:
        return _canonical_json(self.to_dict())

    def save(self, out_path: Path) -> None:
        out_path = out_path.resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(self.canonical_json() + "\n", encoding="utf-8")

    @staticmethod
    def load(path: Path) -> "ScenarioSpec":
        raw = json.loads(path.read_text(encoding="utf-8"))
        # back-compat: list -> tuple
        if "markout_horizons_steps" in raw and isinstance(
            raw["markout_horizons_steps"], list
        ):
            raw["markout_horizons_steps"] = tuple(
                int(x) for x in raw["markout_horizons_steps"]
            )
        return ScenarioSpec(**raw)

    @staticmethod
    def from_args(
        *,
        snap_path: str,
        max_steps: int,
        warmup_steps: int,
        order_every_steps: int,
        log_every_steps: int,
        check_every_steps: int,
        qty_q: int,
        tick_q: int,
        alpha_ppm: int,
        maker_fee_ppm: int,
        taker_fee_ppm: int,
        outbound_latency_ns: int,
        observation_latency_ns: int,
        max_orders: int,
        max_events: int,
        initial_cash_q: int,
        initial_position_qty_q: int,
        cash_residual_tolerance_q: int,
        enable_markout: bool,
        markout_horizons_steps: Optional[list[int]],
    ) -> "ScenarioSpec":
        horizons = tuple(markout_horizons_steps or [100, 1000, 10000])
        return ScenarioSpec(
            snap_path=snap_path,
            max_steps=max_steps,
            warmup_steps=warmup_steps,
            order_every_steps=order_every_steps,
            log_every_steps=log_every_steps,
            check_every_steps=check_every_steps,
            qty_q=qty_q,
            tick_q=tick_q,
            alpha_ppm=alpha_ppm,
            maker_fee_ppm=maker_fee_ppm,
            taker_fee_ppm=taker_fee_ppm,
            outbound_latency_ns=outbound_latency_ns,
            observation_latency_ns=observation_latency_ns,
            max_orders=max_orders,
            max_events=max_events,
            initial_cash_q=initial_cash_q,
            initial_position_qty_q=initial_position_qty_q,
            cash_residual_tolerance_q=cash_residual_tolerance_q,
            enable_markout=enable_markout,
            markout_horizons_steps=horizons,
        )
