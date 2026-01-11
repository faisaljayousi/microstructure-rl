from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List


def _side_sign(side: Any) -> int:
    name = getattr(side, "name", None)
    if name is None:
        s = str(side)
        if "Buy" in s:
            return +1
        if "Sell" in s:
            return -1
        raise ValueError(f"Unknown side: {side!r}")
    return +1 if name == "Buy" else -1


@dataclass
class PendingMarkout:
    fill_idx: int
    fill_ts_ns: int
    order_id: int
    liq: str
    side: str
    side_sign: int
    qty_q: int
    fill_price_q: int
    mid0_q: int
    step0: int
    horizons: List[int]  # steps
    done: Dict[int, int]  # horizon -> markout_price_q


class MarkoutTracker:
    """
    Step-based markout: markout(h) = side_sign * (mid_{t+h} - mid_t).
    """

    def __init__(self, horizons_steps: List[int]) -> None:
        self._h = sorted({int(x) for x in horizons_steps if int(x) > 0})
        self._pending: List[PendingMarkout] = []
        self._completed_rows: List[Dict[str, object]] = []
        self._fill_counter = 0

    def on_fill(self, fill: Any, *, step: int, mid_q: int) -> None:
        liq = getattr(fill.liq, "name", str(fill.liq))
        side = getattr(fill.side, "name", str(fill.side))
        pm = PendingMarkout(
            fill_idx=self._fill_counter,
            fill_ts_ns=int(fill.ts),
            order_id=int(fill.order_id),
            liq=liq,
            side=side,
            side_sign=_side_sign(fill.side),
            qty_q=int(fill.qty_q),
            fill_price_q=int(fill.price_q),
            mid0_q=int(mid_q),
            step0=int(step),
            horizons=self._h,
            done={},
        )
        self._fill_counter += 1
        self._pending.append(pm)

    def update(self, *, step: int, mid_q: int) -> None:
        if not self._pending:
            return
        step = int(step)
        mid_q = int(mid_q)

        still: List[PendingMarkout] = []
        for pm in self._pending:
            for h in pm.horizons:
                if h in pm.done:
                    continue
                if step - pm.step0 >= h:
                    pm.done[h] = pm.side_sign * (mid_q - pm.mid0_q)

            # completed if all horizons done
            if len(pm.done) == len(pm.horizons):
                row: Dict[str, object] = {
                    "fill_idx": pm.fill_idx,
                    "fill_ts_ns": pm.fill_ts_ns,
                    "order_id": pm.order_id,
                    "liq": pm.liq,
                    "side": pm.side,
                    "qty_q": pm.qty_q,
                    "fill_price_q": pm.fill_price_q,
                    "mid0_q": pm.mid0_q,
                    "step0": pm.step0,
                }
                for h in pm.horizons:
                    row[f"markout_price_q_h{h}"] = pm.done[h]
                self._completed_rows.append(row)
            else:
                still.append(pm)

        self._pending = still

    def completed(self) -> List[Dict[str, object]]:
        rows = self._completed_rows
        self._completed_rows = []
        return rows
