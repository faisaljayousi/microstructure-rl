from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


@dataclass
class ConservationSnapshot:
    cash_total_q: int
    pos_total_q: int


class FillConservation:
    """
    Cash/position conservation checks in *ledger units*.
    """

    def __init__(self, initial_cash_total_q: int, initial_pos_total_q: int) -> None:
        self._c0 = int(initial_cash_total_q)
        self._p0 = int(initial_pos_total_q)

        self._realised_cash_delta_q = 0  # signed cash delta from fills
        self._realised_pos_delta_q = 0  # signed position delta from fills

    @staticmethod
    def snapshot_ledger_total(ex: Any) -> ConservationSnapshot:
        led = ex.ledger
        # IMPORTANT (engine contract):
        #   cash_q / position_qty_q are TOTAL balances.
        #   locked_* are ENCUMBERED SUB-BALANCES.
        # So total != cash + locked. Total == cash.
        cash_total = int(getattr(led, "cash_q"))
        pos_total = int(getattr(led, "position_qty_q"))
        return ConservationSnapshot(cash_total_q=cash_total, pos_total_q=pos_total)

    def ingest_fill(self, f: Any) -> None:
        """
        Updates realised deltas from a single FillEvent.

        Convention:
          - Buy consumes cash, increases position
          - Sell produces cash, decreases position
        Fees:
          - Buy: pay notional + fee
          - Sell: receive notional - fee
        """
        side = getattr(f, "side")
        qty = int(getattr(f, "qty_q"))
        notional = int(getattr(f, "notional_cash_q"))
        fee = int(getattr(f, "fee_cash_q", 0))

        # side is an enum; compare by name to avoid import coupling
        side_name = getattr(side, "name", str(side))
        if "Buy" in side_name:
            self._realised_cash_delta_q -= notional + fee
            self._realised_pos_delta_q += qty
        elif "Sell" in side_name:
            self._realised_cash_delta_q += notional - fee
            self._realised_pos_delta_q -= qty
        else:
            raise RuntimeError(f"Unknown Side enum value: {side!r}")

    def check(
        self,
        ex: Any,
        *,
        cash_bound_q: int = 0,
        pos_bound_q: int = 0,
    ) -> Optional[str]:
        """
        Returns an error string if violated, else None.
        Bounds are in ledger units (cash_q units / qty units).
        """
        snap = self.snapshot_ledger_total(ex)

        cash_expected = self._c0 + self._realised_cash_delta_q
        pos_expected = self._p0 + self._realised_pos_delta_q

        cash_residual = snap.cash_total_q - cash_expected
        pos_residual = snap.pos_total_q - pos_expected

        if abs(cash_residual) > int(cash_bound_q):
            return f"cash residual {cash_residual} exceeds bound {cash_bound_q}"
        if abs(pos_residual) > int(pos_bound_q):
            return f"pos residual {pos_residual} exceeds bound {pos_bound_q}"
        return None


def _side_sign(side: Any) -> int:
    # nanobind enum: compare by name if needed
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
class AccountingState:
    expected_cash_q: int
    expected_fee_cash_q: int
    fills_seen: int
    inferred_price_scale: Optional[int] = None

    max_cash_residual_q: int = 0
    max_cash_bound_q: int = 0
    overflow_risk_flag: bool = False


class InvariantChecker:
    """
    Enforces core contracts + accounting residuals at checkpoints.

    Assumes FillEvent.notional_cash_q is quote-cash units (already scaled).
    """

    def __init__(self, *, initial_cash_q: int, tolerance_q: int) -> None:
        self._tol_q = int(tolerance_q)
        self._acc = AccountingState(
            expected_cash_q=int(initial_cash_q),
            expected_fee_cash_q=0,
            fills_seen=0,
        )

        # For reject/terminal checks
        self._reject_events: set[int] = set()

    @property
    def acc(self) -> AccountingState:
        return self._acc

    def observe_fill(self, fill: Any) -> None:
        sign = _side_sign(fill.side)
        notional = int(fill.notional_cash_q)
        fee = int(fill.fee_cash_q)

        # Buy => cash decreases. Sell => cash increases.
        self._acc.expected_cash_q += (-sign) * notional
        self._acc.expected_cash_q -= fee
        self._acc.expected_fee_cash_q += fee
        self._acc.fills_seen += 1

        # infer price scale once (best effort)
        # scale ~= price_q / notional_cash_q  (integer)
        if self._acc.inferred_price_scale is None and notional != 0:
            price_q = int(fill.price_q)
            scale = abs(price_q) // abs(notional) if abs(notional) > 0 else None
            if scale and scale > 0:
                self._acc.inferred_price_scale = int(scale)

    def observe_event(self, event: Any) -> None:
        et = getattr(event, "type", None)
        if et is None:
            return
        et_name = getattr(et, "name", str(et))
        if "Reject" in et_name:
            self._reject_events.add(int(event.order_id))

    def check_reject_implies_terminal(
        self, *, orders: list[Any], strict: bool
    ) -> Optional[str]:
        if not self._reject_events:
            return None
        # For every rejected order id, ensure state is Rejected and reject_reason != None.
        bad: list[str] = []
        order_by_id = {int(o.id): o for o in orders}
        for oid in sorted(self._reject_events):
            o = order_by_id.get(oid)
            if o is None:
                bad.append(f"Reject event for unknown order_id={oid}")
                continue
            st = getattr(o.state, "name", str(o.state))
            rr = getattr(o.reject_reason, "name", str(o.reject_reason))
            if "Rejected" not in st:
                bad.append(f"order_id={oid} had Reject event but state={st}")
            if rr == "None":
                bad.append(f"order_id={oid} state={st} but reject_reason=None")
        if bad:
            msg = " | ".join(bad[:10])
            return msg if strict else f"WARN: {msg}"
        return None

    def check_accounting_residual(
        self,
        *,
        ledger: Any,
        step: int,
        mid_q: Optional[int],
        position_qty_q: Optional[int],
    ) -> Tuple[Dict[str, object], Optional[str]]:
        """
        Computes cash residual against expected cashflow from fills+fees.

        Residual is checked against tolerance_q. Wealth drift is reported (optional).
        """
        cash_q = int(ledger.cash_q)
        locked_cash_q = int(getattr(ledger, "locked_cash_q", 0))
        cash_total_q = cash_q

        expected_cash_q = int(self._acc.expected_cash_q)
        residual_q = cash_total_q - expected_cash_q

        bound_q = int(self._tol_q)
        self._acc.max_cash_residual_q = max(
            self._acc.max_cash_residual_q, abs(residual_q)
        )
        self._acc.max_cash_bound_q = max(self._acc.max_cash_bound_q, bound_q)

        # Optional overflow-risk flag for i64 multiplication if done in C++:
        overflow_risk = False
        wealth_mtm_q: Optional[int] = None
        if mid_q is not None and position_qty_q is not None:
            pos = int(position_qty_q)
            mid = int(mid_q)
            if mid != 0 and pos != 0:
                # if computed in i64: abs(pos) > (2^63-1)//abs(mid) => overflow risk
                i64_max = (1 << 63) - 1
                if abs(pos) > (i64_max // max(1, abs(mid))):
                    overflow_risk = True
            self._acc.overflow_risk_flag = self._acc.overflow_risk_flag or overflow_risk

            # MTM wealth (best effort): cash + pos*mid/scale if scale known
            scale = self._acc.inferred_price_scale
            if scale and scale > 0:
                wealth_mtm_q = cash_total_q + (pos * mid) // scale

        status = "PASS" if abs(residual_q) <= bound_q else "FAIL"
        row = {
            "step": int(step),
            "cash_q": cash_q,
            "locked_cash_q": locked_cash_q,
            "cash_total_q": cash_total_q,
            "expected_cash_q": expected_cash_q,
            "cash_residual_q": residual_q,
            "cash_residual_bound_q": bound_q,
            "inferred_price_scale": self._acc.inferred_price_scale,
            "overflow_risk_flag": overflow_risk,
            "mid_q": mid_q,
            "wealth_mtm_q": wealth_mtm_q,
            "status": status,
        }

        err = (
            None
            if status == "PASS"
            else f"cash residual {residual_q} exceeds bound {bound_q}"
        )
        return row, err
