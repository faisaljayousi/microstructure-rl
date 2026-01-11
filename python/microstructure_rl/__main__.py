from __future__ import annotations

import argparse
from pathlib import Path

from .runner import run_scenario
from .spec import ScenarioSpec


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="microstructure_rl.scenario")

    sub = p.add_subparsers(dest="cmd", required=True)

    mk = sub.add_parser("make-spec", help="Create a pure-JSON scenario spec")
    mk.add_argument("--snap", required=True)
    mk.add_argument("--out", required=True, help="Output spec.json path")
    mk.add_argument("--max-steps", type=int, default=0)
    mk.add_argument("--warmup", type=int, default=1000)
    mk.add_argument("--order-every", type=int, default=5000)
    mk.add_argument("--log-every", type=int, default=5000)
    mk.add_argument("--check-every", type=int, default=5000)
    mk.add_argument("--qty", type=int, default=1)
    mk.add_argument("--tick", type=int, default=1)
    mk.add_argument("--alpha-ppm", type=int, default=0)
    mk.add_argument("--maker-fee-ppm", type=int, default=0)
    mk.add_argument("--taker-fee-ppm", type=int, default=0)
    mk.add_argument("--outbound-latency-ns", type=int, default=0)
    mk.add_argument("--observation-latency-ns", type=int, default=0)
    mk.add_argument("--max-orders", type=int, default=200000)
    mk.add_argument("--max-events", type=int, default=200000)
    mk.add_argument("--initial-cash-q", type=int, default=10**18)
    mk.add_argument("--initial-position-qty-q", type=int, default=10**9)
    mk.add_argument("--cash-residual-tolerance-q", type=int, default=1)
    mk.add_argument("--enable-markout", action="store_true", default=True)
    mk.add_argument("--disable-markout", action="store_false", dest="enable_markout")
    mk.add_argument(
        "--markout-horizons-steps", nargs="*", type=int, default=[100, 1000, 10000]
    )

    rn = sub.add_parser("run", help="Run a scenario and write auditable artifacts")
    rn.add_argument("--spec", help="Path to spec.json; if omitted, use CLI flags")
    rn.add_argument("--snap", help="Snap path (required if --spec not provided)")
    rn.add_argument(
        "--out-root", default="runs", help="Root directory for run artifacts"
    )
    rn.add_argument(
        "--strict", action="store_true", help="Fail hard on invariant violations"
    )
    rn.add_argument("--log-level", default="INFO")

    # If no spec, allow same knobs as make-spec (subset commonly tweaked)
    rn.add_argument("--max-steps", type=int, default=0)
    rn.add_argument("--warmup", type=int, default=1000)
    rn.add_argument("--order-every", type=int, default=5000)
    rn.add_argument("--log-every", type=int, default=5000)
    rn.add_argument("--check-every", type=int, default=5000)
    rn.add_argument("--qty", type=int, default=1)
    rn.add_argument("--tick", type=int, default=1)
    rn.add_argument("--alpha-ppm", type=int, default=0)
    rn.add_argument("--maker-fee-ppm", type=int, default=0)
    rn.add_argument("--taker-fee-ppm", type=int, default=0)
    rn.add_argument("--outbound-latency-ns", type=int, default=0)
    rn.add_argument("--observation-latency-ns", type=int, default=0)
    rn.add_argument("--max-orders", type=int, default=200000)
    rn.add_argument("--max-events", type=int, default=200000)
    rn.add_argument("--initial-cash-q", type=int, default=10**18)
    rn.add_argument("--initial-position-qty-q", type=int, default=10**9)
    rn.add_argument("--cash-residual-tolerance-q", type=int, default=1)
    rn.add_argument("--enable-markout", action="store_true", default=True)
    rn.add_argument("--disable-markout", action="store_false", dest="enable_markout")
    rn.add_argument(
        "--markout-horizons-steps", nargs="*", type=int, default=[100, 1000, 10000]
    )

    return p


def main() -> int:
    p = _build_parser()
    args = p.parse_args()

    if args.cmd == "make-spec":
        spec = ScenarioSpec.from_args(
            snap_path=args.snap,
            max_steps=args.max_steps,
            warmup_steps=args.warmup,
            order_every_steps=args.order_every,
            log_every_steps=args.log_every,
            check_every_steps=args.check_every,
            qty_q=args.qty,
            tick_q=args.tick,
            alpha_ppm=args.alpha_ppm,
            maker_fee_ppm=args.maker_fee_ppm,
            taker_fee_ppm=args.taker_fee_ppm,
            outbound_latency_ns=args.outbound_latency_ns,
            observation_latency_ns=args.observation_latency_ns,
            max_orders=args.max_orders,
            max_events=args.max_events,
            initial_cash_q=args.initial_cash_q,
            initial_position_qty_q=args.initial_position_qty_q,
            cash_residual_tolerance_q=args.cash_residual_tolerance_q,
            enable_markout=args.enable_markout,
            markout_horizons_steps=args.markout_horizons_steps,
        )
        out = Path(args.out)
        spec.save(out)
        print(str(out.resolve()))
        return 0

    if args.cmd == "run":
        if args.spec:
            spec = ScenarioSpec.load(Path(args.spec))
        else:
            if not args.snap:
                p.error("--snap is required when --spec is not provided")
            spec = ScenarioSpec.from_args(
                snap_path=args.snap,
                max_steps=args.max_steps,
                warmup_steps=args.warmup,
                order_every_steps=args.order_every,
                log_every_steps=args.log_every,
                check_every_steps=args.check_every,
                qty_q=args.qty,
                tick_q=args.tick,
                alpha_ppm=args.alpha_ppm,
                maker_fee_ppm=args.maker_fee_ppm,
                taker_fee_ppm=args.taker_fee_ppm,
                outbound_latency_ns=args.outbound_latency_ns,
                observation_latency_ns=args.observation_latency_ns,
                max_orders=args.max_orders,
                max_events=args.max_events,
                initial_cash_q=args.initial_cash_q,
                initial_position_qty_q=args.initial_position_qty_q,
                cash_residual_tolerance_q=args.cash_residual_tolerance_q,
                enable_markout=args.enable_markout,
                markout_horizons_steps=args.markout_horizons_steps,
            )

        run_dir = run_scenario(
            spec=spec,
            out_root=Path(args.out_root),
            strict=bool(args.strict),
            log_level=str(args.log_level),
        )
        print(str(run_dir))
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
