#include "schema.hpp" // for md::l2::PRICE_SCALE
#include "sim.hpp"

#if defined(_MSC_VER)
#  include <intrin.h>
// _umul128/_udiv128 are x64-only. If you build Win32, do NOT attempt to run.
#  if !defined(_M_X64)
#    error "MSVC build must target x64 (/M_X64) for 128-bit mul/div intrinsics."
#  endif
#endif

namespace sim
{

  // Computes floor((a*b)/div) with 128-bit intermediates.
  // Assumptions for v0 fills:
  // - a >= 0 (price_q)
  // - b >= 0 (qty_q or notional)
  // - div > 0
  static inline i64 mul_div_u64_to_i64(i64 a, i64 b, i64 div)
  {
    SIM_ASSERT(a >= 0);
    SIM_ASSERT(b >= 0);
    SIM_ASSERT(div > 0);

#if defined(_MSC_VER)
    unsigned __int64 hi = 0;
    const unsigned __int64 lo =
        _umul128(static_cast<unsigned __int64>(a), static_cast<unsigned __int64>(b), &hi);
    unsigned __int64 rem = 0;
    const unsigned __int64 q = _udiv128(hi, lo, static_cast<unsigned __int64>(div), &rem);
    return static_cast<i64>(q);
#elif defined(__GNUC__) || defined(__clang__)
    __int128 prod = static_cast<__int128>(a) * static_cast<__int128>(b);
    return static_cast<i64>(prod / static_cast<__int128>(div));
#else
#  error "No 128-bit multiply/divide support for this compiler."
#endif
  }

  static inline i64 notional_cash_q(i64 price_q, i64 qty_q)
  {
    // price_q = price * kPriceScale, qty_q = qty * kQtyScale
    // notional_cash_q should be in cash_q quantization.
    // Using the schema scale constant (PRICE_SCALE does not exist).
    return mul_div_u64_to_i64(price_q, qty_q, md::l2::kPriceScale);
  }

  static inline i64 fee_cash_q(i64 notional_q, u64 fee_ppm)
  {
    return mul_div_u64_to_i64(notional_q, static_cast<i64>(fee_ppm), 1'000'000);
  }

  void MarketSimulator::apply_fill_(Order& o, i64 price_q, i64 qty_q, LiquidityFlag liq)
  {
    SIM_ASSERT(qty_q > 0);
    SIM_ASSERT(o.filled_qty_q + qty_q <= o.qty_q);

    const i64 notional_q = notional_cash_q(price_q, qty_q);

    const u64 fee_ppm =
        (liq == LiquidityFlag::Maker) ? params_.fees.maker_fee_ppm : params_.fees.taker_fee_ppm;
    const i64 fee_q = fee_cash_q(notional_q, fee_ppm);

    // Update ledger: buy spends cash, increases position; sell earns cash, reduces position.
    if ( o.side == Side::Buy ) {
      ledger_.cash_q -= notional_q;
      ledger_.cash_q -= fee_q;
      ledger_.position_qty_q += qty_q;
    }
    else {
      ledger_.cash_q += notional_q;
      ledger_.cash_q -= fee_q;
      ledger_.position_qty_q -= qty_q;
    }

    // Update fill state.
    o.filled_qty_q += qty_q;

    // Unlock proportional reserved balances for the filled quantity.
    // At minimum, on FILLED release all remaining locks.
    if ( o.filled_qty_q == o.qty_q ) {
      unlock_on_cancel_(o); // reuse existing "release remaining locks" helper
      o.state = OrderState::Filled;
    }
    else {
      o.state = OrderState::Partial;
    }

    // Emit FillEvent (unbounded for now; introduce max_fills + deterministic overflow later)
    fills_.push_back(FillEvent{
        .ts = now_,
        .order_id = o.id,
        .side = o.side,
        .price_q = price_q,
        .qty_q = qty_q,
        .liq = liq,
        .notional_cash_q = notional_q,
        .fee_cash_q = fee_q});
  }

} // namespace sim
