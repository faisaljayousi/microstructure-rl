#pragma once

#include <cstdint>
#include <limits>

#include "schema.hpp" // md::l2::Record
#include "sim.hpp"    // sim::i64, sim::u64 (or forward declare types if you prefer)

#if defined(_MSC_VER)
#  include <intrin.h>
#endif

namespace sim::lookup
{

  struct LevelLookup
  {
    bool found{false};        // exact price present in top-N
    bool within_range{false}; // within visible [best,worst] range
    std::int16_t idx{-1};     // level index if found
    i64 qty_q{0};             // displayed qty at that level
    i64 best_q{0};            // best price
    i64 worst_q{0};           // worst visible price
  };

  inline bool is_valid_bid_price(i64 p) noexcept { return p != md::l2::kBidNullPriceQ; }

  inline bool is_valid_ask_price(i64 p) noexcept { return p != md::l2::kAskNullPriceQ; }

  // Monotone scan with early exit; O(kDepth) with kDepth=20.
  inline LevelLookup bid_level(const md::l2::Record& rec, i64 price_q) noexcept
  {
    LevelLookup out{};
    const i64 best = rec.bids[0].price_q;
    if ( !is_valid_bid_price(best) )
      return out;

    i64 worst = best;
    std::int16_t last_valid = -1;
    for ( std::int16_t i = 0; i < static_cast<std::int16_t>(md::l2::kDepth); ++i ) {
      const i64 p = rec.bids[i].price_q;
      if ( !is_valid_bid_price(p) )
        break;
      worst = p;
      last_valid = i;
    }
    out.best_q = best;
    out.worst_q = worst;

    if ( price_q > best )
      return out; // within_range false
    if ( price_q < worst )
      return out; // within_range false
    out.within_range = true;

    for ( std::int16_t i = 0; i <= last_valid; ++i ) {
      const i64 p = rec.bids[i].price_q;
      if ( p == price_q ) {
        out.found = true;
        out.idx = i;
        out.qty_q = rec.bids[i].qty_q;
        return out;
      }
      if ( p < price_q ) {
        return out; // passed price; not present but within range
      }
    }
    return out;
  }

  inline LevelLookup ask_level(const md::l2::Record& rec, i64 price_q) noexcept
  {
    LevelLookup out{};
    const i64 best = rec.asks[0].price_q;
    if ( !is_valid_ask_price(best) )
      return out;

    i64 worst = best;
    std::int16_t last_valid = -1;
    for ( std::int16_t i = 0; i < static_cast<std::int16_t>(md::l2::kDepth); ++i ) {
      const i64 p = rec.asks[i].price_q;
      if ( !is_valid_ask_price(p) )
        break;
      worst = p;
      last_valid = i;
    }
    out.best_q = best;
    out.worst_q = worst;

    if ( price_q < best )
      return out;
    if ( price_q > worst )
      return out;
    out.within_range = true;

    for ( std::int16_t i = 0; i <= last_valid; ++i ) {
      const i64 p = rec.asks[i].price_q;
      if ( p == price_q ) {
        out.found = true;
        out.idx = i;
        out.qty_q = rec.asks[i].qty_q;
        return out;
      }
      if ( p > price_q ) {
        return out;
      }
    }
    return out;
  }

  // Deterministic min-depletion rule; avoids stalling with alpha truncation.
  inline i64 effective_depletion(i64 depletion_q, u64 alpha_ppm) noexcept
  {
    if ( depletion_q <= 0 || alpha_ppm == 0 )
      return 0;

    // Use 128-bit where available to avoid overflow of (depl * alpha).
#if defined(_MSC_VER)
    unsigned __int64 hi = 0;
    const unsigned __int64 lo = _umul128(
        static_cast<unsigned __int64>(depletion_q),
        static_cast<unsigned __int64>(alpha_ppm),
        &hi);

    // Divide 128-bit (hi:lo) by 1e6. Since alpha_ppm <= 1e6, hi should be small,
    // but do it correctly:
    // Convert to long double? No. Do manual division:
    // eff = floor((hi*2^64 + lo)/1e6)
    // Use builtin unsigned __int128 not available; approximate with two-step:
    const unsigned __int64 divisor = 1'000'000ULL;

    // First reduce hi portion:
    // q = hi*(2^64/divisor) + carry; but exact 128/64 division isn't built in.
    // Practical simplification: since alpha_ppm <= 1e6, hi is near 0 for realistic sizes.
    // Production-grade option: clamp if hi != 0.
    if ( hi != 0 ) {
      // Saturate to depletion (most conservative).
      return depletion_q;
    }
    const unsigned __int64 eff = lo / divisor;
#else
    ...
#endif

    if ( eff == 0 )
      return 1;
    if ( eff > static_cast<std::uint64_t>(depletion_q) )
      return depletion_q;
    return static_cast<i64>(eff);
  }
} // namespace sim::lookup
