#pragma once

#include "sim.hpp"
#include "sim_lookup.hpp"

namespace sim::queue
{

  // Initialises visibility/queue state when order becomes ACTIVE.
  inline void init_on_activate(const md::l2::Record& rec, Order& o) noexcept
  {
    if ( o.type != OrderType::Limit || o.price_q <= 0 ) {
      o.visibility = Visibility::Blind;
      o.last_level_idx = -1;
      o.last_level_qty_q = 0;
      o.qty_ahead_q = 0;
      return;
    }

    if ( o.side == Side::Buy ) {
      const auto m = lookup::bid_level(rec, o.price_q);
      if ( !m.within_range ) {
        o.visibility = Visibility::Blind;
        o.last_level_idx = -1;
        o.last_level_qty_q = 0;
        o.qty_ahead_q = 0;
        return;
      }

      o.visibility = Visibility::Visible;
      if ( m.found ) {
        o.last_level_idx = m.idx;
        o.last_level_qty_q = m.qty_q;
        o.qty_ahead_q = m.qty_q; // join tail
      }
      else {
        o.last_level_idx = -1;
        o.last_level_qty_q = 0;
        o.qty_ahead_q = 0; // you are the queue
      }
      return;
    }

    const auto m = lookup::ask_level(rec, o.price_q);
    if ( !m.within_range ) {
      o.visibility = Visibility::Blind;
      o.last_level_idx = -1;
      o.last_level_qty_q = 0;
      o.qty_ahead_q = 0;
      return;
    }

    o.visibility = Visibility::Visible;
    if ( m.found ) {
      o.last_level_idx = m.idx;
      o.last_level_qty_q = m.qty_q;
      o.qty_ahead_q = m.qty_q;
    }
    else {
      o.last_level_idx = -1;
      o.last_level_qty_q = 0;
      o.qty_ahead_q = 0;
    }
  }

   // Cached version: caller provides the LevelLookup for the bucket price,
  // and best bid/ask for this tick (computed once per step).
  inline void update_one_cached(
      const SimulatorParams& params,
      const lookup::LevelLookup& m,
      const i64 best_bid,
      const i64 best_ask,
      Order& o) noexcept
  {
    if ( o.type != OrderType::Limit || o.price_q <= 0 )
      return;
    if ( o.state != OrderState::Active && o.state != OrderState::Partial )
      return;

    // Trade-through detection (no fills yet): queue becomes irrelevant.
    if ( o.side == Side::Buy ) {
      if ( lookup::is_valid_ask_price(best_ask) && best_ask <= o.price_q ) {
        o.qty_ahead_q = 0;
      }

      if ( m.found ) {
        if ( o.visibility == Visibility::Frozen || o.visibility == Visibility::Blind ||
             o.last_level_idx < 0 ) {
          o.visibility = Visibility::Visible;
          o.last_level_idx = m.idx;
          o.last_level_qty_q = m.qty_q;
          o.qty_ahead_q = m.qty_q; // pessimistic re-anchor
        } else {
          const i64 prev = o.last_level_qty_q;
          const i64 nowq = m.qty_q;
          const i64 depl = (prev > nowq) ? (prev - nowq) : 0;
          const i64 eff = lookup::effective_depletion(depl, params.alpha_ppm);
          if ( eff > 0 )
            o.qty_ahead_q = (o.qty_ahead_q > eff) ? (o.qty_ahead_q - eff) : 0;
          o.last_level_idx = m.idx;
          o.last_level_qty_q = nowq;
        }
        return;
      }

      // not found
      if ( m.within_range ) {
        if ( o.visibility == Visibility::Blind ) {
          o.visibility = Visibility::Visible;
          o.last_level_idx = -1;
          o.last_level_qty_q = 0;
          o.qty_ahead_q = 0;
        } else if ( o.visibility == Visibility::Visible && o.last_level_idx >= 0 ) {
          o.visibility = Visibility::Frozen;
          o.last_level_idx = -1;
          o.last_level_qty_q = 0;
        }
      } else {
        if ( o.visibility == Visibility::Visible ) {
          o.visibility = Visibility::Frozen;
          o.last_level_idx = -1;
          o.last_level_qty_q = 0;
        }
      }
      return;
    }

    // Sell
    if ( lookup::is_valid_bid_price(best_bid) && best_bid >= o.price_q ) {
      o.qty_ahead_q = 0;
    }

    if ( m.found ) {
      if ( o.visibility == Visibility::Frozen || o.visibility == Visibility::Blind ||
           o.last_level_idx < 0 ) {
        o.visibility = Visibility::Visible;
        o.last_level_idx = m.idx;
        o.last_level_qty_q = m.qty_q;
        o.qty_ahead_q = m.qty_q;
      } else {
        const i64 prev = o.last_level_qty_q;
        const i64 nowq = m.qty_q;
        const i64 depl = (prev > nowq) ? (prev - nowq) : 0;
        const i64 eff = lookup::effective_depletion(depl, params.alpha_ppm);
        if ( eff > 0 )
          o.qty_ahead_q = (o.qty_ahead_q > eff) ? (o.qty_ahead_q - eff) : 0;
        o.last_level_idx = m.idx;
        o.last_level_qty_q = nowq;
      }
      return;
    }

    if ( m.within_range ) {
      if ( o.visibility == Visibility::Blind ) {
        o.visibility = Visibility::Visible;
        o.last_level_idx = -1;
        o.last_level_qty_q = 0;
        o.qty_ahead_q = 0;
      } else if ( o.visibility == Visibility::Visible && o.last_level_idx >= 0 ) {
        o.visibility = Visibility::Frozen;
        o.last_level_idx = -1;
        o.last_level_qty_q = 0;
      }
    } else {
      if ( o.visibility == Visibility::Visible ) {
        o.visibility = Visibility::Frozen;
        o.last_level_idx = -1;
        o.last_level_qty_q = 0;
      }
    }
  }


  // Updates queue/visibility state for one ACTIVE order (Phase 2: no fills).
  inline void
  update_one(const md::l2::Record& rec, const SimulatorParams& params, Order& o) noexcept
  {
    if ( o.type != OrderType::Limit || o.price_q <= 0 )
      return;
    if ( o.state != OrderState::Active && o.state != OrderState::Partial )
      return;

    const i64 best_bid = rec.bids[0].price_q;
    const i64 best_ask = rec.asks[0].price_q;
    const auto m = (o.side == Side::Buy) ? lookup::bid_level(rec, o.price_q)
                                         : lookup::ask_level(rec, o.price_q);
    update_one_cached(params, m, best_bid, best_ask, o);
  }
} // namespace sim::queue
