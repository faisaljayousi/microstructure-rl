#include "schema.hpp"
#include "sim.hpp"
#include "sim_lookup.hpp"

namespace sim
{
  namespace
  {
    inline bool is_resting(OrderState st) noexcept
    {
      return st == OrderState::Active || st == OrderState::Partial;
    }
  } // namespace

  // Applies per-level depletion accounting and passive fills for ONE bucket.
  // Returns nothing; mutates orders in-place, may remove filled orders from active sets.
  void MarketSimulator::apply_passive_fills_one_bucket_(
      const md::l2::Record& rec,
      const i64 bucket_price_q,
      Bucket& b,
      const Side side)
  {
    const i64 best_bid = rec.bids[0].price_q;
    const i64 best_ask = rec.asks[0].price_q;

    // Lookup this bucket price in top-N
    const auto m =
        (side == Side::Buy)
            ? lookup::bid_level(rec, bucket_price_q)
            : lookup::ask_level(rec, bucket_price_q);

    // ----------------------------
    // Bucket-level visibility state machine (mirrors update_one_cached behavior)
    // ----------------------------
    if ( m.found ) {
      if ( b.visibility == Visibility::Frozen || b.visibility == Visibility::Blind ||
           b.last_level_idx < 0 ) {
        b.visibility = Visibility::Visible;
        b.last_level_idx = m.idx;
        b.last_level_qty_q = m.qty_q;

        // Pessimistic re-anchor for all resting orders at this price:
        // update_one_cached did this per order on re-visibility
        // :contentReference[oaicite:4]{index=4}
        for ( u64 cur = b.head; cur != kInvalidIndex; cur = orders_[cur].bucket_next ) {
          Order& o = orders_[cur];
          if ( !is_resting(o.state) || o.type != OrderType::Limit )
            continue;
          o.visibility = Visibility::Visible;
          o.last_level_idx = m.idx;
          o.last_level_qty_q = m.qty_q;
          o.qty_ahead_q = m.qty_q;
        }

        // No depletion inferred on a re-anchor tick.
        return;
      }
    }
    else {
      // not found
      if ( m.within_range ) {
        if ( b.visibility == Visibility::Blind ) {
          b.visibility = Visibility::Visible;
          b.last_level_idx = -1;
          b.last_level_qty_q = 0;
          // mirror onto orders
          for ( u64 cur = b.head; cur != kInvalidIndex; cur = orders_[cur].bucket_next ) {
            Order& o = orders_[cur];
            if ( !is_resting(o.state) || o.type != OrderType::Limit )
              continue;
            o.visibility = Visibility::Visible;
            o.last_level_idx = -1;
            o.last_level_qty_q = 0;
            o.qty_ahead_q = 0;
          }
        }
        else if ( b.visibility == Visibility::Visible && b.last_level_idx >= 0 ) {
          b.visibility = Visibility::Frozen;
          b.last_level_idx = -1;
          b.last_level_qty_q = 0;
          for ( u64 cur = b.head; cur != kInvalidIndex; cur = orders_[cur].bucket_next ) {
            Order& o = orders_[cur];
            if ( !is_resting(o.state) || o.type != OrderType::Limit )
              continue;
            o.visibility = Visibility::Frozen;
            o.last_level_idx = -1;
            o.last_level_qty_q = 0;
          }
        }
      }
      else {
        if ( b.visibility == Visibility::Visible ) {
          b.visibility = Visibility::Frozen;
          b.last_level_idx = -1;
          b.last_level_qty_q = 0;
          for ( u64 cur = b.head; cur != kInvalidIndex; cur = orders_[cur].bucket_next ) {
            Order& o = orders_[cur];
            if ( !is_resting(o.state) || o.type != OrderType::Limit )
              continue;
            o.visibility = Visibility::Frozen;
            o.last_level_idx = -1;
            o.last_level_qty_q = 0;
          }
        }
      }
      return;
    }

    // Contract: passive fills only if level remains visible
    if ( b.visibility != Visibility::Visible )
      return;

    // ----------------------------
    // Bucket-level depletion
    // ----------------------------
    const i64 prev = b.last_level_qty_q;
    const i64 nowq = m.qty_q;
    const i64 depl = (prev > nowq) ? (prev - nowq) : 0;
    i64 Ep = lookup::effective_depletion(depl, params_.alpha_ppm);

    b.last_level_idx = m.idx;
    b.last_level_qty_q = nowq;

    if ( Ep <= 0 || b.head == kInvalidIndex )
      return;

    // FIFO deterministic allocation at this price:
    // First consume Ep by advancing queue positions (qty_ahead),
    // then allocate remaining Ep to passive fills when qty_ahead reaches 0.
    u64 cur = b.head;
    while ( cur != kInvalidIndex && Ep > 0 ) {
      Order& o = orders_[cur];
      const u64 next = o.bucket_next; // capture before any removal

      if ( !is_resting(o.state) || o.type != OrderType::Limit ) {
        cur = next;
        continue;
      }

      // Mirror bucket-level observations for tests/debug.
      o.visibility = b.visibility;
      o.last_level_idx = b.last_level_idx;
      o.last_level_qty_q = b.last_level_qty_q;

      // Trade-through signal (Phase 2 semantics preserved): if crossed, queue is irrelevant.
      // update_one_cached did this per order :contentReference[oaicite:5]{index=5}.
      if ( side == Side::Buy ) {
        if ( lookup::is_valid_ask_price(best_ask) && best_ask <= bucket_price_q ) {
          o.qty_ahead_q = 0;
        }
      }
      else {
        if ( lookup::is_valid_bid_price(best_bid) && best_bid >= bucket_price_q ) {
          o.qty_ahead_q = 0;
        }
      }

      // 1) Consume Ep to move this order forward in the displayed queue
      if ( o.qty_ahead_q > 0 ) {
        const i64 consume = (o.qty_ahead_q < Ep) ? o.qty_ahead_q : Ep;
        o.qty_ahead_q -= consume;
        Ep -= consume;
        if ( Ep == 0 )
          break;
      }

      // 2) If at front, allocate remaining Ep to this order as passive fill
      if ( o.qty_ahead_q == 0 ) {
        const i64 remaining = o.qty_q - o.filled_qty_q;
        if ( remaining > 0 ) {
          const i64 fill = (remaining < Ep) ? remaining : Ep;
          o.filled_qty_q += fill;
          Ep -= fill;

          // State transition
          o.state = (o.filled_qty_q == o.qty_q) ? OrderState::Filled : OrderState::Partial;

          // If fully filled: remove from active sets (also removes from bucket list)
          if ( o.state == OrderState::Filled ) {
            if ( o.side == Side::Buy )
              remove_active_bid_(o.id, cur);
            else
              remove_active_ask_(o.id, cur);
          }
        }
      }

      cur = next;
    }
  }

} // namespace sim
