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

  void MarketSimulator::apply_aggressive_fills_(const md::l2::Record& rec)
  {
    // Require valid top-of-book for marketability checks.
    if ( !md::l2::record_has_top_of_book(rec) )
      return;

    const i64 best_bid = rec.bids[0].price_q;
    const i64 best_ask = rec.asks[0].price_q;

    // Local mutable copy of visible depth so multiple agent orders in the same step
    // consume liquidity sequentially and deterministically.
    std::array<i64, md::l2::kDepth> bid_qty_rem{};
    std::array<i64, md::l2::kDepth> ask_qty_rem{};
    for ( u64 i = 0; i < md::l2::kDepth; ++i ) {
      bid_qty_rem[i] = sim::lookup::is_valid_bid_price(rec.bids[i].price_q) ? rec.bids[i].qty_q : 0;
      ask_qty_rem[i] = sim::lookup::is_valid_ask_price(rec.asks[i].price_q) ? rec.asks[i].qty_q : 0;
    }

    // ----------------------------
    // BUY takers: only buckets with price_q >= best_ask are marketable.
    // Scan bid prices descending (best -> worse).
    // ----------------------------
    if ( sim::lookup::is_valid_ask_price(best_ask) ) {
      for ( u64 pi = static_cast<u64>(bid_prices_.size()); pi-- > 0; ) {
        const i64 limit_q = bid_prices_[pi];
        if ( limit_q < best_ask )
          break; // remaining prices are lower -> not marketable

        Bucket& b = bid_buckets_[pi];
        for ( u64 cur = b.head; cur != kInvalidIndex; ) {
          Order& o = orders_[cur];
          const u64 next = o.bucket_next;

          if ( !is_resting(o.state) || o.side != Side::Buy || o.type != OrderType::Limit ) {
            cur = next;
            continue;
          }

          i64 remaining = o.qty_q - o.filled_qty_q;
          if ( remaining <= 0 ) {
            cur = next;
            continue;
          }

          // Sweep asks from best outward while ask_price <= limit and there is remaining qty.
          for ( u64 lvl = 0; lvl < md::l2::kDepth && remaining > 0; ++lvl ) {
            const i64 px = rec.asks[lvl].price_q;
            if ( !sim::lookup::is_valid_ask_price(px) )
              break;
            if ( px > limit_q )
              break;

            i64& avail = ask_qty_rem[lvl];
            if ( avail <= 0 )
              continue;

            const i64 dq = (remaining < avail) ? remaining : avail;
            apply_fill_(o, px, dq, LiquidityFlag::Taker);
            remaining -= dq;
            avail -= dq;

            if ( o.state == OrderState::Filled ) {
              remove_active_bid_(o.id, cur); // also removes from bucket list
              break;
            }
          }

          cur = next;
        }
      }
    }

    // ----------------------------
    // SELL takers: only buckets with price_q <= best_bid are marketable.
    // Scan ask prices ascending (best -> worse).
    // ----------------------------
    if ( sim::lookup::is_valid_bid_price(best_bid) ) {
      for ( u64 pi = 0; pi < static_cast<u64>(ask_prices_.size()); ++pi ) {
        const i64 limit_q = ask_prices_[pi];
        if ( limit_q > best_bid )
          break; // remaining prices are higher -> not marketable

        Bucket& b = ask_buckets_[pi];
        for ( u64 cur = b.head; cur != kInvalidIndex; ) {
          Order& o = orders_[cur];
          const u64 next = o.bucket_next;

          if ( !is_resting(o.state) || o.side != Side::Sell || o.type != OrderType::Limit ) {
            cur = next;
            continue;
          }

          i64 remaining = o.qty_q - o.filled_qty_q;
          if ( remaining <= 0 ) {
            cur = next;
            continue;
          }

          // Sweep bids from best outward while bid_price >= limit and there is remaining qty.
          for ( u64 lvl = 0; lvl < md::l2::kDepth && remaining > 0; ++lvl ) {
            const i64 px = rec.bids[lvl].price_q;
            if ( !sim::lookup::is_valid_bid_price(px) )
              break;
            if ( px < limit_q )
              break;

            i64& avail = bid_qty_rem[lvl];
            if ( avail <= 0 )
              continue;

            const i64 dq = (remaining < avail) ? remaining : avail;
            apply_fill_(o, px, dq, LiquidityFlag::Taker);
            remaining -= dq;
            avail -= dq;

            if ( o.state == OrderState::Filled ) {
              remove_active_ask_(o.id, cur); // also removes from bucket list
              break;
            }
          }

          cur = next;
        }
      }
    }
  }

} // namespace sim
