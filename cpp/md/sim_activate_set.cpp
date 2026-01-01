#include "sim.hpp"

namespace sim
{

  void MarketSimulator::remove_active_bid_(u64 order_id, u64 order_idx)
  {
    u64& pos = active_bid_pos_[order_id];
    if ( pos == kInvalidIndex )
      return;

    // --- Remove from bid_buckets_ in O(1) using pos_in_bucket ---
    {
      Order& o = orders_[order_idx];
      SIM_ASSERT(o.pos_in_bucket != kInvalidIndex);

      auto bit = bid_buckets_.find(o.price_q);
      SIM_ASSERT(bit != bid_buckets_.end());

      auto& v = bit->second;
      SIM_ASSERT(!v.empty());

      const u64 bpos = o.pos_in_bucket;
      const u64 last_pos = static_cast<u64>(v.size() - 1);
      const u64 moved_oidx = v[last_pos];

      v[bpos] = moved_oidx;
      v.pop_back();

      orders_[moved_oidx].pos_in_bucket = bpos;
      o.pos_in_bucket = kInvalidIndex;

      // If this price bucket becomes empty, erase and maintain best bid scalar
      if ( v.empty() ) {
        const bool removed_best = (o.price_q == best_active_bid_q_);
        bid_buckets_.erase(bit);
        if ( bid_buckets_.empty() ) {
          has_active_bids_ = false;
          best_active_bid_q_ = 0;
        } else if ( removed_best ) {
          best_active_bid_q_ = bid_buckets_.rbegin()->first; // rare path
        }
      }
    }

    SIM_ASSERT(!active_bids_.empty());

    // swap-pop from active_bids_
    const u64 last_pos = static_cast<u64>(active_bids_.size() - 1);
    const u64 last_oidx = active_bids_[last_pos];

    active_bids_[pos] = last_oidx;
    active_bids_.pop_back();

    // Update back-pointer for moved order
    const u64 moved_id = orders_[last_oidx].id;
    active_bid_pos_[moved_id] = pos;

    pos = kInvalidIndex;

   
  }

  void MarketSimulator::remove_active_ask_(u64 order_id, u64 order_idx)
  {
    u64& pos = active_ask_pos_[order_id];
    if ( pos == kInvalidIndex )
      return;

    // --- Remove from ask_buckets_ in O(1) using pos_in_bucket ---
    {
      Order& o = orders_[order_idx];
      SIM_ASSERT(o.pos_in_bucket != kInvalidIndex);

      auto ait = ask_buckets_.find(o.price_q);
      SIM_ASSERT(ait != ask_buckets_.end());

      auto& v = ait->second;
      SIM_ASSERT(!v.empty());

      const u64 bpos = o.pos_in_bucket;
      const u64 last_pos = static_cast<u64>(v.size() - 1);
      const u64 moved_oidx = v[last_pos];

      v[bpos] = moved_oidx;
      v.pop_back();

      orders_[moved_oidx].pos_in_bucket = bpos;
      o.pos_in_bucket = kInvalidIndex;

      if ( v.empty() ) {
        const bool removed_best = (o.price_q == best_active_ask_q_);
        ask_buckets_.erase(ait);
        if ( ask_buckets_.empty() ) {
          has_active_asks_ = false;
          best_active_ask_q_ = 0;
        } else if ( removed_best ) {
          best_active_ask_q_ = ask_buckets_.begin()->first; // rare path
        }
      }
    }


    SIM_ASSERT(!active_asks_.empty());

    // swap-pop from active_asks_
    const u64 last_pos = static_cast<u64>(active_asks_.size() - 1);
    const u64 last_oidx = active_asks_[last_pos];

    active_asks_[pos] = last_oidx;
    active_asks_.pop_back();

    // Update back-pointer for moved order
    const u64 moved_id = orders_[last_oidx].id;
    active_ask_pos_[moved_id] = pos;

    pos = kInvalidIndex;
  }

} // namespace sim