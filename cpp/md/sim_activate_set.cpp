#include "sim.hpp"

namespace sim
{

  void MarketSimulator::remove_active_bid_(u64 order_id, u64 order_idx)
  {
    u64& pos = active_bid_pos_[order_id];
    if ( pos == kInvalidIndex )
      return;

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

    // Update bid price counts and best bid
    auto it = active_bid_price_counts_.find(orders_[order_idx].price_q);
    SIM_ASSERT(it != active_bid_price_counts_.end());
    SIM_ASSERT(it->second > 0);

    if ( --it->second == 0 ) {
      active_bid_price_counts_.erase(it);
    }

    has_active_bids_ = !active_bid_price_counts_.empty();
    best_active_bid_q_ = has_active_bids_ ? active_bid_price_counts_.rbegin()->first : 0;
  }

  void MarketSimulator::remove_active_ask_(u64 order_id, u64 order_idx)
  {
    u64& pos = active_ask_pos_[order_id];
    if ( pos == kInvalidIndex )
      return;

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

    // Update ask price counts and best ask
    auto it = active_ask_price_counts_.find(orders_[order_idx].price_q);
    SIM_ASSERT(it != active_ask_price_counts_.end());
    SIM_ASSERT(it->second > 0);

    if ( --it->second == 0 ) {
      active_ask_price_counts_.erase(it);
    }

    has_active_asks_ = !active_ask_price_counts_.empty();
    best_active_ask_q_ = has_active_asks_ ? active_ask_price_counts_.begin()->first : 0;
  }

} // namespace sim