#include "sim.hpp"

namespace sim
{

  void MarketSimulator::remove_active_bid_(u64 order_id, u64 order_idx)
  {
    u64& pos = active_bid_pos_[order_id];
    if ( pos == kInvalidIndex )
      return;

    {
      const u64 bidx = find_bid_bucket_idx_(orders_[order_idx].price_q);
      // guard in Release builds
      if ( bidx != kInvalidIndex )
        bucket_erase_bid_(bidx, order_idx);
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

    {
      const u64 aidx = find_ask_bucket_idx_(orders_[order_idx].price_q);
      if ( aidx != kInvalidIndex )
        bucket_erase_ask_(aidx, order_idx);
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