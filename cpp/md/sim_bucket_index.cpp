#include <algorithm>

#include "sim.hpp"

namespace sim
{

  u64 MarketSimulator::find_bid_bucket_idx_(i64 price_q) const
  {
    auto it = std::lower_bound(bid_prices_.begin(), bid_prices_.end(), price_q);
    if ( it == bid_prices_.end() || *it != price_q )
      return kInvalidIndex;
    return static_cast<u64>(it - bid_prices_.begin());
  }

  u64 MarketSimulator::get_or_insert_bid_bucket_idx_(i64 price_q)
  {
    auto it = std::lower_bound(bid_prices_.begin(), bid_prices_.end(), price_q);
    const u64 idx = static_cast<u64>(it - bid_prices_.begin());
    if ( it != bid_prices_.end() && *it == price_q )
      return idx;

    bid_prices_.insert(it, price_q);
    bid_buckets_.insert(bid_buckets_.begin() + idx, Bucket{});
    return idx;
  }

  void MarketSimulator::erase_bid_bucket_if_empty_(u64 bidx)
  {
    if ( defer_bucket_erase_ )
      return;

    // precondition: bid_buckets_[bidx].size == 0
    const i64 price_q = bid_prices_[bidx];
    bid_prices_.erase(bid_prices_.begin() + bidx);
    bid_buckets_.erase(bid_buckets_.begin() + bidx);

    if ( bid_prices_.empty() ) {
      has_active_bids_ = false;
      best_active_bid_q_ = 0;
    }
    else {
      has_active_bids_ = true;
      best_active_bid_q_ = bid_prices_.back();
    }
  }

  u64 MarketSimulator::find_ask_bucket_idx_(i64 price_q) const
  {
    auto it = std::lower_bound(ask_prices_.begin(), ask_prices_.end(), price_q);
    if ( it == ask_prices_.end() || *it != price_q )
      return kInvalidIndex;
    return static_cast<u64>(it - ask_prices_.begin());
  }

  u64 MarketSimulator::get_or_insert_ask_bucket_idx_(i64 price_q)
  {
    auto it = std::lower_bound(ask_prices_.begin(), ask_prices_.end(), price_q);
    const u64 idx = static_cast<u64>(it - ask_prices_.begin());
    if ( it != ask_prices_.end() && *it == price_q )
      return idx;

    ask_prices_.insert(it, price_q);
    ask_buckets_.insert(ask_buckets_.begin() + idx, Bucket{});
    return idx;
  }

  void MarketSimulator::erase_ask_bucket_if_empty_(u64 aidx)
  {
    if ( defer_bucket_erase_ )
      return;

    const i64 price_q = ask_prices_[aidx];
    ask_prices_.erase(ask_prices_.begin() + aidx);
    ask_buckets_.erase(ask_buckets_.begin() + aidx);

    if ( ask_prices_.empty() ) {
      has_active_asks_ = false;
      best_active_ask_q_ = 0;
    }
    else {
      has_active_asks_ = true;
      best_active_ask_q_ = ask_prices_.front();
    }
  }

} // namespace sim
