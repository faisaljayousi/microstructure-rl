#include "sim.hpp"

namespace sim
{

  void MarketSimulator::bucket_push_back_bid_(u64 bidx, u64 order_idx)
  {
    auto& b = bid_buckets_[bidx];
    Order& o = orders_[order_idx];
    o.bucket_prev = b.tail;
    o.bucket_next = kInvalidIndex;
    if ( b.tail != kInvalidIndex )
      orders_[b.tail].bucket_next = order_idx;
    else
      b.head = order_idx;
    b.tail = order_idx;
    ++b.size;
  }

  void MarketSimulator::bucket_erase_bid_(u64 bidx, u64 order_idx)
  {
    auto& b = bid_buckets_[bidx];
    Order& o = orders_[order_idx];
    const u64 prev = o.bucket_prev;
    const u64 next = o.bucket_next;
    if ( prev != kInvalidIndex )
      orders_[prev].bucket_next = next;
    else
      b.head = next;
    if ( next != kInvalidIndex )
      orders_[next].bucket_prev = prev;
    else
      b.tail = prev;
    o.bucket_prev = o.bucket_next = kInvalidIndex;
    SIM_ASSERT(b.size > 0);
    --b.size;
    if ( b.size == 0 )
      erase_bid_bucket_if_empty_(bidx);
  }

  void MarketSimulator::bucket_push_back_ask_(u64 aidx, u64 order_idx)
  {
    auto& b = ask_buckets_[aidx];
    Order& o = orders_[order_idx];
    o.bucket_prev = b.tail;
    o.bucket_next = kInvalidIndex;
    if ( b.tail != kInvalidIndex )
      orders_[b.tail].bucket_next = order_idx;
    else
      b.head = order_idx;
    b.tail = order_idx;
    ++b.size;
  }

  void MarketSimulator::bucket_erase_ask_(u64 aidx, u64 order_idx)
  {
    auto& b = ask_buckets_[aidx];
    Order& o = orders_[order_idx];
    const u64 prev = o.bucket_prev;
    const u64 next = o.bucket_next;
    if ( prev != kInvalidIndex )
      orders_[prev].bucket_next = next;
    else
      b.head = next;
    if ( next != kInvalidIndex )
      orders_[next].bucket_prev = prev;
    else
      b.tail = prev;
    o.bucket_prev = o.bucket_next = kInvalidIndex;
    SIM_ASSERT(b.size > 0);
    --b.size;
    if ( b.size == 0 )
      erase_ask_bucket_if_empty_(aidx);
  }

} // namespace sim
