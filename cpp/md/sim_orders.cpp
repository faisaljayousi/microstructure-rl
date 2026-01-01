#include <limits>

#include "sim.hpp"

#if defined(_MSC_VER)
#  include <intrin.h>
#endif

namespace
{
  inline bool mul_i64_overflow(sim::i64 a, sim::i64 b, sim::i64* out)
  {
#if defined(_MSC_VER)
    // MSVC x64: use builtin 128-bit multiply
    // Note: _mul128 returns high 64 bits and stores low 64 bits in *out_low.
    __int64 high = 0;
    __int64 low = 0;
    low = _mul128(static_cast<__int64>(a), static_cast<__int64>(b), &high);

    // If high is not sign-extension of low's sign bit, overflow occurred
    const bool neg = (low < 0);
    const __int64 expected_high = neg ? -1 : 0;
    if ( high != expected_high )
      return true;

    *out = static_cast<sim::i64>(low);
    return false;
#else
    __int128 prod = static_cast<__int128>(a) * static_cast<__int128>(b);
    if ( prod > static_cast<__int128>(std::numeric_limits<sim::i64>::max()) )
      return true;
    if ( prod < static_cast<__int128>(std::numeric_limits<sim::i64>::min()) )
      return true;
    *out = static_cast<sim::i64>(prod);
    return false;
#endif
  }

  inline bool is_terminal(sim::OrderState st)
  {
    return st == sim::OrderState::Filled || st == sim::OrderState::Cancelled ||
           st == sim::OrderState::Rejected;
  }

  inline bool is_resting(sim::OrderState st)
  {
    return st == sim::OrderState::Active || st == sim::OrderState::Partial;
  }
} // namespace

namespace sim
{
  u64 MarketSimulator::place_limit(const LimitOrderRequest& req)
  {
    if ( next_order_id_ == 0 || next_order_id_ > params_.max_orders ) {
      (void)push_event_(
          now_,
          0,
          EventType::Reject,
          OrderState::Rejected,
          RejectReason::InsufficientResources);
      return 0;
    }
    if ( orders_.size() >= params_.max_orders ) {
      (void)push_event_(
          now_,
          0,
          EventType::Reject,
          OrderState::Rejected,
          RejectReason::InsufficientResources);
      return 0;
    }

    const RejectReason vr = validate_limit_(req);
    if ( vr != RejectReason::None ) {
      (void)push_event_(now_, 0, EventType::Reject, OrderState::Rejected, vr);
      return 0;
    }

    // Must be able to log submit for auditability
    if ( events_.size() >= params_.max_events ) {
      (void)push_event_(
          now_,
          0,
          EventType::Reject,
          OrderState::Rejected,
          RejectReason::InsufficientResources);
      return 0;
    }

    const RejectReason rr = risk_check_and_lock_limit_(req.side, req.price_q, req.qty_q);
    if ( rr != RejectReason::None ) {
      (void)push_event_(now_, 0, EventType::Reject, OrderState::Rejected, rr);
      return 0;
    }

    const u64 id = next_order_id_++;
    const u64 idx = static_cast<u64>(orders_.size());

    Order o{};
    o.id = id;
    o.client_order_id = req.client_order_id;
    o.type = OrderType::Limit;
    o.side = req.side;
    o.price_q = req.price_q;
    o.qty_q = req.qty_q;
    o.submit_ts = now_;
    o.activate_ts = now_ + params_.outbound_latency;
    o.state = OrderState::Pending;

    orders_.push_back(o);
    id_to_index_[id] = idx;

    if ( !push_event_(now_, id, EventType::Submit, OrderState::Pending, RejectReason::None) ) {
      // Roll back deterministically (should be unreachable due to pre-check)
      id_to_index_[id] = kInvalidIndex;
      orders_.pop_back();
      unlock_on_cancel_(o);
      return 0;
    }

    pending_.push(PendingEntry{o.activate_ts, next_seq_++, id});
    return id;
  }

  u64 MarketSimulator::place_market(const MarketOrderRequest& req)
  {
    const RejectReason vr = validate_market_(req);
    if ( vr != RejectReason::None ) {
      (void)push_event_(now_, 0, EventType::Reject, OrderState::Rejected, vr);
      return 0;
    }
    // Market orders not supported until pricing/locking rule exists
    (void)
        push_event_(now_, 0, EventType::Reject, OrderState::Rejected, RejectReason::InvalidParams);
    return 0;
  }

  bool MarketSimulator::cancel(u64 order_id)
  {
    if ( order_id == 0 || order_id >= id_to_index_.size() )
      return false;

    const u64 idx = id_to_index_[order_id];
    if ( idx == kInvalidIndex )
      return false;

    Order& o = orders_[idx];
    if ( is_terminal(o.state) )
      return false;

    // Auditability: must be able to log cancel
    if ( events_.size() >= params_.max_events )
      return false;

    if ( is_resting(o.state) ) {
      if ( o.side == Side::Buy )
        remove_active_bid_(o.id, idx);
      else
        remove_active_ask_(o.id, idx);
    }

    unlock_on_cancel_(o);
    o.state = OrderState::Cancelled;

    return push_event_(now_, o.id, EventType::Cancel, OrderState::Cancelled, RejectReason::None);
  }

  RejectReason MarketSimulator::validate_limit_(const LimitOrderRequest& req) const
  {
    if ( req.qty_q <= 0 || req.price_q <= 0 )
      return RejectReason::InvalidParams;
    return RejectReason::None;
  }

  RejectReason MarketSimulator::validate_market_(const MarketOrderRequest& req) const
  {
    if ( req.qty_q <= 0 )
      return RejectReason::InvalidParams;
    return RejectReason::None;
  }

  RejectReason MarketSimulator::risk_check_and_lock_limit_(Side side, i64 price_q, i64 qty_q)
  {
    if ( price_q <= 0 || qty_q <= 0 )
      return RejectReason::InvalidParams;

    if ( side == Side::Buy ) {
      i64 required = 0;
      if ( mul_i64_overflow(price_q, qty_q, &required) )
        return RejectReason::InvalidParams;
      if ( required < 0 )
        return RejectReason::InvalidParams;

      if ( ledger_.cash_q - ledger_.locked_cash_q < required )
        return RejectReason::InsufficientFunds;

      ledger_.locked_cash_q += required;
      return RejectReason::None;
    }

    // Sell: lock base quantity (spot/no-short optionally enforced)
    if ( params_.risk.spot_no_short ) {
      if ( ledger_.position_qty_q - ledger_.locked_position_qty_q < qty_q )
        return RejectReason::InsufficientFunds;
    }
    ledger_.locked_position_qty_q += qty_q;
    return RejectReason::None;
  }

  RejectReason MarketSimulator::risk_check_and_lock_market_(Side, i64)
  {
    return RejectReason::InvalidParams;
  }

  void MarketSimulator::unlock_on_cancel_(const Order& o)
  {
    const i64 remaining = o.qty_q - o.filled_qty_q;
    if ( remaining <= 0 )
      return;

    if ( o.type != OrderType::Limit )
      return;

    if ( o.side == Side::Buy ) {
      i64 delta = 0;
      if ( mul_i64_overflow(o.price_q, remaining, &delta) ) {
        // Should never happen if lock used same arithmetic
        ledger_.locked_cash_q = 0;
      }
      else {
        ledger_.locked_cash_q -= delta;
        if ( ledger_.locked_cash_q < 0 )
          ledger_.locked_cash_q = 0;
      }
      if ( ledger_.locked_cash_q < 0 )
        ledger_.locked_cash_q = 0;
    }
    else {
      ledger_.locked_position_qty_q -= remaining;
      if ( ledger_.locked_position_qty_q < 0 )
        ledger_.locked_position_qty_q = 0;
    }
  }

} // namespace sim
