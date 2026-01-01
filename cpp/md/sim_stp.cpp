#include <cstddef> // std::size_t

#include "sim.hpp"

namespace sim
{
  namespace
  {
    inline bool is_resting(OrderState st)
    {
      return st == OrderState::Active || st == OrderState::Partial;
    }
  } // namespace

  bool MarketSimulator::apply_stp_on_activate_(Order& incoming)
  {
    if ( params_.stp == StpPolicy::None )
      return true;

    // O(1) detection using best resting prices.
    bool self_cross = false;
    if ( incoming.type == OrderType::Market ) {
      self_cross = (incoming.side == Side::Buy) ? has_active_asks_ : has_active_bids_;
    }
    else if ( incoming.side == Side::Buy ) {
      self_cross = has_active_asks_ && (incoming.price_q >= best_active_ask_q_);
    }
    else {
      self_cross = has_active_bids_ && (incoming.price_q <= best_active_bid_q_);
    }

    if ( !self_cross )
      return true;

    if ( params_.stp == StpPolicy::RejectIncoming ) {
      RejectReason rr = RejectReason::SelfTradePrevention;
      if ( !push_event_(now_, incoming.id, EventType::Reject, OrderState::Rejected, rr) ) {
        rr = RejectReason::InsufficientResources; // best-effort: cannot log
      }
      unlock_on_cancel_(incoming);
      incoming.state = OrderState::Rejected;
      incoming.reject_reason = rr;
      return false;
    }

    // CancelResting: cancel ALL crossing opposite resting orders.
    std::size_t cancel_count = 0;
    if ( incoming.side == Side::Buy ) {
      for ( u64 oidx : active_asks_ ) {
        const Order& r = orders_[oidx];
        if ( !is_resting(r.state) )
          continue;
        if ( incoming.type == OrderType::Market || r.price_q <= incoming.price_q )
          ++cancel_count;
      }
    }
    else {
      for ( u64 oidx : active_bids_ ) {
        const Order& r = orders_[oidx];
        if ( !is_resting(r.state) )
          continue;
        if ( incoming.type == OrderType::Market || r.price_q >= incoming.price_q )
          ++cancel_count;
      }
    }

    if ( events_.size() + cancel_count > params_.max_events ) {
      const RejectReason rr = RejectReason::InsufficientResources;
      (void)push_event_(now_, incoming.id, EventType::Reject, OrderState::Rejected, rr);
      unlock_on_cancel_(incoming);
      incoming.state = OrderState::Rejected;
      incoming.reject_reason = rr;
      return false;
    }

    // IMPORTANT: iterate with index because remove_active_* does swap-pop.
    if ( incoming.side == Side::Buy ) {
      std::size_t i = 0;
      while ( i < active_asks_.size() ) {
        const u64 oidx = active_asks_[i];
        Order& r = orders_[oidx];

        const bool cross =
            is_resting(r.state) &&
            (incoming.type == OrderType::Market || r.price_q <= incoming.price_q);

        if ( !cross ) {
          ++i;
          continue;
        }

        unlock_on_cancel_(r);
        r.state = OrderState::Cancelled;
        (void)push_event_(now_, r.id, EventType::Cancel, OrderState::Cancelled, RejectReason::None);

        // swap-pop removal; do not increment i
        remove_active_ask_(r.id, oidx);
      }
    }
    else {
      std::size_t i = 0;
      while ( i < active_bids_.size() ) {
        const u64 oidx = active_bids_[i];
        Order& r = orders_[oidx];

        const bool cross =
            is_resting(r.state) &&
            (incoming.type == OrderType::Market || r.price_q >= incoming.price_q);

        if ( !cross ) {
          ++i;
          continue;
        }

        unlock_on_cancel_(r);
        r.state = OrderState::Cancelled;
        (void)push_event_(now_, r.id, EventType::Cancel, OrderState::Cancelled, RejectReason::None);

        remove_active_bid_(r.id, oidx);
      }
    }

    return true;
  }

} // namespace sim
