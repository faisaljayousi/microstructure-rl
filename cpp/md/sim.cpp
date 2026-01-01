#include "sim.hpp"

#include <algorithm>
#include <limits>

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

} // namespace

namespace sim
{

  namespace
  {

    inline bool is_terminal(OrderState st)
    {
      return st == OrderState::Filled || st == OrderState::Cancelled || st == OrderState::Rejected;
    }

    inline bool is_resting(OrderState st)
    {
      return st == OrderState::Active || st == OrderState::Partial;
    }

  } // namespace

  MarketSimulator::MarketSimulator(const SimulatorParams& params) : params_(params) {}

  void MarketSimulator::reset(Ns start_ts, Ledger initial_ledger)
  {
    SIM_ASSERT(params_.max_orders > 0);
    SIM_ASSERT(params_.max_events > 0);
    SIM_ASSERT(params_.alpha_ppm <= 1'000'000);

    now_ = start_ts;
    ledger_ = initial_ledger;
    market_ = nullptr;

    orders_.clear();
    events_.clear();
    pending_ = decltype(pending_)();

    next_order_id_ = 1;
    next_seq_ = 1;

    id_to_index_.assign(params_.max_orders + 1, kInvalidIndex);
    id_to_index_[0] = kInvalidIndex;

    active_bid_pos_.assign(params_.max_orders + 1, kInvalidIndex);
    active_ask_pos_.assign(params_.max_orders + 1, kInvalidIndex);

    orders_.reserve(params_.max_orders);
    events_.reserve(params_.max_events);

    active_bids_.clear();
    active_asks_.clear();
    active_bids_.reserve(params_.max_orders);
    active_asks_.reserve(params_.max_orders);

    active_bid_price_counts_.clear();
    active_ask_price_counts_.clear();

    has_active_bids_ = false;
    has_active_asks_ = false;
    best_active_bid_q_ = 0;
    best_active_ask_q_ = 0;

    SIM_ASSERT(ledger_.locked_cash_q >= 0);
    SIM_ASSERT(ledger_.locked_position_qty_q >= 0);
  }

  void MarketSimulator::step(const md::l2::Record& rec)
  {
    market_ = &rec;

    SIM_ASSERT(rec.ts_recv_ns >= 0);
    now_ = Ns{static_cast<u64>(rec.ts_recv_ns)};

    while ( !pending_.empty() && pending_.top().activate_ts <= now_ ) {
      const PendingEntry e = pending_.top();
      pending_.pop();

      if ( e.order_id == 0 || e.order_id >= id_to_index_.size() )
        continue;

      const u64 idx = id_to_index_[e.order_id];
      if ( idx == kInvalidIndex )
        continue;

      Order& o = orders_[idx];
      if ( o.state != OrderState::Pending )
        continue;

      if ( !apply_stp_on_activate_(o) )
        continue;

      if ( !push_event_(now_, o.id, EventType::Activate, OrderState::Active, RejectReason::None) ) {
        unlock_on_cancel_(o);
        o.state = OrderState::Rejected;
        o.reject_reason = RejectReason::InsufficientResources;
        continue;
      }

      o.state = OrderState::Active;

      const u64 oid = o.id;

      if ( o.side == Side::Buy ) {
        active_bid_pos_[oid] = static_cast<u64>(active_bids_.size());
        active_bids_.push_back(idx);

        ++active_bid_price_counts_[o.price_q];
        has_active_bids_ = true;
        best_active_bid_q_ = active_bid_price_counts_.rbegin()->first;
      }
      else {
        active_ask_pos_[oid] = static_cast<u64>(active_asks_.size());
        active_asks_.push_back(idx);

        ++active_ask_price_counts_[o.price_q];
        has_active_asks_ = true;
        best_active_ask_q_ = active_ask_price_counts_.begin()->first;
      }
    }

    market_ = nullptr;
  }

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

  // ---------------- promoted helpers: active-set removal ----------------

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

  // ---------------- remaining helpers ----------------

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

  bool MarketSimulator::push_event_(Ns ts, u64 id, EventType et, OrderState st, RejectReason rr)
  {
    if ( events_.size() >= params_.max_events )
      return false;
    events_.push_back(Event{ts, id, et, st, rr});
    return true;
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
