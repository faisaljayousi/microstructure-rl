#include "sim.hpp"

#include <algorithm>

#include "sim_queue.hpp"

namespace sim
{

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

      sim::queue::init_on_activate(*market_, o);

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

    for ( u64 idx : active_bids_ )
      sim::queue::update_one(*market_, params_, orders_[idx]);
    for ( u64 idx : active_asks_ )
      sim::queue::update_one(*market_, params_, orders_[idx]);

    market_ = nullptr;
  }

  bool MarketSimulator::push_event_(Ns ts, u64 id, EventType et, OrderState st, RejectReason rr)
  {
    if ( events_.size() >= params_.max_events )
      return false;
    events_.push_back(Event{ts, id, et, st, rr});
    return true;
  }

} // namespace sim
