#include "sim.hpp"

#include <algorithm>

#include "schema.hpp"
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

    bid_prices_.clear();
    ask_prices_.clear();
    bid_buckets_.clear();
    ask_buckets_.clear();

    SIM_ASSERT(ledger_.locked_cash_q >= 0);
    SIM_ASSERT(ledger_.locked_position_qty_q >= 0);
  }

  void MarketSimulator::step(const md::l2::Record& rec)
  {
    market_ = &rec;
    now_ = Ns{static_cast<u64>(rec.ts_recv_ns)};

    // ------------------------------------------------------------
    // (1) Queue + passive fills are handled bucket-level in
    // apply_passive_fills_one_bucket_(). This is the ONLY place
    // that applies effective depletion to qty_ahead_q (no double depletion).
    // ------------------------------------------------------------
    for ( u64 i = 0; i < bid_buckets_.size(); ++i ) {
      apply_passive_fills_one_bucket_(rec, bid_prices_[i], bid_buckets_[i], Side::Buy);
    }
    for ( u64 i = 0; i < ask_buckets_.size(); ++i ) {
      apply_passive_fills_one_bucket_(rec, ask_prices_[i], ask_buckets_[i], Side::Sell);
    }

    // ------------------------------------------------------------
    // (2) Activate newly-due orders (NOT fill-eligible until next step)
    // ------------------------------------------------------------
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

      // The order becomes fill-eligible only on the next step
      sim::queue::init_on_activate(*market_, o);

      const u64 oid = o.id;

      if ( o.side == Side::Buy ) {
        active_bid_pos_[oid] = static_cast<u64>(active_bids_.size());
        active_bids_.push_back(idx);

        const u64 bidx = get_or_insert_bid_bucket_idx_(o.price_q);
        bucket_push_back_bid_(bidx, idx);

        if ( !has_active_bids_ ) {
          has_active_bids_ = true;
          best_active_bid_q_ = o.price_q;
        }
        else if ( o.price_q > best_active_bid_q_ ) {
          best_active_bid_q_ = o.price_q;
        }
      }
      else {
        active_ask_pos_[oid] = static_cast<u64>(active_asks_.size());
        active_asks_.push_back(idx);

        const u64 aidx = get_or_insert_ask_bucket_idx_(o.price_q);
        bucket_push_back_ask_(aidx, idx);

        if ( !has_active_asks_ ) {
          has_active_asks_ = true;
          best_active_ask_q_ = o.price_q;
        }
        else if ( o.price_q < best_active_ask_q_ ) {
          best_active_ask_q_ = o.price_q;
        }
      }
    }

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
