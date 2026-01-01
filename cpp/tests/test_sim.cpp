#include <cassert>
#include <cstdint>

#include "schema.hpp"
#include "sim.hpp"

namespace
{

  using sim::i64;
  using sim::u64;

  md::l2::Record make_record_ns(
      std::int64_t ts_recv_ns,
      i64 best_bid_p = 100,
      i64 best_bid_q = 10,
      i64 best_ask_p = 101,
      i64 best_ask_q = 10)
  {
    md::l2::Record r{};
    r.ts_event_ms = 0;
    r.ts_recv_ns = ts_recv_ns;

    for ( std::size_t i = 0; i < md::l2::kDepth; ++i ) {
      r.bids[i] = md::l2::Level{md::l2::kBidNullPriceQ, md::l2::kNullQtyQ};
      r.asks[i] = md::l2::Level{md::l2::kAskNullPriceQ, md::l2::kNullQtyQ};
    }

    r.bids[0] = md::l2::Level{best_bid_p, best_bid_q};
    r.asks[0] = md::l2::Level{best_ask_p, best_ask_q};

    return r;
  }

  inline bool is_terminal(sim::OrderState st)
  {
    return st == sim::OrderState::Filled || st == sim::OrderState::Cancelled ||
           st == sim::OrderState::Rejected;
  }

} // namespace

int main()
{
  // ----------------------------
  // 1) Latency gating + lock monotonicity + cancel unlock
  // ----------------------------
  sim::SimulatorParams p{};
  p.max_orders = 2;
  p.max_events = 256;
  p.alpha_ppm = 500'000;
  p.outbound_latency = sim::Ns{10};
  p.stp = sim::StpPolicy::RejectIncoming;

  sim::MarketSimulator s(p);
  sim::Ledger led{};
  led.cash_q = 1'000'000;
  led.position_qty_q = 1'000'000;
  s.reset(sim::Ns{0}, led);

  assert(s.now().value == 0); 

  sim::LimitOrderRequest buy{};
  buy.side = sim::Side::Buy;
  buy.price_q = 100;
  buy.qty_q = 10;

  const u64 id = s.place_limit(buy);
  assert(id != 0); 

  const i64 locked_after_submit = s.ledger().locked_cash_q;
  assert(locked_after_submit > 0); 

  // Before latency: must not be terminal.
  s.step(make_record_ns(5));
  assert(!is_terminal(s.orders().at(0).state)); 

  // After latency: must no longer be PENDING (could be ACTIVE, PARTIAL, FILLED, REJECTED via STP).
  s.step(make_record_ns(10));
  assert(s.orders().at(0).state != sim::OrderState::Pending); 

  // Cancel (if not already terminal) should not increase locked cash; if it cancels, locked should
  // drop.
  const i64 locked_before_cancel = s.ledger().locked_cash_q;
  (void)s.cancel(id); // may be false if already terminal in later phases
  assert(s.ledger().locked_cash_q <= locked_before_cancel); 

  // ----------------------------
  // 2) Hard cap: max_orders is lifetime cap
  // ----------------------------
  sim::LimitOrderRequest buy2 = buy;
  buy2.price_q = 99;
  const u64 id2 = s.place_limit(buy2);
  assert(id2 != 0); 

  sim::LimitOrderRequest buy3 = buy;
  buy3.price_q = 98;
  const u64 id3 = s.place_limit(buy3);
  assert(id3 == 0); // third order rejected due to max_orders cap

  // ----------------------------
  // 3) STP invariants
  // ----------------------------
  // RejectIncoming: crossing incoming must be REJECTED; resting must remain non-cancelled.
  {
    sim::SimulatorParams p0 = p;
    p0.max_orders = 16;
    p0.outbound_latency = sim::Ns{0};
    p0.stp = sim::StpPolicy::RejectIncoming;

    sim::MarketSimulator ex(p0);
    sim::Ledger l{};
    l.cash_q = 1'000'000;
    l.position_qty_q = 1'000'000;
    ex.reset(sim::Ns{0}, l);

    auto r0 = make_record_ns(0);
    ex.step(r0);

    sim::LimitOrderRequest ask{};
    ask.side = sim::Side::Sell;
    ask.price_q = 101;
    ask.qty_q = 10;

    const u64 ask_id = ex.place_limit(ask);
    ex.step(r0); // activate ask (and possibly fill in later phases, but not cancel via STP here)

    sim::LimitOrderRequest cross_buy{};
    cross_buy.side = sim::Side::Buy;
    cross_buy.price_q = 102;
    cross_buy.qty_q = 10;

    const u64 buy_id = ex.place_limit(cross_buy);
    ex.step(r0); // activation attempt -> STP applies

    const sim::Order& incoming = ex.orders().back();
    assert(incoming.id == buy_id && incoming.state == sim::OrderState::Rejected);

    const sim::Order& resting = ex.orders().front();
    assert(resting.id == ask_id && resting.state != sim::OrderState::Cancelled);
  }

  return 0;
}
