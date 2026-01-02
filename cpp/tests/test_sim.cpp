#include <cassert>
#include <cstddef>
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

  md::l2::Record make_record_one_bid_level(
      std::int64_t ts_recv_ns,
      i64 best_bid_p,
      i64 best_bid_q,
      i64 bid1_p,
      i64 bid1_q,
      i64 best_ask_p = 101,
      i64 best_ask_q = 10)
  {
    auto r = make_record_ns(ts_recv_ns, best_bid_p, best_bid_q, best_ask_p, best_ask_q);
    r.bids[1] = md::l2::Level{bid1_p, bid1_q};
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
  // Base params used as a template in per-test scopes.
  sim::SimulatorParams p{};
  p.max_orders = 32;
  p.max_events = 1024;
  p.alpha_ppm = 500'000;
  p.outbound_latency = sim::Ns{10};
  p.stp = sim::StpPolicy::RejectIncoming;

  // ----------------------------
  // 1) Latency gating + lock monotonicity + cancel unlock
  // ----------------------------
  {
    sim::SimulatorParams p1 = p;
    p1.max_orders = 2;
    p1.max_events = 256;
    p1.outbound_latency = sim::Ns{10};

    sim::MarketSimulator s(p1);
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

    // After latency: must no longer be PENDING.
    s.step(make_record_ns(10));
    assert(s.orders().at(0).state != sim::OrderState::Pending);

    const i64 locked_before_cancel = s.ledger().locked_cash_q;
    const bool ok = s.cancel(id);
    const i64 locked_after_cancel = s.ledger().locked_cash_q;
    if ( ok ) {
      assert(locked_after_cancel <= locked_before_cancel);
    }
    else {
      assert(locked_after_cancel == locked_before_cancel);
    }
  }

  // ----------------------------
  // 2) Hard cap: max_orders is lifetime cap
  // ----------------------------
  {
    sim::SimulatorParams p1 = p;
    p1.max_orders = 2;
    p1.max_events = 256;
    p1.outbound_latency = sim::Ns{0};

    sim::MarketSimulator s(p1);
    sim::Ledger led{};
    led.cash_q = 1'000'000;
    led.position_qty_q = 1'000'000;
    s.reset(sim::Ns{0}, led);

    auto r0 = make_record_ns(0);
    s.step(r0);

    sim::LimitOrderRequest buy{};
    buy.side = sim::Side::Buy;
    buy.price_q = 100;
    buy.qty_q = 10;

    const u64 id1 = s.place_limit(buy);
    assert(id1 != 0);

    sim::LimitOrderRequest buy2 = buy;
    buy2.price_q = 99;
    const u64 id2 = s.place_limit(buy2);
    assert(id2 != 0);

    sim::LimitOrderRequest buy3 = buy;
    buy3.price_q = 98;
    const u64 id3 = s.place_limit(buy3);
    assert(id3 == 0); // third order rejected due to max_orders cap
  }

  // ----------------------------
  // 3) STP invariants (RejectIncoming)
  // ----------------------------
  {
    sim::SimulatorParams p0 = p;
    p0.max_orders = 16;
    p0.max_events = 256;
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
    assert(ask_id != 0);
    ex.step(r0); // activate ask

    sim::LimitOrderRequest cross_buy{};
    cross_buy.side = sim::Side::Buy;
    cross_buy.price_q = 102;
    cross_buy.qty_q = 10;

    const u64 buy_id = ex.place_limit(cross_buy);
    assert(buy_id != 0);
    ex.step(r0); // activation attempt -> STP applies

    const sim::Order& incoming = ex.orders().back();
    assert(incoming.id == buy_id);
    assert(incoming.state == sim::OrderState::Rejected);

    const sim::Order& resting = ex.orders().front();
    assert(resting.id == ask_id);
    assert(resting.state != sim::OrderState::Cancelled);
  }

  // ----------------------------
  // Queue semantics
  // ----------------------------

  // Activation "join the tail" when price exists
  {
    sim::SimulatorParams p2 = p;
    p2.max_orders = 8;
    p2.max_events = 256;
    p2.outbound_latency = sim::Ns{0};
    p2.alpha_ppm = 500'000;

    sim::MarketSimulator ex(p2);
    sim::Ledger l{};
    l.cash_q = 1'000'000;
    l.position_qty_q = 1'000'000;
    ex.reset(sim::Ns{0}, l);

    // price 99 exists at bids[1] with qty=40
    auto r0 = make_record_one_bid_level(0, 100, 10, 99, 40, 101, 10);
    ex.step(r0);

    sim::LimitOrderRequest bid{};
    bid.side = sim::Side::Buy;
    bid.price_q = 99;
    bid.qty_q = 5;

    const u64 idb = ex.place_limit(bid);
    assert(idb != 0);
    ex.step(r0); // activate

    const sim::Order& o = ex.orders().back();
    assert(o.id == idb);
    assert(o.state == sim::OrderState::Active);
    assert(o.visibility == sim::Visibility::Visible);
    assert(o.qty_ahead_q == 40); // join tail
    assert(o.last_level_qty_q == 40);
    assert(o.last_level_idx == 1);
  }

  // Activation "you are the queue" when within-range but missing price
  {
    sim::SimulatorParams p2 = p;
    p2.max_orders = 8;
    p2.max_events = 256;
    p2.outbound_latency = sim::Ns{0};

    sim::MarketSimulator ex(p2);
    sim::Ledger l{};
    l.cash_q = 1'000'000;
    l.position_qty_q = 1'000'000;
    ex.reset(sim::Ns{0}, l);

    // Visible bid ladder: 100, then 98 (so 99 is within range but absent)
    auto r0 = make_record_one_bid_level(0, 100, 10, 98, 10, 101, 10);
    ex.step(r0);

    sim::LimitOrderRequest bid{};
    bid.side = sim::Side::Buy;
    bid.price_q = 99; // within [100..98] but not present
    bid.qty_q = 5;

    const u64 idb = ex.place_limit(bid);
    assert(idb != 0);
    ex.step(r0); // activate

    const sim::Order& o = ex.orders().back();
    assert(o.id == idb);
    assert(o.state == sim::OrderState::Active);
    assert(o.visibility == sim::Visibility::Visible);
    assert(o.last_level_idx == -1);
    assert(o.last_level_qty_q == 0);
    assert(o.qty_ahead_q == 0);
  }

  // Blind if outside top-N range (better than best bid)
  {
    sim::SimulatorParams p2 = p;
    p2.max_orders = 8;
    p2.max_events = 256;
    p2.outbound_latency = sim::Ns{0};

    sim::MarketSimulator ex(p2);
    sim::Ledger l{};
    l.cash_q = 1'000'000;
    l.position_qty_q = 1'000'000;
    ex.reset(sim::Ns{0}, l);

    auto r0 = make_record_one_bid_level(0, 100, 10, 99, 10, 101, 10);
    ex.step(r0);

    sim::LimitOrderRequest bid{};
    bid.side = sim::Side::Buy;
    bid.price_q = 101; // better than best bid => outside visible range
    bid.qty_q = 5;

    const u64 idb = ex.place_limit(bid);
    assert(idb != 0);
    ex.step(r0);

    const sim::Order& o = ex.orders().back();
    assert(o.id == idb);
    assert(o.state == sim::OrderState::Active);
    assert(o.visibility == sim::Visibility::Blind);
    assert(o.last_level_idx == -1);
    assert(o.qty_ahead_q == 0);
  }

  // Depletion update uses alpha + min-depletion rule
  {
    sim::SimulatorParams p2 = p;
    p2.max_orders = 8;
    p2.max_events = 256;
    p2.outbound_latency = sim::Ns{0};
    p2.alpha_ppm = 500'000; // 0.5

    sim::MarketSimulator ex(p2);
    sim::Ledger l{};
    l.cash_q = 1'000'000;
    l.position_qty_q = 1'000'000;
    ex.reset(sim::Ns{0}, l);

    auto r0 = make_record_one_bid_level(0, 100, 10, 99, 40, 101, 10);
    ex.step(r0);

    sim::LimitOrderRequest bid{};
    bid.side = sim::Side::Buy;
    bid.price_q = 99;
    bid.qty_q = 5;

    const u64 idb = ex.place_limit(bid);
    assert(idb != 0);
    ex.step(r0); // activate, qty_ahead=40

    const sim::Order& o0 = ex.orders().back();
    assert(o0.qty_ahead_q == 40);

    // Next snapshot: same price level qty drops 40 -> 39 (depl=1 => eff=floor(0.5)=0 => min=1)
    auto r1 = make_record_one_bid_level(1, 100, 10, 99, 39, 101, 10);
    ex.step(r1);

    const sim::Order& o1 = ex.orders().back();
    assert(o1.last_level_qty_q == 39);
    assert(o1.qty_ahead_q == 39);
  }

  // Visible -> Frozen when level disappears from top-N
  {
    sim::SimulatorParams p2 = p;
    p2.max_orders = 8;
    p2.max_events = 256;
    p2.outbound_latency = sim::Ns{0};
    p2.alpha_ppm = 1'000'000; // make effects obvious

    sim::MarketSimulator ex(p2);
    sim::Ledger l{};
    l.cash_q = 1'000'000;
    l.position_qty_q = 1'000'000;
    ex.reset(sim::Ns{0}, l);

    auto r0 = make_record_one_bid_level(0, 100, 10, 99, 40, 101, 10);
    ex.step(r0);

    sim::LimitOrderRequest bid{};
    bid.side = sim::Side::Buy;
    bid.price_q = 99;
    bid.qty_q = 5;

    const u64 idb = ex.place_limit(bid);
    assert(idb != 0);
    ex.step(r0); // activate visible at idx=1

    // Next snapshot: 99 disappears (bids[1] remains null)
    auto r1 = make_record_ns(1, 100, 10, 101, 10);
    ex.step(r1);

    const sim::Order& o = ex.orders().back();
    assert(o.id == idb);
    assert(o.visibility == sim::Visibility::Frozen);
  }

  // Frozen -> Visible re-anchor pessimistically
  {
    sim::SimulatorParams p2 = p;
    p2.max_orders = 8;
    p2.max_events = 256;
    p2.outbound_latency = sim::Ns{0};
    p2.alpha_ppm = 1'000'000;

    sim::MarketSimulator ex(p2);
    sim::Ledger l{};
    l.cash_q = 1'000'000;
    l.position_qty_q = 1'000'000;
    ex.reset(sim::Ns{0}, l);

    auto r0 = make_record_one_bid_level(0, 100, 10, 99, 40, 101, 10);
    ex.step(r0);

    sim::LimitOrderRequest bid{};
    bid.side = sim::Side::Buy;
    bid.price_q = 99;
    bid.qty_q = 5;

    const u64 idb = ex.place_limit(bid);
    assert(idb != 0);
    ex.step(r0); // qty_ahead=40

    // disappear => Frozen
    auto r1 = make_record_ns(1, 100, 10, 101, 10);
    ex.step(r1);
    assert(ex.orders().back().visibility == sim::Visibility::Frozen);

    // reappear with qty=77 => re-anchor qty_ahead=77
    auto r2 = make_record_one_bid_level(2, 100, 10, 99, 77, 101, 10);
    ex.step(r2);

    const sim::Order& o = ex.orders().back();
    assert(o.id == idb);
    assert(o.visibility == sim::Visibility::Visible);
    assert(o.qty_ahead_q == 77);
    assert(o.last_level_qty_q == 77);
    assert(o.last_level_idx == 1);
  }

  // Trade-through sets qty_ahead_q = 0 (no fill yet)
  {
    sim::SimulatorParams p2 = p;
    p2.max_orders = 8;
    p2.max_events = 256;
    p2.outbound_latency = sim::Ns{0};
    p2.alpha_ppm = 1'000'000;

    sim::MarketSimulator ex(p2);
    sim::Ledger l{};
    l.cash_q = 1'000'000;
    l.position_qty_q = 1'000'000;
    ex.reset(sim::Ns{0}, l);

    auto r0 = make_record_one_bid_level(0, 100, 10, 99, 40, 101, 10);
    ex.step(r0);

    sim::LimitOrderRequest bid{};
    bid.side = sim::Side::Buy;
    bid.price_q = 99;
    bid.qty_q = 5;

    const u64 idb = ex.place_limit(bid);
    assert(idb != 0);
    ex.step(r0);
    assert(ex.orders().back().qty_ahead_q == 40);

    // best ask crosses down to 99 => trade-through signal
    auto r1 = make_record_one_bid_level(1, 100, 10, 99, 40, 99, 10);
    ex.step(r1);

    const sim::Order& o = ex.orders().back();
    assert(o.id == idb);
    assert(o.qty_ahead_q == 0);
    assert(o.state == sim::OrderState::Active);
  }

  // ----------------------------
  // STP invariants (CancelResting)
  // ----------------------------
  {
    sim::SimulatorParams p0 = p;
    p0.max_orders = 16;
    p0.max_events = 256;
    p0.outbound_latency = sim::Ns{0};
    p0.stp = sim::StpPolicy::CancelResting;

    sim::MarketSimulator ex(p0);
    sim::Ledger l{};
    l.cash_q = 1'000'000;
    l.position_qty_q = 1'000'000;
    ex.reset(sim::Ns{0}, l);

    auto r0 = make_record_ns(0);
    ex.step(r0);

    // Resting asks at 101 and 103
    sim::LimitOrderRequest ask1{};
    ask1.side = sim::Side::Sell;
    ask1.price_q = 101;
    ask1.qty_q = 10;
    const u64 ask_id1 = ex.place_limit(ask1);
    assert(ask_id1 != 0);

    sim::LimitOrderRequest ask2 = ask1;
    ask2.price_q = 103;
    const u64 ask_id2 = ex.place_limit(ask2);
    assert(ask_id2 != 0);

    ex.step(r0); // activate both asks

    // Incoming buy that crosses only 101 (<=102), not 103.
    sim::LimitOrderRequest buy{};
    buy.side = sim::Side::Buy;
    buy.price_q = 102;
    buy.qty_q = 10;

    const u64 buy_id = ex.place_limit(buy);
    assert(buy_id != 0);

    ex.step(
        r0); // activation attempt -> CancelResting should cancel ask@101 then allow buy to activate

    // ask@101 cancelled
    const sim::Order& a1 = ex.orders().at(ex.orders().size() - 3); // ask1
    assert(a1.id == ask_id1);
    assert(a1.state == sim::OrderState::Cancelled);

    // ask@103 remains active
    const sim::Order& a2 = ex.orders().at(ex.orders().size() - 2); // ask2
    assert(a2.id == ask_id2);
    assert(a2.state == sim::OrderState::Active);

    // incoming buy activates (not rejected)
    const sim::Order& b = ex.orders().back();
    assert(b.id == buy_id);
    assert(b.state == sim::OrderState::Active);
  }

  // ----------------------------
  // Bucket integrity: 2 orders at same price; cancel one; remaining stays removable
  // ----------------------------
  {
    sim::SimulatorParams p2 = p;
    p2.max_orders = 8;
    p2.max_events = 256;
    p2.outbound_latency = sim::Ns{0};

    sim::MarketSimulator ex(p2);
    sim::Ledger l{};
    l.cash_q = 1'000'000;
    l.position_qty_q = 1'000'000;
    ex.reset(sim::Ns{0}, l);

    auto r0 = make_record_one_bid_level(0, 100, 10, 99, 40, 101, 10);
    ex.step(r0);

    sim::LimitOrderRequest b{};
    b.side = sim::Side::Buy;
    b.price_q = 99;
    b.qty_q = 5;

    const u64 id1 = ex.place_limit(b);
    const u64 id2 = ex.place_limit(b);
    assert(id1 != 0 && id2 != 0);

    ex.step(r0); // activate both

    // Both are active
    const sim::Order& o1 = ex.orders().at(ex.orders().size() - 2);
    const sim::Order& o2 = ex.orders().back();
    assert(o1.id == id1);
    assert(o2.id == id2);
    assert(o1.state == sim::OrderState::Active);
    assert(o2.state == sim::OrderState::Active);

    // Cancel first; second must remain active and still cancelable
    assert(ex.cancel(id1));
    assert(ex.orders().at(ex.orders().size() - 2).state == sim::OrderState::Cancelled);

    // Step to process any pending bookkeeping
    ex.step(r0);

    // Second still active
    const sim::Order& o2a = ex.orders().back();
    assert(o2a.id == id2);
    assert(o2a.state == sim::OrderState::Active);

    // Cancel second too (must succeed)
    assert(ex.cancel(id2));
    assert(ex.orders().back().state == sim::OrderState::Cancelled);
  }

  // ----------------------------
  // Best-price scalar maintenance: removing best bid updates STP detection
  // ----------------------------
  {
    sim::SimulatorParams p2 = p;
    p2.max_orders = 16;
    p2.max_events = 256;
    p2.outbound_latency = sim::Ns{0};
    p2.stp = sim::StpPolicy::RejectIncoming;

    sim::MarketSimulator ex(p2);
    sim::Ledger l{};
    l.cash_q = 1'000'000;
    l.position_qty_q = 1'000'000;
    ex.reset(sim::Ns{0}, l);

    auto r0 = make_record_ns(0, 100, 10, 101, 10);
    ex.step(r0);

    // Resting bids at 100 and 99
    sim::LimitOrderRequest b1{};
    b1.side = sim::Side::Buy;
    b1.price_q = 100;
    b1.qty_q = 10;
    const u64 id100 = ex.place_limit(b1);
    assert(id100 != 0);

    sim::LimitOrderRequest b2 = b1;
    b2.price_q = 99;
    const u64 id99 = ex.place_limit(b2);
    assert(id99 != 0);

    ex.step(r0); // activate both

    // Cancel best bid 100
    assert(ex.cancel(id100));
    ex.step(r0); // advance so any best-price maintenance is applied by your implementation

    // Incoming sell at 99 should now self-cross against remaining bid@99 and be rejected.
    sim::LimitOrderRequest s1{};
    s1.side = sim::Side::Sell;
    s1.price_q = 99;
    s1.qty_q = 1;

    const u64 sell_id = ex.place_limit(s1);
    assert(sell_id != 0);
    ex.step(r0); // activate attempt -> STP applies

    const sim::Order& incoming = ex.orders().back();
    assert(incoming.id == sell_id);
    assert(incoming.state == sim::OrderState::Rejected);

    // Remaining bid@99 should still be active
    const sim::Order& remaining = ex.orders().at(1); // second submitted order
    assert(remaining.id == id99);
    assert(remaining.state == sim::OrderState::Active);
  }

  // ----------------------------
  // FIFO bucket integrity: cancel head then tail (same price)
  // ----------------------------
  {
    sim::SimulatorParams p2 = p;
    p2.max_orders = 16;
    p2.max_events = 256;
    p2.outbound_latency = sim::Ns{0};

    sim::MarketSimulator ex(p2);
    sim::Ledger l{};
    l.cash_q = 1'000'000;
    l.position_qty_q = 1'000'000;
    ex.reset(sim::Ns{0}, l);

    auto r0 = make_record_ns(0, /*best_bid*/ 100, /*bid_qty*/ 10, /*best_ask*/ 101, /*ask_qty*/ 10);
    ex.step(r0);

    sim::LimitOrderRequest b{};
    b.side = sim::Side::Buy;
    b.price_q = 99;
    b.qty_q = 1;

    const u64 id1 = ex.place_limit(b);
    const u64 id2 = ex.place_limit(b);
    assert(id1 != 0 && id2 != 0);

    ex.step(r0); // activate both, they should be in FIFO order at price 99

    // Cancel first (head). Must succeed.
    assert(ex.cancel(id1));
    ex.step(r0);

    // Cancel second (now head). Must still succeed and not crash.
    assert(ex.cancel(id2));
    ex.step(r0);

    // Both orders terminal cancelled
    const auto& o1 = ex.orders().at(ex.orders().size() - 2);
    const auto& o2 = ex.orders().back();
    assert(o1.id == id1 && o2.id == id2);
    assert(o1.state == sim::OrderState::Cancelled);
    assert(o2.state == sim::OrderState::Cancelled);
  }

  // ----------------------------
  // FIFO bucket integrity: cancel middle of 3 (same price)
  // ----------------------------
  {
    sim::SimulatorParams p2 = p;
    p2.max_orders = 32;
    p2.max_events = 256;
    p2.outbound_latency = sim::Ns{0};

    sim::MarketSimulator ex(p2);
    sim::Ledger l{};
    l.cash_q = 1'000'000;
    l.position_qty_q = 1'000'000;
    ex.reset(sim::Ns{0}, l);

    auto r0 = make_record_ns(0, 100, 10, 101, 10);
    ex.step(r0);

    sim::LimitOrderRequest b{};
    b.side = sim::Side::Buy;
    b.price_q = 99;
    b.qty_q = 1;

    const u64 id1 = ex.place_limit(b);
    const u64 id2 = ex.place_limit(b);
    const u64 id3 = ex.place_limit(b);
    assert(id1 && id2 && id3);

    ex.step(r0); // activate all 3

    // Cancel middle
    assert(ex.cancel(id2));
    ex.step(r0);

    // Remaining two still cancelable (proves list is still connected)
    assert(ex.cancel(id1));
    ex.step(r0);
    assert(ex.cancel(id3));
    ex.step(r0);
  }

  return 0;
}
