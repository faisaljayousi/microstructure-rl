#include <array>
#include <cstdint>
#include <memory>
#include <nanobind/nanobind.h>
#include <nanobind/ndarray.h>
#include <nanobind/stl/optional.h>
#include <nanobind/stl/shared_ptr.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>
#include <optional>
#include <vector>

#include "replay.hpp"
#include "schema.hpp"
#include "sim.hpp"

namespace nb = nanobind;

// Helper: safe snapshots (copies) to avoid holding references into vectors that
// may reallocate as the simulator runs
template <class T>
static std::vector<T> snapshot_vec(const std::vector<T>& v)
{
  return std::vector<T>(v.begin(), v.end());
}

namespace
{

  // Read-only view over a memory-mapped Record that keeps the ReplayKernel alive
  struct RecordView
  {
    nb::object owner;
    const md::l2::Record* rec{nullptr};

    bool valid() const noexcept { return rec != nullptr; }
    std::int64_t ts_event_ms() const noexcept { return rec->ts_event_ms; }
    std::int64_t ts_recv_ns() const noexcept { return rec->ts_recv_ns; }
    std::int64_t best_bid_price_q() const noexcept { return rec->best_bid_price_q(); }
    std::int64_t best_ask_price_q() const noexcept { return rec->best_ask_price_q(); }

    nb::ndarray<const std::int64_t, nb::numpy> bids() const
    {
      const auto* ptr = reinterpret_cast<const std::int64_t*>(rec->bids.data());
      // Expose (kDepth, 2) int64 view: [price_q, qty_q] for each level
      return nb::ndarray<const std::int64_t, nb::numpy>(
          ptr,
          {(std::size_t)md::l2::kDepth, (std::size_t)2},
          owner, // handle to Python object
          {(std::int64_t)sizeof(md::l2::Level), (std::int64_t)sizeof(std::int64_t)});
    }

    nb::ndarray<const std::int64_t, nb::numpy> asks() const
    {
      const auto* ptr = reinterpret_cast<const std::int64_t*>(rec->asks.data());
      return nb::ndarray<const std::int64_t, nb::numpy>(
          ptr,
          {(std::size_t)md::l2::kDepth, (std::size_t)2},
          owner,
          {(std::int64_t)sizeof(md::l2::Level), (std::int64_t)sizeof(std::int64_t)});
    }
  };

} // namespace

NB_MODULE(_core, m)
{
  m.doc() = "microstructure-rl C++ engine bindings";

  // ---------------------------
  // md::l2
  // ---------------------------
  nb::module_ mdl2 = m.def_submodule("md_l2", "Market data (L2) types");

  nb::class_<md::l2::ReplayKernel>(mdl2, "ReplayKernel")
      .def(nb::init<const std::string&>(), nb::arg("snap_path"))
      .def("size", &md::l2::ReplayKernel::size)
      .def("pos", &md::l2::ReplayKernel::pos)
      .def("reset", &md::l2::ReplayKernel::reset)
      .def(
          "next",
          [](md::l2::ReplayKernel& self) -> nb::object {
            const md::l2::Record* r = self.next();
            if ( !r )
              return nb::none();
            // Keep Python-side ReplayKernel alive inside RecordView
            RecordView v{nb::cast(&self, nb::rv_policy::reference), r};
            return nb::cast(v);
          },
          "Return next RecordView or None at end-of-stream");

  nb::class_<RecordView>(mdl2, "RecordView")
      .def_prop_ro("ts_event_ms", &RecordView::ts_event_ms)
      .def_prop_ro("ts_recv_ns", &RecordView::ts_recv_ns)
      .def_prop_ro("best_bid_price_q", &RecordView::best_bid_price_q)
      .def_prop_ro("best_ask_price_q", &RecordView::best_ask_price_q)
      .def("bids", &RecordView::bids, "Return (depth,2) ndarray view of [price_q, qty_q]")
      .def("asks", &RecordView::asks, "Return (depth,2) ndarray view of [price_q, qty_q]");

  // ---------------------------
  // sim
  // ---------------------------
  nb::module_ msim = m.def_submodule("sim", "Simulator types");

  // Enums
  // Strong type for time (optional to expose; we keep it internal but allow debug)
  nb::class_<sim::Ns>(msim, "Ns").def(nb::init<sim::u64>()).def_rw("value", &sim::Ns::value);

  nb::enum_<sim::Side>(msim, "Side").value("Buy", sim::Side::Buy).value("Sell", sim::Side::Sell);

  nb::enum_<sim::Tif>(msim, "Tif").value("GTC", sim::Tif::GTC).value("IOC", sim::Tif::IOC);

  nb::enum_<sim::OrderState>(msim, "OrderState")
      .value("Pending", sim::OrderState::Pending)
      .value("Active", sim::OrderState::Active)
      .value("Partial", sim::OrderState::Partial)
      .value("Filled", sim::OrderState::Filled)
      .value("Cancelled", sim::OrderState::Cancelled)
      .value("Rejected", sim::OrderState::Rejected);

  nb::enum_<sim::EventType>(msim, "EventType")
      .value("Submit", sim::EventType::Submit)
      .value("Activate", sim::EventType::Activate)
      .value("Cancel", sim::EventType::Cancel)
      .value("Reject", sim::EventType::Reject);

  nb::enum_<sim::RejectReason>(msim, "RejectReason")
      .value("None", sim::RejectReason::None)
      .value("InvalidParams", sim::RejectReason::InvalidParams)
      .value("InsufficientFunds", sim::RejectReason::InsufficientFunds)
      .value("InsufficientResources", sim::RejectReason::InsufficientResources)
      .value("SelfTradePrevention", sim::RejectReason::SelfTradePrevention)
      .value("UnknownOrderId", sim::RejectReason::UnknownOrderId)
      .value("AlreadyTerminal", sim::RejectReason::AlreadyTerminal);

  nb::enum_<sim::StpPolicy>(msim, "StpPolicy")
      .value("None", sim::StpPolicy::None)
      .value("RejectIncoming", sim::StpPolicy::RejectIncoming)
      .value("CancelResting", sim::StpPolicy::CancelResting);

  nb::enum_<sim::Visibility>(msim, "Visibility")
      .value("Visible", sim::Visibility::Visible)
      .value("Blind", sim::Visibility::Blind)
      .value("Frozen", sim::Visibility::Frozen);

  nb::enum_<sim::LiquidityFlag>(msim, "LiquidityFlag")
      .value("Maker", sim::LiquidityFlag::Maker)
      .value("Taker", sim::LiquidityFlag::Taker);

  // Full Order object (queue + markout analytics)
  nb::class_<sim::Order>(msim, "Order")
      .def(nb::init<>())
      .def_rw("id", &sim::Order::id)
      .def_rw("client_order_id", &sim::Order::client_order_id)
      .def_rw("side", &sim::Order::side)
      .def_rw("price_q", &sim::Order::price_q)
      .def_rw("qty_q", &sim::Order::qty_q)
      .def_rw("filled_qty_q", &sim::Order::filled_qty_q)
      .def_rw("qty_ahead_q", &sim::Order::qty_ahead_q)
      .def_rw("last_level_qty_q", &sim::Order::last_level_qty_q)
      .def_rw("last_level_idx", &sim::Order::last_level_idx)
      .def_rw("visibility", &sim::Order::visibility)
      .def_prop_ro("submit_ts_ns",  [](const sim::Order& o) { return o.submit_ts.value; })
      .def_prop_ro("activate_ts_ns",[](const sim::Order& o) { return o.activate_ts.value; })
      .def_rw("state", &sim::Order::state)
      .def_rw("reject_reason", &sim::Order::reject_reason);

  // Nested params structs (bind them and/or flatten in SimulatorParams)
  nb::class_<sim::FeeSchedule>(msim, "FeeSchedule")
      .def(nb::init<>())
      .def_rw("maker_fee_ppm", &sim::FeeSchedule::maker_fee_ppm)
      .def_rw("taker_fee_ppm", &sim::FeeSchedule::taker_fee_ppm);

  nb::class_<sim::RiskLimits>(msim, "RiskLimits")
      .def(nb::init<>())
      .def_rw("max_abs_position_qty_q", &sim::RiskLimits::max_abs_position_qty_q)
      .def_rw("spot_no_short", &sim::RiskLimits::spot_no_short);

  nb::class_<sim::SimulatorParams>(msim, "SimulatorParams")
      .def(nb::init<>())
      .def_rw("max_orders", &sim::SimulatorParams::max_orders)
      .def_rw("max_events", &sim::SimulatorParams::max_events)
      .def_rw("alpha_ppm", &sim::SimulatorParams::alpha_ppm)
      .def_rw("stp", &sim::SimulatorParams::stp)
      .def_rw("fees", &sim::SimulatorParams::fees)
      .def_rw("risk", &sim::SimulatorParams::risk)
      .def_prop_rw(
          "outbound_latency_ns",
          [](const sim::SimulatorParams& p) {
            return static_cast<sim::u64>(p.outbound_latency.value);
          },
          [](sim::SimulatorParams& p, sim::u64 v) { p.outbound_latency = sim::Ns{v}; })
      .def_prop_rw(
          "observation_latency_ns",
          [](const sim::SimulatorParams& p) {
            return static_cast<sim::u64>(p.observation_latency.value);
          },
          [](sim::SimulatorParams& p, sim::u64 v) { p.observation_latency = sim::Ns{v}; })
      // Convenience: flat fee fields (common from Python; avoids nested access)
      .def_prop_rw(
          "maker_fee_ppm",
          [](const sim::SimulatorParams& p) { return p.fees.maker_fee_ppm; },
          [](sim::SimulatorParams& p, sim::u64 v) { p.fees.maker_fee_ppm = v; })
      .def_prop_rw(
          "taker_fee_ppm",
          [](const sim::SimulatorParams& p) { return p.fees.taker_fee_ppm; },
          [](sim::SimulatorParams& p, sim::u64 v) { p.fees.taker_fee_ppm = v; })
      // Convenience: risk knobs
      .def_prop_rw(
          "max_abs_position_qty_q",
          [](const sim::SimulatorParams& p) { return p.risk.max_abs_position_qty_q; },
          [](sim::SimulatorParams& p, sim::i64 v) { p.risk.max_abs_position_qty_q = v; })
      .def_prop_rw(
          "spot_no_short",
          [](const sim::SimulatorParams& p) { return p.risk.spot_no_short; },
          [](sim::SimulatorParams& p, bool v) { p.risk.spot_no_short = v; });

  nb::class_<sim::Ledger>(msim, "Ledger")
      .def(nb::init<>())
      .def_rw("cash_q", &sim::Ledger::cash_q)
      .def_rw("position_qty_q", &sim::Ledger::position_qty_q)
      .def_rw("locked_cash_q", &sim::Ledger::locked_cash_q)
      .def_rw("locked_position_qty_q", &sim::Ledger::locked_position_qty_q);

  nb::class_<sim::LimitOrderRequest>(msim, "LimitOrderRequest")
      .def(nb::init<>())
      .def_rw("side", &sim::LimitOrderRequest::side)
      .def_rw("price_q", &sim::LimitOrderRequest::price_q)
      .def_rw("qty_q", &sim::LimitOrderRequest::qty_q)
      .def_rw("tif", &sim::LimitOrderRequest::tif)
      .def_rw("client_order_id", &sim::LimitOrderRequest::client_order_id);

  nb::class_<sim::MarketOrderRequest>(msim, "MarketOrderRequest")
      .def(nb::init<>())
      .def_rw("side", &sim::MarketOrderRequest::side)
      .def_rw("qty_q", &sim::MarketOrderRequest::qty_q)
      .def_rw("tif", &sim::MarketOrderRequest::tif)
      .def_rw("client_order_id", &sim::MarketOrderRequest::client_order_id);

  // Lifecycle event log object (audit)
  nb::class_<sim::Event>(msim, "Event")
      .def(nb::init<>())
      .def_prop_ro("ts", [](const sim::Event& e) { return e.ts.value; })
      .def_rw("order_id", &sim::Event::order_id)
      .def_rw("type", &sim::Event::type)
      .def_rw("state", &sim::Event::state)
      .def_rw("reject_reason", &sim::Event::reject_reason);

  nb::class_<sim::FillEvent>(msim, "FillEvent")
      .def_prop_ro("ts", [](const sim::FillEvent& e) { return e.ts.value; })
      .def_prop_ro("order_id", [](const sim::FillEvent& e) { return e.order_id; })
      .def_prop_ro("side", [](const sim::FillEvent& e) { return e.side; })
      .def_prop_ro("price_q", [](const sim::FillEvent& e) { return e.price_q; })
      .def_prop_ro("qty_q", [](const sim::FillEvent& e) { return e.qty_q; })
      .def_prop_ro("liq", [](const sim::FillEvent& e) { return e.liq; })
      .def_prop_ro("notional_cash_q", [](const sim::FillEvent& e) { return e.notional_cash_q; })
      .def_prop_ro("fee_cash_q", [](const sim::FillEvent& e) { return e.fee_cash_q; });

  nb::class_<sim::MarketSimulator>(msim, "MarketSimulator")
      .def(nb::init<const sim::SimulatorParams&>(), nb::arg("params"))

      // C++ API: reset(Ns, Ledger)
      .def("reset", &sim::MarketSimulator::reset, nb::arg("start_ts"), nb::arg("initial_ledger"))

      // Python-friendly overload: reset(int start_ts_ns, Ledger)
      .def(
          "reset",
          [](sim::MarketSimulator& self,
             std::uint64_t start_ts_ns,
             const sim::Ledger& initial_ledger) {
            self.reset(sim::Ns{static_cast<sim::u64>(start_ts_ns)}, initial_ledger);
          },
          nb::arg("start_ts_ns"),
          nb::arg("initial_ledger"),
          "Reset simulator with start timestamp in nanoseconds (Python int).")

      // step takes a RecordView (zero-copy) and calls into the engine with *rec
      .def(
          "step",
          [](sim::MarketSimulator& ex, const RecordView& v) { ex.step(*v.rec); },
          nb::arg("record"))
      .def("place_limit", &sim::MarketSimulator::place_limit, nb::arg("req"))
      .def("place_market", &sim::MarketSimulator::place_market, nb::arg("req"))
      .def("cancel", &sim::MarketSimulator::cancel, nb::arg("order_id"))
      .def_prop_ro("now", [](const sim::MarketSimulator& ex) { return ex.now().value; })
      .def_prop_ro("ledger", &sim::MarketSimulator::ledger, nb::rv_policy::reference_internal)

      // .def_prop_ro("fills", &sim::MarketSimulator::fills, nb::rv_policy::reference_internal);
      .def("fills", [](const sim::MarketSimulator& ex) { return snapshot_vec(ex.fills()); })
      // Safe copies for Python analytics/audit (no reference lifetimes)
      .def("events", [](const sim::MarketSimulator& ex) { return snapshot_vec(ex.events()); })
      .def("orders", [](const sim::MarketSimulator& ex) { return snapshot_vec(ex.orders()); })
      // TODO: 
      // Convenience: O(N) lookup (safe). For production O(1), add a C++ method using id_to_index_
      .def(
          "get_order",
          [](const sim::MarketSimulator& ex, sim::u64 order_id) -> std::optional<sim::Order> {
            for ( const auto& o : ex.orders() ) {
              if ( o.id == order_id )
                return o;
            }
            return std::nullopt;
          },
          nb::arg("order_id"));
}