#include <array>
#include <cstdint>
#include <memory>
#include <nanobind/nanobind.h>
#include <nanobind/ndarray.h>
#include <nanobind/stl/shared_ptr.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/vector.h>

#include "replay.hpp"
#include "schema.hpp"
#include "sim.hpp"

namespace nb = nanobind;

namespace
{

  // Read-only view over a memory-mapped Record that keeps the ReplayKernel alive.
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
      // Expose (kDepth, 2) int64 view: [price_q, qty_q] for each level.
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

  // Enums (extend as needed)
  nb::enum_<sim::Side>(msim, "Side").value("Buy", sim::Side::Buy).value("Sell", sim::Side::Sell);

  nb::enum_<sim::Tif>(msim, "Tif").value("GTC", sim::Tif::GTC).value("IOC", sim::Tif::IOC);

  nb::enum_<sim::OrderState>(msim, "OrderState")
      .value("Pending", sim::OrderState::Pending)
      .value("Active", sim::OrderState::Active)
      .value("Partial", sim::OrderState::Partial)
      .value("Filled", sim::OrderState::Filled)
      .value("Cancelled", sim::OrderState::Cancelled)
      .value("Rejected", sim::OrderState::Rejected);

  nb::enum_<sim::LiquidityFlag>(msim, "LiquidityFlag")
      .value("Maker", sim::LiquidityFlag::Maker)
      .value("Taker", sim::LiquidityFlag::Taker);

  nb::class_<sim::SimulatorParams>(msim, "SimulatorParams")
      .def(nb::init<>())
      .def_rw("max_orders", &sim::SimulatorParams::max_orders)
      .def_rw("max_events", &sim::SimulatorParams::max_events)
      // .def_rw("maker_fee_ppm",&sim::SimulatorParams::maker_fee_ppm)
      // .def_rw("taker_fee_ppm",&sim::SimulatorParams::taker_fee_ppm)
      .def_rw("alpha_ppm", &sim::SimulatorParams::alpha_ppm);

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
      .def_prop_ro("fills", &sim::MarketSimulator::fills, nb::rv_policy::reference_internal);
}
