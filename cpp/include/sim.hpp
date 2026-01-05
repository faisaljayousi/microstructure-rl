#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <memory> // std::unique_ptr
#include <memory_resource>
#include <optional>
#include <queue>
#include <vector>

#include "schema.hpp" // md::l2::Record

#ifndef SIM_ASSERT
#  define SIM_ASSERT(x) assert(x)
#endif

#if defined(__GNUC__) || defined(__clang__)
using i128 = __int128;
using u128 = unsigned __int128;
#endif

namespace sim
{

  /// All monetary/qty values are fixed-point int64 in the same quantisation
  /// used by md::l2 (see md::l2::FileHeader::price_scale / qty_scale).
  using i64 = std::int64_t;
  using u64 = std::uint64_t;
  using u32 = std::uint32_t;

  inline constexpr u64 kInvalidIndex = std::numeric_limits<u64>::max();

  /// Strongly-typed nanoseconds for clarity.
  struct Ns
  {
    u64 value{0};
    constexpr Ns() = default;
    constexpr explicit Ns(u64 v) : value(v) {}

    friend constexpr Ns operator+(Ns a, Ns b) { return Ns{a.value + b.value}; }

    friend constexpr bool operator==(Ns a, Ns b) { return a.value == b.value; }
    friend constexpr bool operator!=(Ns a, Ns b) { return a.value != b.value; }
    friend constexpr bool operator<(Ns a, Ns b) { return a.value < b.value; }
    friend constexpr bool operator<=(Ns a, Ns b) { return a.value <= b.value; }
    friend constexpr bool operator>(Ns a, Ns b) { return a.value > b.value; }
    friend constexpr bool operator>=(Ns a, Ns b) { return a.value >= b.value; }
  };

  enum class Side : std::uint8_t
  {
    Buy = 0,
    Sell = 1
  };

  enum class OrderType : std::uint8_t
  {
    Limit = 0,
    Market = 1
  };

  enum class Tif : std::uint8_t
  {
    GTC = 0, // Good-Til-Cancel
    IOC = 1, // Immediate-Or-Cancel (v1+)
    FOK = 2  // Fill-Or-Kill (v1+)
  };

  enum class Visibility : std::uint8_t
  {
    Visible = 0, // order price currently in top-N
    Blind = 1,   // order price not in top-N (deep book)
    Frozen = 2   // was visible, became not visible; queue tracking frozen
  };

  enum class OrderState : std::uint8_t
  {
    Pending = 0,
    Active = 1,
    Partial = 2,
    Filled = 3,
    Cancelled = 4,
    Rejected = 5
  };

  enum class StpPolicy : std::uint8_t
  {
    None = 0,
    RejectIncoming = 1, // reject the activating order if it would self-cross
    CancelResting = 2   // cancel resting opposite-side orders that would self-cross, then activate
  };

  enum class RejectReason : std::uint8_t
  {
    None = 0,
    InvalidParams = 1,
    InsufficientFunds = 2,
    InsufficientResources = 3, // capacity / throttling / logging overflow
    SelfTradePrevention = 4,   // STP rule triggered
    UnknownOrderId = 5,
    AlreadyTerminal = 6
  };

  /// Fee schedule is defined but fees are applied in later phases.
  struct FeeSchedule
  {
    // fee = notional_q * fee_ppm / 1'000'000
    u64 maker_fee_ppm{0};
    u64 taker_fee_ppm{0};
  };

  /// Risk model (spot-like in v0; extend later).
  struct RiskLimits
  {
    // Max absolute position in base qty_q. 0 => disabled.
    i64 max_abs_position_qty_q{0};

    // If true, disallow selling more base than currently held (spot no-short).
    bool spot_no_short{true};
  };

  struct SimulatorParams
  {
    // Outbound order latency (agent -> exchange active time).
    Ns outbound_latency{0};

    // Optional observation latency (exchange -> agent observation).
    Ns observation_latency{0};

    // Hard caps (deterministic capacity; exceeding => rejection).
    std::size_t max_orders{0};
    std::size_t max_events{0};

    // Queue depletion attribution: effective_depl = depletion * alpha_ppm / 1'000'000
    // alpha_ppm ∈ [0, 1'000'000]
    u64 alpha_ppm{0};

    StpPolicy stp{StpPolicy::RejectIncoming};

    FeeSchedule fees{};
    RiskLimits risk{};
  };

  /// Portfolio ledger. All values in fixed-point int64.
  struct Ledger
  {
    // Quote currency cash balance in cash_q.
    i64 cash_q{0};

    // Base currency position in qty_q.
    i64 position_qty_q{0};

    // Locked balances reserved for PENDING/ACTIVE orders.
    i64 locked_cash_q{0};
    i64 locked_position_qty_q{0};
  };

  /// Limit order request.
  struct LimitOrderRequest
  {
    Side side{Side::Buy};
    i64 price_q{0};
    i64 qty_q{0};
    Tif tif{Tif::GTC};

    // Optional client correlation id (not used for lookup; stored as metadata).
    u64 client_order_id{0};
  };

  /// Market order request (supported as a request type).
  struct MarketOrderRequest
  {
    Side side{Side::Buy};
    i64 qty_q{0};
    Tif tif{Tif::IOC}; // market is typically IOC-like
    u64 client_order_id{0};
  };

  /// Cancel request by simulator order id.
  struct CancelRequest
  {
    u64 order_id{0};
  };

  /// Minimal order object stored in the simulator.
  struct alignas(64) Order
  {
    u64 id{0};              // simulator-assigned, dense id
    u64 client_order_id{0}; // metadata only
    OrderType type{OrderType::Limit};
    Side side{Side::Buy};

    i64 price_q{0}; // 0 for Market orders
    i64 qty_q{0};

    i64 filled_qty_q{0};

    // --- Queueing model ---
    // Quantity ahead of the agent at this exact price level when the order becomes ACTIVE.
    // Fixed-point quantity units (same quantisation as qty_q in md::l2::Record).
    i64 qty_ahead_q{0};

    // Last observed displayed quantity at the order's price level (for depletion inference).
    // Only valid if visibility != Blind.
    i64 last_level_qty_q{0};

    // Last observed level index [0, N). -1 means not visible.
    std::int16_t last_level_idx{-1};

    // Visibility state of the order price relative to top-N snapshots
    Visibility visibility{Visibility::Blind};

    // Timestamps in simulator clock domain (ts_recv_ns)
    Ns submit_ts{0};   // when agent called place_*
    Ns activate_ts{0}; // when order becomes ACTIVE (submit + outbound_latency)

    OrderState state{OrderState::Pending};
    RejectReason reject_reason{RejectReason::None};

    // Intrusive per-price FIFO list pointers (indices into orders_)
    // Valid iff order is ACTIVE/PARTIAL and resting in a bucket.
    u64 bucket_prev{kInvalidIndex};
    u64 bucket_next{kInvalidIndex};
  };

  /// Lifecycle/event log entry.
  enum class EventType : std::uint8_t
  {
    Submit = 0,
    Activate = 1,
    Cancel = 2,
    Reject = 3
  };

  struct Event
  {
    Ns ts{0};
    u64 order_id{0};
    EventType type{EventType::Submit};
    OrderState state{OrderState::Pending};
    RejectReason reject_reason{RejectReason::None};
  };

  enum class LiquidityFlag : std::uint8_t
  {
    Maker = 0,
    Taker = 1
  };

  struct FillEvent
  {
    Ns ts{0};
    u64 order_id{0};
    Side side{Side::Buy};
    i64 price_q{0};
    i64 qty_q{0};
    LiquidityFlag liq{LiquidityFlag::Maker};

    // TODO (remove later; useful for debugs)
    i64 notional_cash_q{0};
    i64 fee_cash_q{0};
  };

  /// Simulator
  class MarketSimulator final
  {
  public:
    explicit MarketSimulator(const SimulatorParams& params);

    // Reset internal state for deterministic replay.
    // start_ts sets the simulator clock baseline.
    void reset(Ns start_ts, Ledger initial_ledger);

    // Advance the simulator by one market data record.
    // Sets market_ to a step-scoped pointer for internal helpers.
    void step(const md::l2::Record& rec);

    // Place orders. Return assigned simulator order_id (0 if rejected).
    [[nodiscard]] u64 place_limit(const LimitOrderRequest& req);
    [[nodiscard]] u64 place_market(const MarketOrderRequest& req);

    // Cancel an existing order by simulator order_id.
    // If the order is still PENDING, cancellation is allowed (releases locks).
    bool cancel(u64 order_id);

    // --- Accessors (intended to be O(1)) ---
    Ns now() const { return now_; }
    const SimulatorParams& params() const { return params_; }
    const Ledger& ledger() const { return ledger_; }

    // Read-only view (for tests/debug; NOT for hot-path RL).
    const std::vector<Order>& orders() const { return orders_; }
    const std::vector<Event>& events() const { return events_; }
    const std::vector<FillEvent>& fills() const { return fills_; }

  private:
    // --- Internal helpers ---
    RejectReason validate_limit_(const LimitOrderRequest& req) const;
    RejectReason validate_market_(const MarketOrderRequest& req) const;

    RejectReason risk_check_and_lock_limit_(Side side, i64 price_q, i64 qty_q);
    RejectReason risk_check_and_lock_market_(Side side, i64 qty_q);

    // Attempts to append an event to the log.
    // Returns false if event capacity is exceeded (caller must reject/cancel deterministically).
    bool push_event_(Ns ts, u64 id, EventType et, OrderState st, RejectReason rr);

    void unlock_on_cancel_(const Order& o);

    // STP enforcement happens here (at activate time).
    bool apply_stp_on_activate_(Order& incoming);

    // --- Pending activation queue entry (min-heap by activate_ts then seq) ---
    struct PendingEntry
    {
      Ns activate_ts;
      u64 seq;
      u64 order_id;
    };
    struct PendingCmp
    {
      bool operator()(const PendingEntry& a, const PendingEntry& b) const
      {
        if ( a.activate_ts.value != b.activate_ts.value )
          return a.activate_ts.value > b.activate_ts.value;
        return a.seq > b.seq;
      }
    };

    SimulatorParams params_{};
    Ns now_{0};
    Ledger ledger_{};

    // Step-scoped, read-only view of current market state.
    const md::l2::Record* market_{nullptr};

    // Orders stored in insertion order; simulator order_id maps to index via id_to_index_.
    std::vector<Order> orders_;

    // Direct-address table: order_id -> index in orders_ (kInvalidIndex if not present).
    // Sized to params_.max_orders + 1 in reset().
    std::vector<u64> id_to_index_;

    std::priority_queue<PendingEntry, std::vector<PendingEntry>, PendingCmp> pending_;
    u64 next_order_id_{1};
    u64 next_seq_{1};

    // Active (resting) orders, stored as indices into orders_.
    std::vector<u64> active_bids_;
    std::vector<u64> active_asks_;

    struct Bucket
    {
      u64 head{kInvalidIndex};
      u64 tail{kInvalidIndex};
      u32 size{0};
      i64 last_level_qty_q{0};
      std::int16_t last_level_idx{-1};
      Visibility visibility{Visibility::Blind};
    };

    // Flat ordered buckets (aligned arrays)
    // Bid prices ordered ascending; best bid is rbegin()->first.
    // Ask prices ordered ascending; best ask is begin()->first.
    std::vector<i64> bid_prices_; // sorted ascending
    std::vector<Bucket> bid_buckets_;
    std::vector<i64> ask_prices_; // sorted ascending
    std::vector<Bucket> ask_buckets_;

    // Back-pointers for O(1) remove: order_id -> position in active_* vector.
    // Use kInvalidIndex when not active. Size = max_orders + 1.
    std::vector<u64> active_bid_pos_;
    std::vector<u64> active_ask_pos_;

    // Remove an ACTIVE bid/ask order from active sets.
    // order_id : simulator order id
    // order_idx: index into orders_
    void remove_active_bid_(u64 order_id, u64 order_idx);
    void remove_active_ask_(u64 order_id, u64 order_idx);

    // Fast STP detection summaries
    bool has_active_bids_{false};
    bool has_active_asks_{false};
    i64 best_active_bid_q_{0}; // max price among active bids
    i64 best_active_ask_q_{0}; // min price among active asks

    // Lifecycle/event log. Hard capped by params_.max_events.
    std::vector<Event> events_;

    // Fill log (separate from lifecycle events).
    std::vector<FillEvent> fills_;

    // Apply a single fill (updates ledger, unlocks, emits FillEvent).
    void apply_fill_(Order& o, i64 price_q, i64 qty_q, LiquidityFlag liq);

    // Price-bucket helpers (log P lookup, contiguous iteration)
    u64 find_bid_bucket_idx_(i64 price_q) const;
    u64 find_ask_bucket_idx_(i64 price_q) const;
    u64 get_or_insert_bid_bucket_idx_(i64 price_q);
    u64 get_or_insert_ask_bucket_idx_(i64 price_q);
    void erase_bid_bucket_if_empty_(u64 bidx);
    void erase_ask_bucket_if_empty_(u64 aidx);

    // During matching/filling we must not erase from bid/ask bucket vectors,
    // otherwise Bucket& references become dangling.
    bool defer_bucket_erase_{false};

    // Compacts empty buckets after matching/filling when defer_bucket_erase_ is set
    void cleanup_empty_buckets_();

    void bucket_push_back_bid_(u64 bidx, u64 order_idx);
    void bucket_push_back_ask_(u64 aidx, u64 order_idx);
    void bucket_erase_bid_(u64 bidx, u64 order_idx);
    void bucket_erase_ask_(u64 aidx, u64 order_idx);

    // Passive at-touch fills with per-level depletion accounting (FIFO)
    void apply_passive_fills_one_bucket_(
        const md::l2::Record& rec,
        i64 bucket_price_q,
        Bucket& bucket,
        Side side);

    // Aggressive (taker) fills: marketable resting orders sweep visible top-N depth.
    // Implemented bucket-head-driven (no O(N) scan of orders).
    void apply_aggressive_fills_(const md::l2::Record& rec);
  };

} // namespace sim
