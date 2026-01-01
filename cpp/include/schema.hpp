#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <type_traits>

/*
 * =============================================================================
 *  L2 Snapshot Binary Format (mmappable, fixed-size, versioned, deterministic)
 * =============================================================================
 *
 * Overview
 * --------
 * This header defines the on-disk / memory-mapped format used for replaying
 * top-N L2 order book snapshots (partial depth) at high throughput.
 *
 * Key goals:
 * - Fixed-size records for O(1) random access and zero-copy mmap consumption
 * - Deterministic numeric representation (fixed-point integers; no floats)
 * - Trivially copyable PODs (safe to write/read as raw bytes)
 * - Explicit versioning and self-describing file header
 *
 * File layout:
 *   [FileHeader][Record][Record]...[Record]
 *
 * Producer:
 * - Offline converter (C++) reads raw csv.gz and writes this .snap format.
 * - Producer is responsible for:
 *     - scaling (price/qty to fixed-point)
 *     - filling missing/empty levels with sentinel values
 *     - validating invariants
 *
 * Consumers:
 * - C++ simulator/benchmark: mmap + iterate records
 * - Python: numpy.memmap (with a matching dtype) to view records directly
 *
 * Platform note:
 * - This format assumes little-endian (common on x86_64).
 *
 * Padding / packing policy:
 * - We intentionally do NOT use #pragma pack(1). Packed structs can introduce
 *   unaligned loads and hurt performance.
 * - Instead, we enforce layout stability via static_assert(sizeof/offsetof).
 */

namespace md::l2
{

  /* =========================
   *  Format identifiers
   * ========================= */

  constexpr std::uint32_t kMagic = 0x4C32424F; // "L2BO" in little-endian
  constexpr std::uint16_t kVersion = 1;
  constexpr std::uint16_t kDepth = 20;

  // Endianness marker written into the header.
  // On a little-endian system, endian_check will appear as 04 03 02 01 in memory.
  constexpr std::uint32_t kEndianCheck = 0x01020304;

  /* =========================
   *  Fixed-point scaling
   * =========================
   *
   * Stored integer -> real value:
   *   real = stored / scale
   *
   * These scales are embedded in the file header to make artifacts self-describing.
   */
  constexpr std::int64_t kPriceScale = 100'000'000; // 1e8
  constexpr std::int64_t kQtyScale = 100'000'000;   // 1e8

  /* =========================
   *  Sentinel values
   * =========================
   *
   * Missing levels occur when:
   * - the feed provides fewer than N levels
   * - you intentionally blank levels
   * - data gaps / parsing failures for a level
   *
   * Contract:
   * - An "inactive" level MUST be represented as:
   *     bid: price_q = 0,           qty_q = 0
   *     ask: price_q = INT64_MAX,   qty_q = 0
   *
   * Rationale:
   * - bid side: 0 is an obviously invalid positive price
   * - ask side: INT64_MAX is an obviously invalid ask price and sorts "far away"
   */
  constexpr std::int64_t kBidNullPriceQ = 0;
  constexpr std::int64_t kAskNullPriceQ = (std::numeric_limits<std::int64_t>::max)();
  constexpr std::int64_t kNullQtyQ = 0;

  /* =========================
   *  File header (32 bytes)
   * =========================
   *
   * Written once at the beginning of the file.
   * Allows safe evolution and validation of artifacts.
   */
  struct FileHeader final
  {
    std::uint32_t magic;        // kMagic
    std::uint16_t version;      // kVersion
    std::uint16_t depth;        // kDepth
    std::uint32_t record_size;  // sizeof(Record)
    std::uint32_t endian_check; // kEndianCheck
    std::int64_t price_scale;   // kPriceScale
    std::int64_t qty_scale;     // kQtyScale
    std::uint64_t record_count; // optional; 0 if unknown at write-time
  };

  static_assert(std::is_trivially_copyable_v<FileHeader>,
                "FileHeader must be POD/trivially copyable.");
  static_assert(sizeof(FileHeader) == 40, "FileHeader must be exactly 40 bytes.");

  /* =========================
   *  L2 level (16 bytes)
   * =========================
   *
   * Fixed-point representation.
   */
  struct Level final
  {
    std::int64_t price_q; // price * price_scale
    std::int64_t qty_q;   // qty   * qty_scale
  };

  static_assert(std::is_trivially_copyable_v<Level>, "Level must be POD/trivially copyable.");
  static_assert(sizeof(Level) == 16, "Level must be exactly 16 bytes (2x int64).");

  /* =========================
   *  Snapshot record (656 bytes)
   * =========================
   *
   * Layout:
   * - ts_event_ms: exchange event timestamp in milliseconds since epoch.
   *     - If not provided by the feed, producer MUST write 0.
   * - ts_recv_ns: local receive timestamp in nanoseconds since epoch.
   *     - Producer MUST always write a valid value.
   * - bids[depth]: best bid at index 0 (highest price); non-increasing prices.
   * - asks[depth]: best ask at index 0 (lowest price); non-decreasing prices.
   *
   * Missing levels MUST use sentinel values (see constants above).
   *
   * Record size:
   *   ts_event_ms (8) + ts_recv_ns (8) + bids (20*16) + asks (20*16)
   * = 16 + 320 + 320
   * = 656 bytes
   */
  struct Record final
  {
    std::int64_t ts_event_ms;
    std::int64_t ts_recv_ns;
    std::array<Level, kDepth> bids;
    std::array<Level, kDepth> asks;

    // Convenience (assumes producer wrote valid sentinels)
    std::int64_t best_bid_price_q() const noexcept { return bids[0].price_q; }
    std::int64_t best_ask_price_q() const noexcept { return asks[0].price_q; }
  };

  static_assert(std::is_trivially_copyable_v<Record>, "Record must be POD/trivially copyable.");
  static_assert(alignof(Record) == 8, "Record alignment should remain 8 bytes.");
  static_assert(sizeof(Record) == 656, "Record size must remain 656 bytes.");

  // Layout invariants: catch accidental reordering/padding changes at compile time.
  static_assert(offsetof(Record, ts_event_ms) == 0);
  static_assert(offsetof(Record, ts_recv_ns) == 8);
  static_assert(offsetof(Record, bids) == 16);
  static_assert(offsetof(Record, asks) == 16 + kDepth * sizeof(Level));

  /* =========================
   *  Helper predicates
   * =========================
   *
   * These functions encode the sentinel contract. Use them in:
   * - simulator sanity checks
   * - feature computation (avoid junk)
   * - replay validation
   */
  inline bool is_bid_active(const Level& l) noexcept
  {
    // A bid is active iff it has positive qty and a positive price.
    return (l.qty_q > 0) && (l.price_q > 0);
  }

  inline bool is_ask_active(const Level& l) noexcept
  {
    // An ask is active iff it has positive qty and is not the ask-null sentinel.
    return (l.qty_q > 0) && (l.price_q != kAskNullPriceQ);
  }

  inline bool record_has_top_of_book(const Record& r) noexcept
  {
    return is_bid_active(r.bids[0]) && is_ask_active(r.asks[0]);
  }

  /* =========================
   *  Recommended producer behavior
   * =========================
   *
   * When writing Records:
   * - Always value-initialise to avoid uninitialised bytes:
   *     Record rec{};
   * - Fill missing bids with {kBidNullPriceQ, kNullQtyQ}
   * - Fill missing asks with {kAskNullPriceQ, kNullQtyQ}
   * - If ts_event_ms not provided, set to 0 (consistent across dataset)
   *
   * Optional (strongly recommended) invariants to validate per record:
   * - if both sides active: best_bid < best_ask
   * - bids non-increasing by price across active levels
   * - asks non-decreasing by price across active levels
   *
   * Notes:
   *
   *  1. Padding Record to a cache-line multiple is intentionally NOT done in v1.
   * If benchmarks show a benefit, introduce v2 with explicit padding and bump
   * kVersion.
   *
   *  2. The file header is written provisionally with record_count = 0 and
   * finalized at close by seeking back to the start of the file. Readers must
   * accept record_count == 0 as 'unknown' and may infer the count from file size.
   */

} // namespace md::l2
