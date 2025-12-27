/*
CSV.GZ -> mmappable .snap converter for L2 snapshot records.

Key properties:
- Streams gzip input (zlib) without materialising to disk
- Robust line reading (no fixed-buffer truncation)
- Header-driven column mapping (doesn't rely on positional assumptions)
- Deterministic fixed-point conversion (fast_float, overflow/NaN checks)
- Fills missing levels with schema sentinel values
- Crash-safe output (writes .part then atomic rename)
- Two-phase header finalise (record_count updated at end)
- Basic integrity checks (file size vs record count)

Dependencies:
- zlib
- fast_float (recommended; add via vcpkg: "fast-float")

Build usage (example):
  csv_gz_to_snap <input.csv.gz> <output.snap>

Notes:
- Assumes input CSV columns include:
    ts_event_ms, ts_recv_ns, bid_p1, bid_q1, ... bid_p20, bid_q20, ask_p1, ask_q1, ... ask_p20, ask_q20
- ts_event_ms may be empty; written as 0 in the output record.
*/

#include "schema.hpp" 
#include <zlib.h>

#include <fast_float/fast_float.h>

#include <array>
#include <cerrno>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>


namespace fs = std::filesystem;

namespace md::l2 {

namespace {

/* -----------------------------
 * RAII wrapper for gzFile
 * ----------------------------- */
struct GzFile {
  gzFile f{nullptr};
  explicit GzFile(const char* path) {
    f = gzopen(path, "rb");
    if (!f) {
      throw std::runtime_error(std::string("gzopen failed for: ") + path);
    }
  }
  ~GzFile() {
    if (f) gzclose(f);
  }
  GzFile(const GzFile&) = delete;
  GzFile& operator=(const GzFile&) = delete;
};

/* -----------------------------
 * Robust gzip line reader
 *
 * gzgets() truncates when line > buffer.
 * Here we accumulate until '\n' or EOF.
 * ----------------------------- */
bool gz_readline(gzFile f, std::string& out) {
  out.clear();
  // Tune chunk size: large enough to amortize zlib calls, small enough for stack.
  char buf[8192];

  while (true) {
    char* res = gzgets(f, buf, static_cast<int>(sizeof(buf)));
    if (!res) {
      // EOF or error
      return !out.empty();
    }
    out.append(res);

    // If we captured a newline, full line is ready.
    if (!out.empty() && out.back() == '\n') {
      // Strip trailing newline / CRLF
      while (!out.empty() && (out.back() == '\n' || out.back() == '\r')) out.pop_back();
      return true;
    }

    // Otherwise, line continues (was longer than buf).
    // Continue reading until newline.
  }
}

/* -----------------------------
 * CSV tokenizer (no allocations for tokens)
 * Returns string_views pointing into `line` memory.
 * Assumes no quoted fields (true for your generated wide CSV).
 * ----------------------------- */
void split_csv_views(std::string& line, std::vector<std::string_view>& fields) {
  fields.clear();
  fields.reserve(128);

  const char* s = line.data();
  const std::size_t n = line.size();

  std::size_t start = 0;
  for (std::size_t i = 0; i <= n; ++i) {
    if (i == n || line[i] == ',') {
      fields.emplace_back(s + start, i - start);
      start = i + 1;
    }
  }
}

/* -----------------------------
 * Parse int64 from string_view (base-10).
 * Empty -> false.
 * ----------------------------- */
bool parse_i64(std::string_view sv, std::int64_t& out) {
  if (sv.empty()) return false;
  const char* b = sv.data();
  const char* e = b + sv.size();
  auto res = std::from_chars(b, e, out, 10);
  return res.ec == std::errc{} && res.ptr == e;
}

/* -----------------------------
 * Parse decimal -> fixed-point int64 with checks.
 * - Empty -> false
 * - NaN/inf -> false
 * - Overflow -> false
 * - Rounding: nearest integer (llround)
 * ----------------------------- */
bool parse_fixed(std::string_view sv, std::int64_t scale, std::int64_t& out) {
  if (sv.empty()) return false;

  double v = 0.0;
  const char* b = sv.data();
  const char* e = b + sv.size();

  auto r = fast_float::from_chars(b, e, v);
  if (r.ec != std::errc{} || r.ptr != e) return false;
  if (!std::isfinite(v)) return false;

  // Multiplication range check in double domain before rounding.
  const long double scaled = static_cast<long double>(v) * static_cast<long double>(scale);
  if (!std::isfinite(static_cast<double>(scaled))) return false;

  const long double lo = static_cast<long double>(std::numeric_limits<std::int64_t>::min());
  const long double hi = static_cast<long double>(std::numeric_limits<std::int64_t>::max());

  if (scaled < lo || scaled > hi) return false;

  out = static_cast<std::int64_t>(std::llround(static_cast<double>(scaled)));
  return true;
}

/* -----------------------------
 * Map header names -> indices
 * ----------------------------- */
int find_col(const std::vector<std::string_view>& header, std::string_view name) {
  for (int i = 0; i < static_cast<int>(header.size()); ++i) {
    if (header[i] == name) return i;
  }
  return -1;
}

struct ColumnMap {
  int ts_event_ms{-1};
  int ts_recv_ns{-1};
  std::array<int, kDepth> bid_p{};
  std::array<int, kDepth> bid_q{};
  std::array<int, kDepth> ask_p{};
  std::array<int, kDepth> ask_q{};
};

ColumnMap build_column_map(const std::vector<std::string_view>& header) {
  ColumnMap m{};

  m.ts_event_ms = find_col(header, "ts_event_ms");
  m.ts_recv_ns  = find_col(header, "ts_recv_ns");
  if (m.ts_recv_ns < 0) {
    throw std::runtime_error("Missing required column: ts_recv_ns");
  }

  for (int i = 0; i < kDepth; ++i) {
    const int lvl = i + 1;
    {
      const std::string bp = "bid_p" + std::to_string(lvl);
      const std::string bq = "bid_q" + std::to_string(lvl);
      m.bid_p[i] = find_col(header, bp);
      m.bid_q[i] = find_col(header, bq);
      if (m.bid_p[i] < 0 || m.bid_q[i] < 0) {
        throw std::runtime_error("Missing required bid columns for level " + std::to_string(lvl));
      }
    }
    {
      const std::string ap = "ask_p" + std::to_string(lvl);
      const std::string aq = "ask_q" + std::to_string(lvl);
      m.ask_p[i] = find_col(header, ap);
      m.ask_q[i] = find_col(header, aq);
      if (m.ask_p[i] < 0 || m.ask_q[i] < 0) {
        throw std::runtime_error("Missing required ask columns for level " + std::to_string(lvl));
      }
    }
  }
  return m;
}

/* -----------------------------
 * Initialise record sentinels per schema contract
 * ----------------------------- */
void init_record_sentinels(Record& rec) {
  rec.ts_event_ms = 0;
  rec.ts_recv_ns  = 0;
  for (auto& l : rec.bids) l = Level{kBidNullPriceQ, kNullQtyQ};
  for (auto& l : rec.asks) l = Level{kAskNullPriceQ, kNullQtyQ};
}

/* -----------------------------
 * Parse a CSV row into a Record
 * Returns false if row is invalid.
 * Policy:
 * - ts_recv_ns must parse
 * - ts_event_ms optional (empty -> 0)
 * - Each level: if either price or qty fails -> keep sentinel for that level
 * - qty <= 0 treated as inactive (kept sentinel)
 * ----------------------------- */
bool parse_row_to_record(
    const std::vector<std::string_view>& row,
    const ColumnMap& cm,
    Record& rec,
    std::uint64_t& bad_rows) {

  init_record_sentinels(rec);

  // ts_event_ms optional
  if (cm.ts_event_ms >= 0 && cm.ts_event_ms < static_cast<int>(row.size())) {
    std::int64_t t = 0;
    if (parse_i64(row[cm.ts_event_ms], t)) rec.ts_event_ms = t;
  }

  // ts_recv_ns required
  if (cm.ts_recv_ns < 0 || cm.ts_recv_ns >= static_cast<int>(row.size())) {
    ++bad_rows;
    return false;
  }
  {
    std::int64_t t = 0;
    if (!parse_i64(row[cm.ts_recv_ns], t)) {
      ++bad_rows;
      return false;
    }
    rec.ts_recv_ns = t;
  }

  // bids/asks
  for (int i = 0; i < kDepth; ++i) {
    const int bp = cm.bid_p[i], bq = cm.bid_q[i];
    const int ap = cm.ask_p[i], aq = cm.ask_q[i];

    if (bp >= 0 && bq >= 0 && bp < static_cast<int>(row.size()) && bq < static_cast<int>(row.size())) {
      std::int64_t px = 0, qy = 0;
      if (parse_fixed(row[bp], kPriceScale, px) && parse_fixed(row[bq], kQtyScale, qy) && px > 0 && qy > 0) {
        rec.bids[i] = Level{px, qy};
      }
    }

    if (ap >= 0 && aq >= 0 && ap < static_cast<int>(row.size()) && aq < static_cast<int>(row.size())) {
      std::int64_t px = 0, qy = 0;
      if (parse_fixed(row[ap], kPriceScale, px) && parse_fixed(row[aq], kQtyScale, qy) && px > 0 && qy > 0) {
        rec.asks[i] = Level{px, qy};
      }
    }
  }

  return true;
}

/* -----------------------------
 * Atomic finalise: rename .part -> final
 * ----------------------------- */
void atomic_rename(const fs::path& tmp, const fs::path& final) {
  std::error_code ec;
  fs::create_directories(final.parent_path(), ec);
  if (ec) {
    throw std::runtime_error("Failed to create output directory: " + final.parent_path().string());
  }

  // On Windows, rename over existing may fail; remove existing first.
  fs::remove(final, ec);
  ec.clear();
  fs::rename(tmp, final, ec);
  if (ec) {
    throw std::runtime_error("Failed to rename tmp->final: " + tmp.string() + " -> " + final.string() + " : " + ec.message());
  }
}

} // namespace

/* -----------------------------
 * Public API
 * ----------------------------- */
void convert(const std::string& input_path, const std::string& output_path) {
  const fs::path in  = fs::path(input_path);
  const fs::path out = fs::path(output_path);
  const fs::path tmp = out.string() + ".part";

  if (!fs::exists(in)) {
    throw std::runtime_error("Input not found: " + in.string());
  }
  fs::create_directories(out.parent_path().empty() ? fs::current_path() : out.parent_path());

  // Open gzip input
  GzFile gz(in.string().c_str());

  // Open output temp file
  std::ofstream b_out(tmp, std::ios::binary | std::ios::trunc);
  if (!b_out.is_open()) {
    throw std::runtime_error("Could not open output: " + tmp.string());
  }

  // 1) Write placeholder header (record_count finalised at end)
  FileHeader hdr{};
  hdr.magic        = kMagic;
  hdr.version      = kVersion;
  hdr.depth        = kDepth;
  hdr.record_size  = static_cast<std::uint32_t>(sizeof(Record));
  hdr.endian_check = kEndianCheck;
  hdr.price_scale  = kPriceScale;
  hdr.qty_scale    = kQtyScale;
  hdr.record_count = 0;

  b_out.write(reinterpret_cast<const char*>(&hdr), sizeof(hdr));
  if (!b_out.good()) {
    throw std::runtime_error("Failed to write header to: " + tmp.string());
  }

  // 2) Read CSV header row and build a column map
  std::string line;
  std::vector<std::string_view> fields;
  if (!gz_readline(gz.f, line)) {
    throw std::runtime_error("Input appears empty (no CSV header): " + in.string());
  }
  split_csv_views(line, fields);
  const ColumnMap cm = build_column_map(fields);

  // 3) Stream rows -> records
  std::uint64_t count = 0;
  std::uint64_t bad_rows = 0;

  Record rec{};
  const std::uint64_t log_every = 1'000'000;

  while (gz_readline(gz.f, line)) {
    split_csv_views(line, fields);

    // Basic sanity: tolerate extra columns, but require at least what we map.
    if (fields.size() < 2u) {
      ++bad_rows;
      continue;
    }

    if (!parse_row_to_record(fields, cm, rec, bad_rows)) {
      continue;
    }

    b_out.write(reinterpret_cast<const char*>(&rec), sizeof(Record));
    if (!b_out.good()) {
      throw std::runtime_error("Write failure while writing records to: " + tmp.string());
    }

    ++count;
    if (count % log_every == 0) {
      std::cerr << "[INFO] records_written=" << count << " bad_rows=" << bad_rows << "\n";
    }
  }

  // Flush writes before finalising header
  b_out.flush();
  if (!b_out.good()) {
    throw std::runtime_error("Flush failure for: " + tmp.string());
  }

  // 4) Finalise header with record_count (seek back)
  hdr.record_count = count;
  b_out.seekp(0, std::ios::beg);
  b_out.write(reinterpret_cast<const char*>(&hdr), sizeof(hdr));
  b_out.flush();

  if (!b_out.good()) {
    throw std::runtime_error("Failed to finalise header (seek/write) for: " + tmp.string());
  }

  b_out.close();

  // 5) Integrity check: file size matches header count
  const std::uint64_t file_sz = static_cast<std::uint64_t>(fs::file_size(tmp));
  const std::uint64_t payload_sz = file_sz - sizeof(FileHeader);
  const std::uint64_t expected = payload_sz / sizeof(Record);

  if (payload_sz % sizeof(Record) != 0 || expected != count) {
    throw std::runtime_error(
        "Output size mismatch: file_sz=" + std::to_string(file_sz) +
        " expected_records=" + std::to_string(expected) +
        " header_records=" + std::to_string(count));
  }

  // 6) Atomic finalise
  atomic_rename(tmp, out);

  std::cerr << "[OK] Converted " << count << " records"
            << " (bad_rows=" << bad_rows << ") -> " << out.string() << "\n";
}

} // namespace md::l2

/* -----------------------------
 * CLI entrypoint
 * ----------------------------- */
int main(int argc, char** argv) {
  try {
    if (argc != 3) {
      std::cerr << "Usage: csv_gz_to_snap <input.csv.gz> <output.snap>\n";
      return 2;
    }
    md::l2::convert(argv[1], argv[2]);
    return 0;
  } catch (const std::exception& e) {
    std::cerr << "[FAIL] " << e.what() << "\n";
    return 1;
  }
}
