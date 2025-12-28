#include <benchmark/benchmark.h>

#include "replay.hpp"

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>

namespace fs = std::filesystem;

// -------------------------
// Env helpers (MSVC-safe)
// -------------------------
static std::string get_env_str(const char* key) {
#ifdef _WIN32
    char* buf = nullptr;
    size_t len = 0;
    if (_dupenv_s(&buf, &len, key) != 0 || !buf) {
        return {};
    }
    std::string val(buf);
    free(buf);
    return val;
#else
    if (const char* v = std::getenv(key); v && *v) {
        return std::string(v);
    }
    return {};
#endif
}

// -------------------------
// Dataset discovery
// -------------------------
static std::vector<std::string> discover_snaps_from_processed_root() {
    const auto root = get_env_str("DATA_PROCESSED_ROOT");
    if (root.empty()) {
        throw std::runtime_error(
            "DATA_PROCESSED_ROOT not set. Load .env or export it in the shell.");
    }

    fs::path dir(root);
    if (!fs::exists(dir) || !fs::is_directory(dir)) {
        throw std::runtime_error("DATA_PROCESSED_ROOT is not a directory: " + root);
    }

    std::vector<std::string> out;
    for (const auto& ent : fs::recursive_directory_iterator(dir)) {
        if (!ent.is_regular_file()) continue;
        if (ent.path().extension() == ".snap") {
            out.push_back(ent.path().string());
        }
    }

    if (out.empty()) {
        throw std::runtime_error("No .snap files found under DATA_PROCESSED_ROOT");
    }

    std::sort(out.begin(), out.end());
    return out;
}

// Global cache of snap list for all benchmarks
static std::vector<std::string> g_all_snaps;

// Ensure loaded once
static void ensure_dataset_loaded_or_skip(benchmark::State& state) {
    if (!g_all_snaps.empty()) return;
    try {
        g_all_snaps = discover_snaps_from_processed_root();
    } catch (const std::exception& e) {
        state.SkipWithError(e.what());
    }
}

// Take first N files deterministically
static std::vector<std::string> select_first_n_files(std::size_t n) {
    n = std::min(n, g_all_snaps.size());
    return std::vector<std::string>(g_all_snaps.begin(), g_all_snaps.begin() + n);
}

// -------------------------
// Touch patterns
// -------------------------
static inline void touch_tob(const md::l2::Record* rec) {
    benchmark::DoNotOptimize(rec->bids[0].price_q);
    benchmark::DoNotOptimize(rec->asks[0].price_q);
}

static inline void touch_full(const md::l2::Record* rec) {
    benchmark::DoNotOptimize(rec->ts_recv_ns);
    benchmark::DoNotOptimize(rec->ts_event_ms);

    // TOB
    benchmark::DoNotOptimize(rec->bids[0].price_q);
    benchmark::DoNotOptimize(rec->bids[0].qty_q);
    benchmark::DoNotOptimize(rec->asks[0].price_q);
    benchmark::DoNotOptimize(rec->asks[0].qty_q);

    // deeper
    benchmark::DoNotOptimize(rec->bids[5].price_q);
    benchmark::DoNotOptimize(rec->bids[5].qty_q);
    benchmark::DoNotOptimize(rec->asks[5].price_q);
    benchmark::DoNotOptimize(rec->asks[5].qty_q);

    // edge
    benchmark::DoNotOptimize(rec->bids[19].price_q);
    benchmark::DoNotOptimize(rec->bids[19].qty_q);
    benchmark::DoNotOptimize(rec->asks[19].price_q);
    benchmark::DoNotOptimize(rec->asks[19].qty_q);
}

// -------------------------
// Core benchmark runner
// -------------------------
template <typename TouchFn>
static void RunReplayBench(benchmark::State& state, TouchFn touch) {
    ensure_dataset_loaded_or_skip(state);
    if (state.skipped()) return;

    const std::size_t n_files = static_cast<std::size_t>(state.range(0));
    if (n_files == 0) {
        state.SkipWithError("n_files must be >= 1");
        return;
    }

    const auto snaps = select_first_n_files(n_files);
    if (snaps.empty()) {
        state.SkipWithError("No snaps selected");
        return;
    }

    std::size_t file_idx = 0;
    md::l2::ReplayKernel kernel(snaps[file_idx]);
    kernel.reset();

    std::uint64_t records = 0;

    for (auto _ : state) {
        const md::l2::Record* rec = kernel.next();
        if (!rec) {
            file_idx = (file_idx + 1) % snaps.size();
            kernel = md::l2::ReplayKernel(snaps[file_idx]);
            kernel.reset();
            rec = kernel.next();
            if (!rec) {
                state.SkipWithError("Encountered an empty .snap file");
                break;
            }
        }

        touch(rec);
        ++records;
    }

    state.SetItemsProcessed(static_cast<int64_t>(records));
    state.SetBytesProcessed(static_cast<int64_t>(records) * static_cast<int64_t>(sizeof(md::l2::Record)));

    // Report working set size used (approx): sum(file sizes)
    // Note: This is constant per benchmark instance, so compute once.
    std::uint64_t ws_bytes = 0;
    for (const auto& p : snaps) {
        ws_bytes += static_cast<std::uint64_t>(fs::file_size(p));
    }
    state.counters["workset_MiB"] =
        benchmark::Counter(static_cast<double>(ws_bytes) / (1024.0 * 1024.0), benchmark::Counter::kAvgThreads);
    state.counters["n_files"] = benchmark::Counter(static_cast<double>(snaps.size()), benchmark::Counter::kAvgThreads);
}

// -------------------------
// Benchmarks
// -------------------------
static void BM_Replay_TOB(benchmark::State& state) {
    RunReplayBench(state, touch_tob);
}

static void BM_Replay_FullTouch(benchmark::State& state) {
    RunReplayBench(state, touch_full);
}

BENCHMARK(BM_Replay_TOB)->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Arg(16)->Arg(32);
BENCHMARK(BM_Replay_FullTouch)->Arg(1)->Arg(2)->Arg(4)->Arg(8)->Arg(16)->Arg(32);

BENCHMARK_MAIN();
