// Google Benchmark for the mmap-based ReplayKernel.
//
// Usage (PowerShell):
//   $env:SNAP_GLOB = "C:\Users\33767\Desktop\Code\microstructure-rl\data\processed\*.snap"
//   .\build\Release\bench_replay.exe
//
// Or specify a single file:
//   $env:SNAP_PATH = "C:\...\BTCUSDT_depth20_2025-12-26_23.snap"
//   .\build\Release\bench_replay.exe

#include <benchmark/benchmark.h>

#include "replay.hpp"

#include <cstdlib>
#include <filesystem>
#include <string>
#include <vector>

namespace fs = std::filesystem;

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


static std::vector<std::string> collect_snap_files() {
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
        if (ent.is_regular_file() && ent.path().extension() == ".snap") {
            out.push_back(ent.path().string());
        }
    }

    if (out.empty()) {
        throw std::runtime_error("No .snap files found under DATA_PROCESSED_ROOT");
    }

    std::sort(out.begin(), out.end());
    return out;
}


static std::vector<std::string> g_snaps;

static void ensure_snaps_loaded(benchmark::State& state) {
    if (!g_snaps.empty()) return;
    try {
        g_snaps = collect_snap_files();
    } catch (const std::exception& e) {
        state.SkipWithError(e.what());
    }
}

// Touch a wide set of fields so we actually pull more cache lines of each record.
static inline void touch_more_fields(const md::l2::Record* rec) {
    benchmark::DoNotOptimize(rec->ts_recv_ns);
    benchmark::DoNotOptimize(rec->ts_event_ms);

    // Top-of-book prices/qty
    benchmark::DoNotOptimize(rec->bids[0].price_q);
    benchmark::DoNotOptimize(rec->bids[0].qty_q);
    benchmark::DoNotOptimize(rec->asks[0].price_q);
    benchmark::DoNotOptimize(rec->asks[0].qty_q);

    // A few deeper levels (to force wider record access)
    benchmark::DoNotOptimize(rec->bids[5].price_q);
    benchmark::DoNotOptimize(rec->bids[5].qty_q);
    benchmark::DoNotOptimize(rec->asks[5].price_q);
    benchmark::DoNotOptimize(rec->asks[5].qty_q);

    // Edge levels (likely in later cache lines)
    benchmark::DoNotOptimize(rec->bids[19].price_q);
    benchmark::DoNotOptimize(rec->bids[19].qty_q);
    benchmark::DoNotOptimize(rec->asks[19].price_q);
    benchmark::DoNotOptimize(rec->asks[19].qty_q);
}

// Round-robin over multiple snap files to increase working set beyond L3,
// and avoid "one file fits in cache" best-case results.
static void BM_ReplayNext_MultiFile_FullTouch(benchmark::State& state) {
    ensure_snaps_loaded(state);
    if (state.skipped()) return;

    std::uint64_t total_records = 0;

    std::size_t file_idx = 0;
    md::l2::ReplayKernel kernel(g_snaps[file_idx]);
    kernel.reset();

    for (auto _ : state) {
        const md::l2::Record* rec = kernel.next();
        if (!rec) {
            // Advance to next file to keep cache pressure realistic
            file_idx = (file_idx + 1) % g_snaps.size();
            kernel = md::l2::ReplayKernel(g_snaps[file_idx]);
            kernel.reset();
            rec = kernel.next();
            if (!rec) {
                state.SkipWithError("Encountered an empty .snap file");
                break;
            }
        }

        touch_more_fields(rec);
        ++total_records;
    }

    // Items/sec
    state.SetItemsProcessed(static_cast<int64_t>(total_records));

    // Bytes/sec: report full record size streaming rate
    state.SetBytesProcessed(static_cast<int64_t>(total_records) *
                            static_cast<int64_t>(sizeof(md::l2::Record)));
}

BENCHMARK(BM_ReplayNext_MultiFile_FullTouch);

BENCHMARK_MAIN();
