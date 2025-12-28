#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "schema.hpp"

namespace md::l2 {

/**
 * ReplayKernel
 * -------------
 * A zero-copy, sequential replay engine over memory-mapped L2 snapshot files.
 *
 * Design goals:
 * - Treat the dataset as a contiguous stream of fixed-size Records.
 * - Perform no allocations and no Record copies.
 * - Expose raw pointers so the CPU only loads what is actually accessed.
 * - Keep the hot path branch-free except for end-of-stream checks.
 *
 * Lifetime:
 * - ReplayKernel owns the memory mapping.
 * - Pointers returned by next()/data()/begin()/end() remain valid
 *   until the ReplayKernel is destroyed.
 *
 * Threading:
 * - Intended usage is single-threaded replay in simulators/benchmarks.
 */
class ReplayKernel final {
public:
    /**
     * Construct a replay kernel by memory-mapping a `.snap` file.
     *
     * Performs header validation:
     * - magic / version / depth
     * - record_size consistency
     * - file_size == header + record_count * sizeof(Record)
     *
     * Throws std::runtime_error on failure.
     */
    explicit ReplayKernel(const std::string& snap_path);

    // Non-copyable: mapping ownership must be unique
    ReplayKernel(const ReplayKernel&) = delete;
    ReplayKernel& operator=(const ReplayKernel&) = delete;

    // Movable: allows storing in containers or returning from factories
    ReplayKernel(ReplayKernel&&) noexcept;
    ReplayKernel& operator=(ReplayKernel&&) noexcept;

    ~ReplayKernel();

    /**
     * Total number of records in the mapped file.
     */
    std::size_t size() const noexcept { return size_; }

    /**
     * Current replay cursor position [0, size()].
     * When pos() == size(), replay is exhausted.
     */
    std::size_t pos() const noexcept { return pos_; }

    /**
     * Reset the replay cursor to the beginning.
     * O(1).
     */
    void reset() noexcept { pos_ = 0; }

    /**
     * Advance the replay cursor and return the next record.
     *
     * Returns:
     * - pointer to Record if available
     * - nullptr if end-of-stream is reached
     *
     * Performance:
     * - No allocations
     * - No Record copies
     * - One predictable branch (end-of-stream)
     */
    [[nodiscard]]
    const Record* next() noexcept;

    /**
     * Pointer to the first record.
     * Enables tight pointer-based loops:
     *
     *   for (auto p = rk.begin(); p != rk.end(); ++p) { ... }
     */
    const Record* begin() const noexcept { return data_; }

    /**
     * Pointer one past the last record.
     */
    const Record* end() const noexcept { return data_ + size_; }

    /**
     * Raw pointer to the underlying record array.
     * Equivalent to begin().
     */
    const Record* data() const noexcept { return data_; }

    /**
     * Access a record by index without advancing the cursor.
     * No bounds checking (caller responsibility).
     */
    const Record& operator[](std::size_t idx) const noexcept {
        return data_[idx];
    }

private:
    // ---- Memory-mapped region ----
    const Record*  data_ = nullptr;   // start of records
    std::size_t    size_ = 0;         // number of records
    std::size_t    pos_  = 0;         // replay cursor

    void* view_ = nullptr;  // base address returned by MapViewOfFile

    // ---- Platform-specific state ----
    void* file_handle_ = nullptr;
    void* mapping_handle_ = nullptr;

    // ---- Helpers ----
    void map_file_(const std::string& path);
    void unmap_file_() noexcept;
};

} // namespace md::l2
