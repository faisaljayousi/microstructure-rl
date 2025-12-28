// Windows memory-mapped replay kernel implementation.
// - Maps a .snap file produced by the C++ converter.
// - Validates FileHeader and file size.
// - Exposes Record* for zero-copy sequential replay.

#include "replay.hpp"

#include <cstddef>   // std::byte
#include <stdexcept>
#include <string>
#include <utility>

#define NOMINMAX
#include <windows.h>

namespace md::l2 {

namespace {

// Convert UTF-8 std::string path -> wide string for WinAPI.
std::wstring to_wstring_utf8(const std::string& s) {
    if (s.empty()) return std::wstring();

    const int needed = MultiByteToWideChar(CP_UTF8, 0, s.data(), (int)s.size(), nullptr, 0);
    if (needed <= 0) throw std::runtime_error("MultiByteToWideChar failed for path");

    std::wstring w;
    w.resize((std::size_t)needed);

    const int written = MultiByteToWideChar(CP_UTF8, 0, s.data(), (int)s.size(), w.data(), needed);
    if (written != needed) throw std::runtime_error("MultiByteToWideChar wrote unexpected length");

    return w;
}

std::uint64_t file_size_u64(HANDLE hFile) {
    LARGE_INTEGER sz{};
    if (!GetFileSizeEx(hFile, &sz)) throw std::runtime_error("GetFileSizeEx failed");
    return static_cast<std::uint64_t>(sz.QuadPart);
}

[[noreturn]] void throw_last_error(const char* what) {
    const DWORD e = GetLastError();
    throw std::runtime_error(std::string(what) + " (GetLastError=" + std::to_string(e) + ")");
}

} // namespace

ReplayKernel::ReplayKernel(const std::string& snap_path) {
    map_file_(snap_path);
}

ReplayKernel::ReplayKernel(ReplayKernel&& other) noexcept {
    *this = std::move(other);
}

ReplayKernel& ReplayKernel::operator=(ReplayKernel&& other) noexcept {
    if (this == &other) return *this;

    unmap_file_();

    data_ = other.data_;
    size_ = other.size_;
    pos_  = other.pos_;

    file_handle_    = other.file_handle_;
    mapping_handle_ = other.mapping_handle_;
    view_           = other.view_;

    other.data_ = nullptr;
    other.size_ = 0;
    other.pos_  = 0;
    other.file_handle_ = nullptr;
    other.mapping_handle_ = nullptr;
    other.view_ = nullptr;

    return *this;
}

ReplayKernel::~ReplayKernel() {
    unmap_file_();
}

const Record* ReplayKernel::next() noexcept {
    if (pos_ >= size_) return nullptr;
    return &data_[pos_++];
}

void ReplayKernel::map_file_(const std::string& path) {
    unmap_file_();

    const std::wstring wpath = to_wstring_utf8(path);

    HANDLE hFile = CreateFileW(
        wpath.c_str(),
        GENERIC_READ,
        FILE_SHARE_READ,
        nullptr,
        OPEN_EXISTING,
        FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN,
        nullptr);

    if (hFile == INVALID_HANDLE_VALUE) {
        throw_last_error("CreateFileW failed");
    }

    HANDLE hMap = nullptr;
    void* view = nullptr;

    try {
        const std::uint64_t fsz = file_size_u64(hFile);
        if (fsz < sizeof(FileHeader)) {
            throw std::runtime_error("File too small to contain header");
        }

        hMap = CreateFileMappingW(hFile, nullptr, PAGE_READONLY, 0, 0, nullptr);
        if (!hMap) {
            throw_last_error("CreateFileMappingW failed");
        }

        view = MapViewOfFile(hMap, FILE_MAP_READ, 0, 0, 0);
        if (!view) {
            throw_last_error("MapViewOfFile failed");
        }

        // Store OS resources immediately so unmap_file_() can clean up on any later error.
        file_handle_    = hFile;
        mapping_handle_ = hMap;
        view_           = view;

        const auto* base = static_cast<const std::byte*>(view_);
        const auto* hdr  = reinterpret_cast<const FileHeader*>(base);

        // Header validation
        if (hdr->magic != kMagic) throw std::runtime_error("Bad magic: not a .snap file");
        if (hdr->version != kVersion) throw std::runtime_error("Unsupported version");
        if (hdr->depth != kDepth) throw std::runtime_error("Depth mismatch");
        if (hdr->record_size != sizeof(Record)) throw std::runtime_error("Record size mismatch");
        if (hdr->endian_check != kEndianCheck) throw std::runtime_error("Endian check mismatch");
        if (hdr->price_scale <= 0 || hdr->qty_scale <= 0) throw std::runtime_error("Invalid scales in header");

        const std::uint64_t payload = fsz - sizeof(FileHeader);
        if (payload % sizeof(Record) != 0) throw std::runtime_error("Payload not multiple of Record size");

        const std::uint64_t inferred_count = payload / sizeof(Record);
        if (hdr->record_count != 0 && hdr->record_count != inferred_count) {
            throw std::runtime_error("record_count mismatch: header vs file size");
        }

        data_ = reinterpret_cast<const Record*>(base + sizeof(FileHeader));
        size_ = static_cast<std::size_t>(inferred_count);
        pos_  = 0;

        return; // success
    } catch (...) {
        // If we haven't stored into members yet, close local handles.
        // If we have stored into members, unmap_file_() will handle it.
        if (!view_ && view) UnmapViewOfFile(view);
        if (!mapping_handle_ && hMap) CloseHandle(hMap);
        if (!file_handle_ && hFile != INVALID_HANDLE_VALUE) CloseHandle(hFile);

        // If members were set, clean them up for consistent state.
        unmap_file_();
        throw;
    }
}

void ReplayKernel::unmap_file_() noexcept {
    if (view_) {
        UnmapViewOfFile(view_);
        view_ = nullptr;
    }
    if (mapping_handle_) {
        CloseHandle(static_cast<HANDLE>(mapping_handle_));
        mapping_handle_ = nullptr;
    }
    if (file_handle_) {
        CloseHandle(static_cast<HANDLE>(file_handle_));
        file_handle_ = nullptr;
    }

    data_ = nullptr;
    size_ = 0;
    pos_  = 0;
}

} // namespace md::l2
