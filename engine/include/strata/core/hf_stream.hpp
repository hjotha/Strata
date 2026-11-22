// include/strata/core/hf_stream.hpp - opt-in Hugging Face/HTTP Range -> VRAM streaming.
//
// The engine normally loads a GGUF from a completed local file: the download happens first (setup.py, a browser
// or a shell), then `--native` maps it and the dense upload and the expert cache fill read it again.  This class
// turns that into one phase: the header is fetched, the model is opened, and while the rest of the file arrives
// in parallel HTTP Range chunks the engine reads those bytes through this object (pinned staging -> VRAM).
//
// It is deliberately a small, blocking reader rather than a second model loader.  `read_at` waits for the chunk
// that holds the requested range, then copies from the chunk cache or from the file just written; callers keep
// their existing upload code.  The file is persisted by default, so a later start is an ordinary local start.
//
// Only compiled when libcurl is found (STRATA_HAVE_CURL).  Without it the flags are refused with a clear error.
#pragma once

#include <cstdint>
#include <memory>
#include <string>

namespace strata::core {

struct HfStreamOptions {
    std::string url;              ///< the Hugging Face `resolve` URL (redirects and signed CDN URLs are followed)
    std::string path;             ///< the local GGUF path written in place; the caller keeps using this path
    std::string token;            ///< optional bearer token; empty reads HF_TOKEN / HUGGING_FACE_HUB_TOKEN
    int threads = 8;              ///< parallel HTTP Range connections (1..32)
    uint64_t chunk_bytes = 8ull << 20; ///< one Range request and one staging buffer (1..64 MiB)
    bool disk_tee = true;         ///< persist every chunk to `path`; false keeps only the chunk cache
    bool announce = true;         ///< one line at start, then progress every few seconds
};

/// One active download.  The global helpers below register it by path, so the loaders (which only know the GGUF
/// path) reach it without threading a pointer through every weight table and expert source.
class HfStream {
public:
    HfStream();
    ~HfStream();
    HfStream(const HfStream&) = delete;
    HfStream& operator=(const HfStream&) = delete;

    /// Resolve the URL, create `path` (ftruncated to the remote length), fetch enough header bytes for GgufFile
    /// and start the workers.  False with `err` leaves no partial state behind.
    bool start(const HfStreamOptions& opts, std::string& err);
    /// Absolute file offset `[offset, offset + bytes)`, copied into `dst`.  Blocks until every chunk that range
    /// touches has arrived.  False on a download error or when a no-disk stream's chunk was evicted.
    bool read_at(uint64_t offset, void* dst, uint64_t bytes, std::string& err);
    /// Wait for every byte, fsync and close the descriptor.  Idempotent.  The file stays usable through mmap.
    bool finish(std::string& err);
    /// Stop the workers and release the descriptor.  A partial file remains unless `remove_partial` is set.
    void abort(bool remove_partial = false);

    bool complete() const;
    uint64_t total_size() const;
    uint64_t data_offset() const;
    const std::string& path() const;
    const std::string& error() const;
    double mib_per_second() const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

// ---- process-wide registry (one stream, the model's metadata shard) -----------------------------------------
bool hf_stream_start(const HfStreamOptions& opts, std::string& err);
HfStream* hf_stream_find(const std::string& path);
bool hf_stream_active(const std::string& path);
/// Whether any download is registered, without a path comparison or a lock: the fast path for readers that run
/// once per expert per token and must cost nothing when the feature is off.
bool hf_stream_any();
/// Read through `path`'s stream when it is active; false means "no stream for this path, use the mapping".
bool hf_stream_read(const std::string& path, uint64_t offset, void* dst, uint64_t bytes, std::string& err);
bool hf_stream_finish(std::string& err);
void hf_stream_reset();

/// True when this build can stream (libcurl found at configure time).
bool hf_stream_supported();

}  // namespace strata::core
