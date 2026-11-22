// src/core/hf_stream.cpp - the opt-in HTTP Range -> VRAM feeder (see include/strata/core/hf_stream.hpp).
#include "strata/core/hf_stream.hpp"

#include <algorithm>
#include <atomic>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "strata/artifact/gguf_reader.hpp"

#ifdef STRATA_HAVE_CURL
#include <curl/curl.h>
#endif

#if defined(_WIN32)
#include <io.h>
#define strata_open _open
#define strata_close _close
#define strata_pread _read
#define strata_pwrite _write
#define strata_fsync _commit
#else
#include <fcntl.h>
#include <unistd.h>
#define strata_open ::open
#define strata_close ::close
#define strata_pread ::pread
#define strata_pwrite ::pwrite
#define strata_fsync ::fsync
#endif

namespace strata::core {
namespace {

constexpr uint64_t kMinChunk = 1ull << 20;
constexpr uint64_t kMaxChunk = 64ull << 20;
constexpr int kMaxThreads = 32;
constexpr size_t kCacheChunks = 32;   // 32 x 8 MiB = 256 MiB at the default chunk

bool pread_full(int fd, void* dst, uint64_t bytes, uint64_t off) {
    uint8_t* p = static_cast<uint8_t*>(dst);
    uint64_t done = 0;
    while (done < bytes) {
#if defined(_WIN32)
        const int n = strata_pread(fd, p + done, (unsigned) std::min<uint64_t>(bytes - done, 1u << 30), (long) (off + done));
#else
        const ssize_t n = strata_pread(fd, p + done, (size_t) (bytes - done), (off_t) (off + done));
#endif
        if (n <= 0) return false;
        done += (uint64_t) n;
    }
    return true;
}

bool pwrite_full(int fd, const void* src, uint64_t bytes, uint64_t off) {
    const uint8_t* p = static_cast<const uint8_t*>(src);
    uint64_t done = 0;
    while (done < bytes) {
#if defined(_WIN32)
        const int n = strata_pwrite(fd, p + done, (unsigned) std::min<uint64_t>(bytes - done, 1u << 30), (long) (off + done));
#else
        const ssize_t n = strata_pwrite(fd, p + done, (size_t) (bytes - done), (off_t) (off + done));
#endif
        if (n <= 0) return false;
        done += (uint64_t) n;
    }
    return true;
}

std::string host_of(const std::string& url) {
    const size_t begin = url.find("://");
    if (begin == std::string::npos) return {};
    const size_t start = begin + 3;
    const size_t end = url.find_first_of("/?#", start);
    return url.substr(start, end == std::string::npos ? std::string::npos : end - start);
}

bool local_file_complete(const std::string& path) {
    std::error_code ec;
    if (!std::filesystem::is_regular_file(path, ec) || ec) return false;
    return !std::filesystem::exists(path + ".hfstream");
}

}  // namespace

#ifdef STRATA_HAVE_CURL
namespace {

struct CurlGlobal {
    CurlGlobal() { curl_global_init(CURL_GLOBAL_DEFAULT); }
    ~CurlGlobal() { curl_global_cleanup(); }
};

CurlGlobal& curl_global() {
    static CurlGlobal g;
    return g;
}

struct RangeAnswer {
    long status = 0;
    std::string effective_url;
    std::string content_range;
    std::string etag;
    std::string body;
    uint64_t total = 0;
    bool has_total = false;
};

struct ResponseBuffer {
    std::string body;
    uint64_t limit = 0;
    bool overflow = false;
};

size_t write_body(char* data, size_t size, size_t nmemb, void* user) {
    auto* out = static_cast<ResponseBuffer*>(user);
    const size_t n = size * nmemb;
    if (out->limit && out->body.size() + n > out->limit) {
        out->overflow = true;
        return 0;   // aborts the transfer
    }
    out->body.append(data, n);
    return n;
}

size_t collect_header(char* data, size_t size, size_t nmemb, void* user) {
    auto* answer = static_cast<RangeAnswer*>(user);
    const size_t n = size * nmemb;
    std::string line(data, n);
    auto lower = [](std::string s) {
        std::transform(s.begin(), s.end(), s.begin(), [](unsigned char c) { return (char) std::tolower(c); });
        return s;
    };
    const std::string low = lower(line);
    auto value = [&](const char* name) -> std::string {
        const std::string k = std::string(name) + ":";
        if (low.rfind(k, 0) != 0) return {};
        size_t at = k.size();
        while (at < line.size() && (line[at] == ' ' || line[at] == '\t')) ++at;
        size_t end = line.find_first_of("\r\n", at);
        return line.substr(at, end == std::string::npos ? std::string::npos : end - at);
    };
    if (const std::string v = value("content-range"); !v.empty()) answer->content_range = v;
    if (const std::string v = value("etag"); !v.empty()) answer->etag = v;
    return n;
}

bool parse_content_range(const std::string& value, uint64_t& total) {
    // bytes 0-0/123456
    const size_t slash = value.rfind('/');
    if (slash == std::string::npos || slash + 1 >= value.size()) return false;
    const std::string tail = value.substr(slash + 1);
    if (tail == "*") return false;
    char* end = nullptr;
    const unsigned long long n = std::strtoull(tail.c_str(), &end, 10);
    if (end == tail.c_str() || n == 0) return false;
    total = (uint64_t) n;
    return true;
}

bool http_range(const std::string& url, const std::string& token, uint64_t off, uint64_t len,
                RangeAnswer& answer, std::string& err) {
    CURL* curl = curl_easy_init();
    if (curl == nullptr) { err = "curl_easy_init failed"; return false; }
    ResponseBuffer body;
    body.limit = len;
    char range[96];
    std::snprintf(range, sizeof range, "%llu-%llu", (unsigned long long) off,
                  (unsigned long long) (off + len - 1));
    struct curl_slist* headers = nullptr;
    const std::string host = host_of(url);
    if (!token.empty() && host.find("huggingface.co") != std::string::npos) {
        const std::string auth = "Authorization: Bearer " + token;
        headers = curl_slist_append(headers, auth.c_str());
    }
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl, CURLOPT_MAXREDIRS, 10L);
    curl_easy_setopt(curl, CURLOPT_CONNECTTIMEOUT, 30L);
    curl_easy_setopt(curl, CURLOPT_LOW_SPEED_LIMIT, 1024L);
    curl_easy_setopt(curl, CURLOPT_LOW_SPEED_TIME, 60L);
    curl_easy_setopt(curl, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(curl, CURLOPT_USERAGENT, "strata-hf-stream/1");
    curl_easy_setopt(curl, CURLOPT_RANGE, range);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_body);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &body);
    curl_easy_setopt(curl, CURLOPT_HEADERFUNCTION, collect_header);
    curl_easy_setopt(curl, CURLOPT_HEADERDATA, &answer);
    if (headers) curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    const CURLcode rc = curl_easy_perform(curl);
    long status = 0;
    curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &status);
    char* effective = nullptr;
    curl_easy_getinfo(curl, CURLINFO_EFFECTIVE_URL, &effective);
    answer.status = status;
    answer.effective_url = effective != nullptr ? effective : url;
    answer.body = std::move(body.body);
    if (headers) curl_slist_free_all(headers);
    curl_easy_cleanup(curl);
    if (rc != CURLE_OK) {
        err = std::string("HTTP range request failed: ") + curl_easy_strerror(rc);
        return false;
    }
    if (body.overflow) { err = "HTTP range answer was larger than requested"; return false; }
    if (status == 206) {
        uint64_t total = 0;
        if (parse_content_range(answer.content_range, total)) {
            answer.total = total;
            answer.has_total = true;
        }
    }
    return true;
}

}  // namespace
#endif  // STRATA_HAVE_CURL

struct HfStream::Impl {
    HfStreamOptions opts;
    std::string final_url;
    uint64_t total = 0;
    uint64_t data_offset = 0;
    uint64_t chunk = 8ull << 20;
    uint64_t n_chunks = 0;
    int fd = -1;
    std::string path;

    mutable std::mutex mu;
    std::condition_variable cv;
    std::vector<uint8_t> state;   // 0 = missing, 1 = active, 2 = ready, 3 = failed
    bool failed = false;
    bool cancelled = false;
    bool finished = false;
    std::string error;
    uint64_t downloaded = 0;
    std::chrono::steady_clock::time_point began;
    std::chrono::steady_clock::time_point last_progress;

    std::deque<uint64_t> lru;
    std::unordered_map<uint64_t, std::shared_ptr<std::vector<uint8_t>>> cache;

    std::vector<std::thread> workers;

    ~Impl() {
        if (fd >= 0) strata_close(fd);
        if (opts.disk_tee == false && !path.empty()) {
            std::error_code ec;
            std::filesystem::remove(path, ec);
            std::filesystem::remove(path + ".hfstream", ec);
        }
    }

    uint64_t chunk_len(uint64_t index) const {
        const uint64_t off = index * chunk;
        return std::min(chunk, total - off);
    }

    void fail_locked(const std::string& why) {
        if (!failed) { failed = true; error = why; }
        cv.notify_all();
    }

    void progress_locked(bool force) {
        if (!opts.announce) return;
        const auto now = std::chrono::steady_clock::now();
        if (!force && last_progress.time_since_epoch().count() != 0 && now - last_progress < std::chrono::seconds(5)) return;
        last_progress = now;
        const double s = std::chrono::duration<double>(now - began).count();
        const double mib = (double) downloaded / (1024.0 * 1024.0);
        std::fprintf(stderr, "strata hf-stream: %.1f%% (%.1f MiB, %.1f MiB/s)\n",
                     100.0 * (double) downloaded / (double) (total ? total : 1), mib, s > 0 ? mib / s : 0.0);
        std::fflush(stderr);
    }
};

HfStream::HfStream() : impl_(std::make_unique<Impl>()) {}
HfStream::~HfStream() { abort(false); }

bool HfStream::start(const HfStreamOptions& opts, std::string& err) {
#ifdef STRATA_HAVE_CURL
    if (opts.url.empty()) { err = "hf stream: no URL"; return false; }
    if (opts.path.empty()) { err = "hf stream: no local path"; return false; }
    impl_->opts = opts;
    impl_->path = opts.path;
    impl_->chunk = std::clamp(opts.chunk_bytes, kMinChunk, kMaxChunk);
    const int threads = std::clamp(opts.threads, 1, kMaxThreads);
    curl_global();

    std::string token = opts.token;
    if (token.empty()) {
        if (const char* v = std::getenv("HF_TOKEN")) token = v;
        else if (const char* w = std::getenv("HUGGING_FACE_HUB_TOKEN")) token = w;
    }

    RangeAnswer probe;
    if (!http_range(opts.url, token, 0, 1, probe, err)) return false;
    if (probe.status != 206 || !probe.has_total) {
        err = "hf stream: the server did not answer a ranged request (HTTP " + std::to_string(probe.status) + ")";
        return false;
    }
    impl_->total = probe.total;
    impl_->final_url = probe.effective_url;
    if (impl_->total < 1024 * 1024) { err = "hf stream: remote file is too small to stream"; return false; }
    impl_->n_chunks = (impl_->total + impl_->chunk - 1) / impl_->chunk;
    if (impl_->n_chunks > (1ull << 32)) { err = "hf stream: too many chunks"; return false; }

    std::error_code ec;
    std::filesystem::create_directories(std::filesystem::path(opts.path).parent_path(), ec);
    impl_->fd = strata_open(opts.path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (impl_->fd < 0) { err = "hf stream: cannot create " + opts.path; return false; }
#if defined(_WIN32)
    if (_chsize_s(impl_->fd, (long long) impl_->total) != 0) { err = "hf stream: cannot size " + opts.path; return false; }
#else
    if (ftruncate(impl_->fd, (off_t) impl_->total) != 0) { err = "hf stream: cannot size " + opts.path; return false; }
#endif
    std::ofstream marker(opts.path + ".hfstream", std::ios::trunc);
    marker << opts.url << "\n" << impl_->total << "\n";
    marker.close();

    // The GGUF header must be readable before `--native` can open the file.  Fetch growing prefixes; the parser
    // is the same one the engine uses, so there is no second interpretation of the metadata.
    uint64_t probe_bytes = std::min<uint64_t>(impl_->total, 8ull << 20);
    bool parsed = false;
    std::string parse_error;
    while (!parsed) {
        RangeAnswer head;
        if (!http_range(opts.url, token, 0, probe_bytes, head, err)) return false;
        if (head.status != 206 || head.body.size() != probe_bytes) {
            err = "hf stream: header range answer was not " + std::to_string(probe_bytes) + " bytes (HTTP " +
                  std::to_string(head.status) + ")";
            return false;
        }
        if (!pwrite_full(impl_->fd, head.body.data(), head.body.size(), 0)) {
            err = "hf stream: writing the header to " + opts.path;
            return false;
        }
        try {
            strata::GgufFile g(opts.path);
            impl_->data_offset = g.data_start();
            parsed = impl_->data_offset <= probe_bytes && impl_->data_offset <= impl_->total;
            if (!parsed) parse_error = "the header needs more bytes than " + std::to_string(probe_bytes);
        } catch (const std::exception& e) {
            parse_error = e.what();
        }
        if (parsed) break;
        if (probe_bytes >= impl_->total || probe_bytes >= (128ull << 20)) {
            err = "hf stream: cannot parse the GGUF header: " + parse_error;
            return false;
        }
        probe_bytes = std::min<uint64_t>(impl_->total, probe_bytes * 2);
    }

    impl_->state.assign((size_t) impl_->n_chunks, 0);
    {   // chunks wholly inside the header are already on disk
        std::lock_guard<std::mutex> lock(impl_->mu);
        for (uint64_t i = 0; i < impl_->n_chunks; ++i) {
            if ((i + 1) * impl_->chunk <= probe_bytes) {
                impl_->state[(size_t) i] = 2;
                impl_->downloaded += impl_->chunk;
            } else {
                break;
            }
        }
    }
    impl_->began = std::chrono::steady_clock::now();
    if (opts.announce) {
        std::fprintf(stderr, "strata hf-stream: %s -> %s, %.2f GiB in %llu chunks, %d connection%s\n",
                     opts.url.c_str(), opts.path.c_str(), (double) impl_->total / (1024.0 * 1024.0 * 1024.0),
                     (unsigned long long) impl_->n_chunks, threads, threads == 1 ? "" : "s");
        std::fflush(stderr);
    }

    impl_->workers.reserve((size_t) threads);
    for (int t = 0; t < threads; ++t) {
        impl_->workers.emplace_back([this, t, token, threads] {
            CURL* curl = curl_easy_init();
            std::vector<uint8_t> buffer;
            struct curl_slist* auth = nullptr;
            if (!token.empty() && host_of(impl_->final_url).find("huggingface.co") != std::string::npos) {
                const std::string header = "Authorization: Bearer " + token;
                auth = curl_slist_append(auth, header.c_str());
            }
            if (curl == nullptr) {
                std::lock_guard<std::mutex> lock(impl_->mu);
                impl_->fail_locked("curl_easy_init failed in a worker");
                return;
            }
            while (true) {
                uint64_t index = 0;
                {
                    std::unique_lock<std::mutex> lock(impl_->mu);
                    if (impl_->cancelled || impl_->failed) break;
                    // find the next missing chunk; read chunks in file order, which is how the model loader walks
                    bool found = false;
                    for (uint64_t i = 0; i < impl_->n_chunks; ++i) {
                        if (impl_->state[(size_t) i] == 0) {
                            impl_->state[(size_t) i] = 1;
                            index = i;
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        // another worker is still downloading the tail, or everything is ready
                        if (impl_->downloaded >= impl_->total) break;
                        impl_->cv.wait(lock, [&] {
                            if (impl_->cancelled || impl_->failed || impl_->downloaded >= impl_->total) return true;
                            return std::any_of(impl_->state.begin(), impl_->state.end(),
                                               [](uint8_t s) { return s == 0; });
                        });
                        continue;
                    }
                }
                const uint64_t off = index * impl_->chunk;
                const uint64_t len = std::min(impl_->chunk, impl_->total - off);
                buffer.resize((size_t) len);
                bool ok = false;
                std::string last_error;
                for (int attempt = 0; attempt < 3 && !ok; ++attempt) {
                    CURL* c = curl;
                    ResponseBuffer body;
                    body.limit = len;
                    char range[96];
                    std::snprintf(range, sizeof range, "%llu-%llu", (unsigned long long) off,
                                  (unsigned long long) (off + len - 1));
                    curl_easy_reset(c);
                    curl_easy_setopt(c, CURLOPT_URL, impl_->final_url.c_str());
                    curl_easy_setopt(c, CURLOPT_FOLLOWLOCATION, 1L);
                    curl_easy_setopt(c, CURLOPT_CONNECTTIMEOUT, 30L);
                    curl_easy_setopt(c, CURLOPT_LOW_SPEED_LIMIT, 1024L);
                    curl_easy_setopt(c, CURLOPT_LOW_SPEED_TIME, 60L);
                    curl_easy_setopt(c, CURLOPT_NOSIGNAL, 1L);
                    curl_easy_setopt(c, CURLOPT_USERAGENT, "strata-hf-stream/1");
                    curl_easy_setopt(c, CURLOPT_RANGE, range);
                    curl_easy_setopt(c, CURLOPT_WRITEFUNCTION, write_body);
                    curl_easy_setopt(c, CURLOPT_WRITEDATA, &body);
                    if (auth) curl_easy_setopt(c, CURLOPT_HTTPHEADER, auth);
                    const CURLcode rc = curl_easy_perform(c);
                    long status = 0;
                    curl_easy_getinfo(c, CURLINFO_RESPONSE_CODE, &status);
                    if (rc == CURLE_OK && status == 206 && body.body.size() == len) {
                        std::memcpy(buffer.data(), body.body.data(), (size_t) len);
                        ok = true;
                    } else {
                        last_error = rc != CURLE_OK ? curl_easy_strerror(rc)
                                                    : "HTTP " + std::to_string(status) + ", " +
                                                          std::to_string(body.body.size()) + " of " +
                                                          std::to_string(len) + " bytes";
                        std::this_thread::sleep_for(std::chrono::milliseconds(250 * (attempt + 1)));
                    }
                }
                if (!ok) {
                    std::lock_guard<std::mutex> lock(impl_->mu);
                    impl_->fail_locked("chunk " + std::to_string(index) + ": " + last_error);
                    break;
                }
                bool write_ok = true;
                if (impl_->opts.disk_tee) write_ok = pwrite_full(impl_->fd, buffer.data(), len, off);
                if (!write_ok) {
                    std::lock_guard<std::mutex> lock(impl_->mu);
                    impl_->fail_locked("chunk " + std::to_string(index) + ": writing " + impl_->path);
                    break;
                }
                auto shared = std::make_shared<std::vector<uint8_t>>(std::move(buffer));
                {
                    std::lock_guard<std::mutex> lock(impl_->mu);
                    impl_->cache[index] = shared;
                    impl_->lru.push_back(index);
                    while (impl_->lru.size() > kCacheChunks) {
                        const uint64_t old = impl_->lru.front();
                        impl_->lru.pop_front();
                        impl_->cache.erase(old);
                    }
                    impl_->state[(size_t) index] = 2;
                    impl_->downloaded += len;
                    impl_->progress_locked(false);
                    impl_->cv.notify_all();
                }
                buffer = std::vector<uint8_t>();
                if (t == 0 && impl_->downloaded >= impl_->total) {
                    // one more pass for stragglers: no special case, the loop below notices the empty queue
                }
            }
            if (auth) curl_slist_free_all(auth);
            if (curl) curl_easy_cleanup(curl);
            (void) threads;
        });
    }
    return true;
#else
    (void) opts;
    err = "this engine was built without libcurl: --hf-stream-vram is unavailable";
    return false;
#endif
}

bool HfStream::read_at(uint64_t offset, void* dst, uint64_t bytes, std::string& err) {
    if (bytes == 0) return true;
    if (offset > impl_->total || bytes > impl_->total - offset) {
        err = "hf stream: read past the end of the file";
        return false;
    }
    const uint64_t first = offset / impl_->chunk;
    const uint64_t last = (offset + bytes - 1) / impl_->chunk;
    for (uint64_t i = first; i <= last; ++i) {
        std::unique_lock<std::mutex> lock(impl_->mu);
        impl_->cv.wait(lock, [&] {
            return impl_->state[(size_t) i] == 2 || impl_->failed || impl_->cancelled;
        });
        if (impl_->failed || impl_->cancelled) {
            err = impl_->error.empty() ? "hf stream cancelled" : impl_->error;
            return false;
        }
    }
    uint8_t* out = static_cast<uint8_t*>(dst);
    uint64_t at = offset;
    while (at < offset + bytes) {
        const uint64_t index = at / impl_->chunk;
        const uint64_t chunk_off = index * impl_->chunk;
        const uint64_t chunk_end = std::min(impl_->total, chunk_off + impl_->chunk);
        const uint64_t from = std::max(at, chunk_off);
        const uint64_t to = std::min(offset + bytes, chunk_end);
        const uint64_t n = to - from;
        std::shared_ptr<std::vector<uint8_t>> cached;
        {
            std::lock_guard<std::mutex> lock(impl_->mu);
            const auto it = impl_->cache.find(index);
            if (it != impl_->cache.end()) cached = it->second;
        }
        if (cached != nullptr && cached->size() >= from - chunk_off + n) {
            std::memcpy(out + (at - offset), cached->data() + (from - chunk_off), (size_t) n);
        } else if (impl_->opts.disk_tee) {
            if (!pread_full(impl_->fd, out + (at - offset), n, from)) {
                err = "hf stream: cannot read back the bytes just written";
                return false;
            }
        } else {
            err = "hf stream: the chunk cache rotated past a requested range (disk tee is off)";
            return false;
        }
        at = to;
    }
    return true;
}

bool HfStream::finish(std::string& err) {
    if (impl_->finished) return !impl_->failed;
    {
        std::unique_lock<std::mutex> lock(impl_->mu);
        impl_->cv.wait(lock, [&] { return impl_->downloaded >= impl_->total || impl_->failed || impl_->cancelled; });
        if (impl_->failed || impl_->cancelled) {
            err = impl_->error.empty() ? "hf stream cancelled" : impl_->error;
            return false;
        }
    }
    for (auto& t : impl_->workers) if (t.joinable()) t.join();
    if (impl_->opts.disk_tee && impl_->fd >= 0) {
        if (strata_fsync(impl_->fd) != 0) {
            err = "hf stream: fsync failed";
            return false;
        }
    }
    impl_->finished = true;
    std::error_code ec;
    if (impl_->opts.disk_tee) std::filesystem::remove(impl_->path + ".hfstream", ec);   // `none` keeps the marker: the file is not complete on disk
    {
        std::lock_guard<std::mutex> lock(impl_->mu);
        impl_->cache.clear();
        impl_->lru.clear();
        if (impl_->opts.announce) {
            const double s = std::chrono::duration<double>(std::chrono::steady_clock::now() - impl_->began).count();
            std::fprintf(stderr, "strata hf-stream: complete, %.2f GiB in %.1f s (%.1f MiB/s)\n",
                         (double) impl_->total / (1024.0 * 1024.0 * 1024.0), s,
                         s > 0 ? (double) impl_->total / (1024.0 * 1024.0) / s : 0.0);
            std::fflush(stderr);
        }
    }
    return true;
}

void HfStream::abort(bool remove_partial) {
    if (!impl_) return;
    {
        std::lock_guard<std::mutex> lock(impl_->mu);
        impl_->cancelled = true;
        impl_->cv.notify_all();
    }
    for (auto& t : impl_->workers) if (t.joinable()) t.join();
    impl_->workers.clear();
    if (impl_->fd >= 0) { strata_close(impl_->fd); impl_->fd = -1; }
    if (remove_partial && !impl_->path.empty()) {
        std::error_code ec;
        std::filesystem::remove(impl_->path, ec);
        std::filesystem::remove(impl_->path + ".hfstream", ec);
    }
}

bool HfStream::complete() const {
    std::lock_guard<std::mutex> lock(impl_->mu);
    return impl_->finished && !impl_->failed;
}
uint64_t HfStream::total_size() const { return impl_->total; }
uint64_t HfStream::data_offset() const { return impl_->data_offset; }
const std::string& HfStream::path() const { return impl_->path; }
const std::string& HfStream::error() const { return impl_->error; }
double HfStream::mib_per_second() const {
    std::lock_guard<std::mutex> lock(impl_->mu);
    const double s = std::chrono::duration<double>(std::chrono::steady_clock::now() - impl_->began).count();
    return s > 0 ? (double) impl_->downloaded / (1024.0 * 1024.0) / s : 0.0;
}

// ---- the registry ------------------------------------------------------------------------------------------
namespace {
std::mutex g_hf_mu;
std::unique_ptr<HfStream> g_hf_stream;
std::string g_hf_path;
std::atomic<bool> g_hf_on{false};
}  // namespace

bool hf_stream_supported() {
#ifdef STRATA_HAVE_CURL
    return true;
#else
    return false;
#endif
}

bool hf_stream_start(const HfStreamOptions& opts, std::string& err) {
    std::lock_guard<std::mutex> lock(g_hf_mu);
    if (g_hf_stream) { err = "hf stream: one download is already active"; return false; }
    if (local_file_complete(opts.path)) {
        std::error_code ec;
        const uint64_t have = (uint64_t) std::filesystem::file_size(opts.path, ec);
        if (!ec && have > 0) return true;   // a completed local file: nothing to stream
    }
    auto stream = std::make_unique<HfStream>();
    if (!stream->start(opts, err)) {
        stream->abort(true);
        return false;
    }
    g_hf_path = opts.path;
    g_hf_stream = std::move(stream);
    g_hf_on.store(true, std::memory_order_release);
    return true;
}

HfStream* hf_stream_find(const std::string& path) {
    if (!g_hf_on.load(std::memory_order_acquire)) return nullptr;
    std::lock_guard<std::mutex> lock(g_hf_mu);
    if (!g_hf_stream || g_hf_path != path) return nullptr;
    return g_hf_stream.get();
}

bool hf_stream_active(const std::string& path) {
    return hf_stream_find(path) != nullptr;
}

bool hf_stream_any() { return g_hf_on.load(std::memory_order_acquire); }

bool hf_stream_read(const std::string& path, uint64_t offset, void* dst, uint64_t bytes, std::string& err) {
    HfStream* stream = hf_stream_find(path);
    if (stream == nullptr) return false;
    return stream->read_at(offset, dst, bytes, err);
}

bool hf_stream_finish(std::string& err) {
    std::lock_guard<std::mutex> lock(g_hf_mu);
    if (!g_hf_stream) return true;
    const bool ok = g_hf_stream->finish(err);
    return ok;
}

void hf_stream_reset() {
    std::lock_guard<std::mutex> lock(g_hf_mu);
    g_hf_on.store(false, std::memory_order_release);
    if (g_hf_stream) g_hf_stream->abort(false);
    g_hf_stream.reset();
    g_hf_path.clear();
}

}  // namespace strata::core
