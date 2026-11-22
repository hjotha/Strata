# Streaming a GGUF download straight into VRAM (`--hf-stream-vram`)

This is an opt-in engine mode for the native Flash-Next path (`--native`). It joins the two phases an ordinary
remote start runs one after the other:

1. the GGUF is downloaded to disk, then
2. the engine re-opens it and uploads the dense projections, the output head and the profile's expert cache fill.

With `--hf-stream-vram` the engine fetches the metadata shard itself in parallel HTTP Range chunks and the loaders
read each arriving chunk. The file is still written to `--native`'s path (`--hf-stream-disk-mode none` skips that),
so the next start is an ordinary local start.

It is the same idea as beellama.cpp's `--hf-stream-vram`: pinned staging buffers feed the GPU while the network is
still busy, instead of a second pass over a multi-gigabyte file.

## Requirements

- A build with libcurl (the configure step prints `--hf-stream-vram enabled (libcurl ...)`), and `--serve`: a
  one-shot generation has no `READY` point at which the download must be complete.
- An HTTP server that answers `Range` requests with `206` (Hugging Face's `resolve` URLs do).
- The other shards of a split model already on disk. This mode streams the metadata shard named by `--native`;
  a split's remaining shards, the PLE table and the pack are read normally.
- No resident expert arena (`--resident-experts`, `--resident-budget-gib`): that mode reads the whole GGUF before
  the arena is built, so streaming would not overlap anything.

## Flags

| Flag | Default | Meaning |
| --- | --- | --- |
| `--hf-stream-vram` | off | enable the mode (requires `--native PATH` and `--hf-stream-url URL`) |
| `--hf-stream-url URL` | | the Hugging Face `resolve` URL of `--native`'s GGUF |
| `--hf-stream-threads N` | `8` | parallel HTTP Range connections (1..32) |
| `--hf-stream-chunk-mb N` | `8` | one Range request and staging buffer, MiB (1..64) |
| `--hf-stream-disk-mode MODE` | `sync` | `sync` persists the file; `background` is accepted as the same I/O path for now; `none` keeps only the small chunk cache |
| `--hf-stream-token T` | `HF_TOKEN` | bearer token, sent only to `huggingface.co` |

The serving config can carry the settings in its `env` map instead of argv: `STRATA_HF_STREAM=1`,
`STRATA_HF_STREAM_URL`, `STRATA_HF_STREAM_TOKEN`, `STRATA_HF_STREAM_THREADS`, `STRATA_HF_STREAM_CHUNK_MB`,
`STRATA_HF_STREAM_DISK_MODE`. An explicit flag wins.

## What it does with the bytes

- The first request resolves the redirect, learns the total size, creates `--native`'s path and fetches a growing
  header prefix until the GGUF parser accepts it. A `<path>.hfstream` marker says the file is incomplete.
- The workers fetch the remaining chunks in file order. Every chunk is written to the file (unless the mode is
  `none`) and kept in a 256 MiB chunk cache; `read_at` waits for the chunks a tensor touches and copies from that
  cache, or reads the just-written range from the file cache.
- The dense projections, the output head and the embedding read through this path instead of the sparse mapping.
- The expert source's startup profile fill reads the arriving expert blobs through it too.
- Before `READY` the engine waits for every byte, fsyncs, removes the marker and (for a persisted file) releases
  the stream; later expert misses use the completed mapping.
- A failed or interrupted download leaves the marker in place, so the next start downloads again rather than
  trusting a partial file.

## Limits

- Only the metadata shard is streamed. The PLE table and a split's other shards must be present.
- `--hf-stream-disk-mode none` keeps only the chunk cache. It is useful when every weight is resident (for example
  a model that fits the card), but a later expert miss outside the cache cannot be served; the download is still
  written to a sparse placeholder while the process runs and removed on exit.
- The mode does not verify a SHA-256. The URL's revision is the recipe's pin; a file that must be checked
  byte-for-byte should be downloaded with setup.py, which keeps the checks.
- It is currently wired into the native Flash-Next path (`--native` / `--native-dense-gguf` / `--native-head-gguf`).
  The Qwen3.6 engine path parses its own arguments and does not take these flags yet.
