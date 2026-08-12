# Linux io_uring validation

## Decision

The production default remains `blocking`. `uring` is available as an explicit opt-in and
`auto` remains available for staged deployments, but the current host results do not satisfy the
no-material-throughput-regression gate for making `auto` the default.

## Reproduce

```bash
cargo build --release --example local_io_validation
target/release/examples/local_io_validation \
  --engine blocking --files 8 --size-mib 128 --repeats 3 --checksum
target/release/examples/local_io_validation \
  --engine uring --rings 2 --ring-entries 64 \
  --files 8 --size-mib 128 --repeats 3 --checksum
```

The harness uses eight concurrent `StorageEnum::copy_file` calls, matching Terrasync's default
migration concurrency. Each completed file is read back and BLAKE3 verified. Results are emitted
as CSV. Performance numbers are informational and are not a CI threshold.

## Environment

- Date: 2026-08-12
- Kernel: Linux 6.8 (io_uring READ, WRITE and FSYNC probe passed)
- Filesystem: host temporary filesystem; warm page cache
- Workload: eight concurrent 128 MiB files, checksum enabled, 2 MiB production chunk ceiling
- Defaults under test: 2 read rings + 2 write rings, 64 entries, global read/write budgets 32/64,
  256 MiB buffered-byte budget

Cold-cache testing was not performed because dropping the host page cache would be a global,
disruptive operation. The filesystem is fast enough that these results primarily characterize the
software pipeline and page-cache path, not physical-media throughput.

## Results

| Engine / pool | Throughput MiB/s | User ticks | System ticks | Peak RSS KiB | Verified |
|---|---:|---:|---:|---:|---|
| blocking | 2223.5 | 278 | 416 | 289624 | yes |
| uring 2+2 / 64 | 1267.4 | 211 | 124 | 255752 | yes |
| blocking | 2255.6 | 274 | 407 | 309076 | yes |
| uring 2+2 / 64 | 1172.3 | 214 | 135 | 253600 | yes |
| uring 1+1 / 32 | 926.0 | 211 | 126 | 272128 | yes |
| uring 4+4 / 64 | 1353.7 | 213 | 205 | 257416 | yes |

io_uring substantially reduced process CPU ticks and modestly reduced peak RSS, but default 2+2
throughput was roughly 43–48% lower than Blocking. The 4+4 pool improved throughput but still
remained about 40% below Blocking. This is too large a throughput regression for default enablement.

## Correctness and compatibility gates

- Blocking and real pooled io_uring run the same positional read/write/fsync contract.
- Eight concurrent multi-chunk files complete with identical size and BLAKE3.
- Resume, stale-tail truncation, durability, integrity, fallback, pool sharing, affinity and budget
  behavior are covered by the normal test suite.
- Unsupported initialization falls back in `auto`; forced `uring` reports an error.
- Runtime fallback only retries explicit `ENOSYS`/`EOPNOTSUPP`; uncertain writes are never replayed.
- Pool permits and inflight counters return to zero in deterministic stress tests.

Future hosts with slower physical storage may show a different throughput/CPU tradeoff. Operators
can select `auto` or `uring` explicitly and rerun this harness before deployment.
