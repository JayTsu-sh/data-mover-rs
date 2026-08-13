# blocking vs mmap multi-file copy prototype

**PROTOTYPE ONLY — do not integrate this code into production.**

Question: for thousands of 4–64 MiB local files at bounded file concurrency,
does whole-file `mmap` copying outperform the current blocking model of 2 MiB
positional reads/writes when both paths call `fsync`?

Run on Linux:

```bash
TMPDIR=/work/bench cargo run --release \
  --manifest-path prototypes/mmap-bench/Cargo.toml -- \
  --files 2048 --sizes-mib 4,16,64 --concurrency 8 --repeats 2
```

Each engine runs in a fresh child process so CPU time and peak RSS are isolated.
Engine order reverses every repeat. Destinations are BLAKE3-verified outside the
timed interval and deleted between runs. Source fixtures are deleted on exit.
