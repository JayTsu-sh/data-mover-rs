# io_uring copy benchmark prototype

Throwaway Linux-only prototype for comparing positional blocking I/O with
io_uring before integrating an engine into `LocalStorage`.

Run from the repository root:

```bash
cargo run --release --manifest-path prototypes/io-uring-bench/Cargo.toml -- \
  --size-mib 512 --repeats 2 --ring-entries 64 \
  --pipelines 4:8:8,8:8:8,8:16:16,16:16:16,16:32:32,32:32:32
```

The program creates its fixture under `/tmp`, runs both engines with the same
file and block size. Both engines use independent read and write inflight
windows separated by the same bounded channel. Each pipeline value is
`read_inflight:write_inflight:channel_capacity`. The io_uring SQ/CQ capacity is
configured separately through `--ring-entries`.

The prototype uses one read ring on a producer thread and one write ring on the
consumer thread; `--ring-entries` applies to each ring. This preserves the
production pipeline's independent inflight windows, but a future implementation
that multiplexes both directions through one shared ring worker must be measured
again after integration.

Every destination is verified with BLAKE3 and the temporary directory is
removed before exit.

Odd-numbered repetitions visit pipeline configurations in the supplied order;
even-numbered repetitions reverse it to reduce systematic cache and temperature
bias.

The benchmark intentionally uses ordinary buffered file I/O. It does not use
`O_DIRECT`, registered files, registered buffers, SQPOLL, or IOPOLL.
