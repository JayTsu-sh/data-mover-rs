# Test plan: <title>

Issue: #<number>
Design: `docs/specs/<issue>-<slug>/design.md`

## Unit tests

- Parsing, encoding, state transitions, and boundary values.

## Contract and golden tests

- RFC examples, captured packets, and stable expected results.

## Integration tests

- NFSv3 against `10.10.1.12` and `10.10.1.13`.
- NFSv4.1 against `10.10.1.12` and `10.10.1.13`.

## Failure injection

- Timeout, disconnect, truncated response, restart, and retry exhaustion.

## Compatibility

- Previous public API and mixed-version behavior where applicable.

## Performance

- Define workload, baseline, variance, and failure threshold.

## Evidence

- Link CI runs, logs, checksums, and benchmark reports.
