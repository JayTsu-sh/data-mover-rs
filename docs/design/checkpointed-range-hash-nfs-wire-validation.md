# Checkpointed range hash: NFS wire validation

Validated 2026-10-23 on the actual ordinary streaming transfer binary run by the
main worker on `.17`. This note records an offline audit of those captures. It is
not a dependency upgrade, a backend crash-durability certification, or proof of
production source reads traversing multiple pNFS data servers.

## Scope and evidence

The fixed source was
`nfs://192.168.9.101/pnfs_test2/resume-hash-20261023-uQqo3e/source/payload.bin`,
**402,653,517 bytes**. Separate destination directories were `cancel`, `kill`,
and `unordered` under the same unique export root.

The main worker performed cancellation/resume, SIGKILL/resume with changed chunk
sizes, and a fresh unshaped transfer. Each completed with pinned destination
readback enabled. The main worker also reported matching independently computed
kernel source/final SHA-256 values; the offline parser did not recompute file
contents or treat destination verification READs as source observations.

The three pcaps and final audit JSONs remain outside Git in the private directory:

```text
/work/data-mover-hash-validation-pcaps-20261023-puqx1E/
  cancel-resume.pcap
  cancel-resume-read-audit.json
  kill-resume.pcap
  kill-resume-read-audit.json
  unordered.pcap
  unordered-read-audit.json
```

Original pcap names in `/tmp/data-mover-hash-validation-evidence.puqx1E/` are
symlinks to that directory. The saved pointers and run logs remain in the original
evidence directory. Raw captures, pointers and reports are not repository files.
The main worker reported zero kernel capture drops for all three captures; this
value is explicitly supplied to the auditor, not inferred from a classic pcap.
The full audits found zero truncated frames, unresolved READ filehandles, failed
source READ replies, or unanswered source READs.

## Exact source selection

All final audits explicitly selected this opaque **MDS source filehandle**:

```text
0101000000000000000000006e00000048b9bb0a4a0a85800000000000000000
```

In `cancel-resume.pcap`, source OPEN is frame **47**, followed by the matching
GETFH reply in frame **48**. The OPEN names `payload.bin`; its PUTFH parent is:

```text
0101000000000000000000006b0000002f26b40a4a0a85800000000000000000
```

The source directory identity is correlated with the main run's source endpoint
and this parent/OPEN/GETFH exchange. The capture begins after some pathname
resolution: it does **not** independently reconstruct the complete absolute
source directory path. Final selection is by the exact FH above, not by basename
`payload.bin`, which could also name a destination file. The helper refuses
basename-only selection.

The auditor processes each COMPOUND operation in order, resetting CURRENT_FH at
the next COMPOUND and implementing PUTFH/SAVEFH/RESTOREFH explicitly. It associates
READ offset/count with CURRENT_FH at that READ. GETFH replies are paired with
their own request operation. It never zips a packet's flattened filehandle,
opcode and offset lists: unrelated metadata and destination filehandles can
coexist in one COMPOUND. Sparse files-layout DS aliases learned for the selected
source are included, but no selected source READ used those aliases here.

## Recovery and byte coverage

Ranges below are half-open logical byte ranges. Saved D comes from the pointer
decoded by the main worker: cancellation generation **1024**, SIGKILL generation
**2048**; each saved H encoding was **106 bytes**.

| Run | Saved D | Unique source READ operations | Requested and returned bytes | Source READ union | READ intersection with D |
| --- | --- | ---: | ---: | --- | --- |
| Cancel/resume | `[0, 67,108,864)` | 5,121 | 335,544,653 | `[67,108,864, 402,653,517)` | Empty |
| SIGKILL/resume | `[0, 134,217,728)` | 8,192 | 268,435,789 | `[134,217,728, 402,653,517)` | Empty |
| Fresh unordered | Empty | 12,287 | 402,653,517 | `[0, 402,653,517)` | Empty |

Cancellation resumed with 65,536-byte source reads. SIGKILL recovery changed the
source read size from 65,536 to **32,771**, with **49,157**-byte destination
geometry. The fresh unordered run used 32,771/49,157 and no source QoS. Returned
byte unions equal requested unions, and D plus each source READ union exactly
covers the fixed file size. All final JSONs report `no_overlap_and_complete=true`.

The cancellation capture contains **11,284 total READ operations**, whereas only
**5,121** are source payload READs. Pointer reads and pinned destination readback
must not be added to the source totals. Retransmit deduplication uses TCP stream
plus RPC XID, with operation indices preserving multiple READs within a compound;
conflicting XID reuse fails closed. These three captures contained zero duplicate
audited RPC calls or READ replies.

## Completion order

The metric sorts successful source READs by reply-completion frame and counts
adjacent decreases in their request-order rank, ignoring same-reply-frame ties.
This is evidence of **wire completion order**, not instrumentation of application
callback delivery.

| Run | Adjacent request-rank inversions in reply order |
| --- | ---: |
| Cancel/resume | 0 |
| SIGKILL/resume | 67 |
| Fresh unordered | 66 |

Concrete unordered example: request frame **2926**, offset **8,356,605**, completes
in reply frame **2937**, before request frame **2925**, offset **8,323,834**,
completes in frame **2940**. Thus actual ordinary-transfer source READ completion
was not always in request/offset order. The main run log reports 3,835 ms for the
unshaped transfer; no throughput comparison or performance gate is inferred.

## pNFS coverage gap and bounded explanation

**Every selected source payload READ in all three production captures went to
MDS `192.168.9.101` with the MDS FH. These runs do not prove production source DS
bulk reads, multiple source DS endpoints, or multi-DS striping.** Destination
verification does use DS aliases; that traffic is not evidence for source DS I/O.
The earlier standalone registry-0.8.6 DS probe is separate evidence and must not
be represented as this production transfer path's coverage.

The setup exchange provides a concrete explanation consistent with the fixed
dependency, rather than an unexplained absence of DS traffic:

- Source LAYOUTGET succeeds and returns a sparse files layout and a DS alias.
- Its device ID is `010900004a0a85800000000001000000`.
- Source GETDEVICEINFO succeeds but advertises **`192.168.9.101.8.1`**, i.e. the
  MDS itself at TCP port **2049**. This appears in cancellation reply frame **58**,
  SIGKILL reply frame **46**, and unordered reply frames **64/67**.
- For comparison, destination GETDEVICEINFO advertises `.103` in cancellation
  frame **76**, and `.102` in SIGKILL frame **83** / unordered frames **90/91**.

The locally inspected `nfs-rs 0.8.6` source calls `pnfs_read` before falling back
to MDS reads (`src/nfs41/read.rs:28`). `pnfs_read` returns `None` for a degenerate
device (`src/nfs41/pnfs_io.rs:679` onward); `is_degenerate_device` checks whether
every preferred DS address equals the MDS (`src/nfs41/layout.rs:698`). The captured
source device address satisfies that condition. This is the supported explanation
for the observed fallback; it is not evidence of an engine bypass, an unavailable
DS, or a failed LAYOUTGET. No dependency or production code was changed during
this audit. A nondegenerate source placement remains necessary to close the DS
coverage gap.

## Reproduction and helper checks

```sh
python3 tests/lab/nfs_read_audit.py \
  /work/data-mover-hash-validation-pcaps-20261023-puqx1E/cancel-resume.pcap \
  --source-fh 0101000000000000000000006e00000048b9bb0a4a0a85800000000000000000 \
  --mds 192.168.9.101 --saved-prefix 67108864 --size 402653517 \
  --logical-offsets --capture-drops 0 --assert-no-overlap
```

Use `kill-resume.pcap` with prefix `134217728`, or `unordered.pcap` with prefix
`0`. The final reports name the `/work` paths. Saved D can alternatively be
supplied as a bounded JSON range list; the helper does not authenticate or decode
the binary pointer itself. Dense layouts and non-MDS reads without a captured
sparse identity mapping are refused for positive completeness assertions.

Focused validation after the final Python files were added:

```text
python3 -m unittest discover -s tests/lab -p test_nfs_read_audit.py -v
12 tests passed
ruff check tests/lab/nfs_read_audit.py tests/lab/nfs_read_audit_core.py tests/lab/test_nfs_read_audit.py
All checks passed
```

Fixtures cover exact current-FH transitions, request/reply association, multiple
RPCs/READs, retransmits and conflicting XIDs, source/destination exclusion, sparse
DS alias mapping, refusal of dense/unmapped layouts, incomplete replies, capture
loss, ambiguous basename selection, and wire completion inversions. Parsing uses
operation-scoped tshark PDML plus strictly partitioned compact reply fields,
streams packets, and caps retained RPC calls. The primary protocol and dissector
references are linked in the helper source. Source-version continuity and real
backend persistence guarantees remain assumptions outside a wire READ audit.
