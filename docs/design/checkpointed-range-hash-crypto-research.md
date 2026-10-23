# Checkpointed range hashing: standard BLAKE3 and recoverable coverage

Research/design note, 2026-10-22. Repository baseline inspected: `6aa1633e749c0f5255c82d628f966102bcc39e81`. This is a proposed cryptographic and recovery design, not a production implementation or a backend durability certification. Backend capability surveying and real pNFS testing belong to the main investigation and are deliberately outside this note.

## 1. Recommendation

Use the standard BLAKE3 tree, with absolute logical offsets, and persist an application-defined representation of **durable coverage plus canonical subtree chaining values (CVs) plus durable bytes from incomplete chunks** at the destination. Use the public `blake3::hazmat` APIs already available in the lockfile's 1.8.5. Where a backend can recover/adopt payload ahead of the last completed checkpoint, **publish batched hash intents before issuing those adoptable payload writes**, then intersect independently proven backend durability with recoverable hash coverage (§6.4). Resume the missing ranges of the same immutable source version, reconstruct the standard source digest, and compare it with a fresh standard full-file destination digest. Keep this state only while recovering or completing the transfer; delete it after durable publication and required verification.

The ordinary data-first completed-checkpoint protocol meets the no-source-reread property for every byte represented by its last published checkpoint, subject to source-version continuity and destination recovery guarantees. It does not by itself cover backend-recovered payload ahead of that checkpoint. The proposed intent protocol closes this publication gap for **discoverable backend-adoptable bytes with adequate anchored hash material**. Hardware-durable but undiscoverable bytes, or bytes outside that recoverability contract, are not automatically completed work. The experiments validate tree/codec behavior, not this new intent protocol or backend crash durability.

Do not substitute a custom Merkle root, concatenate independently finalized range hashes, or serialize private `Hasher` memory. These do not provide the required standard full-file BLAKE3 result. The construction below follows the official [tree specification, §§2.1, 2.4–2.6](https://github.com/BLAKE3-team/BLAKE3-specs/blob/master/blake3.pdf), [public subtree API](https://docs.rs/blake3/1.8.5/blake3/hazmat/trait.HasherExt.html), and [versioned implementation](https://github.com/BLAKE3-team/BLAKE3/blob/1.8.5/src/hazmat.rs).

## 2. Repository and crate facts

The manifest requests `blake3 = "1.8.4"`, while [Cargo.lock](../../Cargo.lock#L807) resolves **1.8.5**. No dependency change is needed for the proposed subtree APIs. This note evaluated 1.8.5 specifically, rather than assuming whatever version a future dependency resolution selects.

The current [ordered producer](../../src/transfer/engine/producer.rs#L96) creates a fresh sequential hasher and selects source read offset zero whenever content hashing is enabled. It hashes the prefix again, but sends only bytes after `write_start` to the writer. [Payload orchestration](../../src/transfer/engine/payload.rs#L26) supplies that flag and uses a scalar recovered prefix. Thus source replay for hashing is a concrete current behavior, not a theoretical concern.

The existing [recovery binding](../../src/transfer/identity.rs#L107) already includes transfer identity, source path, entry identity, optional size/content version, destination identity and final path with length-prefixed fields. Reuse this concept and strengthen its version semantics as described below. An object identity is not automatically an immutable content version; [SourceIdentity](../../src/model/observation.rs#L99) describes backend identity bytes, not a universal snapshot guarantee.

The crate has no documented stable export/import format for `Hasher` and no public way to construct an ordinary `Hasher` from an arbitrary CV stack. `Clone` supports in-process duplication. The `serde` feature serializes **`Hash`**, not `Hasher`. `Hasher` contains private chunk state, a CV stack and implementation details; inspect the [1.8.5 source](https://github.com/BLAKE3-team/BLAKE3/blob/1.8.5/src/lib.rs#L1065) and [feature documentation](https://docs.rs/blake3/1.8.5/blake3/#cargo-features). Raw memory dumps, `transmute`, layout guesses and private-field mirrors are unsuitable persistence formats, even with the crate version pinned.

Persist mathematical CV bytes and explicit coordinates in our own versioned format. Recovery continues with our own tree reducer through public APIs; it does not restore a private crate hasher.

## 3. Canonical coordinates and valid nodes

Fix the source length `L` and immutable source version for the attempt. All ranges are logical half-open byte ranges `[a,b)` with checked bounds `0 <= a <= b <= L`. Backend request, response, write and persistence boundaries have no cryptographic significance.

Let `B = 1024`, and for `L > 0` compute `N = L / B + (L % B != 0)` without overflowing `L + B - 1`. Leaf `i` covers `[i*B, min((i+1)*B,L))`. A transport completion must carry the logical file offset and actual returned length, independent of arrival order or physical pNFS data-server coordinates. Reassemble and hash by these logical coordinates; never infer offsets by incrementing an arrival-order cursor.

Maintain these node forms:

| Form | Coordinates and meaning | Persisted payload |
|---|---|---|
| Complete subtree | `start_chunk = s`, `height = h`; `q = 2^h` complete chunks, `s % q == 0` | Non-root CV, exactly 32 bytes |
| Final short leaf | `i = floor(L/B)`, only when `L % B != 0` and `L > B`; actual leaf ends at `L` | Non-root CV plus explicit kind/length |
| Incomplete chunk fragments | Any known-durable subset of one leaf, with exact within-chunk spans | Original source bytes and span positions |
| Whole input of at most one chunk | `0 <= L <= B` | Durable raw fragments; hash normally once complete |

Use explicit record tags so a short leaf, complete subtree, final digest and raw fragment cannot be confused. Include `L`, mode and schema in the enclosing state. CV bytes are the public `[u8;32]` representation; store them verbatim, with no host-word reinterpretation. Integers need a specified encoding, such as unsigned little-endian values, and checked arithmetic on decode.

For a leaf or permitted bulk subtree, use:

```rust
use blake3::hazmat::HasherExt;
let mut h = blake3::Hasher::new();
h.set_input_offset(logical_start); // multiple of 1024, before update
h.update(bytes_in_logical_order);
let cv = h.finalize_non_root();
```

The chunk counter depends on the absolute offset. Hashing a later leaf at zero computes a different value. A nonzero input offset must pair with `finalize_non_root`, never ordinary `finalize`. Empty non-root subtrees are invalid. These conditions are specified and checked in [HasherExt's implementation](https://github.com/BLAKE3-team/BLAKE3/blob/1.8.5/src/hazmat.rs#L229).

### 3.1 Legal intermediate merges

For full subtrees `(s,h)` and `(t,h)`, merge only when `t = s + 2^h` and `s % 2^(h+1) == 0`, with left and right in that order. Both ranges must be wholly durable before irreversible checkpoint compaction. The result is `(s,h+1)` with

```rust
merge_subtrees_non_root(&left_cv, &right_cv, Mode::Hash)
```

These are buddy siblings, not merely adjacent equal-sized ranges. For example, leaves 0 and 1 may merge; leaves 1 and 2 may not. A general `[a,b)` transport range must be split at leaf boundaries and then into aligned dyadic subtrees. A bulk subtree starting at nonzero chunk `s` cannot exceed `2^trailing_zeros(s)` chunks, and a non-full subtree can occur only on the actual right edge of the whole input. Length below that upper bound alone is not permission to finalize an interior partial subtree. See [max_subtree_len](https://github.com/BLAKE3-team/BLAKE3/blob/1.8.5/src/hazmat.rs#L253) and [left_subtree_len](https://github.com/BLAKE3-team/BLAKE3/blob/1.8.5/src/hazmat.rs#L344).

For a simple auditable implementation, compact only **full** dyadic nodes during transfer and keep the final short leaf separate. Unequal merges are deferred to the final EOF reduction below. This avoids making intermediate nodes whose validity depends on an uncertain EOF.

### 3.2 Preserve enough information for the root

Root output is a compression with `ROOT` set, not a transformation of an already-compressed non-root CV. For a multichunk file, preserve the two final child CVs, or a frontier from which those children can be built. Never replace the entire file by its single non-root CV and discard its children. In particular, when `L` is an exact power-of-two number of full chunks, **stop compaction one merge before the whole-file node**.

This distinction follows directly from the [reference `Output` representation](https://github.com/BLAKE3-team/BLAKE3/blob/1.8.5/reference_impl/reference_impl.rs#L114): the final compression inputs are retained before choosing non-root CV versus root output. The optimized crate's [Output implementation](https://github.com/BLAKE3-team/BLAKE3/blob/1.8.5/src/lib.rs#L432) has the same distinction.

Once coverage equals `[0,L)`, sort the maximally compacted full dyadic nodes and optional short leaf by offset. Check exact contiguous coverage, no overlaps, the permitted shape, and that at least two nodes remain for `L > B`. Fold the right suffix first:

```text
suffix = last_node.cv
for node in reverse(nodes[1 .. last]):
    suffix = merge_subtrees_non_root(node.cv, suffix, Hash)
digest = merge_subtrees_root(nodes[0].cv, suffix, Hash)
```

The inner slice excludes the first and last nodes. Every suffix merge follows the actual right edge of the specified BLAKE3 tree. As examples, seven full chunks compact to `[0,4), [4,6), [6,7)` in chunk coordinates; merge the latter two non-root, then merge with `[0,4)` as root. Four full chunks must retain `[0,2), [2,4)` and merge them as root. Do not fold from the left or merge nodes in completion order.

Alternatively, recursively split the known file tree at `left_subtree_len` and retrieve its canonical children from the coverage index. Both approaches implement the same tree. The [official hazmat example](https://github.com/BLAKE3-team/BLAKE3/blob/1.8.5/src/hazmat.rs#L39) demonstrates a three-chunk case and the distinction between root/non-root merging.

### 3.3 Empty, single-chunk and short final cases

* `L = 0`: ordinary `blake3::hash(b"")`; create no non-root leaf or synthetic empty right sibling. The canonical default digest is `af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262` ([official vectors](https://github.com/BLAKE3-team/BLAKE3/blob/1.8.5/test_vectors/test_vectors.json)).
* `1 <= L <= 1024`, including exactly 1024: assemble the actual input and call ordinary `hash`/`finalize`. A persisted non-root leaf CV alone cannot produce this root. Retaining up to 1024 raw bytes makes this case simple. A separately tagged final root digest could be cached after complete durability, but it cannot resume a partially assembled input.
* `L > 1024`, non-multiple of 1024: only the actual final leaf may be short. Hash its true bytes at its true chunk offset. The crate handles the last compression block's length and padding. Never pad file data to 1024 bytes.
* An exact multiple of 1024 has no extra empty leaf. An exact multiple of 64 has no extra empty compression block. A partially durable 1024-byte chunk is not a short final leaf.

The root merge API explicitly excludes single-chunk input; see [merge_subtrees_root source/docs](https://github.com/BLAKE3-team/BLAKE3/blob/1.8.5/src/hazmat.rs#L473).

## 4. Coverage projection at arbitrary persistence barriers

Distinguish received bytes `R`, acknowledged written bytes `W`, and checkpoint-eligible durable bytes `D_g` for generation `g`. Require `D_g` to be a normalized union of byte ranges contained in both the writer's proven durability result and the source-derived hash material available for that generation. A successful write callback alone is not this proof. Across valid generations for one immutable stage, `D_g` is monotone.

The checkpoint representation must satisfy:

```text
union(full-subtree ranges, final-short-leaf range, raw-fragment spans) = D_g
```

The union must be exact and disjoint, apart from explicitly checked duplicate records. This invariant concerns the **completed projection**; pending hash intents from §6.4 are a separate collection and may describe bytes outside `D_g` without marking them complete. The completed-range index may be stored separately for scheduling, but recovery must validate its equality with hash coverage. An independent range bitmap that says more is complete than the hash state can recover violates the no-source-reread guarantee.

### 4.1 Byte-unaligned boundaries

Suppose all bytes `[0,4096)` have been received and hashed, but only `[0,2500)` are durable. The checkpoint needs CVs covering the first two full chunks and raw bytes `[2048,2500)`. The CV of `[2048,3072)` is not a state for its 452-byte prefix, and a CV of `[0,4096)` cannot be cut down to `[0,2500)`.

For each partially durable chunk, persist only the durable spans of the original bytes, with exact within-chunk coordinates. A general sparse barrier may leave several spans in the same chunk. On resume, fetch only the missing source spans, combine them with persisted durable fragments, and compute the leaf when its actual byte domain is complete. Replay within-chunk bytes into a fresh public hasher; this is a small rehash of saved bytes, **not a source reread**. Alternatively, reload the durable boundary bytes from the destination stage if that is reliably supported. That variant avoids storing raw fragments but makes recovery depend on stage reads.

Do not finalize a partial nonfinal chunk and later try to append through its CV. `CHUNK_END` and final block length have already been applied. A persisted raw fragment may be finalized only when it constitutes the true whole single-chunk input or true final leaf, as determined by the bound `L`.

### 4.2 Compaction cannot precede the information it would discard

Projection is not invertible after child data has been discarded. Keep full leaf CVs, raw boundary bytes, or an unpruned tree until the relevant durability boundary is known. Then select nodes wholly within `D_g` and compact only those. Volatile nodes crossing an undecided barrier must retain their children; volatile partial-chunk bytes must remain available until the generation is published or abandoned.

A practical arrangement has an immutable compact durable frontier and a bounded tentative window containing leaves/fragments. Publishing a generation promotes only proven-durable material. A writer that reports its arbitrary barrier after all raw data and descendants have been freed cannot be retrofitted with this projection without reading bytes again. Durability reporting and hash-state lifetime therefore need an explicit handshake.

If a node lies entirely in `D_g`, its descendants can be dropped after publication, because subsequent checkpoints for the same immutable stage never retract those bytes. If recovery can invalidate an interior subset, the compacted CV cannot be split; reconstruct it from destination data, retain descendants in an external temporary index, or reject that state. Silent partial reuse is invalid.

### 4.3 Sparse gaps, duplicates and transport split/merge

An unwritten gap is unknown, even if reading the sparse destination happens to return zeros. A verified source hole denotes actual logical zero bytes and must contribute to standard BLAKE3 at the correct offsets. There is no special standard-BLAKE3 shortcut that treats missing bytes as absent tree leaves.

Positioned arrivals may cross leaf boundaries or cover only fractions of several leaves. Route slices by absolute offset, track completeness, and compute each leaf from bytes in logical order. Reject out-of-bounds, overflow and unexplained short reads. Do not count overlapping/retried responses twice. Exact duplicate bytes can be coalesced; conflicting overlaps must fail rather than selecting whichever response arrived last. Prefer disjoint admitted reads, and track completed requests so known retries can be discarded safely once bytes have compacted away.

The no-reread contract concerns logical source requests: their ranges must exclude `D_g`. It requires the source adapter to support requesting the needed missing spans, including unaligned boundary spans. If an adapter requires larger aligned source reads that overlap saved coverage, it cannot advertise this exact contract without an additional mechanism. The transfer cannot promise that a server's invisible cache, stripe or disk implementation never fetches adjacent bytes internally.

Destination writes may split or merge those slices independently. Associate durable acknowledgements with the exact logical spans they cover, regardless of request grouping. No read size, write size, pNFS stripe size or persistence interval is required to equal 1024, or each other. This is a scheduling/coverage contract, not a claim about a particular pNFS implementation.

## 5. Bounded memory and metadata

An ordered completed prefix has a binary-decomposition frontier with `O(log N)` CVs; retaining the final root children changes only a constant. For `k` separated completed intervals, aligned dyadic compaction uses `O(k log N)` nodes as a conservative bound, capped by `O(N)`. It does **not** make unrestricted unordered sparse completion logarithmic. Alternating complete/missing leaves require one CV per completed isolated leaf and many coverage intervals.

At one CV per full chunk, CV payload alone is `32/1024 = 3.125%` of covered bytes. One TiB can therefore require 32 GiB of leaf CV payload before coordinates and indexing. Fragmented partial durability can also require raw bytes approaching the data volume. With `k` normalized durable intervals, only chunks intersecting interval endpoints need raw fragments (at most `2k` such chunks, and at most 1023 bytes per incomplete full chunk). The special whole-file single-chunk case can retain 1024 bytes.

To obtain a real per-transfer bound, limit admitted work to a rolling logical window of `A` bytes beyond the lowest missing position, with bounded read/write queues and a bound on unsettled durability generations. Hash metadata for a worst-case fragmented active window is `O(A/1024)` CVs plus coverage spans, boundary bytes and `O(log N)` compacted prefix state. Transport payload memory is an additional term: bound it in **bytes**, including retained buffers, retries and backend inflight requests, not merely the number of queued responses. Small `Bytes` slices can pin large backing allocations.

Independent limit mechanisms are needed for incomplete-leaf assemblers, tentative CVs, outstanding writer acknowledgements, manifest encoding buffers, metadata cache and persisted sparse index. Apply backpressure before admitting more distant work; do not evict the only recovery representation of a completed durable range. When remote gaps stall the window, bounded memory necessarily limits further out-of-order progress. An alternative is a paged destination-side temporary index with a bounded RAM cache, which bounds memory but not disk metadata or random I/O.

Bulk hashing of aligned power-of-two groups can preserve SIMD performance when all bytes and their pending lifetime are controlled. If a later barrier may split a group, keep sufficient descendants/raw data until that barrier resolves. Benchmark this tradeoff; invoking one hasher per 1 KiB leaf is correct but may give up substantial batching performance. The [official implementation discussion, §§5.1–5.4](https://github.com/BLAKE3-team/BLAKE3-specs/blob/master/blake3.pdf), explains stack compaction and parallel subtree processing.

## 6. Destination checkpoint format and publication ordering

Proposed format; these are design obligations, not established cross-backend guarantees:

```text
root pointer: schema, transfer binding, stage incarnation, owner fence,
              generation, manifest location, manifest length/digest
manifest:     schema, standard-BLAKE3/hash mode, fixed L, source version binding,
              stage identity, generation, normalized durable ranges,
              subtree records, short-leaf records, fragment records/references,
              references to anchored pending hash intents (when adopting ahead),
              optional transient completion/publication phase
```

Use a distinct schema/domain for integrity hashes of checkpoint metadata. Do not prepend a domain string or encode offsets into the **file bytes** being hashed: that would change the required file digest. Include all coordinates, tags, lengths, child references and identity fields in the manifest's integrity envelope. Limits on record counts, total encoded size, integer ranges, sorting and reference depth must be checked before allocations.

### 6.1 Generation-root protocol

1. Under a valid exclusive owner/fence, choose the next generation and freeze its exact candidate coverage. Settle the corresponding writes, length/extent metadata and required data durability barrier. Ensure the stage's name/identity is itself recoverable. Later writes may add uncovered ranges but must not mutate any previously checkpointed bytes or the frozen candidates.
2. Build the hash projection for those ranges. Write new immutable CV/fragment/index blobs and make their contents and discovery metadata durable. Do not overwrite blobs referenced by the current generation.
3. Write an immutable manifest naming exactly these durable blobs and ranges; make the manifest and its discoverability durable.
4. Atomically publish the root pointer to that manifest with the appropriate owner/generation comparison, then make the pointer publication durable. This is the recovery commit point. A compact inline manifest can combine steps 2 and 3, but the root must still never lead its dependencies.
5. Only after publication durability is confirmed, report the checkpoint and release projection buffers. Reclaim unreachable previous-generation state only when no possible recovered pointer or active reader still needs it. A deliberate fallback slot is still a reference and retains its dependencies.

The required dependency order is **stage data → hash blobs → manifest → root pointer → reclamation**. Atomic visibility and durability are separate properties. For local Linux, `fsync(file)` does not guarantee the containing directory entry is durable; [the Linux fsync manual](https://man7.org/linux/man-pages/man2/fsync.2.html) requires a directory sync for that. [rename](https://man7.org/linux/man-pages/man2/rename.2.html) provides atomic name replacement, not a substitute for persistence barriers. Mapping this protocol onto each backend needs the main survey; a successful API return must have the stated semantics.

### 6.2 Crash outcomes and invariants

| Crash point | Permitted recovery |
|---|---|
| Before data durability | Old root only; never trust acknowledged-but-volatile bytes |
| After data durability, before root switch | Old root; new bytes/blobs may be unreachable orphans |
| During root replacement | Entire old or entire new valid pointer, never mixed fields |
| After root durability | New manifest and every referenced byte/blob must be durable |
| During old-generation reclamation | Current root and any retained fallback remain fully readable |

On restart, validate the root, manifest, binding, exact hash/range coverage and stage incarnation before scheduling any skip. Do not choose an unpublished orphan manifest just because its generation number is larger. A two-slot pointer implementation needs checksums and an explicit committed-slot protocol; it does not acquire atomicity from generation numbers alone.

Fallback to an older root is safe only if its stage bytes remain unchanged and its dependencies still exist. A newer failed flush, truncate, partial overwrite or stale writer must not have destroyed older checkpointed data. This is why completed spans are immutable and old writers need real fencing, not just a process-local lock. At byte-unaligned barriers, later disjoint logical writes may still share a physical sector/block with saved bytes: the backend's contract must preserve previously durable spans through such writes and crashes, or the implementation needs a stronger copy-on-write/journaling mechanism. The hash model alone supplies no torn-write protection. If the stage disappeared or the binding is stale, the checkpoint cannot authorize skipping reads.

### 6.3 Completion without permanent result records

Keep recovery metadata until complete source evidence, destination verification, durable final publication and any required post-publication verification have succeeded. Before publication, a transient full-coverage generation may preserve the reconstructed expected source digest and publication phase. It is still recovery state and is deleted at success.

If a crash occurs after final publication but before cleanup, recovery must reconcile the remaining checkpoint with the actual final object/version. If a rename removed the stage name, do not assume its disappearance means success. Verify the bound final object's size and standard digest against the transient source evidence, or safely restart according to the destination contract. Existing [publication/verification code](../../src/transfer/engine/publication.rs#L12) already distinguishes verification before versus after publication; a failed check after publication cannot promise that the final name remained unchanged.

Once publication and required verification are durable, remove the discoverable recovery pointer and persist that removal before reclaiming its manifest/blobs; then reclaim other unreachable temporary generations. Cleanup after a crash can be retried. Do not retain a terminal-success database row or hash sidecar as a permanent result.

With no permanent result record, a crash after cleanup but before the caller receives success leaves no persistent proof of that particular completed invocation. A retry can inspect/hash the final object or repeat work. Exactly-once delivery of a success response across that window is not implied by this requirement.

### 6.4 Closing the backend-adoption gap with pre-payload hash intents

The main backend investigation reports a critical additional recovery model: a recovered HDFS tail or listed S3 upload part can be adoptable beyond the engine's last completed hash checkpoint. Treat those as supplied backend requirements here; this note does not independently certify them. If recovery adopts that extra payload, the data-first protocol in §6.1 can leave it without saved source hash evidence. Merely intersecting with the old checkpoint avoids false completion but can require replaying already backend-durable source bytes. For the stronger no-reread requirement, use this proposed protocol:

1. Read a bounded source batch from the bound version, compute its source-derived hash material, and retain the payload until it can be issued. Create an immutable **hash intent** containing logical ranges, hash material, source/stage binding, generation/intent identity and the intended backend operation/part identity. The payload digest/identity must distinguish retries or replacement parts where necessary.
2. Make the intent blobs and their namespace durable. Publish and durably commit a root/manifest that **references the pending intent** while retaining the previous completed coverage. The referenced status is pending: it must not add bytes to completed ranges or claim that payload exists.
3. Only after the root anchor is durable may those payload writes/parts be issued in a form the backend could later adopt. An orphan intent file without a root anchor is insufficient. Pipeline multiple batches only if each potentially adoptable operation has an already anchored intent and every later root retains all unsettled references.
4. Independently obtain backend durability/adoption evidence bound to this stage/upload/owner and the exact written operation. Let `D_backend` be its byte coverage and `H` the source coverage whose hash representation can actually be recovered from anchored intents and prior completed state. Candidate adopted coverage is **`D_backend ∩ H`**, followed by the projection/representation checks below. Intent without backend evidence remains pending; backend data without anchored recoverable hash material must not be claimed as hashed completion.
5. Publish a completed generation only after the projected adopted state is durable, following §6.1's completed-root ordering. It may replace the corresponding pending intent with compacted completed nodes and exact partial fragments. Reclaim old intent information only after no recoverable root/fallback or unsettled operation still needs it.

There are two different publications: **intent/root anchor → adoptable payload**, then **proven durable payload → completed hash projection/root**. A generation can include completed old ranges and pending new intents simultaneously. One coordinator/fence must serialize root changes or use a backend-supported comparison; concurrent publication must not accidentally drop another live intent. On lost acknowledgements, recover the committed anchor/operation identity rather than assuming either payload absence or success.

Crash before the anchor commits must imply the corresponding adoptable payload was never issued. Crash after anchoring but before payload leaves only harmless pending intent. Crash after payload durability but before completion publication permits recovery to query backend evidence and reconstruct the intersection. Pending hash material may describe nondurable bytes; **that is permitted, provided it is never mislabeled as completed or used to skip a missing source span**.

#### Projection must remain possible, not just set-theoretically nonempty

`D_backend ∩ H` is not sufficient if `H` was represented only by an unsplittable aggregate CV. A recovered cut can land inside an intended subtree or even inside a 1024-byte leaf. Pending intents must preserve enough information for every promised adoption boundary:

* For whole-part adoption with known operation boundaries, partition each part into full canonical subtrees plus original bytes for its leading/trailing partial leaves. The union of selected whole-part intents can then assemble cross-part leaves without source replay. A finalized arbitrary-part digest is not this material.
* For an unpredictable recovered contiguous tail, retaining leaf CVs handles every **full-leaf** cut, but not a byte cut inside a leaf. To adopt that tail exactly, either read the durable boundary bytes from the destination stage, or persist their original source bytes in advance. Since any leaf might become the recovered boundary, saving raw bytes for only the intended batch endpoint is insufficient. With neither a constrained boundary nor destination reads, a generic intent may need the full batch's raw bytes (or another explicitly designed equally recoverable representation).
* Aggregate dyadic intent nodes can be used if their descendants remain available, or if destination readback is permitted to reconstruct a cut-straddling node. A compact CV cannot be subdivided merely by learning the new durable length. Dropping a partial adopted tail and rereading it from source weakens the exact no-reread contract; call that out instead of hiding it behind an intersection.

This makes the choice explicit: more persisted hash/raw material before writes, or bounded destination rehashing of cut-straddling regions, or a backend contract restricting adoption cuts. The 5 MiB + 17 experiment demonstrates serialization **after that cut is known**; it does not prove that endpoint-only intent data could predict arbitrary recovered cuts.

#### Bitmap and partial-tail requirements

A coarse completion bitmap may set a cell only when the cell's entire logical byte domain is durable **and** recoverably hashed. Keep exact partial-cell ranges/tail length separately, along with partial-leaf bytes or a reliable destination reconstruction path. For a contiguous recovered prefix, use whole completed cells plus an exact byte tail; for unordered islands, keep exact fragments per affected cell. `5 MiB + 17` cannot be represented by rounding the cut up to a 64 KiB/96 KiB/part cell or down and then claiming no source replay. The partial final file cell is interpreted against the fixed `L`, not a padded length. A bitmap alone never supplies the partial-chunk hash state.

#### Cost and memory consequences

Batched intents add a metadata durability/anchor step **before** payload can start, on top of the completed-checkpoint step. Larger batches amortize root updates and metadata requests but increase startup latency, retained/spooled payload bytes, pending CV/fragment data and recovery projection work. Smaller batches reduce those windows but increase round trips and write amplification. Compute and anchor the next batch while the previous batch transfers where the backend contract permits; bound total unissued-plus-issued-but-unsettled batches in bytes.

Leaf-CV intents cost about 3.125% of intended full-chunk payload before coordinates/encoding and may later be compacted. Pre-persisting raw batches to cover every arbitrary byte cut can approach an additional temporary copy of the payload plus its write bandwidth. Using compact nodes and destination reconstruction reduces metadata at the expense of destination reads; fixed known part boundaries need raw bytes only for boundary chunks. Persisted intents do not remove the need to retain payload buffers or spool them before issue: discarding those bytes and rereading source to send the payload would itself introduce another source pass.

This intent protocol is a design recommendation prompted by the main backend findings. It has **not** been implemented or crash-tested in these isolated experiments. Required fault cases now include every intent-blob/root-anchor boundary, root acknowledgement loss, payload acceptance with response loss, stale/replaced part identities, arbitrary recovered byte tails, incomplete bitmap cells and reclamation while an intent remains reachable.

### 6.5 Alternative selected by the integrated design: persistent H independent of D

The [integrated design](checkpointed-range-hash-recovery.md#6-hash-与完成范围分别记录) selects a useful alternative to this note's D-only projection: retain source hash coverage **H** independently of backend recovery coverage **D**. H may contain hash-only intent. Its source-version-bound CVs remain valid even if their ranges span a gap or a byte-unaligned end in D. Resume reads payload only for missing D and feeds the hash tree only for missing H. Thus payload re-read inside H does not double-count hashing, and H itself never establishes D.

In this representation a recovered D boundary inside a compact H subtree does **not** require splitting that CV or projecting it to D. Retain the whole H node. The inability to split discussed in §6.4 remains true, but its projection requirement is specific to converting intent into a D-only hash representation. Keeping H avoids that conversion and can substantially reduce HDFS-tail metadata. Partial leaves are still required wherever H itself is incomplete, as are canonical coordinates, root children, an unchanged source binding and independent final destination verification.

The write-ahead anchor must ensure every independently adoptable range has sufficient source hash coverage, ideally D contained in H. Merely taking an intersection cannot manufacture source evidence for D outside H. In the ordinary completed checkpoint contract only the proven intersection authorizes skip; in the stronger backend-tail/part guarantee, missing H is a protocol failure or needs a separately justified recovery procedure. Do not substitute destination hash evidence for a missing trusted source root.

This alternative was reviewed mathematically during integration. The experiments below exercise the D-only serialized representation, not a production implementation, fault test or additional experiment of independent H/D persistence. Both representations produce the same standard file root when complete, but their recovery metadata invariants must not be conflated.

## 7. Source binding and consistency limits

Bind the checkpoint to source endpoint/path, immutable content version or snapshot identity, fixed `L`, destination endpoint/path, stage incarnation, transfer identity, algorithm/schema and owner fencing epoch. Source equality checks must be made against the version actually read, not merely against a pathname observed before the operation. A label override is not source identity; the repository's existing binding correctly includes the underlying fields separately.

Every newly read range after resume must come from the same version as the saved CVs/fragments. If the version differs, discard/restart that recovery attempt; do not combine old CVs with newly read current bytes. Resuming without rereading durable source ranges is compatible with a retained immutable snapshot, named immutable version, or another source contract that truly guarantees one fixed byte sequence for the whole attempt.

File handles, inode numbers, size/mtime tuples and pre/post metadata checks alone do not prove immutability. A writer can alter bytes between reads, restore metadata where permitted, or produce a mixture that was never a single source state. This is an information limit: the saved digest contains observations of old bytes, so no calculation on those observations can prove that unobserved later mutations did not occur. Strong consistency needs a real immutable version, a suitable enforced lock/lease contract or an externally supplied trusted whole-file digest. If only mutation detection is available, document that weaker guarantee and fail detected changes; do not advertise snapshot semantics.

Even a source and destination digest match establishes equality to the bytes observed and bound by the transfer. Without an immutable source, that equality does not prove a coherent point-in-time copy. A version/snapshot that expires before recovery also limits no-reread resume: report the inability to rebind or restart, rather than silently treating the new source as the old one.

## 8. Final comparison and security boundaries

Compute the source root from the saved **source-derived** CVs/fragments and newly read missing source bytes. Independently read the complete destination logical file and compute ordinary standard BLAKE3; require both length equality and equality of all 32 digest bytes. This rereads the destination, not completed source ranges. The existing [integrity reader](../../src/integrity/mod.rs#L260) illustrates the ordinary sequential full-file hash, and [verification orchestration](../../src/transfer/engine/publication.rs#L101) checks destination size and digest.

Hashing bytes handed to the writer twice is not independent stored-data verification. A checkpoint CV proves neither that the write persisted nor that the stage later remained intact. If resumable destination-readback verification is later desired, it needs separately source-independent readback state, its own generation and object-version binding, and protection against mutation after verification. Do not infer that facility from this source-hash checkpoint design.

Metadata checksums detect accidental corruption, not a malicious party able to replace metadata and its checksum. Such a party could replace source CVs and destination bytes together. Use destination access controls and authenticated transport; if the threat model includes an untrusted checkpoint store, authenticate the metadata with a key/trusted digest kept outside that store. A MAC is a separate checkpoint-envelope concern; the file digest remains unkeyed standard BLAKE3. Persisted raw fragments carry actual file contents and need the same confidentiality protections as the stage.

Validate shape, offsets and lengths before calling hazmat: merge APIs accept CVs without coordinates and cannot reject an invalid tree. Never rely on panics as the input-validation boundary. Do not mix keyed/derive-key CVs with `Mode::Hash`, root digests with CVs, or CVs from different versions. A valid authenticated old generation can still be replayed unless the threat model provides trusted anti-rollback state; an incrementing number stored beside the data is not such a mechanism. These cautions follow the [official hazmat warnings](https://github.com/BLAKE3-team/BLAKE3/blob/1.8.5/src/hazmat.rs#L1) and the proposed recovery trust boundary.

## 9. Standard BLAKE3 versus a custom Merkle root

| Choice | Arbitrary positioned transfer | Standard `blake3::hash(file)` result | Recovery cost and risk |
|---|---|---|---|
| Canonical BLAKE3 CV tree | Yes; byte fragments assembled independently of transport blocks | Yes, with correct counters, shape and root handling | Need coverage projection, fragment retention and strict node validation |
| Independently finalized hashes for arbitrary backend blocks | Can retain per-block checks | No general composition into the standard file digest | Finalized hashes discard the inputs needed for standard internal/root compression; different block splits define different records |
| Custom domain-separated Merkle tree | Yes, if leaf coordinates/lengths and tree shape are specified | No; produces a distinct protocol digest | Must define and audit leaf/parent/root/empty encodings, source binding and equivalent crash protocol; standard digest still requires another source pass or canonical BLAKE3 state |

Even `hash(hash(A) || hash(B))` for two aligned blocks is not BLAKE3 of `A || B`. Ordinary `hash(B)` uses a new file's counter and root domain. The correct parent operation consumes **non-root CVs** from the canonical absolute positions and sets the parent/root domains appropriately. A concatenation operation on finalized digests is not an associative substitute.

A custom Merkle format can be useful if the product explicitly wants a separately named block-verification protocol. It offers no simplification for this request: exact durable ranges, byte-unaligned barriers, source snapshots, crash publication and metadata lifetime remain necessary, while the standard final digest is lost. Recommend canonical standard BLAKE3 with narrowly encapsulated hazmat use and differential tests.

## 10. Isolated experiments actually run

### 10.1 Sparse durable-mask simulation

Created a throwaway Rust project at `/tmp/checkpointed-blake3-research.sjss49/` with exactly `blake3 = "=1.8.5"`. Downloaded the official tagged reference implementation and vectors from the URLs below. No repository dependencies or production files were changed. Ran the following with a target directory inside that temporary project:

```sh
CARGO_TARGET_DIR=/tmp/checkpointed-blake3-research.sjss49/target \
  cargo run --offline --release \
  --manifest-path /tmp/checkpointed-blake3-research.sjss49/Cargo.toml \
  --bin experiment
```

Observed output:

```text
PASS: 16440 sparse recovery simulations; 35 official hash vectors; reference implementation agrees; 13211479 missing-byte source reads; maximum final frontier 9 nodes; independently finalized digests and wrong offset rejected by differential comparison
```

The simulation used every length 0–4097, plus lengths immediately below/at/above 8 KiB, 16 KiB, 64 KiB and 256 KiB, each with four deterministic seeds. It split reads into variable spans of up to 1709 bytes, shuffled arrival order, checked duplicate responses, selected sparse byte-unaligned durable coverage, retained durable fragment bytes and complete-node CVs, compacted only eligible nodes while preserving the root children, round-tripped CV coordinates/payloads through a simple application encoding, and fetched missing bytes. It asserted that the reconstructed coverage equalled the durable mask and that no simulated resumed source read touched a durable byte. It compared every result with both `blake3::hash` and the official reference implementation. Separately, all 35 official default-hash vectors passed both ordinary and reconstructed-tree hashing; two negative constructions confirmed that independently finalized hashes and an incorrect absolute counter disagree with the required digest.

The 13,211,479 reads in that output are instrumented **byte accesses in the simulation**, not backend requests. The nine-node maximum is for the **final** compacted frontiers in those inputs, not a bound on arbitrary sparse state. The simulation assembled the source before choosing its durability mask; it does not implement a streaming backpressure system. It does not test actual process crashes, storage barriers, snapshot semantics, pNFS, malicious metadata, performance, portable wire decoding, multi-owner fencing or a production recovery serializer. Those remain implementation/test obligations. Temporary artifacts are not a committed test harness and may be removed by normal temporary-file cleanup.

Artifact SHA-256 identifiers:

```text
main.rs             72118cafa726a5db18c1e0cc78179ab767b08c03a2088662f7f9d7e51ae02133
reference_impl.rs   6e89a18be72e3c4d838644e1796e04d896cb4a16bd7bb803d4380d7a363fbfd2
test_vectors.json   dcb91ea8accc77e6d6e632af7cdc1a99a9f3ae78cf648da595c7d064db32f624
blake3.pdf          ce179e62f29a6e43ec1ac2fe62b5e063a18632cf936c430d1373bfa0b81fb349
```

### 10.2 Exact 5 MiB + 17 checkpoint and fresh-process recovery

After the main investigation requested larger backend boundaries, added a second isolated binary, `barrier.rs`, in the same temporary project. It does not change production code or the original simulation. It uses read chunks of **32,771**, **65,536** and **98,304 (96 KiB)** bytes, delivered in reverse request order with explicit offsets. An independent write pass uses a rotating sequence of 98,304, 32,771 and 65,536 bytes. Its large input is `7 MiB + 333 = 7,340,365` bytes, with deterministic byte `i = (13*i + floor(i/251)) mod 251`.

The checkpoint cut is exactly **`5 MiB + 17 = 5,242,897`**. The published mathematical coverage is represented by two full-subtree CVs, covering `[0,4 MiB)` and `[4 MiB,5 MiB)`, plus the original **17 bytes** at `[5 MiB,5 MiB+17)`. That final fragment is part of a nonfinal 1024-byte leaf, not a finalized short leaf. The process writes this state to a temporary file and exits. A separate invocation reads only that file and resumes from the cut; its source accessor asserts every request starts at or above the cut.

The experiment's application-defined **version 1** encoding is byte-specified:

```text
8 bytes       magic ASCII DMRHCP01
u16 little    schema version = 1
u8            mode = 0 (unkeyed standard BLAKE3)
u8            reserved = 0
4 x u64 LE    file length, durable cut, generation, backend read size
u32 LE        completed-range count
repeated      (u64 LE start, u64 LE end)
u32 LE        node count
repeated      (u64 LE start_chunk, u64 LE chunk_count, 32 raw CV bytes)
u32 LE        fragment count
repeated      (u64 LE absolute start, u32 LE byte length, raw fragment bytes)
32 bytes      BLAKE3 of every preceding encoded byte
```

This format has no `Hasher` bytes, Rust layout, pointer, padding, native-endian integer or platform-specific state. The decoder uses bounded counts and lengths, checked coordinate arithmetic, known version/mode checks, payload integrity checking and exact equality between prefix coverage and node/fragment spans. Unsupported versions are rejected even when the checksum is recomputed. Corruption and truncation are rejected as well. The codec round-trip is canonical for its generated records. It is an **experimental prefix-only codec**, not the production sparse format proposed in §6: it omits source/stage binding, owner fencing, transactional publication and authentication, and its unkeyed BLAKE3 checksum provides only accidental-corruption detection. Its short final leaf is identified from the fixed file length and singleton coordinates; production should retain the explicit record tags recommended in §3.

Actual commands, in two separate processes:

```sh
CARGO_TARGET_DIR=/tmp/checkpointed-blake3-research.sjss49/target \
  cargo run --offline --release \
  --manifest-path /tmp/checkpointed-blake3-research.sjss49/Cargo.toml \
  --bin barrier -- prepare
CARGO_TARGET_DIR=/tmp/checkpointed-blake3-research.sjss49/target \
  cargo run --offline --release \
  --manifest-path /tmp/checkpointed-blake3-research.sjss49/Cargo.toml \
  --bin barrier -- resume
```

Observed large-input results:

| Backend read size | Checkpoint bytes | Durable partial-leaf bytes | Resumed source requests | Resumed source bytes | Requests overlapping saved prefix |
|---|---|---|---|---|---|
| 32,771 | 229 | 17 | 65 | 2,097,468 | 0 |
| 65,536 | 229 | 17 | 33 | 2,097,468 | 0 |
| 98,304 | 229 | 17 | 22 | 2,097,468 | 0 |

Each produced standard digest `dc76b6d57b74254ba7142e9a935d4bf7b1a67128d77d31b68b30005e6428acb4`, matching ordinary crate hashing and the official reference implementation. The oracle regenerates the deterministic full input independently; those oracle accesses are outside the instrumented resumed source path and are not hidden transfer prefix reads.

For each backend size, the same persisted codec also exercised `(length, cut)` pairs `(0,0)`, `(1,0)`, `(1,1)`, `(63,17)`, `(64,63)`, `(65,64)`, `(1023,512)`, `(1024,1000)`, `(1024,1024)`, `(1025,1024)`, `(1025,1025)`, `(2048,2048)` and `(4096,4096)`. These cover partial single-chunk state, exactly one complete chunk, a short final leaf, and full power-of-two roots whose children must survive serialization. Including the three large cases, observed summaries were:

```text
PREPARE PASS: 42 version-1 checkpoints persisted; positioned reverse completions; independently split writes; encode/decode roundtrip; corruption/truncation/version rejection
RESUME PASS: 42 fresh-process versioned checkpoint recoveries; zero prefix source reads; raw partial leaves and root special cases match crate and reference
```

`barrier.rs` SHA-256: `79aa92e4da45114c7a78ee6e32ab2c6583aa3daed42b234b8b4477a024c4f7d3`. The first 229-byte checkpoint (`barrier-0.bin`) SHA-256: `03c29a6ee64307eba3fa5b3ac63cdc1433360b7841ac5690c630c14b18546255`.

This is actual serialization to files and recovery in a fresh process. It is **not** a kill-at-barrier crash test or proof of persistence: the temporary files are written normally, and the durability set is supplied by the simulation. Both experiments retain large initial input buffers and therefore validate boundary-independent correctness, not the bounded streaming implementation described in §5. Backend protocol/durability and real pNFS measurements remain the main investigation's work.

## 11. Required implementation tests

1. All [official vectors](https://github.com/BLAKE3-team/BLAKE3/blob/1.8.5/test_vectors/test_vectors.json), plus random inputs against both ordinary hashing and the [reference implementation](https://github.com/BLAKE3-team/BLAKE3/blob/1.8.5/reference_impl/reference_impl.rs). Include every length around 0, 64, 1024, 2048, all relevant powers of two and SIMD batch boundaries. Explicitly exercise empty, exactly-one-full-chunk and short-final-leaf roots.
2. Independent read/write/barrier partitions; arbitrary offsets, shuffles, duplicate responses, conflicting overlaps, short reads, 64-bit offset arithmetic and sparse gaps. Check invariant equality between completed ranges and recovered hash coverage after every generation.
3. Exhaustive durability masks on small inputs and randomized larger masks; byte-unaligned prefix and island barriers, multiple fragments per chunk, final short leaf durable before preceding leaves, and resume reads instrumented to reject every checkpointed durable byte.
4. Reject malformed coordinates, misaligned buddy merges, invalid right-edge subtrees, ordinary final hashes used as CVs, wrong counters/modes/source versions, whole-file CV with missing root children, duplicates with conflicting payloads, integer overflow, huge declared allocation sizes and missing/corrupt referenced blobs.
5. Inject crash/failure at every stage-data, blob, manifest, root-switch, root-sync, old-state reclamation, final-publication and cleanup boundary. Recover through a fresh process with no RAM state. Prove old-or-new coverage and forbid a published root from referencing nondurable data. Use the real backend's crash model; the pure Rust experiment cannot certify this.
6. Source replacement/mutation, snapshot expiry, stale owner resuming writes, stage replacement/truncation, concurrent generations and older-root fallback. Verify that no checkpoint skip survives a failed binding or loss of stage durability.
7. Destination corruption detected by independent full readback; before-/after-publication failure semantics; a crash after publication before cleanup; cleanup failure; a lost success response after all transient records are gone.
8. Long gaps and adversarially sparse completions under configured memory budgets. Account for queue payloads, backing allocations, partial-chunk maps, tentative descendants, checkpoint encoding and metadata cache. Verify backpressure rather than just measuring the compacted final root.

## 12. Primary sources consulted

* [BLAKE3 specification](https://github.com/BLAKE3-team/BLAKE3-specs/blob/master/blake3.pdf), downloaded PDF version `20211102173700`; §§2 and 5 for construction and implementation, §6.4 for the distinction between interior CV evidence and independently trusted roots. The downloaded file hash is recorded above because `master` is mutable.
* [Official Rust crate 1.8.5 docs](https://docs.rs/blake3/1.8.5/blake3/) and [HasherExt docs](https://docs.rs/blake3/1.8.5/blake3/hazmat/trait.HasherExt.html).
* [Official 1.8.5 hazmat source](https://github.com/BLAKE3-team/BLAKE3/blob/1.8.5/src/hazmat.rs), [crate source](https://github.com/BLAKE3-team/BLAKE3/blob/1.8.5/src/lib.rs), [reference implementation](https://github.com/BLAKE3-team/BLAKE3/blob/1.8.5/reference_impl/reference_impl.rs), and [test vectors](https://github.com/BLAKE3-team/BLAKE3/blob/1.8.5/test_vectors/test_vectors.json). The corresponding cached registry source at `/root/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/blake3-1.8.5/` was also inspected.
* [Linux fsync manual](https://man7.org/linux/man-pages/man2/fsync.2.html) and [Linux rename manual](https://man7.org/linux/man-pages/man2/rename.2.html), for the limited local-filesystem atomicity/durability claims, not for remote-backend guarantees.
* Local code linked inline establishes current behavior; all proposed formats, invariants, bounds and protocols are this note's design analysis, not claims that the repository already implements them.
