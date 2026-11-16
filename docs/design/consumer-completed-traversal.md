# Consumer-completed traversal

Ordinary scans keep `consumer_completion_budget = None`. Reuse one empty Vec with
`session.next_item(&mut completed)`; ordinary output remains `Entry`, with no work IDs or ACK
allocation. External `TraversalSource` adapters using the public `bounded`/`send` pair must
refuse `Some(_)` explicitly. That producer cannot issue consumer work IDs.

`StorageTraversalSource::traverse` supports `Some(byte_budget)` for unordered directory
namespaces without directory objects. Scan checkpoints, resume positions and
`recoverable_traversal` are refused before I/O. Source mutation capabilities are not required.
A file root is explicitly Unsupported in this mode, never an empty success.
Root shape is checked using stat; the root is an internal barrier, never a consumer entry,
never reobserved for ACL, and never finalized by this mode. The caller owns root metadata.

`ConsumerEntry(Box<(id, ObservedEntry)>)` requests one file/link operation or directory prepare.
Only successful directory prepare permits descendant listing. `SubtreeReady` carries a fresh
phase ID and the directory's unchanged first observation for finalization, after direct EOF
and all selected descendants succeed. Pruned/depth-limited directories use an explicit private
policy boundary: they still prepare/finalize, but do not claim physical EOF. Hidden descended
directories remain topology only, following the existing filter/metadata dependency rules.
Finalization adds no second entry count. Metadata failures remain in observations, supplementary
events and the final snapshot; prepare success does not erase them. A failed consumer stage
blocks ancestor finalization without fabricating errors for those ancestors.

Ready subtrees advance through a private generation-indexed arena and deduplicated ready queue.
Waiting for a file/prepare/finalize receipt does not block an unrelated sibling. Each turn does
one bounded admission/publication/page unit, with receipt and completion processing first.
Parent dependencies release only after child finalization is acknowledged; hidden directories
release their internal dependency after their selected range completes. One child output bundle
retains publication ownership until its facts and entries are sent: output backpressure can
still receive receipts/cancellation, but another child's facts cannot split that bundle.

In consumer mode only, a whole asynchronous metadata-role `Entry(PermissionDenied)` may
retain a known listed descriptor for a data attempt when every family in that call's actual
I/O plan is BestEffort and no still-missing filter dependency is involved. The existing inline
merge preserves Value/NotApplicable and marks each requested but unobtained family Failed.
Known Unsupported and InlineOnly families are not relabelled. No metadata request is repeated.
The first failed snapshot remains unchanged at finalization; a caller must still decide its
consumer result from the actual data and target-metadata outcomes.

Ordinary `None`, Required I/O, inline-role errors, other entry classes, session errors, and link
read errors keep their existing paths. This does not make the source readable: later describe,
expected-source identity and actual read checks remain mandatory and can fail independently.
Family events retain only path/family/class/transience; the original redacted whole-role
message is logged, not added to the model or promised as typed callback text.

Feed back `(id, success)` via the Vec passed to `next_item`. IDs bind a session, execution and
independent phase sequence. Unknown/unissued, duplicate, cross-session and late feedback cannot
settle current work. Submission commits synchronously on first poll before clearing the Vec
or awaiting anything. Unpolled cancellation leaves the Vec unchanged; cancelling after poll
does not undo submission. Feedback longer than `consumer_max_inflight` is a typed protocol
failure before any of its confirmations apply. `true` proves only that phase; `false` is
`ConsumerFailed`, not proof of an unchanged target. Keep the actual typed operation cause in
the application; cancel on an unknown target result. No ACK deadline is imposed.

The coordinator must poll traversal, dispatch work and receive results together. Do not wait
for a huge dispatch batch or EOF before processing feedback. Cancelling a pending `next_item`
to receive a worker result and repoll with feedback is supported. Positive EOF requires all
issued work settled. At output EOF the session also observes mandatory producer evidence:
Drop/panic is `Internal`; known source failure and Cancelled settle waiters; a pending terminal
sender is not failure, and Completed still waits for feedback. The consumed terminal is cached
for `finish`. Known source causes survive derived cancellation, and source failures are
exposed before waiting for outstanding work.

Controlled cancellation seals the already observed diagnostic set and closes normal output
and pending work. Continue `next_item` to drain those facts before `None`; no unknown I/O is
awaited. Normal delivery removes its registered fallback copy, while sealed draining never
reads late normal-channel copies. Detached results after sealing cannot add facts or replace
the terminal. The pending set contains only undelivered admitted-page/output facts, bounded by
those page/window/output limits; it retains no delivered history. Direct Drop/crash does not
claim delivery. Ordinary scans retain their original cancellation behavior.

Bounds are separate:

- W (`consumer_max_inflight`) bounds issued phases across Native output, dispatch, workers,
  results and feedback. Directory state between prepare/finalize does not occupy a phase slot.
- The logical byte budget reserves every active node, including root/hidden/policy nodes:
  fixed Node and arena/queue storage reserves, visible work/listing/statistics paths and
  descent-hint bytes, and its original encoded snapshot. Therefore active nodes are at most
  `budget / fixed_node_reserve` in this build; the caller separately budgets its own target
  identities. Reused arena/free/queue Vec capacity retains a bounded concurrency high-water
  allocation rather than scan history; allocator capacity retention is outside this logical
  live-object budget.
- Acquired pages, staged child/slot/result containers, visible descriptor/observation payloads,
  metadata, and retained prepared entries/descents are charged too. Additional observation
  results reserve credit before they are handed back. A shared page charge follows a detached
  observer if cancellation stops the actor first; prepared items own individual charges,
  released as their payload transfers or drops. Waiting phase records have fixed charges.
  Directory admission also reserves its later decoded finalization payload/receipt credit,
  so resident first snapshots cannot exhaust every byte needed to finalize and release them.
  This byte credit does not occupy a W phase slot. Consumed page buffers release before another
  page read; a subsequent read reserves the measured prior page peak and defers when other
  ready work can release credit. An EOF probe needed to free a parked listing may run without
  that estimate; any actual returned page must still pass measurement and reservation.
  Measurements use existing getters, not extra snapshot encoding or whole-entry cloning.
- Listing page size is `min(W, DEFAULT_PAGE_HINT)` children, independent of observation slots.
  The actor keeps at most one prepared page per resident node. Resident listing count follows
  the positive node-byte/count budget, additionally capped by the namespace's real parked
  listing limit (e.g. native handles); it is not capped by `max_inflight_operations`. Root counts
  toward both. At the parked limit, new admission pauses while other listings can finish.
  With no pending event or ready operation able to release credit, exhaustion fails typed
  without relisting or waiting for impossible credit.
- `max_inflight_operations` caps actor page tasks and additional metadata-role observations
  separately, each at most 64. It does
  **not** bound all backend I/O: the existing stream producer may read ahead while its parent
  is parked. A capacity-one channel can hold one page while the producer holds the next page
  awaiting send. That blocked producer cannot start another read; SDK response buffers and
  partial-page construction during a read belong to the adapter and remain opaque.

This is not a total RSS/allocator cap. Logical lengths, fixed state reserves and visible
container capacities do not measure allocator slack, shared backing allocations, producer
closures/cursors, SDK buffers or downstream consumer-owned payloads. The stream and newly
returned raw/inline/async observation value are acquired before measurement; an oversized
variable child can temporarily exceed the reservation and then fails typed before new consumer
work. There is no fixed byte bound on those opaque/transient allocations. Diagnostic copies
retained for cancellation draining have the existing admitted-page/output ownership bound;
completion credit is not a claim to measure their shared allocator footprint. Ordinary `None`
keeps its existing scheduling/measurement behavior. No backend/model/SDK changes, restart-root
replay or private recovery loop are introduced. Native source failures keep their typed causes.
The additional measurement/reservation cost has not been performance-certified against 5%.

Local deterministic tests exercise production scheduling, first-snapshot equality, gates,
filters, byte/handle limits, cancellation and source/metadata errors. SourceMock exercises W=128,
16 concurrent workers, 4096 items and capacity-one output/result channels using the same session
state. Real remote fault and performance acceptance remains separate; no remote backend is
certified by those local tests alone.
