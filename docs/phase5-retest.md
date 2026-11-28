# Phase 5 dense-buffer retest checkpoint

Status as of November 28, 2026.

**Correctness and fixed-load comparison: Passed. Final Phase 5 qualification: Blocked by sustained reference drift.**

The final dense correctness checks and five-pair fixed-load comparison pass. The latest bounded campaign stopped after 1,159.11 s at the frozen three-refinement limit. It did not establish five stable capacity sets or qualify later gates. Phase 5 remains Blocked. The approved task plan and execution evidence follow.

## Goal

Improve the real Bend WebSocket path through dense byte storage and retained byte leases. Keep native Bend datatype declarations as the schema authority. Generate byte-level packing and decoding from those declarations, with a reusable semantic model for the later Bend–SpacetimeDB generators.

The delivery order remains:

1. Close Phase 5 qualification.
2. Complete the single-worker Bend–SpacetimeDB connector.
3. Qualify the Phase 6 computation pool.
4. Revisit optional Phase 7 only if measured evidence justifies replacing Rust transport.

## Issues found during qualification

| Issue | Evidence and consequence | Approach taken |
| --- | --- | --- |
| The original qualification matrix was too long | Thirteen fully repeated workloads, separate half-load comparisons, repeated overload trials, and two 30-minute soaks scheduled roughly nine hours of core trials before additional work. | Introduced the approved one-hour-capped `local-perf-v2` profile. Concentrate repeated performance qualification on three anchors, screen the other ten cases, run W4 once per condition, and use three-minute stability intervals. |
| Capacity errors lost their specific cause | The controller reduced errors to a Boolean. A failed trial could be recorded without identifying latency, backlog, or invalid evidence. | Added typed capacity failures and retained exact gate errors in trial evidence. Invalid observations cannot silently establish a failed capacity boundary. |
| An overloaded reference generator produced sequence gaps | The first bounded attempt ended inconclusively after 19.38 seconds. At 500,000 scheduled messages/s per direction, the reference peer filled its 4,096-descriptor queue and rejected messages before a forward sequence gap. | Recognize only that specifically accounted reference rejection outcome as a failed-rate boundary. Keep unrelated actor failures and invalid observations inconclusive. |
| Short calibration did not predict sustained tail latency | The retry selected 228,125 reference messages/s per direction in short calibration. Its first 20-second capacity trial reported p99 of 4.939 ms outbound and 7.291 ms inbound, above the 2 ms limit. Both actors completed delivery validation. | Preserve the failed result. Require a repeatable qualified-duration reference boundary before accepting ratios. Calibration candidates remain unqualified. |
| WSL2 profiling was not usable through the default wrapper and flags | `/usr/bin/perf` looked for tools matching the WSL2 kernel. Default call-chain sampling also failed under the current access settings. | Use `/usr/lib/linux-tools/6.8.0-151-generic/perf` explicitly and record user-only call chains with `-g --user-callchains`. `strace` is installed. |
| Intrusive tracing disturbed the workload | Native failed during a `strace` diagnostic. The retained profiles also had too few samples for precise cost attribution. | Keep profiling separate from uninstrumented acceptance trials. Treat the trace as diagnostic evidence, not a capacity measurement or complete limiter assessment. |
| The Bend path allocated and traversed byte lists | Single-message sends allocated 1 MiB of temporary C scratch, copied a Bend byte list, and traversed it again to consume accepted input. Receive materialized per-byte Bend list nodes. | Replace public byte-list transport with packed native arrays and lease-backed receive metadata. Borrow outgoing array storage only during the effect; Rust commits its TX copy before returning. |
| Fixture generation and validation repeated work | The old recipe generated a complete C payload but exported only its header and seed. Bend rebuilt the body and an entire expected receive list. | Keep expected recipe generation to header and seed metadata. Use pure Bend packed-word generation and full-byte validation rather than constructing complete expected byte lists. |
| The first codec design assumed nested DTO fields stayed boxed | A pinned-compiler probe observed native root arity 7 where the design expected 4. Padding did not prevent flattening. | Derive the flattened layout of generated DTO declarations, verify constructor arities, and use typed Bend adapters to handle the original source datatypes. Do not guess offsets in application records. |
| Direct writes could modify shared or static array storage | An affine value can still refer to backing storage requiring copy-on-write. | Reuse unshared dynamic output arrays. Allocate a fresh dense output array for shared/reference-counted or static storage before writing. |

The earlier failed campaign and its diagnostic evidence are retained in the [November 27 checkpoint](ws-phased-plan.md#phase-5-checkpoint-november-27-2026), [qualification summary](../results/ws/cargo-1795774532680549456-6542/phase-5/summary.json), and [diagnostic manifest](../results/ws/cargo-1795774532680549456-6542/phase-5/limiter.json).

## Implementation approach

### Dense owned buffers

`src/bytes/main.bend` defines an affine `Buffer` backed by native `Array<U32>` storage and a logical byte length. Each word holds four bytes. Native storage is contiguous, and unused capacity is not transmitted.

The byte layer checks allocation size, array representation, capacity, and logical length. Empty buffers still have valid owned storage. The selected native backend is little-endian Linux with the pinned Bend compiler.

Single and batch WebSocket admission return the original owned buffers in every outcome, including acceptance. This permits reuse after the native TX copy commits. A second accepted offer is another message, not a retry of the first.

### Retained receive leases

The public receive path returns an aggregate lease and message metadata containing kind, index, and length. It does not automatically build a byte list or copy a message into a Bend buffer.

`ws_socket_lease_view` checks generation, role, message index, and span bounds. Native pointers remain within the C effect while the affine lease is retained. Explicit copying remains available when an application needs independently owned data. Typed blob decoding materializes dense owned arrays rather than byte lists.

The existing limits remain in force: 1 MiB per message, 128 messages and 64 KiB per public batch, eight outstanding receive leases, and 2 MiB of leased payload bytes. Held leases remain readable after connection close until release.

### Bend-defined schema compiler

`tools/bend-codec` parses native Bend declarations and resolves local imports into one ordered semantic model. It generates typed Bend adapters, native C codecs, and `contract.json` from that model.

The model is intended for reuse by the later SpacetimeDB table, reducer, and binding generators. Application developers do not maintain a second handwritten Rust schema.

The implemented initial type set includes nested products and sums, U32, Bool, String, List<U32>, Array<U32>, dense byte blobs, and the canonical 64-bit signed/unsigned word wrappers. Unsupported types and recursive/generic shapes fail with source diagnostics. This is an explicit initial subset, not support for every possible Bend type.

The default wire backend is BSATN. The explicit `raw-be` fixture backend preserves the existing benchmark's seven big-endian header words and trailing payload bytes. Runtime constructor IDs never become wire tags.

Generated decoders check tags, UTF-8, lengths, overflow, allocation bounds, and complete-message consumption. Packing errors leave the output's logical length zero. Typed Bend adapters reconstruct the original source values.

### Caller and fixture migration

The public WebSocket API and production C adapter no longer expose byte-list payload compatibility paths.

Phase 4 retains explicit test-only list bridges for its established generators and validators. Phase 5 uses `bench/ws/phase5-schema.bend`, generated codecs, pure Bend packed-word body generation, and full header/body validation. Consumption timestamps remain after application validation.

Fixture cache identities include the byte implementation, schema, generator, generated artifacts, native archive, toolchain, and flags. Completed builds are checksum-validated before reuse.

## Test status

The November 28 checks below supersede the preparatory November 27 results. Correctness and the fixed-load comparison do not establish a throughput qualification pass.

| Final check | Status | Required coverage |
| --- | --- | --- |
| Schema compiler semantic checks | Passed | Five source ordering, alias, nested sum, deterministic generation, unsupported-declaration, and malformed-wire checks. Cargo preparation 0.20 s, semantic test time 0.01 s. |
| Native BSATN and raw-be roundtrips | Passed | Both native fixtures passed with cache hits. Native target time 0.72 s, including tool identity checks. Evidence under `target/codec-fixtures/fixture_27ff9db79d5cf89e` and `fixture_f4681b89f5dda640`. |
| Real Bend Phase 4 transport | Passed | Final full dense qualification passed in 106.16 s, including one million messages each way, retained leases, 100 pauses, and 1,000 lifecycle cycles per instrumentation mode. [Evidence](../results/ws/cargo-1795844941452124051-22546/phase-4/summary.json). |
| Phase 5 routine checks | Passed | Final eight checks passed in 10.83 s after 10.41 s Cargo compilation. One fixture build and seven hits. [Bend evidence](../results/ws/cargo-1795848271330447788-29685/phase-5/summary.json). Each check has its own evidence directory from that invocation. |
| Paired baseline-versus-dense comparison | Passed | Final five pairs at 12,000 messages/s each way, 5 s warmup, and 20 s measurement passed in 253.11 s with a cache hit. [Comparison](../results/ws/cargo-1795849540217807091-30682/phase-5/comparison.json). |
| Bounded Phase 5 qualification | Blocked | Latest campaign ended inconclusively at the three-refinement limit in 1,159.11 s. No stable five-set anchor, qualified ratios, half-load, headroom, screens, W4, or W5 pass. [Campaign](../results/ws/cargo-1795848288875281882-30078/phase-5/campaign.json). |
| Limiter assessment and final evidence review | Partial | Fresh readable profiles, software counters, and syscall traces are retained and mapped below. Numerical qualification is incomplete. No complete limiter assessment or evidence-based promotion is available. |

Preparatory transport evidence is retained at [the dense Phase 4 run](../results/ws/cargo-1795784810770524234-19551/phase-4/summary.json). Native codec fixture evidence is under `target/codec-fixtures/`. These pointers are implementation checkpoints, not final qualification artifacts.

No compiler, test, benchmark, or profiler process remained at the process inspection performed before writing this checkpoint.

## Remaining work

1. Resolve the sustained reference drift before another campaign. The frozen CPUs 0 and 1 are sibling threads of physical core 0. A proposed new profile would use distinct physical cores, for example CPUs 0 and 2, while retaining numerical targets. That profile change needs approval and does not guarantee a pass. Increasing retries alone does not resolve the observed drift.
2. Establish five stable qualified-duration boundaries and comparison sets. Do not reduce the reference denominator to improve ratios. Tune measured Native and Bend costs if the qualified ratios remain below 80% median or 75% per pair.
3. Complete the remaining anchor, reference-noise, half-load latency, headroom, and screening gates within the approved budget.
4. Complete all twelve W4 conditions and each candidate's 180-second stability interval with persistent lifecycle and resource restoration.
5. Complete artifact-bound limiter assessments and the final evidence gate. Keep interrupted, failed, and inconclusive results distinct from passes.
6. Mark Phase 5 Passed only after every bounded gate passes. Complete the BS tasks afterward, before Phase 6.

### Prepared commands

These are the available commands. The status table records completed selections. Select only affected checks after another change:

```sh
cargo test --release --locked -p bend-codec -- --nocapture

cargo test --release --locked -p ws-reference --no-default-features --features phase4 --test phase4 phase4_real_bend -- --exact --nocapture

cargo test --release --locked -p ws-reference --test phase5 -- --nocapture

WS_PHASE5_BASELINE_DIR="<retained-baseline-fixture-directory>" cargo test --release --locked -p ws-reference --test phase5 bounded::phase5_dense_comparison -- --ignored --exact --test-threads=1 --nocapture

timeout --signal=TERM --kill-after=5s 3600s env WS_PHASE5_PERF=/usr/lib/linux-tools/6.8.0-151-generic/perf WS_PHASE5_STRACE=/usr/bin/strace cargo test --release --locked -p ws-reference --test phase5 phase5_full_qualification -- --ignored --exact --test-threads=1 --nocapture
```

## Approved plan of action

Approved November 28, 2026. Execute P5-1 through P5-8 in this session. Leave BS-1 through BS-10 for a later session. The delivery order is Phase 5 closure, the single-worker Bend–SpacetimeDB connector, Phase 6, and optional Phase 7.

### Close Phase 5

| ID | Task | Work required and completion criteria |
| --- | --- | --- |
| P5-1 | Review the integrated dense-buffer changes | Review `src/bytes/`, WebSocket effects and native ABI, generated codecs, and fixture migration together. Check buffer reuse after accepted admission, copy-on-write, logical length and partial words, lease validity after close, generation and role checks, decoder cleanup, and compiler-layout guards. Resolve findings before final measurements. |
| P5-2 | Establish final build and evidence identities | Identify the exact dense candidate and retained pre-dense baseline, including manifests, checksums, compiler, native archive, generator, and flags. Map preparatory evidence to final inputs. Record reusable evidence, required reruns, and fixture-cache identity. |
| P5-3 | Complete affected correctness verification | Run schema/compiler checks, native BSATN and raw-be fixtures, real Bend Phase 4 coverage, and Phase 5 routine checks. Cover malformed input, Unicode, containers, 64-bit values, copy-on-write, retained leases, dense delivery, 259-byte partial words, corruption, overload, and cleanup. Select earlier checks only where the diff changes their boundary. |
| P5-4 | Complete the baseline-versus-dense comparison | Run five alternating pairs of 256-byte duplex traffic at 12,000 messages/s per direction, with 5 s warmup and 20 s measurement, on the same endpoint CPU budget. Report latency, CPU, allocations, and exact delivery. Distinguish fixture improvements from production transport improvements. |
| P5-5 | Establish sustained capacity boundaries | Diagnose sustained reference tail latency and correct qualified-duration refinement in the bounded controller. Preserve passing/failing brackets, uncertainty, typed failures, and invalid-evidence rejection. Three-second calibration chooses candidates only. Establish repeatable 20-second boundaries before calculating ratios. |
| P5-6 | Tune remaining measured limiters | Use fresh profiles and counters to determine whether Native/Reference or Bend/Native misses its target. Change one demonstrated limiter at a time, verify the affected boundary, and repeat the affected comparison. Completion requires the targets to pass. |
| P5-7 | Complete the bounded numerical campaign | Qualify all three anchors, half-load comparisons, reference spread, generator/peer headroom, and ten screening cases. Complete twelve W4 candidate/receiver/pause conditions and 180-second W5 stability per candidate, including persistent normal-close and queued-disconnect cycles and resource restoration. |
| P5-8 | Validate evidence and record closure | Map diagnostics to source and explain each path and peer limiter. Validate actor counts, peer receipts, queues, resources, and artifact identities. Use saved-evidence assessment when numerical results are complete but assessment remains pending. Update the phase plan, controller reference, and this checkpoint with final outcomes and evidence. |

The unchanged closure targets are 80% median and 75% per-pair capacity ratios, 2 ms capacity p99, 1 ms native outbound and 2 ms Bend half-load p99, 5% reference spread including uncertainty, and 1.25-times generator/peer headroom. Integrity, backlog, memory, overload, stability, and cleanup gates must pass. Keep the one-hour campaign and six-minute calibration limits. Any necessary policy revision requires approval.

Before execution, settle the final evidence-selection matrix, baseline/candidate identities, and sustained-boundary controller design and time budget. The current Phase 5 routine target has eight checks, including `phase5_bend_partial_word`; older references to seven checks describe the pre-dense target.

### Complete Bend–SpacetimeDB in a later session

| ID | Task | Work required and completion criteria |
| --- | --- | --- |
| BS-1 | Freeze the connector contract and first example | Confirm the pinned release, protocol, declaration syntax, first service, table keys, subscriptions, operation identity, checkpoints, dependencies, and limits. Sketch the generated public API and session state before implementation. |
| BS-2 | Extend the shared semantic model | Extend `tools/bend-codec` with service exports, persistence/table declarations, indexes, read models, conversions, and supported commit conditions. Generate all application contracts from this model. Unsupported declarations produce source diagnostics. |
| BS-3 | Complete protocol-required BSATN coverage | Add narrow scalar representations, exact 128/256-bit values, collections, and protocol wrappers. Decode identity, connection metadata, timestamps/offsets, and `BsatnRowList`. Compare bytes with the pinned SpacetimeDB SATS implementation. |
| BS-4 | Generate and verify a deployable module | Generate Rust tables, indexes, reducer/result registrations, and Bend bindings. Build WASM and compare extracted `RawModuleDefV10` with generated contracts. Deliberate field/variant-order and result-type mismatches fail. |
| BS-5 | Implement the Bend-owned protocol session | Connect anonymously over loopback with `v2.bsatn.spacetimedb` and `compression=None&confirmed=true`. Decode metadata, strip the server compression prefix, correlate bounded calls, and handle results/errors. Distinguish non-admission, confirmation, and uncertain calls. |
| BS-6 | Implement consistent subscription publication | Publish readiness after the complete snapshot. Stage multi-table updates atomically, including result-embedded updates. Handle repeated tables and overlapping-query membership. Bound cache/staging memory and invalidate readiness on disconnect or unrecoverable overflow. |
| BS-7 | Generate transactional commit shims | Implement the proposal contract and raw-BSATN insertion. Validate contract version, operation/proposal identity, descriptor coverage, row bounds, dependencies, checkpoints, and fencing. Commit rows, checkpoint, and outcome atomically. Verify fixed/variable rows, generated columns, malformed rows, and whole-batch rollback. |
| BS-8 | Implement single-worker coordination and reconciliation | Compute from consistent subscribed inputs in real Bend, submit a proposal, and reconcile a dropped reply through durable operation records. Reconnect anonymously with a new identity and prevent duplicate accepted effects. Recover through durable events/checkpoints. |
| BS-9 | Provide the generated build/publish workflow | Produce the native Bend executable, Rust transport, WASM module, frontend bindings, and query helpers through one workflow. Check compatibility before publication. Define local deployment, lifecycle, and schema-change handling. |
| BS-10 | Qualify the single-worker connector | Verify real calls/subscriptions, cache publication, rejection/rollback, reconnect/reconciliation, bounds, and cleanup against persistent SpacetimeDB. Demonstrate progress during long Bend computation and backpressure. Verify independent standard-SDK observation of committed state. |

Before BS implementation, settle the declaration/API sketch, a small deterministic projection example with durable input and a multi-table update, connector limits and recovery rules, and separate Cargo checks for codec, schema, session/cache, persistence, and recovery boundaries.

### Dependency order

```text
P5-1 → P5-2 → P5-3
                   ↘ P5-4
                   ↘ P5-5 → P5-6
                              ↓
                         P5-7 → P5-8
                                  ↓
                                 BS-1
                                  ↓
                            BS-2 → BS-3
                                  ↓
                            BS-4 → BS-5 → BS-6
                                  ↓           ↓
                                 BS-7 → BS-8 → BS-9 → BS-10
                                                        ↓
                                                      Phase 6
                                                        ↓
                                                Optional Phase 7
```

BS-4 depends on BS-2 and BS-3. BS-7 depends on BS-4. BS-8 joins the BS-6 session/cache work and BS-7 commit-shim work. Phase 5 closure requires both P5-4 comparison evidence and P5-5/P5-6 sustained qualification readiness before P5-7/P5-8.

### Execution record, November 28

Before execution, the host has eight available CPUs and load averages below 0.11. No compiler, fixture, or profiler is running. The fixed-load comparison tests whether dense storage reduces validated end-to-end latency, CPU, and allocation cost at the same scheduled load. Capacity qualification separately decides whether both frozen 80%/75% ratios pass. Profiles and traces are diagnostic runs, separate from acceptance timing.

The retained pre-dense baseline is `target/ws-fixtures/25715fb289039ccc2c36f9ad08e5f3bb106812d49aad38987f8c1a417e805aca`. Its `complete.json` matches the fixture identity used by the failed November 27 campaign. The comparison validates its artifact checksums and embedded native-build marker before reuse.

| Selected check | Reason selected before execution |
| --- | --- |
| `cargo test --release --locked -p bend-codec -- --nocapture` | Final semantic model, generated native BSATN/raw-be codecs, copy-on-write, and constructor layouts have no completed final retest. |
| `cargo test --release --locked -p ws-reference --no-default-features --features phase4 --test phase4 phase4_real_bend -- --exact --nocapture` | Production send/receive representation, retained lease access, and real Bend fixtures changed. |
| `cargo test --release --locked -p ws-reference --test phase5 -- --nocapture` | Final dense scheduled actors, partial words, evidence controls, overload, and lifecycle inputs changed. |

Reuse the unchanged protocol engine, worker scheduling, and Phase 1 in-memory ownership evidence. The diff adds socket admission/view ABI operations but does not change handshake, framing, or socket scheduling. Phase 4 directly exercises those operations and lease lifetime. Reconsider earlier coverage after any subsequent production change. The historical million-message pass describes the old byte path; select full Phase 4 qualification if the final dense inputs have no matching full evidence.

The selected initial checks passed: codec semantics (five checks) and both cached native codec fixtures, Phase 4 routine in 8.15 s with a cache hit, and all eight Phase 5 routine checks in 16.10 s after 15.56 s Cargo compilation. Phase 5 built one fixture input set and reused it seven times. The candidate key is `e3d4635bedb0a1891eed4e89a749effa8ddb78a79a14735edc2e052e12388547`.

Full Phase 4 first hit the terminal tool's 120-second timeout. A cached retry failed a pause-observer race: `phase4_notice` signaled the peer before sampling the starting worker status. The failed case recorded all nine messages before that snapshot. Move the snapshot before the peer signal. The affected full qualification then passed in 106.16 s, including one million messages each way, 100 pauses, and 1,000 lifecycle cycles per instrumentation mode. Evidence: `results/ws/cargo-1795844941452124051-22546/phase-4/summary.json`.

The first fixed-load comparison stopped after three complete pairs and the dense half of pair four. That dense trial delivered correctly but exceeded the 2 ms latency ceiling. Its receive maximum was 339.47 ms while producer p99 lateness was 148 µs, and both directions reached the histogram's 10 ms overflow bin at p99. Preserve `results/ws/cargo-1795845060445609586-25125/phase-5` as a failed comparison, not a five-pair result. The host has no cgroup CPU quota or throttling. Investigate fresh diagnostics before accepting a performance claim.

The sustained-boundary design keeps short calibration and long qualification separate. Each path establishes a fresh passing/failing 20-second bracket with at most the existing 32 search steps and 1% resolution. Later reference sets independently recheck both ends; drift is inconclusive. These long trials consume the one-hour campaign budget, not the six-minute short-calibration budget. Ratios use only complete long paired trials. Invalid observations never establish a boundary.

Select `bounded::phase5_limiter_probe` for diagnostic collection before another performance attempt. Collect five-second profiles/counters for Reference at 100,000, Native at 100,000, and Bend at 30,000 messages/s per direction. Run intrusive syscall traces separately at at most 1,000 messages/s; record the actual offered rates. These are diagnostic rates, not qualified capacities. Five-second sampling replaces the previous one-second collection to improve attribution.

The first sustained campaign ended inconclusively in 386.78 s. It established a Reference bracket of 199,071 passing and 200,390 failing messages/s per direction at qualified duration, but no repeated reference-noise result. Native passed individual 20-second trials through 146,487, then rejected outbound work at 183,109. A narrow rejection classifier recognizes only a peer sequence gap covered by the sender's recorded rejection interval, a full 4,096-descriptor queue, adapter disconnect code 22, and valid remaining evidence. Four damage controls against that real trial passed. A later Native trial at 146,487 remained inconclusive because the peer's conservative flush-completion counter was lower than independently received messages. Do not treat that inconsistent evidence as a capacity boundary.

Select the Phase 5 routine target after the receive-idle correction. `phase5_next` previously slept for 50 µs whenever no send was due, even when the native receive queue was nonempty. Cache the receive-ready count from its existing worker snapshot and skip that sleep while receive work is ready. This changes the fixture schedule, not production transport or numerical thresholds. Reuse codec and Phase 4 results. Rerun the fixed-load comparison and fresh limiter collection for the new fixture identity.

The corrected fixed-load comparison completed all five pairs in 253.01 s with a cache hit: `results/ws/cargo-1795846341479996642-26873/phase-5/comparison.json`. Baseline/dense median outbound p99 is 224/166 µs, inbound p99 is 269/240 µs, and whole-trial client-process CPU is 12.388/7.568 s. Baseline ranges are 216–232 µs outbound, 263–290 µs inbound, and 12.191–12.604 s CPU. Dense ranges are 163–188 µs, 238–245 µs, and 7.252–7.680 s. C peak allocation is 1,048,640/96 bytes. These results combine dense transport and fixture improvements and do not establish capacity ratios or a Phase 5 pass.

Six routine checks passed after the receive-idle correction. Two lifecycle checks exposed fixture issues. The routine lifecycle path incorrectly enabled the qualification-only zero-missed-arrival gate. Remove that flag from routine calls while retaining schedule accounting and cutoff checks. A faster receive consumer also drained the fault input before the fixed-time disconnect, leaving no native-owned queued data. Add an explicit per-session receive pause to the persistent barrier and use it for queued-disconnect sessions. This retains native receive ownership until the injected disconnect. Select only `overload_lifecycle` after these edits; unchanged codec, Phase 4, and other routine checks stay out of the rerun.

Both affected lifecycle checks passed in 11.58 s after 12.16 s Cargo compilation, with one fixture build and one hit. A focused Bend sustained search then established 52,370 passing and 52,737 failing messages/s per direction in 227.82 s. This is one bracket, not five-pair qualification. Fresh profiles at the passing rate show Bend runtime continuation/effect overhead: `Clo_apply` 12.50%, `IO_bind` 6.02%, and `io_loop` 5.56% of sampled user CPU. Evidence: `results/ws/cargo-1795847032912576464-27791/phase-5/limiter.json`.

The next fixture optimization removes unnecessary effects without batching or changing the wire: return receive eligibility in the existing schedule result, avoid empty receive calls, drop the decoded owned array through affine ownership after full Bend validation, and invoke error reporting only for nonzero lease-release codes. C constructor guards check the revised 17-slot schedule DTO. Select the full Phase 5 routine target because schedule, validation cleanup, and paused-consumer behavior changed, then the affected Bend sustained search. Keep the native transport and numerical profile unchanged.

That experiment passed all eight routine checks in 11.00 s but did not move the sustained boundary: 52,042 passing and 52,370 failing, within the prior bracket's variation. Restore the simpler fixture. Retain only the receive-idle correction, which completed the five-pair fixed-load comparison.

The conservative transmission counter is a library-flush completion count. On a failed flush, a complete prefix can reach the receiver while bytes remain buffered. Correct fault accounting therefore bounds that prefix by admitted messages, while preserving the published flush counter and strict native-worker completion counts. Normal capacity trials still require exact attempted/admitted/transmitted/validated equality. Recheck candidate rejection controls against the retained partial-flush trial, including a damaged receive count, and rerun shared evidence controls and overload/lifecycle checks after this validator change. No numerical threshold changes.

The partial-flush rejection controls passed against the real Native trial. Shared evidence controls passed in 2.40 s, and both lifecycle checks passed in 2.81 s, all with cache hits. A subsequent full campaign stopped after 188.05 s on the opposite reference overload direction: a 18,816-message forward gap exactly matched 18,816 client rejections at a full queue. Add that specific accounted outcome. Five evidence-damage controls against the real reference trial passed after 12.26 s Cargo compilation. Bare transport errors, unsupported fault shapes, malformed metadata, and invalid counts remain inconclusive.

Use one typed boundary-outcome classifier for calibration, sustained search, and paired trials. If a valid capacity failure or fresh reference bracket drift occurs, re-establish only the affected sustained boundary and restart all five comparison sets, up to the profile's three refinements. Preserve abandoned trial sets. Exhausted refinements or the campaign deadline remain inconclusive. Select the bounded full qualification next to exercise this final controller change.

That campaign stopped after 294.95 s because the Bend actor collapsed an incoming sequence gap into generic adapter code 2. The peer had reached 4,096 descriptors and rejected work, but the missing expected/received identities prevented a valid boundary classification. Preserve that result. Record the first exact sequence/duplicate failure in the native and Bend fixture reports. One rejection classifier now handles both directions for all paths using the actual gap, rejection accounting, and queue saturation. Corruption and unaccounted failures remain inconclusive. Select Phase 5 routine checks after this telemetry change, then the full bounded campaign.

### Final November 28 checkpoint

The final candidate fixture is `target/ws-fixtures/b570d4be2e58999a2ca3527e65dde34048f71fdf580e73f24f8c7dedf62b421d`. All eight routine checks passed on that input set in 10.83 s after 10.41 s Cargo compilation. One fixture build served seven subsequent cache hits.

The final bounded campaign used a cache hit and ended inconclusively in 1,159.11 s. Calibration consumed 111.09 s of the 360 s allowance. The campaign stopped before the 3,600 s deadline because reference drift exhausted three refinements. The 20-second reference lower boundaries were 291,015, 248,817, 234,820, and 293,624 messages/s per direction. The first three then failed their paired latency/backlog trials. The last passed one three-path set, but its freshly rechecked upper boundary passed too, invalidating that bracket. These are changing trial boundaries, not qualified capacity claims. The final campaign has zero completed anchors and `numerical_complete: false`.

Preserve [the campaign](../results/ws/cargo-1795848288875281882-30078/phase-5/campaign.json), [trial records](../results/ws/cargo-1795848288875281882-30078/phase-5/trials.json), and [failure diagnostics](../results/ws/cargo-1795848288875281882-30078/phase-5/limiter.json). No five-set ratios, half-load, headroom, W1 anchors, screens, W4 matrix, or W5 stability result is established.

The final input set also completed five fixed-load baseline/dense pairs in 253.11 s after 0.02 s Cargo preparation, with a cache hit. Each run used 256-byte duplex messages, 12,000 scheduled messages/s per direction, 5 s warmup, and 20 s measurement. Each side validated the complete admitted stream and restored resources. [Final comparison](../results/ws/cargo-1795849540217807091-30682/phase-5/comparison.json).

| Fixed-load metric | Pre-dense baseline, median (range) | Final dense path, median (range) |
| --- | --- | --- |
| Outbound scheduled-arrival p99 | 212 µs (209–217) | 157 µs (154–159) |
| Inbound application-consumption p99 | 260 µs (258–273) | 228 µs (225–243) |
| Client-process CPU across the whole trial | 11.825 s (11.786–12.138) | 6.893 s (6.830–7.248) |
| Peak C allocation | 1,048,640 bytes | 96 bytes |

The comparison includes dense transport, generated packing, packed Bend body construction/validation, and the receive-idle correction. It does not isolate the production adapter's improvement. CPU includes warmup and measurement. Latency is measured on the scheduled measurement stream. One message remains one WebSocket message; no application-record aggregation or batching was introduced.

Fresh diagnostic source mapping is preliminary because the reference and generator/peer headroom gates have not passed:

| Path | Observed sampled costs and source | Remaining evidence gap |
| --- | --- | --- |
| Reference | Trial 0074 shows time queries at 16.34%, payload generation at 11.76%, and `Transport::receive` at 9.15%. These map to `bench/ws/reference-rs/src/main.rs`. | A further 28.10% is an unresolved libc offset. The generator and peer are not ruled out as limiters. |
| Native | Trial 0075 shows time queries at 13.07%, worker scheduling at 9.80%, registry access at 7.84%, and tracked allocation at 6.54%. These map to `worker.rs`, `socket.rs`, and the native allocator. | No qualified denominator or complete per-process limiter assessment. |
| Bend | Trial 0076 shows `Clo_apply` at 14.56%, `IO_bind` at 4.37%, and `io_loop` at 4.37%. The pinned runtime and `bench/ws/bend-phase5.bend` supply those continuations. | Effect reduction did not move the measured boundary. Larger gains cannot be inferred from these samples. |
| Independent peer | Trial 0074's peer profile shows payload generation at 21.31%, send at 8.20%, and `epoll_wait` at 4.92%. These map to `reference-native/main.cpp` and uWebSockets. | Peer headroom and the CPU ceiling remain unqualified. All three paths retain separate peer profiles and traces. |

The perf software counters report user-only context-switch counts as zero. Treat those zeros as unavailable attribution, not proof of no switching. Native and Bend actor reports retain their actual `getrusage` switch counts. A complete assessment must resolve the remaining counter and symbol gaps. An assessment cannot promote the incomplete numerical campaign.

| Task | Final status |
| --- | --- |
| P5-1 | Integrated review completed. |
| P5-2 | Baseline, final fixture, and scoped evidence identities recorded. |
| P5-3 | Selected final correctness gates passed. |
| P5-4 | Final five-pair fixed-load comparison passed. |
| P5-5 | Sustained search and bounded refinement implemented and exercised. Repeatable reference qualification remains Blocked. |
| P5-6 | Receive-idle correction retained. Unsuccessful effect-reduction experiment reverted. Qualified ratio targets remain pending. |
| P5-7 | Campaign executed. Frozen drift stop condition blocks remaining qualification gates. |
| P5-8 | Failed/inconclusive evidence reviewed and docs updated. Passing closure and complete limiter assessment remain pending. |
| BS-1 through BS-10 | Deferred to a later session. |

`git diff --check` passed. Process inspection found no compiler, test, benchmark, or profiler left running. No commit, deployment, or external publication was performed.

Select `cargo test --release --locked -p ws-reference --no-default-features --features phase4 --test phase4 phase4_full_qualification -- --ignored --exact --test-threads=1 --nocapture` next. The dense path has matching routine evidence but no final full evidence for million-message delivery, computation progress, repeated pauses, and instrumented lifecycle restoration.

## Final status

**Phase 5 is Blocked. Dense correctness and the final five-pair fixed-load comparison pass. Sustained throughput qualification remains incomplete.**

BS-1 through BS-10 remain deferred to a later session. The frozen reference-drift stop condition prevents closing P5-5 through P5-8 as qualification tasks.
