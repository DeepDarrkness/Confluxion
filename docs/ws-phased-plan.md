# WebSocket phased implementation plan

Build a reusable local WebSocket client library that maintains full-duplex IO during Bend computation, with bounded memory and measurable message throughput. The computation pool is an integration layer above the library. These phases establish that path before adding SpacetimeDB envelopes, BSATN, generated bindings, or durable coordinator recovery.

Design: when changing socket ownership or computation scheduling, read the [selected worker model](bend-websocket-design.md#selected-worker-model).

## Current status

Status as of November 23, 2026. Planning is complete; implementation has not started. No implementation gate has run or passed. The status table is authoritative for phase progress.

| Phase | Status | Checkable outcome | Required gate before proceeding |
| --- | --- | --- | --- |
| 0 | Not started | Trustworthy local measurement and build environment | Reference peers agree on validated counts; baseline and noise are recorded |
| 1 | Not started | Valid native resource/effect ownership | Real Bend start, transfer, release, and shutdown cycles pass |
| 2 | Not started | Correct incremental WebSocket protocol | Protocol vectors, fault injection, and independent decoding pass |
| 3 | Not started | Independent full-duplex native transport | Real-peer interoperability, client conformance, and bounded-overload tests pass |
| 4 | Not started | Complete Bend message path | Bend payloads cross the effects correctly; native IO progresses during computation |
| 5 | Not started | Useful local throughput at bounded latency | Paired reference and Bend-overhead targets pass under the frozen profile |
| 6 | Not started | Bounded reusable computation workers | Pool limits, result correlation, compute correctness, and ordering pass under load |

Set a phase to `In progress` when implementation begins, `Blocked` when its stop condition prevents progress, and `Passed` only when its gate passes. A passed phase links its verification artifacts; a blocked phase records the failing check and next action. Advance after the current gate passes.

## Baseline and scope

Planning baseline on November 23, 2026: this repository contains design documents, with no WebSocket sources, build harness, or benchmark results. A local tool check found `cc`, Rust/Cargo, Node, and Python, but did not find `bend` or `clang` on `PATH`. Phase 0 establishes the actual compiler and benchmark environment.

The first implementation targets Linux, IPv4 loopback, plaintext client connections, no compression, and persistent connections. Native framing is implemented locally rather than delegated to another WebSocket library. Established libraries supply independent peers, reference behavior, and comparison measurements. Production server support requires its own conformance gate; a test peer is not a production server implementation.

The smallest implementation units are organized by ownership:

| Proposed location | Responsibility |
| --- | --- |
| `src/ws/native/protocol.*` | Incremental handshake, frames, fragmentation, masks, controls, and close state |
| `src/ws/native/transport.*` | Descriptor ownership, readiness, deadlines, native queues, and worker lifecycle |
| `src/ws/main.bend` and `src/ws/effects/` | Affine session API, bounded admission, batched transfer, release, and close |
| `examples/ws-worker-pool/` | Bounded job dispatch and reusable BendRT processes, outside the transport library |
| `tests/ws/` | Protocol vectors, fault injection, lifecycle checks, and conformance adapter |
| `bench/ws/` | Independent peers, workloads, compute fixtures, and machine profiles |
| `scripts/ws/verify.py` | Build and run each gate, check results, and return a failing exit status when evidence is missing |

These paths and commands are planned interfaces, not existing files. Phase 0 creates the verifier, reference peers, profiles, and artifact format. Later phases add their own real checks; an unimplemented phase returns `not implemented`, never success. `python3 scripts/ws/verify.py all --profile local-v1` is the final cumulative command.

Each invocation writes `results/ws/<run-id>/phase-N/summary.json`, raw per-trial samples, the effective profile, source/build pins, command lines, and check names. Conformance phases also retain case-level Autobahn output. Results are evidence artifacts produced during implementation, not measurements established by this plan.

## Phase 0. Establish the measurement contract

- Outcome: a reproducible native baseline and a verifier that cannot mistake enqueueing for delivered work.
- Owns: `scripts/ws/verify.py`, `bench/ws/`, and shared fixtures in `tests/ws/`. Depends on no implementation phase.
- Smallest unit: select the Bend pin and compatible native compiler, build optimized reference client/peer adapters, then run two separate processes on loopback. Add length-delimited plain TCP as a diagnostic baseline. Pin Tungstenite, uWebSockets/uSockets, and Autobahn revisions or image digests in the profile.
- Verification: `python3 scripts/ws/verify.py phase-0 --profile local-v1`. The command also compiles and runs a minimal native Bend program, establishing that the chosen compiler works before phase 1.
- Gate: receiver counts, sequence IDs, and payload checks agree for at least one million messages in each direction. Deliberately duplicate or corrupt a message and confirm that the verifier fails. Capture five paired trials per core workload, with 10 seconds of warmup and 60 seconds of measurement. Reference throughput spread must be at most 5% of its median before ratios become blocking gates. Establish that the generator and peer can sustain at least 1.25 times the chosen operating rate; otherwise mark the result peer-limited.
- Stop: a missing compiler, unvalidated counts, excessive noise, or a peer/generator bottleneck blocks performance claims. Record baseline commands and limitations. Do not invent a throughput baseline for the currently absent library.

## Phase 1. Prove native effect ownership

- Outcome: Bend can safely own a persistent native worker service and exchange bounded byte payloads through the actual effect ABI.
- Owns: minimal `src/ws/main.bend`, `src/ws/effects/`, native resource lifecycle, and lifecycle fixtures. Depends on phase 0.
- Smallest unit: use an in-memory native queue and wake mechanism, without WebSocket framing. Validate the session capability accepted by the pinned compiler. Keep native byte storage separate from Bend terms; construct Bend values only on the runtime thread.
- Verification: `python3 scripts/ws/verify.py phase-1 --profile local-v1`, compiling and running the real Bend fixture, not a C-only simulation.
- Gate: 1,000 start/transfer/release/close cycles have zero live native allocations, descriptors, or helper threads left after each cycle. Empty/full queue admission, failed startup, stale generations, repeated shutdown, and close with a waiting receiver have defined results. Native-thread joins finish within one second after close initiation in this fixture. The ownership/fault suite passes with native memory instrumentation and sanitizers where the selected compiler supports them.
- Stop: an unusable handle representation, off-thread Bend heap access, lost wakeup, or unresolved leak blocks framing work. Revise the effect representation and rerun this same gate before building callers around it.

## Phase 2. Implement the incremental protocol core

- Outcome: correct wire messages independent of socket timing and Bend application logic.
- Owns: `src/ws/native/protocol.*` and protocol fixtures in `tests/ws/`. Depends on phase 1's resource and buffer representation.
- Smallest unit: implement HTTP upgrade state, frame reader/writer state, then fragmented-message and control handling. Test input bytes and exact serialized output without a real network first.
- Verification: `python3 scripts/ws/verify.py phase-2 --profile local-v1`.
- Gate: RFC accept vectors and negative upgrades pass; header splits are tested at every byte boundary; small frame fixtures are split at every boundary and larger frames use seeded random splits. Preserve a data frame coalesced with the upgrade response. Cover lengths 0, 125, 126, 65,535, and 65,536, configured limits, malformed lengths/opcodes/masks, fragmented UTF-8, Ping/Pong, and Close. Encode our frames and decode them with an independent reference, and test the reverse direction. Force one-to-seven-byte writes alternating with `WouldBlock`; the collected wire bytes must match the expected frame sequence exactly. All allocation limits are checked before allocation.
- Stop: a missing protocol case, changed message boundary, duplicate accepted frame, or incorrect partial-write suffix blocks socket integration. Keep codec timing as a diagnostic; self-roundtrip speed is not a release gate. Tungstenite's exact-wire trickle-write tests are the implementation reference. [Frame codec][ws-tungstenite-frame]

## Phase 3. Implement the full-duplex native socket worker

- Outcome: a working native client interoperates with independent local peers while keeping memory and service times bounded.
- Owns: `src/ws/native/transport.*`, native connection queues, and the conformance adapter. Depends on phases 1 and 2.
- Smallest unit: one client connection owned by one native readiness loop, with exact write offsets, wakeups, deadlines, and bounded incoming/outgoing admission. Add multiple connections only after the single-connection gate passes. Queue-full handling remains nonblocking for the native worker.
- Verification: `python3 scripts/ws/verify.py phase-3 --profile local-v1`. The verifier drives the production native client against Tungstenite and uWebSockets peers and against an Autobahn fuzzing server.
- Gate: freeze the complete non-extension client case selection in `local-v1` before implementation; missing or skipped required cases block the gate. All selected cases pass with exact case IDs recorded. Text cases are included; compression groups `12.*` and `13.*` are excluded explicitly. Group `9.*` limits/performance cases run separately with size limits and expected limit rejection recorded, and handshake-negative tests run outside Autobahn. Independent streams deliver at least one million validated messages each way, with zero unexpected loss, duplication, reordering, or corruption. Across 100 pause/fill/drain cycles, native retained memory never exceeds the derived budget and returns to its established idle allocation state. Under unsaturated service, readable/writable connections and pending controls are serviced within the profile's 10 ms test budget. A nonprogressing incoming or outgoing partial frame closes within its own configured deadline plus that service budget; unrelated traffic does not extend either deadline.
- Stop: failed conformance, a breached memory cap, or starvation of an eligible direction blocks the Bend API. A queued control frame is not considered eligible to bypass a partially written frame or unread TCP bytes. No heartbeat guarantee is made during indefinite receive saturation. [Autobahn coverage][ws-autobahn]

## Phase 4. Connect the real Bend message API

- Outcome: application bytes travel from a real Bend producer to the peer and from the peer to a real Bend consumer, while socket work remains independent of computation.
- Owns: `src/ws/main.bend`, `src/ws/effects/`, and `bench/ws/bend-*` fixtures. Depends on phase 3.
- Smallest unit: expose connect, bounded send admission, receive, explicit buffer release, status, and close. Add batched send/receive with count and byte limits. Distinguish not-admitted messages from accepted messages that are still buffered. Keep this API independent of BSATN and SpacetimeDB.
- Verification: `python3 scripts/ws/verify.py phase-4 --profile local-v1`.
- Gate: the real Bend path preserves payloads and message boundaries for at least one million messages each direction, including batched and unbatched transfer. Run a pure Bend computation whose measured effect-free interval is at least 100 ms. During that interval, prove native receipt through worker counters and send a previously queued result that the external peer validates before computation ends. Record runtime-boundary, worker-send, worker-receive, and peer-receipt timestamps; transport events must occur inside the effect-free interval, not before it. Exercise Ping/Pong and check phase 3's eligible-control service budget. Pause each receiver in turn; opposite-direction traffic must progress, memory stays bounded, and resumed consumption drains retained messages exactly once. Shutdown retains phase 1's resource guarantees.
- Stop: a C-only result, computation eliminated by optimization, receipt demonstrated only by kernel buffering, or accepted-but-not-consumed payload is insufficient evidence. Benchmark the native byte-to-Bend conversion and effects separately to locate costs before tuning.

## Phase 5. Tune and qualify local message throughput

- Outcome: the standalone library meets explicit throughput, latency, and memory targets on the frozen local profile.
- Owns: transport batching, buffer reuse, effect batching, and `bench/ws/` measurement adapters. Depends on phase 4; rerun earlier correctness gates after each change.
- Smallest unit: change one measured limiter at a time. Compare immediate writes with bounded batches; compare reusable buffers and transfer representations; coalesce wakeups where correct. Preserve WebSocket message boundaries when batching writes. Do not merge application records into one message to inflate messages/second.
- Verification: `python3 scripts/ws/verify.py phase-5 --profile local-v1`, followed by `python3 scripts/ws/verify.py all --profile local-v1` for phases 0 through 5.
- Gate: for W1 and W2 below, median native capacity is at least 80% of the equivalently configured Tungstenite client capacity against the same peer. Median full Bend-path capacity is at least 80% of our native-only capacity. Each paired ratio must be at least 75%; reference spread stays within phase 0's 5% noise budget. At half the lower of the compared capacities, native scheduled-send-to-peer-receipt p99 is at most 1 ms and Bend scheduled-producer-to-application-consumer p99 is at most 2 ms, excluding job computation. Five 60-second trials pass with zero unexpected rejection, deadline expiry, integrity failure, or growing queue backlog. W4 overload and W5 soak must also pass.
- Stop: these 80%, 75%, 1 ms, and 2 ms values are proposed engineering targets, not measured capabilities or universal standards. Phase 0 freezes them in `local-v1`; an infeasible target requires an explicit profile revision and explanation, never silent lowering after a failed run. Noise-dominated or peer-limited comparisons are inconclusive and block a pass. Capture a profile and counters that identify the limiter before accepting a performance conclusion.

## Phase 6. Validate the bounded computation pool

- Outcome: a local example reuses a configured number of BendRT processes and completes correlated jobs without breaking transport limits.
- Owns: `examples/ws-worker-pool/` and compute workloads in `bench/ws/`. Depends on phase 5. Pool scheduling and application job IDs remain outside `src/ws/`.
- Smallest unit: start with one process, a bounded job queue, and a deterministic computation with validated results. Add two and four processes only when machine capacity permits. Bound active jobs and reserve completed-result capacity before admitting work. Configure process count and CPU threads per process together.
- Verification: `python3 scripts/ws/verify.py phase-6 --profile local-v1`, followed by the cumulative `all` command.
- Gate: W3 and W4 never exceed configured process, queued-job, active-job, or result-byte limits; no worker is created per request. Normal W3 capacity and latency trials require exactly one validated successful result per admitted job and zero unexpected terminal failures. Fault-injection trials require exactly one terminal outcome per admitted job, matched to its injected fault. Failed and unfinished jobs remain visible in qualification results. Job IDs correlate variable-duration and out-of-order completion, while a partitioned fixture preserves its declared ordering. At 50% of the measured fixed-pool compute capacity, job p99 is no more than the matching compute-only p99 plus 2 ms. Both trials use identical scheduled arrivals, input order, queue limits, scheduling policy, process count, and thread budgets; both measure from scheduled job arrival until the benchmark coordinator validates its result. The compute-only path bypasses the WebSocket adapter using the reference local test channel; pure computation duration is reported separately. In a mixed long/short-job test, independent worker processes make observable progress while one runs a long job. At a scheduled job rate of 70% of compute-only capacity, transport p99 stays within phase 5's 2 ms budget on eligible unsaturated traffic. Shutdown completes within the configured drain deadline plus one second and leaves no child or native worker behind.
- Stop: arithmetic results without verified execution, unbounded spawning, duplicate dispatch, broken ordering, or CPU oversubscription blocks pool readiness. More processes need not increase throughput when one job already uses the machine's CPU/GPU capacity. Report process-count results rather than require a fabricated linear speedup.

## Real-world local workloads

These are proposed workload models, not production traces. Each produces deterministic payloads containing direction, connection, sequence or job identity, and a payload-integrity check. Metadata counts toward the stated message size. W1 and W2 establish transport capacity; W3 establishes compute behavior. Warmup, measurement, and drain have separate counts.

| Workload | Traffic and variations | Required evidence |
| --- | --- | --- |
| W1. Event or update stream | One-way 64-byte and 256-byte messages, reversing direction in a separate run; sweep 1 KiB and 16 KiB as diagnostics | Delivered messages/second, payload bytes/second, scheduled-arrival p99, queue occupancy, and CPU cost per message |
| W2. Independent full duplex | Both peers send 256-byte messages without waiting for replies; symmetric rates, then a 10:1 rate split; 1, 4, and 16 connections | Capacity and latency per direction and connection, fairness, and exact validated counts; an aggregate total cannot hide a stalled direction |
| W3. Requests plus background events | 256-byte jobs, 1 KiB results, and concurrent 64-byte events; deterministic compute inputs calibrated near 1, 10, and 100 ms, plus a mixed-duration distribution | Job latency including admission and computation, verified computation results, transport latency separately, process/thread count, and result-queue limits |
| W4. Bursts and slow consumers | Five times the chosen steady offered rate for 100 ms; stop each receiver for 10, 100, then 1,000 ms; repeat 100 times; use smaller socket buffers to force partial writes | Attempted/admitted/rejected/delivered counts, exact peak retained memory, opposite-direction progress, deadlines, and drainage. Once offered traffic stops and the peer resumes, admitted messages drain within two seconds in this finite fixture or have an explicit terminal connection failure |
| W5. Soak and lifecycle | Thirty minutes at half the qualified capacity; repeated connect/close and injected disconnects while data is queued | No growth in live resources across cycles; bounded memory; all terminal outcomes accounted for. Phase 1's instrumented idle state must be restored after each lifecycle cycle |
| W6. Framing and bulk diagnostic | 0, 125/126, and 65,535/65,536-byte payload boundaries; 1 MiB maximum-sized messages; single frames and fragments with controls | Exact payload preservation, allocation limits, mask/codec cost, syscall counts, and control behavior; bulk bandwidth is separate from small-message throughput |

The 0-byte and other tiny W6 frames use separate test-case counters because their payloads cannot hold the normal workload metadata. W6 is diagnostic, not a claim that a 1 MiB message rate represents typical requests.

## Measurement rules and initial limits

`local-v1` contains explicit starting policies. They are proposed test defaults, not production sizing claims:

| Policy | Initial profile value |
| --- | --- |
| Maximum application message and frame | 1 MiB, with fragment-count and header limits recorded separately |
| Native queue budget per direction per connection | 2 MiB and 4,096 descriptors; both limits enforced before admission |
| Control reservation | 4 KiB and 16 descriptors, outside the application queue budget |
| Batch sweep | 1, 8, 32, and 128 messages, capped at 64 KiB; compare immediate flush with maximum 50 and 200 microsecond wait policies. Larger single messages use the bounded fragmentation or direct-write path |
| Partial-frame progress deadline | 250 ms independently per direction; outgoing progress means bytes of that frame transmitted, incoming progress means bytes of that frame received. Unrelated traffic resets neither deadline |
| Connect, upgrade, and close deadlines | One second each for the local fixture |
| Generator model | Scheduled offered arrivals independent of replies; closed-loop ping-pong retained as a separate latency diagnostic |

The allocator budget includes incoming/outgoing queues, fragment assembly, retained partial writes, IO scratch space, batch storage, reserved controls, and descriptors. The implementation must derive and publish the exact maximum from its allocation layout. Queue high-water marks alone are not memory ceilings. Report instrumented native allocations, Bend heap use, process RSS, and kernel socket buffers separately; do not substitute RSS for exact native ownership accounting.

Capacity is the highest offered rate that passes the profile's latency, correctness, admission, and stable-backlog checks for the measurement interval. Use a common 2 ms transport p99 ceiling for reference, native-only, and Bend-path capacity searches; phase 5's half-load checks add the stricter 1 ms native target. W2 uses a rate vector, reporting both directions. Freeze comparable admission and memory budgets, TCP_NODELAY, flush policy, connection count, and CPU allocation. Reference defaults with unlimited buffering do not qualify as comparable configurations.

Compute-only capacity is workload-specific: the highest scheduled job rate that the fixed pool completes with validated successful results, no rejection or failure, and stable queues across the same trial interval. It is not transport capacity or CPU utilization. The paired job trials retain the same arrival schedule and latency endpoints so the measured difference includes WebSocket integration costs rather than a changed scheduler.

Count scheduled, attempted, admitted, transmitted, and peer-validated messages separately. Latency starts at scheduled arrival, not successful enqueue or the first socket write. Record generator lateness, rejected work, and deadline-expired work. A closed-loop sender slows when the system slows; its latency alone can conceal overload.

For reference comparisons, alternate baseline and candidate runs in paired order. For optimization commits, run the same workload on the parent implementation and changed head, with matching inputs and profile. If the parent lacks a new behavior, label that absence and validate the new path; do not represent missing behavior as zero throughput. After warmup, every admitted benchmark message must be validated or assigned an explicit terminal failure during the bounded drain.

A correctness fixture deliberately injects faults, but a capacity trial must have zero unexpected faults. Capture CPU profiles, allocations, syscalls, context switches, wakeups, queue levels, and peers' CPU consumption. Identify what prevents the measured rate from doubling. If the generator or peer is limiting, improve that fixture or report a lower bound rather than claim library capacity.

## Existing-library references

- Tungstenite's frame codec separates queued frames from unsent bytes and tests alternating partial writes with `WouldBlock`. Use it as a wire-behavior reference and a configured client baseline. Its application API distinguishes rejected frames from accepted frames awaiting flush. [Frame implementation][ws-tungstenite-frame], [protocol implementation][ws-tungstenite-protocol]
- uWebSockets batches writes in its native loop and exposes distinct `SUCCESS`, `BACKPRESSURE`, and `DROPPED` outcomes. Its admission threshold checks existing backpressure before the next send, so that threshold alone is not a strict prospective byte cap. Use its server as an independent peer with compression disabled and explicit bounds. [Socket implementation][ws-uws-socket], [send implementation][ws-uws-send]
- The inspected uWebSockets `load_test.c` sends the next message after receiving an echo-byte count and reports response rate. It does not validate message identity or provide independent offered-load latency. Its fixed-mask prebuilt frames are not a conforming production client. Use the benchmark as a construction example, not as the acceptance harness. [Load-test source][ws-uws-load]
- Autobahn supplies framing, UTF-8, control, and close conformance cases. Its documentation distinguishes fuzzing-server mode for testing clients from fuzzing-client mode for testing servers, and labels opening-handshake coverage as under development. The verifier therefore supplies separate handshake tests and saves exact case selection. Unsupported compression is excluded by name. [Test-suite instructions][ws-autobahn]

## Phase completion and later connector work

Each phase reruns preceding load-bearing correctness checks before proceeding. No phase passes from an agent's summary, a successful `send`, or a self-roundtrip alone. The verifier must read the actual peer results, resource counters, and case-level failures. Missing evidence is a failed or inconclusive gate.

Phase 5 qualifies the standalone local WebSocket library. Phase 6 qualifies its bounded computation-pool example. The [connector validation gates](bend-websocket-design.md#validation-gates-before-declaring-the-connector-usable) remain necessary for later SpacetimeDB integration. A live SpacetimeDB test must eventually confirm one reducer call, one persistent subscription, atomic updates, and uncertain-commit recovery. Those checks cannot be inferred from a synthetic job peer.

[ws-tungstenite-frame]: https://github.com/snapview/tungstenite-rs/blob/master/src/protocol/frame/mod.rs
[ws-tungstenite-protocol]: https://github.com/snapview/tungstenite-rs/blob/master/src/protocol/mod.rs
[ws-uws-socket]: https://github.com/uNetworking/uWebSockets/blob/master/src/AsyncSocket.h
[ws-uws-send]: https://github.com/uNetworking/uWebSockets/blob/master/src/WebSocket.h
[ws-uws-load]: https://github.com/uNetworking/uWebSockets/blob/master/benchmarks/load_test.c
[ws-autobahn]: https://github.com/crossbario/autobahn-testsuite/blob/master/README.md
