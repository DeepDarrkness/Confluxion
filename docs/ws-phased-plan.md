# WebSocket phased implementation plan

Connect BendRT reliably to SpacetimeDB while keeping application logic and authoritative schema declarations in Bend. Use Rust's Tungstenite WebSocket implementation behind a bounded native worker and a Bend API. Phases 1 through 4 qualify that transport boundary; the SpacetimeDB delivery track then adds Bend sessions, BSATN, generated bindings, and transactional module shims. Throughput tuning and the computation pool remain separate qualification work. An all-Bend WebSocket implementation is deferred to optional Phase 7.

Design: when changing socket ownership or computation scheduling, read the [selected worker model](bend-websocket-design.md#selected-worker-model).

## Current status

Status as of November 28, 2026. Phases 0 through 4 retain their recorded passes. The final dense Bend path also passed full Phase 4 qualification, including one million messages each direction, 100 pauses, and instrumented lifecycle restoration. Phase 3's recorded qualification includes all 301 selected Autobahn cases under the approved fixture rules. The temporary adapter and Python runtime are removed. All eight Phase 5 routine checks and the final five-pair fixed-load dense comparison pass. The bounded `local-perf-v2` campaign remains Blocked by sustained reference drift after three refinements. See the [dense retest checkpoint](phase5-retest.md#final-november-28-checkpoint). The status table is authoritative for phase progress.

| Phase | Status | Checkable outcome | Required gate before proceeding |
| --- | --- | --- | --- |
| 0 | Passed | Cargo owns native builds, million-message integrity each way, four fault controls, four reference-bound trials, and damaged-evidence rejection | Cargo correctness gate rerun passed November 24, 2026; [verification](ws-phase0-verification.md#completed-qualification), [evidence](../results/ws/cargo-1795507779180348826-69045/phase-0/summary.json) |
| 1 | Passed | Valid native resource/effect ownership | Post-cleanup rerun passed 1,000 real Bend cycles per instrumentation mode, resource baseline restoration, faults, and waiting-close November 24, 2026; [evidence](../results/ws/cargo-1795507824100303680-69116/phase-1/summary.json) |
| 2 | Passed | Qualified bounded Tungstenite protocol integration | Post-cleanup gate passed in the Phase 3 integration run November 24, 2026; [verification](ws-phase2-verification.md), [evidence](../results/ws/cargo-1795511937858175300-97357/phase-2/summary.json) |
| 3 | Passed | Independent full-duplex Rust transport | Real-peer interoperability, all 301 required cases under approved fixture rules, bounded overload, deadlines, and cleanup passed November 24, 2026; [verification](ws-phase3-verification.md), [evidence](../results/ws/cargo-1795512093612704375-99189/phase-3/summary.json) |
| 4 | Passed | Complete Bend message path | Real Bend million-message transfer each way, bounded batches, independent IO during pure computation, pauses, and cleanup passed November 24, 2026; [verification](ws-phase4-verification.md#integration-qualification), [evidence](../results/ws/cargo-1795520741553854400-164059/phase-4/summary.json) |
| 5 | Blocked | Eight parallel routine checks with cached builds; qualified dense correctness and five-pair fixed-load comparison | November 28 `local-perf-v2` campaign stopped inconclusively after 1,159.11 s at the three-refinement reference-drift limit. No stable five-set anchor, qualified ratios, half-load, headroom, or stability pass. [Campaign](../results/ws/cargo-1795848288875281882-30078/phase-5/campaign.json), [current checkpoint](#phase-5-checkpoint-november-28-2026). Historical `local-perf-v1` remains unpassed. |
| 6 | Not started | Bounded reusable computation workers | Pool limits, result correlation, compute correctness, and ordering pass under load |
| 7 | Deferred; optional | All-Bend WebSocket implementation behind the same Bend API | Revisit after reliable Rust-backed SpacetimeDB integration; qualify equivalent interoperability, independent progress, bounds, and cleanup before replacement |

Set a phase to `In progress` when implementation begins, `Blocked` when its stop condition prevents progress, and `Passed` only when its gate passes. A passed phase links its verification artifacts; a blocked phase records the failing check and next action. Advance after the current gate passes.

## Baseline and scope

Initial planning baseline on November 23, 2026: this repository contained design documents, with no WebSocket sources, build harness, or benchmark results. A local tool check found Rust/Cargo but did not find `bend` or `clang` on `PATH`. Revised Phase 0 establishes the compiler and reference correctness environment.

The first implementation targets Linux, IPv4 loopback, plaintext client connections, no compression, and persistent connections. Transport decision, November 24, 2026: reuse Tungstenite for handshake and framing rather than implement another protocol core. An embedded Rust static library owns an independent IO worker; a small C effect adapter connects it to BendRT. Application code does not depend on Rust or WebSocket types. The [transport-library decision](bend-websocket-design.md#transport-library-decision) records the selected shape; Phase 1 below records the qualified ownership ABI.

The [trusted-local access policy](confluxion-arch1.md#trusted-local-access-policy) applies to the SpacetimeDB delivery track: bind to `127.0.0.1`, connect without client credentials, and generate allow-all local access. Auth checks are not a Phase 1 or initial connector requirement. Schema, bounds, version, fencing, and atomic-commit checks remain required.

Tungstenite 0.28.0 is already pinned for the reference actor; Phase 1 pins the production worker's dependencies in the root Cargo lockfile. Tokio is not required for the initial dedicated-thread worker. uWebSockets remains an independent peer, and Autobahn supplies client conformance checks. Comparing two Tungstenite-based paths measures integration overhead, not independent protocol correctness. Production server support requires its own conformance gate.

The smallest implementation units are organized by ownership:

| Proposed location | Responsibility |
| --- | --- |
| `src/ws/native/` Rust crate | Tungstenite integration, configured limits, handshake policy, socket readiness, deadlines, native queues, and worker lifecycle |
| `src/ws/native/` private C ABI | Fixed-width status records and bounded byte transfer; Rust objects and allocations remain private |
| `src/ws/main.bend` and `src/ws/effects/` | Affine session API, bounded admission, batched transfer, release, and close |
| `examples/ws-worker-pool/` | Bounded job dispatch and reusable BendRT processes, outside the transport library |
| `tests/ws/` | Protocol vectors, fault injection, lifecycle checks, and conformance adapter |
| `bench/ws/` | Independent peers, workloads, compute fixtures, and machine profiles |
| `Cargo.toml` and `tests/ws/phase0.rs` | Build native fixtures and run the correctness gate through Cargo's Rust test framework |

Phase 0 supplies the Cargo test, reference peers, correctness profile, and artifact format. Phase 1 implements `src/ws/` ownership and real Bend lifecycle fixtures. Phase 2 implements the bounded protocol adapter and prepared-IO qualification. Phase 3 adds the real socket worker and Cargo qualification. Phase 4 qualifies the real Bend socket path. Phase 5 adds the scheduled controller and ignored performance qualification. Phases 6, 7, and the SpacetimeDB delivery track remain planned coverage. The current routine command does not qualify throughput or those future phases.

Each Phase 0 invocation writes `results/ws/cargo-<run-id>/phase-0/summary.json`, actor reports, the profile, native build artifacts, command lines, and trial results. Planned conformance phases also retain case-level Autobahn output. Results are evidence artifacts produced during implementation, not measurements established by this plan.

## Testing dependency policy

Routine testing uses `cargo test --release --locked` and Cargo's native Rust test runner. The default is the current phase's short correctness suite. Earlier phases are optional and selected when changed code touches their tested boundaries. Clang and prepared Bend/reference sources remain build inputs. Routine tests require no Python runtime, Autobahn installation, Docker, or separate test framework.

Autobahn's Python dependency is temporary protocol-qualification tooling, not a library or deployment dependency. Phase 3 conformance has already passed. Phase 4 removes the optional adapter and ignored Cargo test after focused real Bend qualification. Phases 4 onward do not require Autobahn unless a concrete protocol change requires renewed conformance, such as a protocol-library upgrade or changed handshake, framing, control, or UTF-8 behavior. Routine queue, Bend-effect, throughput, and computation-pool checks stay on the Rust and clang path.

Phase 4 qualification removed the temporary Autobahn Python runtime and optional test adapter. The recorded Phase 3 case reports remain as evidence. Later conformance work can use separately provisioned tooling if a concrete protocol change requires it. Earlier-phase correctness reruns in this plan mean the routine Cargo checks; they do not automatically reinstall or rerun Autobahn.

### Phase-scoped test selection

Each Cargo integration target requires its matching `phase0` through `phase5` feature. The features are independent. `phase5` is the default. Selecting a phase does not automatically select its predecessors.

```sh
# Current phase, routine correctness
cargo test --release --locked

# Specific earlier dependencies, example: ownership and protocol
cargo test --release --locked -p ws-reference --no-default-features --features phase1,phase2

# Current phase plus a changed native worker dependency
cargo test --release --locked -p ws-reference --features phase3

# Explicit complete Phase 4 qualification
cargo test --release --locked -p ws-reference --no-default-features --features phase4 --test phase4 phase4_full_qualification -- --ignored --exact --test-threads=1
```

For a single selected target, add `--test phaseN` and enable its feature. Disabled targets are not compiled or executed. `--all-features` opts into all earlier suites. Run ignored qualification tests separately from routine checks.

| Changed boundary | Optional earlier coverage |
| --- | --- |
| Compiler, native build, reference integrity, or evidence validation | Phase 0 |
| Capability handles, lease ownership, allocation accounting, release, or shutdown ownership | Phase 1 |
| Handshake, framing, UTF-8, configured limits, or partial-write adapter | Phase 2 |
| Socket scheduling, queue admission, controls, duplex progress, or directional deadlines | Phase 3 |

Choose only the earlier suites relevant to the diff and record that choice. Implementation dependencies do not impose an automatic cumulative test run. Phase 4 routine checks transfer 4,096 messages each direction, including 1,024 singles and 3,072 batch messages, and run six receiver pauses and eight ownership lifecycle cycles. They retain focused fault, boundary, lease, saturation, and effect-free computation checks. Sanitizers and bulk-boundary campaigns run only in the ignored full qualification, alongside one million messages each way, 100 pauses, and 1,000 lifecycle cycles per instrumentation mode. Routine evidence is labeled `routine`; it does not replace full phase qualification evidence.

## Phase 0. Establish native builds and reference correctness

- Outcome: reproducible native builds and a small correctness suite that cannot mistake enqueueing for delivered work.
- Owns: the root Cargo workspace, `bench/ws/`, and the native integration test in `tests/ws/`. Depends on no implementation phase.
- Smallest unit: select the Bend pin and compatible native compiler, build optimized reference client and peer adapters, then run two separate processes on loopback. Use length-delimited TCP for reference-bound checks. Pin actual dependencies; defer Autobahn setup until Phase 3 needs it.
- Verification: `cargo test --release --locked -p ws-reference --no-default-features --features phase0 --test phase0`. The command also compiles and runs a minimal native Bend program with clang, establishing that the chosen compiler works before phase 1.
- Gate: a fresh native Bend ELF and exact reference vectors pass. Receiver counts, sequence IDs, and every payload byte agree for one million messages in each direction. Four duplicate and corruption controls fail at the exact injected identity. Four paused-receiver trials prove bounded rejection and drainage over WebSocket and TCP. Damaged real evidence cannot pass. One native Rust integration test owns all results.
- Stop: a missing compiler, count or identity mismatch, incorrect fault result, breached resource bound, or missing evidence fails the suite. This revised Phase 0 does not qualify noise, sustained headroom, latency, or throughput.

The user removed the original long benchmark gate to keep testing minimalist. Historical failed and interrupted qualification artifacts remain unchanged. A pass here does not turn those runs into passing performance evidence.

## Phase 1. Prove native effect ownership

- Outcome: Bend can safely own a persistent Rust worker service and exchange bounded byte payloads through the actual effect ABI.
- Owns: minimal `src/ws/main.bend`, `src/ws/effects/`, the Rust static library and private C ABI, native resource lifecycle, and lifecycle fixtures. Depends on phase 0.
- Smallest unit: link the Rust static library through the C effect adapter and use an in-memory native queue and wake mechanism, without WebSocket framing. Validate the session capability accepted by the pinned compiler. Keep Rust-owned byte storage separate from Bend terms; construct Bend values only on the runtime thread. Define copy, admission, and release ownership and contain Rust panics at the C boundary.
- Verification: select `--no-default-features --features phase1 --test phase1` with `cargo test --release --locked`. The real Bend ownership fixture supplies the gate, not a C-only simulation.
- Gate: 1,000 start/transfer/release/close cycles leave zero client-owned live native allocations, descriptors, or helper threads after each cycle and restore the measured Bend runtime baseline. Empty/full queue admission, failed startup, stale generations, repeated shutdown, and close with a waiting receiver have defined results. Native-thread joins finish within one second after close initiation in this fixture. The ownership/fault suite passes with native memory instrumentation and sanitizers where the selected compiler supports them.
- Stop: an unusable handle representation, off-thread Bend heap access, lost wakeup, or unresolved leak blocks framing work. Revise the effect representation and rerun this same gate before building callers around it.

Completed November 24, 2026. The [recorded qualification](../results/ws/cargo-1795505106060709584-53967/phase-1/summary.json) passed 1,000 real Bend cycles in each ownership and ASan/UBSan mode, 28 fault records, and 11 Bend boundary checks. Every cycle restored zero client-owned native resources and the measured Bend baseline of two blocks and 24 bytes. The same invocation passed Phase 0.

The implementation uses affine `Session`, `Closer`, and `Lease` wrappers around Base `File` handles, generation-checked registry keys, eight 256-byte native cells, and four descriptors with 512 queued bytes per direction. The independent close capability wakes a parked receiver; leases remain explicitly releasable after shutdown. Only the runtime thread constructs Bend values.

`tests/ws/phase1.rs` retains the real Bend fixture, live allocation accounting, affine duplication rejection, damaged-evidence checks, and the omitted-release leak control. It selects Cargo's static archive using the linked build marker. Clang ASan/UBSan cover Bend and C; Rust allocations are intercepted, but Rust code is not compiler-sanitizer-instrumented. The sanitizer build uses `-O0` for the pinned runtime's calling-convention constraint.

Testing cleanup removed the redundant `42` ABI smoke fixture, Rustup fallback self-test and diagnostics, synthetic stale-archive files, duplicate C fault cases, and empty Cargo unit/doc-test targets. The retained suite defines 22 fault records and 11 Bend boundary checks; report-damage controls run once rather than once per instrumentation mode. The separate qualification report and temporary design/probe artifacts were removed. Historical results remain unchanged. Tests were not rerun at cleanup time, at the user's request. The Phase 2 integration run subsequently passed the retained suite in both instrumentation modes.

## Phase 2. Qualify the Rust WebSocket protocol integration

Completed November 24, 2026. The [qualification report](ws-phase2-verification.md) records the adapter API, configured bounds, independent wire checks, and derived dependency allocation ceiling. The full Cargo suite passed Phases 0, 1, and 2 in the integration checkout.

Subsequent cleanup removed upstream-only parser probes, repeated admission and invalid-Close checks, an unused test lifetime, and the session's temporary design files and implementation worktree. Adapter rejection cases and allocation checks remain in the Cargo gate. Tests were not rerun after cleanup, at the user's request.

- Outcome: configured Tungstenite behavior preserves correct wire messages independently of Bend application logic.
- Owns: Tungstenite configuration, handshake/subprotocol policy, IO adapter, and integration fixtures in `tests/ws/`. Depends on phase 1's resource and buffer representation.
- Smallest unit: use the pinned Tungstenite implementation for upgrade, incremental framing, masks, fragmentation, UTF-8, controls, and close. Configure frame, message, read-buffer, and write-buffer limits. Test the actual adapter with prepared read/write sources before real networking; do not copy or fork the protocol core.
- Verification: select `--no-default-features --features phase2 --test phase2` with `cargo test --release --locked`. Upstream tests are references, not a substitute for testing our integration.
- Gate: RFC accept vectors and negative upgrades pass; header splits are tested at every byte boundary; small frame fixtures are split at every boundary and larger frames use seeded random splits. Preserve a data frame coalesced with the upgrade response. Cover lengths 0, 125, 126, 65,535, and 65,536, configured limits, malformed lengths/opcodes/masks, fragmented UTF-8, Ping/Pong, and Close. Decode emitted frames with an independent reference and test the reverse direction; another Tungstenite instance alone is not independent evidence. Force one-to-seven-byte writes alternating with `WouldBlock`; the collected wire bytes must match the expected frame sequence exactly. Admission bounds apply before enqueueing, configured limits reject excess data, and dependency allocations count toward the derived native memory budget.
- Stop: a missing required integration case, changed message boundary, duplicate accepted frame, or incorrect partial-write suffix blocks socket integration. Keep codec timing as a diagnostic; self-roundtrip speed is not a release gate. The dependency retains unsent wire bytes; the adapter must not resubmit accepted messages after `WouldBlock`. [Frame codec][ws-tungstenite-frame]

## Phase 3. Implement the full-duplex Rust socket worker

Completed November 24, 2026. The [qualification report](ws-phase3-verification.md) records the native API, memory derivation, real peers, and case-level evidence. All required checks pass; none were skipped or remain pending. The operator approved a two-second conformance timeout for six official cases with one-second fragment delays and narrowly defined `NON-STRICT` protocol-close outcomes for seven cases. Production partial-frame deadlines remain 250 ms. The exact selection and fixture rules are frozen in `local-v1`.

- Outcome: a working native client interoperates with independent local peers while keeping memory and service times bounded.
- Owns: the Rust worker in `src/ws/native/`, native connection queues, and the conformance adapter. Depends on phases 1 and 2.
- Smallest unit: one nonblocking client connection owned by one Rust readiness loop, with Tungstenite retaining partial-write state. Instrument actual byte progress for independent per-direction deadlines. Drive reads, pending writes, and automatic Pong/Close flushing without blocking the opposite direction. Add multiple connections only after the single-connection gate passes. Queue-full handling remains nonblocking for the native worker.
- Verification: Cargo drives the production native client against Tungstenite and uWebSockets peers. The initial qualification also ran the official Autobahn fuzzing server. Its historical reports remain evidence after Phase 4 removes the temporary adapter under the testing dependency policy above.
- Gate: freeze the complete non-extension client case selection in `local-v1` before implementation; missing or skipped required cases block the gate. All selected cases pass with exact case IDs recorded. Text cases are included; compression groups `12.*` and `13.*` are excluded explicitly. Group `9.*` limits/performance cases run separately with size limits and expected limit rejection recorded, and handshake-negative tests run outside Autobahn. Independent streams deliver at least one million validated messages each way, with zero unexpected loss, duplication, reordering, or corruption. Across 100 pause/fill/drain cycles, native retained memory never exceeds the derived budget and returns to its established idle allocation state. Under unsaturated service, readable/writable connections and pending controls are serviced within the profile's 10 ms test budget. A nonprogressing incoming or outgoing partial frame closes within its own configured deadline plus that service budget; unrelated traffic does not extend either deadline.
- Stop: failed conformance, a breached memory cap, or starvation of an eligible direction blocks the Bend API. A queued control frame is not considered eligible to bypass a partially written frame or unread TCP bytes. No heartbeat guarantee is made during indefinite receive saturation. [Autobahn coverage][ws-autobahn]

## Phase 4. Connect the real Bend message API

Completed November 24, 2026. The [integration qualification](ws-phase4-verification.md#integration-qualification) records the public API, ownership bounds, measured computation interval, independent receipt, and full Cargo rerun. All routine gates for Phases 0 through 4 passed in the main checkout.

- Outcome: application bytes travel from a real Bend producer to the peer and from the peer to a real Bend consumer, while socket work remains independent of computation.
- Owns: `src/ws/main.bend`, `src/ws/effects/`, and `bench/ws/bend-*` fixtures. Depends on phase 3.
- Smallest unit: expose connect, bounded send admission, receive, explicit buffer release, status, and close. Add batched send/receive with count and byte limits. Distinguish not-admitted messages from accepted messages that are still buffered. Keep this API independent of BSATN and SpacetimeDB.
- Verification: `cargo test --release --locked` runs the short real Bend message path. Select `phase4_full_qualification` with `--ignored --exact` for the complete gate described below.
- Gate: the real Bend path preserves payloads and message boundaries for at least one million messages each direction, including batched and unbatched transfer. Run a pure Bend computation whose measured effect-free interval is at least 100 ms. During that interval, prove native receipt through worker counters and send a previously queued result that the external peer validates before computation ends. Record runtime-boundary, worker-send, worker-receive, and peer-receipt timestamps; transport events must occur inside the effect-free interval, not before it. Exercise Ping/Pong and check phase 3's eligible-control service budget. Pause each receiver in turn; opposite-direction traffic must progress, memory stays bounded, and resumed consumption drains retained messages exactly once. Shutdown retains phase 1's resource guarantees.
- Stop: a C-only result, computation eliminated by optimization, receipt demonstrated only by kernel buffering, or accepted-but-not-consumed payload is insufficient evidence. Benchmark the native byte-to-Bend conversion and effects separately to locate costs before tuning.

## Phase 5. Tune and qualify local message throughput

### Approved bounded campaign, November 27, 2026

The user approved a one-hour campaign replacing the routine Phase 5 closure matrix. `local-perf-v2` retains the 80% median and 75% paired ratios, 1 ms native outbound and 2 ms Bend half-load latency, 5% reference-noise limit, 1.25-times generator/peer headroom, and existing integrity/resource limits. It qualifies W1 64-byte messages in both directions and one-connection symmetric W2 with five alternating capacity sets at 5 seconds warmup and 20 seconds measurement. Each comparison retains five alternating half-load pairs at 5 seconds warmup and 10 seconds measurement. The other ten cases receive 3-second warmup and 5-second screening only, not qualified capacity claims.

W4 runs once for each of the twelve candidate, receiver, and pause conditions. W5 runs 180 seconds per candidate with three persistent normal-close/queued-disconnect cycles. This is short stability evidence, not a 30-minute soak guarantee. Unchanged Phases 1–4 protocol, ownership, million-message, repeated-pause, and lifecycle qualifications are reused. Earlier checks are selected only if their boundary changes.

The campaign reserves one endpoint CPU pair and has a 3,600-second deadline. Calibration has a six-minute cap, using two minutes of the original seven-minute preparation/reserve allowance; the overall limit remains one hour. Reference noise uses fresh passing/failing boundaries, not repeated delivery at one fixed rate. Accepted capacity, half-load, and headroom streams require at least 10,000 measured observations. Missing evidence or exhausted budgets are inconclusive. `WS_PHASE5_EXTENDED=1` selects historical `local-perf-v1`; it is outside this duration limit.

After Phase 5 passes, complete the single-worker SpacetimeDB delivery track before Phase 6. Phase 7 remains optional.

### Phase 5 checkpoint, November 28, 2026

The approved P5 task plan and deferred BS task plan are recorded in [phase5-retest.md](phase5-retest.md#approved-plan-of-action). P5-1 through P5-4 are complete. P5-5 through P5-8 have implementation and evidence-review progress, but their qualification closure remains Blocked.

Final selected results:

- Five semantic codec checks and both native BSATN/raw-be fixtures passed with cache hits.
- Full dense Phase 4 qualification passed in 106.16 s. [Evidence](../results/ws/cargo-1795844941452124051-22546/phase-4/summary.json).
- All eight Phase 5 routine checks passed in 10.83 s after 10.41 s Cargo compilation, with one fixture build and seven hits.
- Five final fixed-load pairs passed in 253.11 s with a fixture cache hit. At 12,000 messages/s per direction, median outbound p99 was 212 µs baseline and 157 µs dense. Inbound p99 was 260 µs and 228 µs. Whole-trial client CPU was 11.825 s and 6.893 s. These compare the complete fixture paths, not isolated production-adapter costs or qualified capacities. [Comparison and ranges](phase5-retest.md#final-november-28-checkpoint).
- Final bounded qualification ended inconclusively in 1,159.11 s. Short calibration used 111.09 s. Fresh 20-second reference boundaries drifted and exhausted three refinements. Zero anchors completed. Numerical targets, the 360-second calibration limit, and the 3,600-second campaign limit remain unchanged.

The controller now establishes sustained passing/failing boundaries before paired ratios, retains typed outcomes, and restarts all five sets after an affected boundary refinement. Overload reports distinguish sequence gaps from corruption and retain exact identities. Fault accounting preserves conservative partial-flush completion counts. Normal capacity trials still require exact delivery. Routine lifecycle tests retain cutoff/count checks without enabling qualification-only generator performance checks. Queued-disconnect sessions explicitly pause application receipt so native-owned queued data remains observable.

The frozen CPUs 0 and 1 are sibling threads on physical core 0. Resolve reference drift before another campaign. A new profile using distinct physical cores needs approval; changing topology is not a guaranteed fix. Do not lower the reference denominator or increase retries silently. Complete stable anchor sets, ratios, half-load latency, headroom, screens, W4, W5, and artifact-bound limiter assessments before marking Phase 5 Passed. The single-worker SpacetimeDB connector remains next, ahead of Phase 6.

### Phase 5 checkpoint, November 27, 2026

The approved campaign was run twice. The first invocation ended inconclusively after 19.38 seconds because its calibration parser did not recognize a reference rejection outcome. At 500,000 scheduled messages per second per direction, the peer reached 4,096 descriptors and rejected messages; the client's forward sequence gap followed that recorded saturation. The retry recognizes only this specific, accounted reference outcome as a failed-rate boundary. Arbitrary actor failures and invalid observations still make calibration inconclusive. User-only call chains also fixed `perf record` access under WSL2 without changing kernel settings.

The final [campaign](../results/ws/cargo-1795774532680549456-6542/phase-5/summary.json) failed after 126.86 seconds, with a 126.88-second whole-command elapsed time and a fixture cache hit. Calibration used 88.39 seconds and produced unqualified candidates of 228,125 Reference, 92,968 Native, and 16,288 Bend messages per second per direction. The first reference capacity trial then ran 5 seconds of warmup and 20 seconds of measurement at 228,125 per direction. Both actors exited successfully, but scheduled-arrival p99 was 4.939 ms toward the peer and 7.291 ms toward the client. Both exceed the unchanged 2 ms capacity ceiling. See [trial 0022](../results/ws/cargo-1795774532680549456-6542/phase-5/trial-0022/trial.json) and [trial results](../results/ws/cargo-1795774532680549456-6542/phase-5/trials.json).

Five complete comparison sets, independent reference-noise qualification, paired ratios, half-load latency, headroom, W1 anchors, remaining screens, W4, and three-minute W5 were not established. The run stopped at the first failed qualification gate rather than consuming its one-hour allowance. No worker, fixture, or profiler process remained after cleanup inspection. No unchanged earlier phase was rerun.

Selected correctness evidence:

| Check | Result | Build and cache evidence |
| --- | --- | --- |
| Initial seven routine checks | Six passed; Bend overload/lifecycle was inconclusive from missed fault-burst arrivals | 9.77 s Cargo compilation; 11.02 s test time; one fixture build and six cache hits for that input set |
| Isolated Bend overload/lifecycle rerun | Passed in 2.82 s | 0.02 s Cargo preparation; cache hit; [summary](../results/ws/cargo-1795773934837839360-5387/phase-5/summary.json) |
| Final affected evidence controls | Passed in 2.35 s | 9.68 s Cargo compilation; cache hit; [summary](../results/ws/cargo-1795774530305659457-6490/phase-5/summary.json) |
| Real-peer cancellation control | Passed; cancellation reaped the waiting reference peer and its process group | 6.26 s test time including the final fixture input-set build; [evidence](../results/ws/cargo-1795774315191024784-6014/phase-5/cancellation-control/summary.json) |
| Final qualification retry | Failed the reference latency gate in 126.86 s | 0.01 s Cargo preparation; final fixture cache hit; [build identity](../results/ws/cargo-1795774532680549456-6542/phase-5/build/identity.json) |

`strace` 6.8 is installed. The working profiling binary is `/usr/lib/linux-tools/6.8.0-151-generic/perf`; the `/usr/bin/perf` wrapper does not match the running WSL2 kernel. CPU sampling requires `-g --user-callchains`. The final [diagnostic collection](../results/ws/cargo-1795774532680549456-6542/phase-5/limiter.json) has readable profiles and software counters for each path and peer. Native failed during its intrusive `strace` trial. These short profiles do not establish a complete evidence-backed limiter assessment or qualified capacities.

Next action: use the retained reference evidence to establish a repeatable 20-second capacity boundary, then rerun the affected comparison. Investigate time-query, generation, scheduling, and peer costs from profiles without assuming which is limiting. Keep the frozen targets, preserve previous failures, and select only affected checks after a change. Phase 5 remains Blocked until the complete bounded gate passes.

### Historical extended campaign

The default Cargo command runs seven independent scheduled delivery, integrity, bounds, and cleanup checks with shared cached builds. The seven-test run passed before the latest scoped fixes. Full qualification failed the long-capacity gate after the frozen refinement limit. Native soak was interrupted after a lifecycle-validator fault; the queued Bend soak did not start. The latest calibration command was interrupted during Cargo compilation. The [controller reference](ws-phase5-verification.md) records the frozen profile, commands, and failing gate. Performance targets remain unchanged.

- Outcome: the standalone library meets explicit throughput, latency, and memory targets on the frozen local profile.
- Owns: transport batching, buffer reuse, effect batching, and `bench/ws/` measurement adapters. Depends on phase 4; select earlier correctness suites only for boundaries affected by a change.
- Smallest unit: change one measured limiter at a time. Compare immediate writes with bounded batches; compare reusable buffers and transfer representations; coalesce wakeups where correct. Preserve WebSocket message boundaries when batching writes. Do not merge application records into one message to inflate messages/second.
- Verification: extend the Cargo suite with real Phase 5 checks and a separately frozen performance profile. No performance controller remains in revised Phase 0.
- Gate: first qualify references with five paired trials per W1 and W2 core workload, 10 seconds of warmup, and 60 seconds of measurement. Reference throughput spread must be at most 5% of its median, and the generator and peer must sustain at least 1.25 times the chosen operating rate. Then median native capacity must be at least 80% of the equivalently configured Tungstenite client capacity against the same peer. Median full Bend-path capacity must be at least 80% of our native-only capacity. Each paired ratio must be at least 75%. At half the lower of the compared capacities, native scheduled-send-to-peer-receipt p99 is at most 1 ms and Bend scheduled-producer-to-application-consumer p99 is at most 2 ms, excluding job computation. Five 60-second trials pass with zero unexpected rejection, deadline expiry, integrity failure, or growing queue backlog. W4 overload and W5 soak must also pass.
- Stop: these 80%, 75%, 1 ms, and 2 ms values are proposed engineering targets, not measured capabilities or universal standards. Phase 5 freezes its performance profile before comparison; an infeasible target requires an explicit revision and explanation, never silent lowering after a failed run. Noise-dominated or peer-limited comparisons are inconclusive and block a pass. Capture a profile and counters that identify the limiter before accepting a performance conclusion.

### Phase 5 checkpoint, November 24, 2026

Work is paused for documentation and temporary-file cleanup. Phase 5 is **Blocked**, not Passed. No benchmark or fixture process remains running.

Implemented changes:

- Independent `phase5` Cargo feature and default target.
- Seven separately selectable routine tests, isolated ports and evidence, and bounded CPU-pair leases.
- Shared native fixture caching with artifact checksums, per-input builder locks, parallel compilation units, and atomic publication. The final cache key fingerprints the fixture build recipe and C generator rather than unrelated test bodies.
- Frozen performance profile, scheduled native and real Bend actors, short calibration, explicit long paired trials, bounded overload, persistent lifecycle checks, and checkpoint-driven soak.
- `AGENTS.md` rules for scoped reruns, cached builds, independent correctness tests, and reserved performance CPU budgets.
- Reference-peer close handling fixed after overload exposed reuse of a closed socket.
- Injected-disconnect lifecycle trials retain missed arrivals without applying the normal W4 generator-capacity gate. Steady soak cancellation on lifecycle failure is implemented.

Verified results:

| Selected check | Result | Evidence |
| --- | --- | --- |
| Phase 5 routine target | Seven tests passed in 9.88 s after 7.63 s Cargo compilation; one fixture build, six cache hits | [Bend four-connection](../results/ws/cargo-1795535350842106141-203228/phase-5/summary.json), [native duplex](../results/ws/cargo-1795535350842135987-203228/phase-5/summary.json), [Bend overload](../results/ws/cargo-1795535350842124053-203228/phase-5/summary.json), and other test-owned directories from that invocation |
| `phase5_reference_duplex` after the peer-close fix | Passed in 1.58 s with a cache hit, using CPUs 2 and 3 while qualification reserved 0 and 1 | [Summary](../results/ws/cargo-1795536489676483852-205108/phase-5/summary.json) |
| Full qualification | Failed after 635.09 s at the frozen long-capacity refinement limit; five passing pairs were not established | [Summary](../results/ws/cargo-1795536337824877458-204758/phase-5/summary.json), [trials](../results/ws/cargo-1795536337824877458-204758/phase-5/trials.json) |
| `overload_lifecycle` after the injected-fault rule and cancellation edits | Both native and Bend tests passed in 8.22 s after 7.62 s Cargo compilation; one build, one cache hit | [Native](../results/ws/cargo-1795537984389109583-208933/phase-5/summary.json), [Bend](../results/ws/cargo-1795537984389100334-208933/phase-5/summary.json) |
| Native standalone soak | Interrupted; no 30-minute pass | [Interruption record](../results/ws/cargo-1795537370992791852-207644/phase-5/interruption.json) |
| Bend standalone soak | Cancelled while waiting for the reserved CPU pair; no traffic trial started | No pass |
| Latest `phase5_calibrate_soak` | Interrupted during Cargo compilation after the final build-recipe fingerprint edit | No final checkpoint or pass |

The final recipe-fingerprint edit has not been executed. Cancellation after a lifecycle failure has not been exercised in a completed soak. Previously passed unchanged phases were not rerun during the test-cleanup work.

Resume in this order:

1. Run the build/cache probe against the final source: `cargo test --release --locked --test phase5 phase5_fixture_build_probe -- --ignored --exact --nocapture`.
2. Regenerate the measured-rate checkpoint with `phase5_calibrate_soak`. Old checkpoints can have a different fixture key and must not be relabeled as final-build measurements.
3. Run `phase5_standalone_soak` with `WS_PHASE5_SOAK_CHECKPOINT` pointing to that new checkpoint and `WS_PHASE5_SOAK_PATH=Native`, then `Bend`. Verify every persistent lifecycle cycle, exact outcomes, resource restoration, and the complete steady interval.
4. Capture CPU profiles and counters to identify the long-trial limiter, establish repeatable capacity, and rerun the affected qualification. `perf` and `strace` must be provisioned again; this session's temporary tools were removed. This host supports `cpu-clock:u` and software counters, but not hardware cycles or instructions.
5. Mark Phase 5 Passed only after the approved bounded noise, headroom, paired-ratio, half-load latency, W4, and W5 gates pass. Complete the single-worker SpacetimeDB delivery track afterward, before Phase 6. The historical extended profile remains separate qualification.

## Phase 6. Validate the bounded computation pool

- Outcome: a local example reuses a configured number of BendRT processes and completes correlated jobs without breaking transport limits.
- Owns: `examples/ws-worker-pool/` and compute workloads in `bench/ws/`. Depends on phase 5. Pool scheduling and application job IDs remain outside `src/ws/`.
- Smallest unit: start with one process, a bounded job queue, and a deterministic computation with validated results. Add two and four processes only when machine capacity permits. Bound active jobs and reserve completed-result capacity before admitting work. Configure process count and CPU threads per process together.
- Verification: extend the Cargo suite with pool checks and select earlier suites for changed transport, ownership, or build boundaries.
- Gate: W3 and W4 never exceed configured process, queued-job, active-job, or result-byte limits; no worker is created per request. Normal W3 capacity and latency trials require exactly one validated successful result per admitted job and zero unexpected terminal failures. Fault-injection trials require exactly one terminal outcome per admitted job, matched to its injected fault. Failed and unfinished jobs remain visible in qualification results. Job IDs correlate variable-duration and out-of-order completion, while a partitioned fixture preserves its declared ordering. At 50% of the measured fixed-pool compute capacity, job p99 is no more than the matching compute-only p99 plus 2 ms. Both trials use identical scheduled arrivals, input order, queue limits, scheduling policy, process count, and thread budgets; both measure from scheduled job arrival until the benchmark coordinator validates its result. The compute-only path bypasses the WebSocket adapter using the reference local test channel; pure computation duration is reported separately. In a mixed long/short-job test, independent worker processes make observable progress while one runs a long job. At a scheduled job rate of 70% of compute-only capacity, transport p99 stays within phase 5's 2 ms budget on eligible unsaturated traffic. Shutdown completes within the configured drain deadline plus one second and leaves no child or native worker behind.
- Stop: arithmetic results without verified execution, unbounded spawning, duplicate dispatch, broken ordering, or CPU oversubscription blocks pool readiness. More processes need not increase throughput when one job already uses the machine's CPU/GPU capacity. Report process-count results rather than require a fabricated linear speedup.

## SpacetimeDB delivery track

Start after Phase 4 establishes the reliable Bend/Rust message path. Phases 5 and 6 do not block the first usable single-worker SpacetimeDB connection. A deployed computation pool still requires its own Phase 6 qualification. This track is not yet implemented.

1. Generate concrete BSATN contracts, Bend bindings/codecs, and Rust module tables and reducer registrations from explicit Bend schema, export, and persistence declarations. Apply the trusted-local access policy. Compare the built module schema with those contracts; reject unsupported declarations or disagreement.
2. Implement the Bend-owned SpacetimeDB session over complete binary messages from the Rust worker. Connect anonymously, negotiate the pinned protocol, decode connection metadata, handle the server compression prefix, correlate calls, and publish consistent subscription snapshots and complete transaction updates.
3. Generate transactional Rust shims that validate declared commit conditions, write rows and checkpoints atomically, and record durable operation outcomes. Demonstrate one real Bend computation, reducer call, subscription update, and disconnect/reconciliation path against persistent SpacetimeDB. Verify that an anonymous reconnect with a new connection identity can reconcile the same durable operation without duplicate effects. Application developers do not hand-maintain a second Rust schema or business algorithm.
4. Provide one build workflow for the Bend application, native Rust transport, generated WASM module, and frontend bindings. Qualify the [connector gates](bend-websocket-design.md#validation-gates-before-declaring-the-connector-usable) and [shim targets](bend-shim-design.md#verification-targets) relevant to the deployed single-worker configuration before declaring it usable; qualify pool-specific gates when adding a pool.

## Phase 7. Revisit an all-Bend WebSocket implementation

- Status: deferred and optional. Rust transport is the initial delivery path.
- Outcome: a Bend implementation can replace the Rust WebSocket implementation without changing application declarations, the public Bend API, BSATN contracts, or generated SpacetimeDB shims.
- Depends on: a reliable Rust-backed SpacetimeDB connector and measured reasons to revisit the transport. Phase 7 is not a prerequisite for that connector.
- Smallest unit: probe the scheduling and packed-byte representation required for independent IO before implementing the protocol. A separate Bend transport process or future runtime support may be needed; current same-process Base TCP cannot progress during long pure computation.
- Gate: qualify equivalent protocol interoperability, independent duplex progress, memory bounds, cleanup, and real SpacetimeDB behavior. Compare performance against the qualified Rust path before replacement. No replacement capability or speedup is established by this plan.
- Stop: a shared-event-loop stall, schema change caused only by transport replacement, or failed equivalence gate keeps Rust as the production transport. See the [deferred Bend implementation constraints](bend-websocket-design.md#deferred-bend-implementation).

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

The following policies are proposed defaults for later implementation and qualification, not production sizing claims. Revised `local-v1` contains only Phase 0 dependency pins and process deadlines; native reference bounds remain enforced in the endpoints.

| Policy | Proposed starting value |
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

Each change runs the current phase's relevant checks and selects older suites only for affected dependencies. Full qualification runs explicitly when qualifying a phase or when changed behavior invalidates its recorded gate. No phase passes from an agent's summary, a successful `send`, or a self-roundtrip alone. The verifier must read the actual peer results, resource counters, and case-level failures. Missing evidence is a failed or inconclusive gate.

Phase 5 qualifies standalone transport performance. Phase 6 qualifies its bounded computation-pool example. The SpacetimeDB delivery track begins after Phase 4 and must independently confirm a reducer call, persistent subscription, atomic updates, and uncertain-commit recovery. Those checks cannot be inferred from a synthetic job peer. Optional Phase 7 changes only the transport implementation after the Rust-backed connector is reliable.

[ws-tungstenite-frame]: https://github.com/snapview/tungstenite-rs/blob/master/src/protocol/frame/mod.rs
[ws-tungstenite-protocol]: https://github.com/snapview/tungstenite-rs/blob/master/src/protocol/mod.rs
[ws-uws-socket]: https://github.com/uNetworking/uWebSockets/blob/master/src/AsyncSocket.h
[ws-uws-send]: https://github.com/uNetworking/uWebSockets/blob/master/src/WebSocket.h
[ws-uws-load]: https://github.com/uNetworking/uWebSockets/blob/master/benchmarks/load_test.c
[ws-autobahn]: https://github.com/crossbario/autobahn-testsuite/blob/master/README.md
