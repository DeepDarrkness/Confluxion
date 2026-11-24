# Phase 5 controller reference

Phase 5 implements scheduled message checks and explicit performance qualification. The user initially selected fast checks, then requested full qualification and soak. Phase 5 is **Blocked** because full qualification failed its long-capacity gate. The performance targets remain unchanged.

## Cargo targets

`cargo test --release --locked` selects the independent `phase5` feature by default. Seven independent routine tests run real scheduled transfers, evidence controls, overload, and lifecycle checks. They do not search capacity or enforce benchmark latency ratios.

The explicit long entry point is:

```sh
cargo test --release --locked -p ws-reference --test phase5 phase5_full_qualification -- --ignored --exact --test-threads=1
```

Failed or inconclusive qualification exits unsuccessfully and leaves `phase_pass: false` in its summary.

`WS_CACHE` selects the existing pinned Bend, uSockets, and uWebSockets sources. The default location is `.ws-cache`. Actor reports belong to a fresh `results/ws/cargo-<run>-<pid>/phase-5` directory for each test.

Native fixtures are cached in `target/ws-fixtures/<key>`. The key includes source, dependencies, native archive identity, toolchain, and compiler flags. Each reuse verifies artifact checksums. A per-key builder lock protects publication. A cache miss compiles independent peer units with at most four builders and distinct object files. Each test retains the build identity and cache-hit result.

Correctness tests lease separate CPU pairs, with at most four groups active. Performance runs exclusively reserve their selected pair for the entire comparison. Each test owns its ports, child process groups, and evidence files. The final recipe fingerprint and its cache reuse remain unexecuted after the user's checkpoint request.

## Scheduled actors

`tests/ws/phase5.rs` owns the fixed profile, trials, process groups, evidence validation, capacity search, and scripted peer. Each process owns its report. The controller alone writes the combined summary.

W1 and W2 use the existing independent uWebSockets server. The reference client is the existing Tungstenite executable. The native client uses `phase5-native.c` and the existing socket ABI. The Bend client is a real ELF compiled from `bend-phase5.bend`.

The native and Bend fixtures use the same finite schedule and payload contract in `phase5-common.h`. An arrival is attempted once, regardless of admission or replies. Rejection does not cause a hidden retry. A prospective `payload + 14` charge stays outstanding until worker transmission completes.

W4 generators check the 100 ms cutoff at the offer boundary, after payload construction. Reports contain `last_offer_ns` and `generator_missed`. Missed scheduled arrivals are not admission rejections. A nonzero missed count makes full qualification inconclusive. Short checks verify the cutoff but do not qualify generator capacity.

The scripted peer publishes its final admitted inbound count through `WS_PEER_ADMITTED`, tagged with the trial start timestamp. Normal close waits for that count to reach application validation and for admitted outbound messages to finish transmission. `allow_gaps` permits rejected sequence identities, not incomplete drainage. Unfinished identities require an injected disconnect or worker timeout code 16 or 17. Peer reports preserve the actual operation, error kind, OS error, and timestamp. Generic peer errors alone cannot justify unfinished delivery.

The Bend producer builds the stamped header and body as Bend byte lists. The receive probe decodes identity metadata and constructs an expected recipe. Bend compares every application byte against that recipe. The `consumed` effect forces the comparison before it records the receive timestamp, then Bend releases the lease. Native receipt and Bend application consumption are separate events.

One message per application effect is the frozen baseline. Native receive uses the batch ABI with a count of one. No batching optimization is selected without qualification evidence. Both fixtures retain the existing 128-message and 64 KiB effect limits.

The reference client keeps its existing multi-connection process. Native and Bend use one persistent process per connection because the production socket registry permits one session per process. All client processes and native workers share one selected CPU. The peer owns the other selected CPU. The frozen machine report records this topology rather than treating additional processes as additional CPU budget.

Reference changes are opt-in fixture controls. `WS_EARLY_DRAIN=1` permits exact completed trials to end after measurement. The server also requires peer closure. Without that setting, Phase 0 retains its full drain and evidence rules. `WS_LONG_MEASURE=1` raises only the independent server's measurement limit to 1,800 seconds for soak.

## Routine checks

The short target checks:

- One scheduled 256-byte duplex transfer through each of the reference, native, and Bend paths.
- Four simultaneous real Bend connections on the same client CPU.
- Corruption detected by real Bend validation at sequence 50.
- One shared evidence-control test rejects a damaged validated count and a missing stream. Deterministic sample-evaluator controls derive a labeled synthetic timing fixture from real delivery evidence. They do not impose performance timing on routine checks.
- Finite 100 ms overload with 10 ms peer and 100 ms application receiver pauses, opposite-direction progress, and bounded drainage.
- One lifecycle cycle per candidate in the routine test, with a normal close and a queued disconnect in the same process. Qualification repeats the persistent cycle. Both paths check explicit admitted-identity outcomes, joined close, and idle ownership restoration.

Reports distinguish scheduled, attempted, admitted, worker-transmitted, validated-in-window, validated-after-window, and rejected counts. Bounded identity ranges identify admitted and validated messages in overload and disconnect trials. Their set difference gives terminal identities. Unknown validated identities fail the check.

Native layout-derived budget, exact native peak and final live ownership, Bend live and peak allocation, C scratch, allocation counts, RSS, kernel buffers, context switches, and worker progress are separate fields. Before its first connection, the Bend fixture observes its C IO-work and Bend continuation allocation baselines. The short run measured 64 C bytes and 16 Bend bytes in this compiler layout. Every cycle compares against those original values. Final native live ownership is zero. Bend and C live ownership return to their measured baselines. Per-cycle facts and Bend and C peaks reset between connections. Native allocation counts exclude earlier cycles.

Cycle reports publish through atomic rename. The controller checks the same PID, increasing cycle number, original allocation baseline, descriptor restoration, and thread restoration for each report. The persistent barrier line contains the start timestamp and symmetric session rate. Normal-close sessions use at most `routine_steady_rate` and must have no worker or peer failure. Queued-disconnect sessions retain the high burst rate. W5 uses the same persistent lifecycle actor alongside the continuous traffic actor, rather than starting a new runtime for each cycle.

The private `ws_socket_fixture_evidence` getter exposes existing worker snapshots. It adds no worker scheduling or protocol change. Partial ages, service gaps, and worker turns are measurement evidence. The 16 MiB Bend allocation-observer bitmap is instrumentation storage, separate from the 32 MiB application allocation ceiling.

## Ignored qualification

`bench/ws/profiles/local-perf-v1.json` is frozen before comparison. Its original 80% median, 75% per-pair, 1 ms native outbound, and 2 ms Bend targets remain unchanged.

The full controller includes:

- W1 at 64 and 256 bytes in each direction.
- W2 at 256 bytes, symmetric and both 10:1 orientations, with 1, 4, and 16 connections.
- One short bracketed calibration search per path and core case, with 1 second of warmup and 3 seconds of measurement. These trials select candidate rates, not qualified capacity.
- Five alternating paired capacity trials per core case, with 10 seconds of warmup and 60 seconds of measurement at the calibrated candidate rates. A failed long trial supplies a refinement bracket. Refinement restarts all five pairs, with a frozen maximum of three refinements.
- Finite exponential bracketing and binary refinement. A passing ceiling without a failing bracket is inconclusive.
- Per-stream latency and fixed tail-backlog envelopes. Each sample must fall within 10 ms after its scheduled boundary. Delayed catchup samples fail. Backlog uses sample field 2, the in-window validated count, rather than field 1, the total validated count. Drain receipt cannot increase capacity.
- Reference spread at most 5%, paired ratios, and five further paired half-load latency trials for each comparison.
- Actual TCP generator and peer trials at 1.25 times the chosen rate vector. A WebSocket peer above the frozen CPU ceiling still blocks qualification.
- W4 5-times steady offered rate for exactly 100 ms, each receiver paused for 10, 100, and 1,000 ms, with 100 repetitions per condition and 16 KiB requested socket buffers.
- Directional partial-frame observations, timed incoming partial-frame faults, and outgoing expiry timing from the existing worker snapshot. Missing outgoing partial-write activity or expiry evidence is inconclusive.
- W5 continuous 30-minute half-load traffic through each candidate, with the same per-stream latency, timely samples, and stable-backlog checks as capacity trials. One persistent lifecycle actor repeatedly reconnects, closes, and disconnects queued data on the same client CPU. That actor restores its original pre-connection ownership after every cycle. Fault bursts are separate from the steady half-load schedule.
- Diagnostic 1 KiB and 16 KiB W1 transfers. These do not establish core capacity.
- Separate actual diagnostic runs under `perf record`, `perf stat`, and `strace -f -c`. CPU profiles use the supported `cpu-clock:u` event. Software counters record task time, context switches, migrations, and faults. Every client process and the peer receive profiles and counters, including multiconnection cases. Unavailable permissions, unreadable profiles, or missing syscall evidence make qualification inconclusive.

`limiter-case-*.json` retains per-case diagnostic directories, capacities, assessment, and missing evidence. `limiter.json` contains the latest case. Producing readable `perf` output does not constitute a limiter assessment.

The optional local file `bench/ws/profiles/local-perf-v1-limiter-assessments.json` contains an array of assessments. Each assessment names the exact `workload` string, `capacities` array, and complete frozen `profile` object. Its `actors` array covers each client and peer for Reference, Native, and Bend. Each actor names `path`, `actor`, `limiter`, `why_not_double`, `source`, and `ruled_out_generator_and_peer`. Its `evidence` array contains `kind`, `file`, and `observation` for `profile`, `counters`, `syscalls`, and `actor-report`. File paths are local, relative to the repository root or absolute. Each file must match that actor's current diagnostic artifact and each observation must occur in that artifact. Stale runs and other actors' files cannot satisfy the check. These records hold the source mapping and the evidence-backed explanation of why capacity cannot double. No limiter assessment has been established. Missing, incomplete, or mismatched assessment makes qualification explicitly inconclusive after diagnostic collection.

Peer drainage uses scalar `last_receipt_ns` across all validated messages. The first 4,096 receipt timestamps remain bounded progress evidence, not the drainage deadline authority.

Partial-start timestamps derive from the worker's already-recorded partial age and the controller's monotonic observation. The frozen 10 ms service allowance covers snapshot and polling delay. The independent incoming partial-frame fault also retains the peer's actual first-write timestamp.

The full target is compiled, not qualified by shortened trials. No throughput, noise, headroom, or soak result is established by routine success.

## Verification selection

Phase 5 routine checks exercise the new actors, evidence getter, and controller. Older Phase 0 coverage was selected earlier for reference changes. It was not repeated during the scoped test cleanup. Production worker, protocol, and Bend effects are unchanged, so Phase 4 was not rerun for this fixture-only addition.

The tightened November 24 routine command passed seven independent tests in 9.88 seconds after 7.63 seconds of Cargo compilation. One fixture build served all seven tests, with six cache hits. Each test-owned directory retains its separate result. Phase 0 was not rerun during this cleanup.

The first full invocation [failed](../results/ws/cargo-1795535394015966534-203602/phase-5/summary.json) after 40.10 seconds when the reference peer used a closed socket during overload calibration. The reference close callback now clears that pointer and records the terminal outcome. A later foreground invocation was interrupted by the tool timeout and is not a pass.

The corrected [full invocation](../results/ws/cargo-1795536337824877458-204758/phase-5/summary.json) failed after 635.09 seconds with `long capacity failed after frozen refinement limit`. It did not establish five passing long pairs. Its [trial records](../results/ws/cargo-1795536337824877458-204758/phase-5/trials.json) retain each calibration and long result. No median ratio, reference-noise, headroom, or complete Phase 5 performance pass is established.

The [measured checkpoint](../results/ws/cargo-1795536337824877458-204758/phase-5/capacities.json) selects 59,390 native messages per second per direction and 8,874 Bend messages per second per direction for separate 30-minute soaks. These are half-load rates derived from short calibration candidates, not qualified capacities. Each standalone soak revalidates its source reports and build identity and leaves `phase_pass: false`.

The additional ignored targets are `phase5_fixture_build_probe`, `phase5_calibrate_soak`, and `phase5_standalone_soak`. The build probe checks reuse without transferring application data. Standalone soak requires `WS_PHASE5_SOAK_CHECKPOINT` and `WS_PHASE5_SOAK_PATH`, which selects `Native`, `Bend`, or `both`. Separate native and Bend invocations reserve the same CPU pair sequentially, so either path's failure cannot skip the other requested run.

## Paused checkpoint

The native soak was interrupted after the lifecycle validator rejected missed arrivals in an injected-disconnect trial. The queued Bend invocation was stopped before its traffic trial began. Neither run passed a 30-minute soak. The [native interruption record](../results/ws/cargo-1795537370992791852-207644/phase-5/interruption.json) distinguishes the stopped invocation from its runner-written `running` summary.

The fault-trial rule and prompt steady-actor cancellation are now implemented. Both scoped `overload_lifecycle` tests passed afterward. The final build-recipe fingerprint edit followed that run; its next calibration command was interrupted during compilation. It has no execution pass yet.

The [phase-plan checkpoint](ws-phased-plan.md#phase-5-checkpoint-november-24-2026) is the current resume record. Temporary worktrees, sketches, helper scripts, logs, and locally extracted profiling tools from this session were removed. Implementation files, cached native fixtures, and result evidence remain. No tests are running.

All qualification artifacts remain local and ignored by Git. No commit or external-state operation is part of this implementation.
