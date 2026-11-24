# Phase 3 socket worker qualification

Phase 3 passed on November 24, 2026. The [qualification summary](../results/ws/cargo-1795512093612704375-99189/phase-3/summary.json) records seven passing checks. All 301 selected Autobahn cases ran. No required checks were skipped or remain pending.

## Native client

`src/ws/native/src/worker.rs` owns one nonblocking IPv4 connection on an independent Rust thread. `poll` watches the socket and an `eventfd` wakeup. Each turn caps socket IO at 64 KiB per direction and application work at 32 messages. Full queues stop admission or further reads without parking the worker on queue capacity.

`Client::connect` performs TCP connection and Upgrade. `offer` copies binary or text bytes only after admission. `receive` returns one complete message. `status` reports progress, allocations, queue occupancy, and terminal state. `request_close` starts shutdown, and `close` performs a bounded, repeatable join. `Client` supports concurrent producer and consumer access.

Tungstenite owns accepted partial-write suffixes and automatic Pong and Close frames. `src/ws/native/src/protocol.rs` tracks actual ingress and egress frame bytes independently. Complete controls and opposite-direction traffic do not reset a stalled data-message deadline. The adapter also validates text incrementally, so invalid UTF-8 in an unfinished frame fails before that frame completes.

Protocol, UTF-8, and size failures send Close 1002, 1007, and 1009 when output remains possible. Shutdown preserves already-received messages until the caller drains them. Terminal sessions reject new sends.

## Qualified limits

`bench/ws/profiles/local-v1.json` contains the authoritative limits and exact case selection.

| Policy | Value |
| --- | --- |
| Maximum frame and aggregate message | 1,048,576 bytes each |
| Application queue per direction | 2,097,152 bytes and 4,096 descriptors |
| Reserved control allowance | 4,096 bytes and 16 descriptors |
| Unsaturated read, write, and control service budget | 10 ms |
| Partial-frame progress deadline | 250 ms independently per direction |
| Connect, Upgrade, and Close deadline | 1 second each |
| Worker stack reservation | 262,144 bytes |

Before another receive, the worker reserves room for one maximum-sized message. A stream of 1,024-byte messages therefore saturates receive admission at 1,049,600 retained queue bytes. This is expected backpressure, not an increased queue limit. Tungstenite coalesces automatic controls within its smaller audited storage allowance rather than allocating the full control reservation.

## Allocation accounting

The connection-storage ceiling extends the [Phase 2 allocation derivation](ws-phase2-verification.md#derived-native-allocation-ceiling). Let $Q$ be bytes per queue, $D$ descriptors per queue, $d$ the descriptor layout, $C$ the Core Arc layout, $T$ captured target and subprotocol capacities, and $F$ the configured frame limit.

$$B=19,974,752+2(Q+Dd)+C+T+F+128.$$

The two queue byte arrays and descriptor arrays have fixed capacities. The formula includes their actual layouts, the Core and Arc counters, captured String capacities, a frame-sized scratch buffer, and a bounded authority allowance. The Phase 2 term includes dependency buffer growth, fragment assembly, controls, request storage, and the thread-local random generator.

The default overload fixture has a 25,349,449-byte ceiling. Every observed peak stays below that ceiling, and all 100 cycles restore the established idle allocation state after maximum-size warmup. The scope counts allocator requests and allocate-copy-free reallocations. Returned message vectors become caller-owned storage.

Rust thread-start bookkeeping is outside the connection scope. A separate global allocator check includes that bookkeeping and TLS teardown: all 100 socket lifecycle cycles restore the same live bytes, live blocks, file descriptors, and thread count after dropping the client. The fixture baseline is 79,831 bytes, 1,189 blocks, four descriptors, and two threads. This baseline belongs to the Cargo test process, not the client. Stack reservations, allocator overhead, process RSS, and kernel socket buffers are separate from requested connection heap layouts. This qualification does not report an RSS or kernel-buffer ceiling.

Post-close scoped snapshots can retain a conservative 352-byte random-generator residual because the scope finishes before TLS teardown. The global post-join checks verify that this allocation actually releases. Queued received data remains accounted until drained.

## Gate results

| Check | Result |
| --- | --- |
| Native lifecycle | 100 real connection cycles restore global allocations, descriptors, and threads; post-close receive and repeated close pass |
| Tungstenite interoperability | 1,000,000 independent 256-byte messages validate in each direction |
| uWebSockets interoperability | 1,000,000 independent 256-byte messages validate in each direction |
| Real negative Upgrades | 13 cases pass, including protocol policy, header limits, and Upgrade timeout |
| Eligible service | 100 real-wire read, write, and Pong samples meet the 10 ms budget |
| Partial progress | Incoming and outgoing stalls terminate within 250 ms plus 10 ms while unrelated traffic continues |
| Bounded overload | 100 pause, fill, and drain cycles pass in both directions, with opposite-direction progress and two-second drainage |
| Autobahn conformance | All 247 selected cases pass the recorded acceptance policy |
| Autobahn limits | All 54 selected cases pass: 30 require official success and 24 require native size-limit rejection |

Peer results validate sequence, direction, message boundaries, and every payload byte. The uWebSockets peer is a fresh clang-built native fixture with compression disabled. The independent streams do not wait for replies.

## Approved Autobahn fixture rules

The official server runs from cached revision `9afe7cf359e27cc6bdadc2a3d6926439b215abc9`. Cargo owns server startup, client driving, report verification, and cleanup. The operator authorized the upstream Python server as a temporary external peer. There are no custom Python tests or separate test runners.

Compression groups `12.*` and `13.*` are excluded by name. All selected case IDs remain required, including text. Group `9.*` runs separately with each case's message and frame sizes recorded. Oversized cases require native `Limit` termination. Their original official outcomes remain available.

The operator approved two scoped rules after the first real run exposed conflicts with asynchronous clients:

- Cases `5.19`, `5.20`, and `6.4.1` through `6.4.4` use a two-second conformance stall timeout. Their official stimuli wait one second between fragments. Production configuration and the independent deadline tests retain 250 ms.
- Cases `3.2`, `3.3`, `4.1.3`, `4.1.4`, `4.2.3`, `4.2.4`, and `5.15` may return official `NON-STRICT`. Acceptance requires the case's explicit empty-echo alternative, Close 1002, successful official close behavior, native protocol failure, and evidence that the preceding valid message reached the receive queue. New sends remain rejected after terminal failure.

The pinned suite defines `7.1.6`, `7.13.1`, and `7.13.2` as informational. These cases run and retain their raw outcomes. No `FAILED`, skipped, missing, or unapproved non-strict case passes the conformance gate.

Official Python 2 reports can contain lone UTF-16 surrogate escapes in invalid-text wirelogs. The verifier preserves the original report and writes a sidecar that represents only those characters as literal escape text. It records every affected byte offset. Valid surrogate pairs retain their meaning, and malformed JSON still fails verification.

The [conformance results](../results/ws/cargo-1795512093612704375-99189/phase-3/autobahn-conformance/results.json) and [limits results](../results/ws/cargo-1795512093612704375-99189/phase-3/autobahn-limits/results.json) link every exact ID to its raw case report and native snapshot.

## Verification commands and preceding gates

The full Cargo invocation passed [Phase 0](../results/ws/cargo-1795511886178421683-94105/phase-0/summary.json), [Phase 1](../results/ws/cargo-1795511931031429310-94265/phase-1/summary.json), and [Phase 2](../results/ws/cargo-1795511937858175300-97357/phase-2/summary.json) against the final native implementation. Phase 3 then needed report-reader and fixture-policy corrections. The final focused Phase 3 invocation passed. Earlier failing artifacts remain unchanged.

The initial qualification used PyPy 7.3.11, Python 2.7.18, extracted OpenSSL 1.1.1w, and the pinned upstream requirements. Phase 4 removed that temporary runtime after qualification.

## Cleanup and routine testing

The temporary worker probe under `/tmp/opencode/ws-worker-probe`, its temporary documents, the runtime verification script, startup artifacts, installer caches, and temporary decision log were removed after qualification. The qualification reports and production fixtures remain.

Phase 3 is now an optional suite selected with `--no-default-features --features phase3 --test phase3`. It uses the Rust toolchain and clang for the prepared native fixtures. `native_socket_correctness` retains the real peers, allocation and lifecycle checks, overload, controls, negative handshakes, and directional deadlines. The [phase selection policy](ws-phased-plan.md#phase-scoped-test-selection) describes when to select it.

The post-cleanup full Cargo run passed all four routine tests with Autobahn and Python environment settings unset. The optional Autobahn test was ignored as intended. The [native-only summary](../results/ws/cargo-1795513231362273636-107641/phase-3/summary.json) records `scope: native-correctness`. Formatting and diff checks passed. Autobahn itself was not rerun during cleanup; its earlier qualification evidence remains valid.

Phase 4 removed `autobahn_client_conformance` and its temporary Python tooling. Renewed conformance work requires separately provisioned tooling for a concrete protocol change. A native-only run does not claim new Autobahn evidence.

The original final focused command used `-p ws-reference --test phase3 -- --nocapture` before this separation. Its passing 301-case evidence remains unchanged. Phase 4 can now connect the real Bend message API to this native client.
