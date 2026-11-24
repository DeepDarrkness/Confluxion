# Phase 1 native ownership qualification

Phase 1 qualifies a persistent Rust in-memory worker through real Bend C effects. The Cargo suite passed on November 24, 2026. The [Phase 1 summary](../results/ws/cargo-1795505106060709584-53967/phase-1/summary.json) records the build identity, limits, instrumentation, and coverage. The same invocation passed all nine preceding [Phase 0 trials](../results/ws/cargo-1795505060696688630-53818/phase-0/summary.json).

## Reproduce the qualification

Run the existing Cargo suite from the repository root:

```sh
CC=/usr/lib/llvm-19/bin/clang \
CXXFLAGS=--gcc-install-dir=/usr/lib/gcc/x86_64-linux-gnu/13 \
cargo test --release --locked
```

`tests/ws/phase1.rs` builds the ownership fixture with Bend 2.0.40, emits C, and links Cargo's Rust static library with clang. The test selects the archive by the build marker of its linked Rust dependency. A stale-archive control verifies that historical artifacts cannot change the selection. Compiler discovery accepts `WS_CACHE`, `WS_BEND`, `BEND`, `CC`, and `CLANG`, following Phase 0's precedence. Rustup diagnostics are optional.

The test runs the same real Bend fixture in instrumented ownership and clang AddressSanitizer/UndefinedBehaviorSanitizer modes. Each native command has a 60-second process deadline. Results are written to `results/ws/cargo-<run-id>/phase-1/`.

## Implemented ownership contract

`src/ws/main.bend` exposes affine `Session`, `Closer`, and `Lease` wrappers around Base `File` handles. The pinned compiler supports these wrappers. The native ABI probe returns `42` through a real effect, and a compile-negative fixture rejects duplicated ownership. Bend imports expose wrapper constructors, so the native registry also validates key generations and roles.

`start` returns a session and an independent close capability. `offer` copies bytes only on successful admission. Full or rejected offers return the original Bend list. `take` copies one complete native payload into Bend values on the runtime thread and returns a lease. Releasing the lease restores native cell capacity. A zero-byte payload is distinct from an empty queue.

`stop` consumes the close capability, wakes a waiting receiver, and joins the worker. `Session.release` retires the remaining session. `close` performs both operations and returns their status codes. Leases retain their cells after shutdown and remain explicitly releasable. Dropping an affine value does not close native resources.

The Rust worker owns packed bytes and never receives Bend terms. A fixed generation registry carries keys within the compiler's 56-bit handle encoding. One service has eight 256-byte cells, four descriptors and 512 queued bytes per direction, and two notification eventfds. Worker commands use a condition variable. The implementation uses no detached Bend `io_work` helper threads.

## Gate results

Both [ownership records](../results/ws/cargo-1795505106060709584-53967/phase-1/ownership.json) and [sanitizer records](../results/ws/cargo-1795505106060709584-53967/phase-1/asan-ubsan.json) passed the following checks:

- 1,000 real Bend start, transfer, lease-release, and close cycles per mode. Each cycle validates an empty payload, `[0, 255, 128, 0]`, and a 256-byte payload containing every byte value.
- Zero Rust live allocations, cells, registry entries, client descriptors, and worker threads after every cycle. OS descriptor and thread counts return to the warmed baseline.
- Exact restoration of the cycle continuation's Bend baseline, two live blocks and 24 bytes. The emitted-C allocation baseline is three blocks and 185 bytes. The terminal Bend checkpoint has zero live blocks and bytes.
- Every recorded native close-to-join interval is below one second. Completion includes actual OS task retirement after Rust join.
- 28 recorded fault cases and 11 Bend boundary checks. Coverage includes empty queues, descriptor-full, byte-full, cell-full, invalid bytes, oversized input, failed startup at five stages, wrong roles, foreign handles, stale generations, repeated shutdown and release, queued close, panic containment, and close with a confirmed parked Bend receiver.
- A held Bend lease preserves exact copied bytes after shutdown and releases the remaining native ownership afterward.
- A retained Bend list changes the live-block observer. Deliberately omitting an adapter argument release makes the same validator fail. Damaged records, native ownership counts, delivery counts, and fault outcomes are rejected.

The observed Rust allocation peak is 2,576 bytes. Summed per-layout allocation peaks give a 2,577-byte bound for the qualified layout, below the fixture's 4,096-byte ceiling. These counts include the core allocation and Rust thread bookkeeping. The 2,048-byte payload storage, 400-byte static registry, 256 KiB worker stack, Bend corpus, observer storage, process RSS, and OS resources are recorded separately. Queue occupancy and RSS are not substitutes for live allocation accounting.

## Instrumentation and sanitizer coverage

The Cargo test instruments the emitted CPU runtime's actual `heap_alloc` and `heap_free` paths. Checked source anchors fail if the pinned compiler changes. An address/class ledger detects duplicate or mismatched ownership accounting. Direct emitted-C allocation calls and all Rust `GlobalAlloc` events have separate ledgers.

Clang ASan and UBSan instrument the Bend runtime and C adapters. ASan intercepts Rust System allocation calls, but the installed stable Rust toolchain does not supply compiler-instrumented Rust sanitizer coverage. The summary records that distinction. Both builds enable `BEND_POISON`.

The sanitizer build uses `-O0`. Clang 19's backend fails stack realignment for the optimized runtime's `preserve_*` calling conventions with ASan. The ordinary ownership build uses `-O2`. This qualification establishes lifecycle correctness rather than throughput.

## Scope and next gate

Phase 1 passes native effect ownership with bounded in-memory transfer. Phase 2 still needs configured Tungstenite handshake and framing qualification. This fixture does not establish WebSocket interoperability, SpacetimeDB sessions, full-duplex socket behavior, or performance targets.
