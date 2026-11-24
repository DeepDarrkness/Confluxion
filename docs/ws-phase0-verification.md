# Phase 0 verification

Phase 0 checks native builds and reference correctness. The user replaced the original multi-hour benchmark gate with this smaller suite on November 23, 2026. Throughput qualification is deferred to Phase 5. The original baseline, noise, and headroom gate has not passed.

## Run the tests

Run the native Rust test framework from the repository root:

```sh
cargo test --release --locked
```

The workspace builds the actual Tungstenite reference binary and runs `tests/ws/phase0.rs` as one serial integration test. There is no nested Cargo invocation or separate test runner. Root `Cargo.lock` pins the Rust dependency graph.

Tests require Linux, two allowed CPUs, Rust/Cargo, clang, standard platform C/C++ headers and libraries, and the prepared Bend/uWebSockets/uSockets fixtures. The tests do not download dependencies. Missing fixtures or compilers fail; no live checks are skipped. Clang compiles each uSockets C source and both C++ reference sources directly, then links their object files with the standard C++ library and pthreads. `LIBUS_NO_SSL` and `UWS_NO_ZLIB` disable TLS and compression. Native testing does not invoke Python, git, make, an archiver, a GCC executable, or a package manager, and does not link zlib.

`WS_CACHE` selects the source cache, defaulting to `.ws-cache`. `WS_BEND` or `BEND` selects the Bend executable; otherwise the test uses `.ws-cache/bend/bin/bend` or `bend` on `PATH`. `CC` or `CLANG` selects clang, defaulting to `clang` on `PATH`. An optional `CXXFLAGS` contains whitespace-separated C++/link flags for a relocated compiler. Stock runner clang discovers the system C++ installation automatically; no distro path or package-version check is built into the test.

## Prepare local fixtures

Install clang if it is not already available. From the repository root, prepare the pinned upstream sources and Bend archive:

```sh
mkdir -p .ws-cache
git clone https://github.com/uNetworking/uWebSockets.git .ws-cache/uWebSockets
git -C .ws-cache/uWebSockets checkout --detach 05e6c8b55b4d28293cf23570f0f61318f8600560
git clone https://github.com/uNetworking/uSockets.git .ws-cache/uSockets
git -C .ws-cache/uSockets checkout --detach 166a04810c9fa970fc22096a7b35b792378364fb
curl --fail --location --output .ws-cache/bend.tar.gz https://github.com/bendlang/bend/releases/download/v2.0.40/bend-2.0.40-linux-x64.tar.gz
printf '%s  %s\n' c9ff8d67bd903fb6ad9a26b43d8a62c01021de82a44d55ce4a3dcd4732113d6b .ws-cache/bend.tar.gz | sha256sum --check
tar -xzf .ws-cache/bend.tar.gz -C .ws-cache
cargo test --release --locked
```

For existing checkouts, use the checkout commands rather than cloning again. `bench/ws/profiles/local-v1.json` records the upstream and Bend pins and lifecycle deadlines. Fixture preparation selects the source revisions and checks the archive SHA-256; the test checks Bend version. Compiler hashes, host Rust version strings, distro package versions, and a second lockfile hash are not test prerequisites.

## Coverage

The integration test builds once and supervises two independent native actors on separate allowed CPUs:

- A fresh Bend ELF prints a per-run sentinel. The release-built Tungstenite and uWebSockets actors agree with the exact payload vector.
- Both directions independently schedule 50,000 messages per second for 20 seconds. Each receiver validates all 1,000,000 messages, including payload bytes and sequence identity. Counts and identity digests must agree.
- Four 1-second trials duplicate or corrupt sequence 50 in each direction. Ordinary success validation must fail; the exact receiver fault, receive count, and exit status must match. Missing evidence cannot count as successful fault rejection.
- Four 1-second trials pause each receiver for 200 ms over WebSocket and TCP, using 1 MiB messages. They must force rejection, respect the 2 MiB queue, 4,096-descriptor limit, and 128 MiB tracked-heap ceiling, and drain admitted work.
- Damaged copies of the actual million-message evidence must reject a missing report, count mismatch, malformed report, and false successful fault control.

The Rust test framework owns the verdict. Build, lifecycle, evidence, and assertion failures make Cargo fail. RAII guards kill and reap children on returned errors, deadlines, and panic unwinding. Actors receive a shared same-host `CLOCK_MONOTONIC` start time only after exact readiness messages. Reports must match the current token, start/end times, complete stream identities, count types, finish deadline, and resource limits.

## Evidence and limitations

Each run creates `results/ws/cargo-<run-id>/phase-0/`. It retains a compact summary, the profile, build and actor commands, build stdout/stderr, a fresh Bend source/binary, native object files/binary, and vector evidence. Each of nine trials retains its plan, both actor reports, statuses, stderr, and validation result. The summary starts with `phase_pass=false` and changes to `true` only after every live and damaged-evidence assertion passes.

Native reports still contain histograms, but this suite makes no latency or throughput claim. These references are not the later Confluxion library. Allocations inside dynamically linked native libraries remain outside the tracked allocator wrappers.

Historical ignored evidence remains unchanged, including the previously passing correctness suite `phase0-minimal-correctness`, the failed original noise-gate run `phase0-qualification-reviewed`, and the interrupted qualification run `phase0-qualification-complete`. Those runs do not verify the Rust migration or establish completed throughput qualification.

## Completed qualification

Phase 0 passed on November 24, 2026. `phase0_native_correctness` passed under Cargo with no failed, ignored, or filtered integration tests. The [fresh summary](../results/ws/cargo-1795497449219896432-3075/phase-0/summary.json) records `phase_pass=true`, nine passing trials, and `damaged_evidence_rejected=true`.

The exact command on this host was:

```sh
CC=/usr/lib/llvm-19/bin/clang CXXFLAGS=--gcc-install-dir=/usr/lib/gcc/x86_64-linux-gnu/13 cargo test --release --locked
```

Clang 19 is installed outside `PATH`. Its default GCC 14 selection lacks C++ headers on this host, so `CXXFLAGS` selects the installed GCC 13 headers and libraries. The initial run failed at C++ compilation; the corrected run built fresh native fixtures and passed. No implementation or test-source changes were needed.

Preparation confirmed both cached upstream revisions against `local-v1`, verified the Bend archive SHA-256, and restored Bend 2.0.40. The fresh build verified Bend's version, compiled and executed the per-run native ELF sentinel, and matched both reference actors to the exact payload vector.

| Check | Observed result | Evidence in the fresh run |
| --- | --- | --- |
| Native builds and reference vectors | Fresh Bend ELF executes the exact sentinel; both reference vectors match | `build/`, `build/vectors.json`, `commands.json` |
| Million-message integrity | Each actor transmits and validates 1,000,000 messages; zero rejection; sender and receiver digests agree | `trials/integrity-million/` |
| Four fault controls | Both receivers detect duplicate and corrupt messages at sequence 50 with exit status 1 | `trials/integrity.duplicate.{0,1}/`, `trials/integrity.corruption.{0,1}/` |
| Four paused-receiver trials | Every trial rejects excess work and drains all admitted messages; queue and tracked-heap peaks stay within the checked limits | `trials/bounds/{ws,tcp}/{client,server}/` |
| Damaged-evidence rejection | Missing, mismatched, malformed, and false-success evidence all reject | `summary.json`, [Cargo assertions](../tests/ws/phase0.rs) |

All evidence paths in the table are relative to [the fresh Phase 0 run](../results/ws/cargo-1795497449219896432-3075/phase-0/). These generated artifacts are local and ignored by Git. This qualification verifies the Cargo migration and completes the revised Phase 0 gate. Throughput, latency, noise, and sustained headroom remain Phase 5 work.
