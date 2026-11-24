# Phase 4 verification

Phase 4 connects real Bend message producers and consumers to the qualified Rust socket worker. Application bytes cross native effects without moving socket ownership onto Bend's event loop.

Status is **Passed**. The main-checkout `cargo test --release --locked` invocation passed all routine gates for Phases 0 through 4 after implementation review and corrections. The [integration qualification](#integration-qualification) records the final evidence. Earlier focused results below describe development runs.

## Implemented boundary

`src/ws/main.bend` exposes `connect`, `offer`, `offer_batch`, `take`, `take_batch`, `status`, `Lease.release`, `stop`, `Session.release`, and `close`. Session, closer, and lease capabilities are affine `File` wrappers.

Outbound operations preserve the existing binary `List<U32>` interface. Incoming `Message{kind, bytes}` identifies binary as `0` and text as `1`. `Endpoint` contains an IPv4 loopback host, port, target, and protocol string. An empty protocol string requests no subprotocol. BSATN and SpacetimeDB remain outside this API.

`src/ws/effects/native.c` validates and copies byte trees on the runtime thread. Failed admission returns the original tree. Successful admission frees its cells exactly once. A batch is one foreign effect and one all-or-none native queue transaction, not a loop over single effects.

`src/ws/native/src/socket.rs` owns a separate ten-slot, generation-checked socket registry. Socket keys have a distinct tag from the historical loopback registry. A receive batch owns one detached aggregate lease. A lease retains its bytes, descriptors, and shared accounting, but no client, socket, queue, or worker.

The existing `worker::Client` remains the sole socket owner. Startup, receive waits, and shutdown park through `io_wait_on`. Ready and done eventfds belong to the worker core. Close joins only a finished thread and checks that its Linux TID has disappeared. The synchronous Rust convenience API remains available to preceding tests. No relay or per-operation helper thread was added.

The loopback Bend binding now lives under `tests/ws/loopback*`. Phase 1 imports that fixture binding. The application API no longer exposes loopback `start`.

## Limits and accounting

The production profile retains 1 MiB single messages, 2 MiB and 4,096 descriptors per queue, 250 ms directional partial deadlines, one-second startup and close deadlines, and the 10 ms eligible-control budget. Protocol framing is unchanged.

Batches contain at most 128 messages and 64 KiB. Receive waits only for the first message. A head larger than the requested byte cap stays queued. A failed receive reservation pops nothing. Aggregate leases allow eight outstanding batches and 2 MiB of payload in total, including leases held after close. Empty messages consume descriptors and lease count.

The aggregate payload and span ceiling is $2{,}097{,}152 + 8\times128\times16 = 2{,}113{,}536$ bytes. Boxes allocate their exact selected sizes. Session and account allocations add 72 requested bytes on this build. The published native ceiling reserves 96 bytes for those allocations, including 24 bytes of conservative padding.

The worker ceiling remains its qualified dependency ceiling plus the current ring, core, startup-string, and frame layouts. `Core` includes both new eventfds and the added snapshot fields. For the fixture endpoint, `WsSocketStatus.native_budget` reports **27,463,145 bytes**, including aggregate ownership. Maximum supported target and protocol lengths add 1,087 bytes to that fixture ceiling. Static registry storage and the allocation-layout recorder are separate from intercepted heap allocations. The recorder has 512 layout slots and retains overflow detection.

C send scratch is at most 1 MiB for a single effect or 64 KiB for a batch, with 128 stack spans. The observed C peak includes the runtime's retained 64-byte `IoWork` baseline. Native lease release does not free Bend lists.

The fixture's Bend ceiling is 32 MiB. Its largest 1 MiB byte list uses 16 bytes per cell, and validation uses an additional eight-byte return frame per byte. The remaining allowance covers bounded message constructors, rejected trees, and continuations. This is a fixture bound, not a limit on arbitrary application-held Bend values. RSS, kernel buffers, native requested bytes, C scratch, Bend heap bytes, and the 256 KiB worker stack reservation have separate records.

The lifecycle loop restores a fresh checkpoint of two Bend blocks and 32 bytes after every cycle. Its final checkpoint has zero Bend blocks and bytes. Native blocks and bytes return to zero, and C blocks, C bytes, fds, and threads return to their measured baselines. Retained-lease cases restore fds and threads before releasing the detached bytes.

## Cargo qualification

`tests/ws/phase4.rs` uses the existing Cargo, prepared Bend 2.0.40, marked Rust static archive, and clang pipeline. It emits and instruments actual Bend C, asserts exact compiler anchors, and checks the fresh ELF. Optimized ownership builds use `-O2`. ASan and UBSan use the pinned runtime's required `-O0`. Rust allocations are intercepted; Rust code is not compiler-sanitizer-instrumented.

The focused command is:

```sh
WS_CACHE=/home/narasimhaai/projects/Confluxion/.ws-cache cargo test --release --locked --test phase4 -- --nocapture
```

Build products and qualification artifacts stay under the isolated worktree. The shared cache supplies read-only prepared inputs.

The final focused qualification passed in 88.52 seconds after test cleanup. Its manifest is `results/ws/cargo-1795519394432071647-149256/phase-4/summary.json` under `/tmp/opencode/confluxion-ws-phase4`. The manifest has `pass: true`. It records a 140,874,271 ns pure interval, native send at 23,444,292,185,928 ns, native receipt at 23,444,292,299,863 ns, and peer validation at 23,444,292,240,150 ns. The runtime interval starts at 23,444,240,786,836 ns and ends at 23,444,381,661,107 ns. These are correlated host monotonic timestamps, not throughput measurements.

The temporary development-only case selector and unused debug output were removed before this final run. The lead's complete-suite run will verify the integration checkout.

| Gate | Executed evidence |
| --- | --- |
| Real Bend bytes and message boundaries | `million-bend.stdout`, `peer.json`, and `summary.json.million` agree on 1,000,000 messages each way, zero terminal send failures, and complete Bend consumption. Each direction includes 250,000 singles and 750,000 batch messages. The uWebSockets peer validates every outgoing byte independently. |
| Pure computation and independent IO | `compute-128000000.stdout` and `summary.json.compute_attempts` record the pending result, actual runtime evaluation boundaries, native complete-send and complete-receive events, peer validation, and later Bend validation of input identity 777. All transport timestamps lie strictly inside an interval of at least 100 ms. |
| Real backpressure | The peer stops reading after upgrade. A small socket buffer holds two Bend-generated fillers and a previously computed result incomplete before the marker. The inherited nonblocking pipe announces evaluation start. Peer reads resume at least 10 ms later. No artificial worker send gate is used. |
| Workload is real | The seed arrives over the socket. Bend evaluates the bounded serial recurrence and supplies its checksum to the next effect. Rust independently recomputes both that checksum and the prior result. Runtime instrumentation rejects an intervening effect or wrong next request. |
| Damaged evidence | Five controls reject a short interval, shifted send event, missing complete-receive event, wrong checksum, and absent later Bend consumption. |
| Each receiver pause | 100 cases alternate directions and cover 10, 100, and 1,000 ms holds. Each record checks opposite progress inside the hold, queue bounds, exact validated identities, zero residual queue counts, and drainage within two seconds. A one-second healthy peer pause occurs at an idle frame boundary. |
| Directional stall deadlines | The retained Phase 3 native suite exercises genuine stalled partial frames and independent opposite traffic with unchanged production deadlines. Expected terminal stalls are not counted as healthy drains. |
| Controls | The compute peer validates the exact Pong and its arrival within 10 ms of Ping. Native eligible-control service is also at most 10 ms. Real Close runs through the Bend API. |
| Boundary lengths and kinds | Finite cases cover 0, 125, 126, 65,535, 65,536, and 1 MiB. Too-small receive capacity leaves the head queued. Mixed binary and UTF-8 text, including NUL, reach Bend with identifiable kinds. |
| Admission and leases | Descriptor-full and byte-full batches return original trees without admission. Count and byte lease exhaustion leave the next head queued; releasing credit delivers that head. Zero-byte leases count toward capacity. |
| Fault ownership | Pre-commit panic returns the original bytes. Post-commit panic returns Accepted and records complete or failed terminal ownership. Worker panic returns an explicit terminal reason. Startup faults after each eventfd and ring acquisition restore resources. Bad upgrades fail without leaked capabilities. |
| Lifecycle | 1,000 socket start, transfer, close, and post-close release cycles pass in each ownership and sanitizer mode. Parked single and batch receivers wake through the independent closer. Retired and foreign handles have defined errors. A deliberately omitted release fails the resource observer. |
| Corruption and duplicates | Four finite real-path controls detect outgoing and incoming corruption and duplicate identity 1. Outbound faults are generated in Bend and checked by the peer. Inbound faults fail Bend's validator. |
| Conversion and effects | `diagnostics` records separate native-take, byte-to-Bend conversion, and effect-dispatch time with actual message counts. These overlapping accumulated costs are diagnostics, not additive end-to-end time or throughput qualification. |

The integration section below links the final main-checkout artifacts. Earlier emitted sources, binaries, command records, stdout, stderr, and JSON summaries remain under `results/ws/cargo-*/phase-4` in the implementation worktree.

## Preceding gates and cleanup

Focused Phase 1 and Phase 2 tests passed with the relocated loopback fixture. Phase 3's native correctness gate passed after the worker changes and optional adapter removal. The integration section records the subsequent complete main-checkout pass.

The temporary Autobahn adapter, ignored runner, and adapter-only report reader were removed from `tests/ws/phase3.rs`. Historical Phase 3 reports and frozen profile pins remain unchanged. The lead removed `.ws-cache/autobahn` and `.ws-cache/autobahn-runtime` after the integration pass.

## Design adjustments

Binary outbound lists, numeric incoming kind codes, and an empty protocol string preserve the existing operation shape without adding text-send scope. The compiler flattens nested singleton record fields, so the C adapter packs the observed constructor arities explicitly.

The million-message uWebSockets peer keeps its existing count-completion exit policy. Separate raw-socket finite peers qualify clean shutdown and retained leases. Pause, lifecycle, and fault records use finite connections rather than adding modes to the transport protocol. Conversion diagnostics remain fixture instrumentation.

The lead reviewed and integrated the intended files and recorded the complete-suite evidence below. Phase 5 tuning, additional connections, and SpacetimeDB integration remain out of scope.

## Accepted review fixes and follow-up evidence

The follow-up focused command passed in 97.33 seconds:

```sh
WS_CACHE=/home/narasimhaai/projects/Confluxion/.ws-cache cargo test --release --locked --test phase4 -- --nocapture
```

The manifest is `results/ws/cargo-1795519976279072580-153847/phase-4/summary.json` in the isolated worktree. Its `pass` value is `true`. The run includes the existing million-message, pure-computation, 100-pause, fault, boundary, and 1,000-cycle gates. Both ownership and ASan/UBSan builds execute the new finite cases. The first follow-up attempt stopped at Bend emission because two new binders required `+`. The corrected run passed. Historical results above remain unchanged.

### Close retirement and delayed startup

`ws_stop_more` keeps its consumed capability until `ws_socket_stop_finish` retires the registry entry. Expiration of the first C poll does not return a terminal failure while native shutdown remains pending. The worker retains its existing deadline and sole socket ownership. The registry keeps the borrowed notification fd alive until the wait continuation finishes. No helper or reaper thread was added.

`adverse-lifecycle.stdout` records a successful close in **906,131,329 ns**. The sanitizer case records **905,530,794 ns**. Both restore native allocations to zero and fds, threads, and C allocations to their measured baselines before process exit. The peer receives the outgoing bytes and Close, then withholds its Close reply for 1,200 ms. The checks require native retirement within two seconds.

The emitted fixture shortens only the first C close poll to 1 ms. The native close deadline remains unchanged. `legacy-close.c` restores the previous early-return branch as an executable negative control. `legacy-close-control.stderr` contains `check tag 19: 9 != 0`. The fixed binary passes the same case and releases its detached receive lease after close.

The same peer delays the upgrade by 200 ms. Bend forks `WS.connect`, sleeps for 50 ms, and executes another effect before joining startup. `summary.json.adverse_lifecycle` checks that the sibling effect's monotonic timestamp lies between peer acceptance and upgrade completion.

### Connected saturation, drainage, and peak memory

Case 29 warms both directions, records a connected-idle baseline, and repeats two receive-saturation cycles without reconnecting. In each cycle the peer produces 4,608 distinct 256-byte messages while Bend pauses consumption. The native queue reaches its actual reservation boundary of **4,096 descriptors and 1,048,576 payload bytes**. The worker reserves room for a maximum-sized incoming message before another read. The remaining messages stay upstream until Bend drains the queue.

The peer independently validates eight opposite-direction Bend sends inside each hold. Bend then validates all 4,608 inbound identities and bytes exactly once. Each drain completes within two seconds. `ownership-case-29.stdout` and `asan-ubsan-case-29.stdout` end with 9,217 received and consumed messages, including the initial warmup message, and 24 completed outbound messages.

All three connected-idle checkpoints in each build report **5,379,633 live native bytes** with no queued messages or leases. The worker snapshot reports **5,379,401 live and idle bytes** at each checkpoint. The worker's expected idle value includes both allocated rings, `Core` and its Arc header, the startup string, and retained protocol and IO scratch. The snapshot adds the worker's intercepted live allocations to that fixed layout. Whole-process native accounting also includes session ownership and Rust thread bookkeeping. The check compares whole-process live bytes before and after each cycle and independently requires worker live bytes to equal its established idle value. Shutdown-to-zero is a separate check.

The new receive instrumentation checks the intercepted high-water mark against `WsSocketStatus.native_budget`, including detached lease payload and span storage. Final `memory_peak` rows repeat that check after cleanup. Case 29 records a **5,391,617-byte native peak** against the **27,463,145-byte derived budget**. C scratch and Bend heap bounds remain separate. These figures describe allocation ownership, not throughput.

### Result construction and runtime limitation

`ws_take_more` now checks for a reported runtime error after the final `Payload` or `Messages` construction and the returned tuple allocation. An undelivered aggregate lease is released if that check observes an error. The earlier byte-conversion check remains in place. Already-popped data is not returned as retryable input.

The pinned Bend 2.0.40 CPU runtime does not provide recoverable allocation failure. In the emitted `original.c`, `heap_alloc_miss` calls `err_post` on exhaustion. The host `err_post` calls `err_fail`, which invokes `_exit(1)`. Host `err_peek` is false because `DEVICE` is false. An actual CPU allocation failure therefore terminates the process before adapter cleanup can run. OS process cleanup releases its resources. No unwind, transactional rollback, or executable recoverable-OOM claim is made. The post-construction check covers only errors that return to the adapter.

At the end of the follow-up delegate run, the complete Cargo suite was pending and phase status was In progress. The subsequent integration run below passed.

## Integration qualification

The main-checkout suite passed November 24, 2026 with the installed compiler paths:

```sh
CLANG=/usr/lib/llvm-19/bin/clang \
CPLUS_INCLUDE_PATH=/usr/include/c++/13:/usr/include/x86_64-linux-gnu/c++/13 \
LIBRARY_PATH=/usr/lib/gcc/x86_64-linux-gnu/13 \
cargo test --release --locked
```

The initial invocation stopped before Phase 0 fixtures because `clang` was absent from `PATH`. Subsequent preparation checks identified missing default C++ header and library paths. The environment above selects the installed GCC 13 files through the existing compiler overrides. No test gate was changed for those failures.

| Gate | Result | Final evidence |
| --- | --- | --- |
| Phase 0 | Passed | [Summary](../results/ws/cargo-1795520669126219112-160090/phase-0/summary.json) |
| Phase 1 | Passed | [Summary](../results/ws/cargo-1795520713713031563-160467/phase-1/summary.json) |
| Phase 2 | Passed | [Summary](../results/ws/cargo-1795520720727344630-163636/phase-2/summary.json) |
| Phase 3 routine native checks | Passed | [Summary](../results/ws/cargo-1795520720810661587-163638/phase-3/summary.json) |
| Phase 4 real Bend path | Passed | [Summary](../results/ws/cargo-1795520741553854400-164059/phase-4/summary.json) |

Each Cargo target passed its single integration test, with zero failures or ignored tests. Phase 4 completed in 99.28 seconds. `cargo fmt --all -- --check` and `git diff --check` passed after integration.

The final Phase 4 manifest records 1,000,000 validated messages each direction, including 250,000 singles and 750,000 batch messages. Its pure interval lasts **138,153,864 ns**, with zero intervening effect dispatches. The measured runtime interval is 24,797,506,451,052 through 24,797,644,604,916 ns. Native complete-send, native complete-receive, and peer-validation timestamps are 24,797,560,226,156, 24,797,560,313,941, and 24,797,560,280,851 ns respectively. All three lie strictly inside that interval. Bend subsequently validates and consumes the received identity.

Both instrumentation modes pass 1,000 real socket lifecycle cycles. The final run also passes the 100 finite receiver pauses, persistent receive-saturation cycles, connected-idle and peak bounds, fault controls, delayed startup, and withheld-close retirement. The optional Autobahn adapter and temporary runtime are removed; recorded Phase 3 evidence is preserved. No Phase 5 throughput qualification is claimed.
