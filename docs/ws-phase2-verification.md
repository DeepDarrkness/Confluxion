# Phase 2 protocol qualification

Phase 2 qualifies the bounded native client adapter independently of Bend application logic. The complete Cargo suite passed Phases 0, 1, and 2 in the integration checkout on November 24, 2026.

## Ownership and policy

`src/ws/native/src/protocol.rs` separates handshake and established-protocol ownership. `Upgrade<S>::resume` consumes the same handshake continuation and returns either the next `Upgrade` or a `Protocol<S>`. Both own generic `Read + Write` IO. Tungstenite, HTTP, and Bytes representations remain behind the native Rust API.

`Protocol::offer` accepts a borrowed binary or text message once. `Accepted` transfers responsibility, even when writing encounters `WouldBlock`. `Rejected` does not copy the input. Only `flush` resumes accepted output. One pending application frame occupies the admission slot, including an empty frame. There is no new application queue. A read conservatively requires a flush because Tungstenite may queue automatic controls. Two successful dependency flush calls drain a deferred automatic control before clearing admission.

`read` returns a borrowed view of one dependency-owned delivered message. The next read releases that slot before receiving another message. Fatal errors latch terminal state. Peer Close rejects later data; local Close is initiated once after older output drains. Flush completion means IO completion, not peer delivery.

The handshake wrapper stages at most 12,288 bytes until `CRLF CRLF`. Its 8,192-byte header cap excludes the bounded coalesced frame suffix. Replay preserves both Tungstenite's `from_partially_read` tail and the wrapper's remaining bytes. Underlying one-byte reads do not become one-byte successful reads inside Tungstenite's `AttackCheck`.

The adapter has a fixed 14-byte ingress header guard. The recorded qualification probes showed that Tungstenite 0.28.0 accepts nonminimal extended lengths and parses a forbidden 64-bit high bit. The guard checks only those numeric rules, including the coalesced handshake suffix. Tungstenite still owns opcodes, masking, fragmentation, UTF-8, controls, and Close. No protocol core is copied or forked.

## Limits

| Native policy | Qualified bound |
| --- | --- |
| Frame and aggregate message payload | Configurable from 1 byte through 1,048,576 bytes independently |
| Dependency read size and wrapper read quantum | 4,096 bytes |
| Response header bytes | 8,192 bytes |
| Header lines | 124, the pinned parser's cap |
| Request authority, target, selected protocol | 128, 1,024, and 64 bytes |
| Encoded request ceiling used in the audit | 2,048 bytes |
| Application admission | One frame, no adapter queue |
| Dependency write buffer at the maximum configuration | 1,048,868 bytes: payload plus 14 plus two 139-byte control reservations |
| Dependency write target | Zero, so admitted frames attempt immediate output |
| Compression, extensions, TLS | Not supported |
| Subprotocol | None or exactly one required token |
| Fragment count | No count cap; aggregate message bytes remain bounded |

The wrapper rejects any extension response and duplicate Upgrade, Connection, Accept, or subprotocol fields. The pinned verifier rejects a valid comma-separated Connection token list unless the whole value equals `Upgrade`. That remains a supported-library limitation, not a claim of full RFC handshake conformance. Scheduling, deadlines, real sockets, and Autobahn qualification belong to Phase 3.

## Derived native allocation ceiling

The budget is an exact evaluated, conservative upper bound on requested heap layouts, not a fitted peak or a tight attainable maximum. The audit is for Rust 1.99.1, `x86_64-unknown-linux-gnu`, and the root lockfile: Tungstenite 0.28.0, Bytes 1.12.2, HTTP 1.5.0, Rand 0.9.5, and Rand ChaCha 0.9.0. A compiler, target, pin, or capacity-policy change requires a new audit. The test records the public type sizes used in the equations.

For maximum configuration, let $F=M=1,048,576$, $R=C=4,096$, $H=8,192$, $Q=2,048$, $N=F+R+14$, and $W=F+14+278$.

The inspected growth rules are `Vec::reserve` doubling, `Vec::reserve_exact`, Bytes `reserve_inner`, and HTTP's adaptive header map. Reallocation is instrumented as allocate-copy-free, so old and new layouts coexist in the measured peak.

| Audited storage | Layout bound and source reasoning |
| --- | --- |
| Receive backing | Three backing allocations of at most $4N$ each, including an old buffer retained by automatic Pong and old/new growth. `frame/mod.rs:170–238` reserves one advertised payload and reads at most $R$. Between header reservations, unconsumed lookahead is at most $R+14$. Bytes `bytes_mut.rs:631–809` shifts an offset when possible, otherwise doubles or replaces shared storage. Growth with a nonreusable offset needs less than two $N$ before doubling. Original shared-capacity representation is capped at 65,536 bytes. |
| Fragment assembly | $3M$ covers old/new Vec or String growth. A complete fragmented message transfers its assembly backing into Bytes without a second payload copy. |
| Wire Vec | $3W$ covers old/new growth. An allocation grows only while its old capacity is below the prospective logical length, which is capped at $W$. |
| Outbound copy | $M$, while delivered data, assembly capacity, and wire storage remain retained. |
| Control copies | $4\cdot139$. Close builds an exact reason-plus-two payload; Ping shares its receive backing. |
| Bytes metadata | $D_b=8\cdot40=320$. BytesMut Shared contains a 24-byte Vec and two 8-byte words. Bytes Shared is smaller. |
| Bounded diagnostics | $D_e=3\cdot128=384$. This covers the boxed HTTP response and bounded UTF-8 diagnostic formatting. No unbounded request headers or error-body fetch exists. |

HTTP map allocation uses a 4-byte `Pos`, a bucket stride bound of $8+32+40+24=104$, and a 72-byte duplicate-value entry. These come from `http/header/map.rs:294–350`; the 8-byte hash slot includes alignment padding. With 124 input lines, the collision-growth threshold of five gives a maximum raw table of 1,024 positions. Old/new tables contain at most 1,536 positions. Their entry capacities total at most 1,152. Duplicate-value Vec growth totals at most 192 entries. Name/value byte copies, temporary insertion names, and at most 248 Bytes metadata records are included:

$$D_h=1536\cdot4+1152\cdot104+192\cdot72+2H+248\cdot40=166,080.$$

Request construction conservatively uses the same map bound. Six $Q$ include the formatted URI, HTTP URI storage, header copies, serialized Vec capacity, and old/new serialization growth. One offered String and its verification copies, the fixed accept/key strings, and metadata give:

$$D_q=6Q+D_h+3\cdot64+28+24+8\cdot40=178,932.$$

Staging, handshake header Vec old/new growth, the dependency's boxed 4,096-byte chunk, the parsed response, request lifetime overlap, and initial frame backing give:

$$B_u=(H+C)+3(H+C)+R+D_h+D_q+4N+D_b+D_e=4,609,708.$$

Established retention and transient growth give:

$$B_p=12N+3M+3W+M+4\cdot139+D_b+D_e=19,974,400.$$

Rand's thread-owned `Rc<UnsafeCell<ReseedingRng<ChaCha12Core, OsRng>>>` has an independently derived 352-byte layout. The test computes that layout with `Layout::extend` from the authoritative dependency types and the two Rc counters. Cold production request construction observes exactly one persistent block of that size after dropping the upgrade. That thread singleton is included, not mistaken for a connection leak:

$$B_{native}=352+\max(B_u,B_p)=19,974,752\text{ bytes}.$$

The observed largest connection peak was 4,198,353 bytes, below the derived ceiling. The observed largest single layout was 2,097,180 bytes. These are checks of the derivation, not inputs to it. Adapter ownership is stack-resident: the recorded prepared-IO `Protocol` and `Upgrade` sizes are 440 and 312 bytes. Stack frames, allocator overhead, RSS, kernel buffers, and caller-owned prepared IO are not heap-layout bytes.

## Gate and evidence

`cargo test --release --locked -p ws-reference --test phase2 -- --nocapture` runs one Cargo test. The test writes a failing `summary.json` before any gate checks, then replaces it with passing case and memory records only after all checks complete. Earlier incomplete and failed invocation artifacts remain unchanged.

The [integration summary](../results/ws/cargo-1795507831166507249-72209/phase-2/summary.json) records case names, seed `379889150468`, limits, layout terms, allocation counts, reallocations, zeroed allocations, largest layouts, and cleanup. The gate covers:

- The literal RFC accept request, every response-header split, one-byte interrupted reads, exact header cap and cap plus one, negative upgrade fields, malformed headers, truncated upgrade, and subprotocol policy.
- Independent literal RFC frame fixtures, both directions at 0, 125, 126, 65,535, 65,536, and 1 MiB, every small fixture split, and seeded larger partitions.
- Complete and partial coalesced frames, including header replay across dependency chunks, exact frame and message limits, advertised oversize without a body, and excess fragment aggregates.
- Nonminimal lengths, high-bit lengths, reserved opcodes and bits, masked server frames, illegal continuations, fragmented or oversized controls, invalid and incomplete UTF-8, and EOF inside headers or payloads.
- UTF-8 characters crossing fragments and reads with interleaved Ping, unsolicited Pong, local and remote Close, invalid Close reason/code behavior, send after Close, and abrupt EOF.
- One-to-seven-byte writes alternating with `WouldBlock`, including length fields, masks, payloads, Pong, and Close. The independent decoder extracts random masks and re-encodes the exact expected sequence. Extra bytes, duplicated frames, and missing suffixes fail.
- Allocation-free Busy and Oversize rejection, saturated output with retained controls, maximum binary and UTF-8 assembly, 124 unique and duplicate header lines, large names and values, header exhaustion, interrupted and failed upgrade, fatal IO, and drop.
- Twenty warmed send/drain cycles preserve their idle allocation value. Every measured connection releases all scoped blocks. A retained 37-byte control allocation is detected and clears after release.

The existing global allocator now has an allocation-free current-thread aggregate scope. Its original Phase 1 counters and layout table retain their behavior. Aggregate overflow and attribution underflow fail the scope. The test preallocates prepared inputs, bounded wire collectors, and expected values before arming it. Formatting evidence happens after disarming it. The integration contract requires bounded nonblocking IO with no unaccounted allocating queue inside `S`. No extra runner, Python path, native application queue, or socket worker is introduced.

The full integration command uses the compiler settings already documented for Phase 0:

```sh
CC=/usr/lib/llvm-19/bin/clang CXXFLAGS=--gcc-install-dir=/usr/lib/gcc/x86_64-linux-gnu/13 cargo test --release --locked
```

All three Cargo tests passed. The [Phase 0 summary](../results/ws/cargo-1795507779180348826-69045/phase-0/summary.json) records nine passing trials and damaged-evidence rejection. The [Phase 1 summary](../results/ws/cargo-1795507824100303680-69116/phase-1/summary.json) records 1,000 cycles, 22 fault records, and 11 Bend boundary checks per ownership and ASan/UBSan mode. Each mode restored the two-block, 24-byte Bend baseline. `rustfmt --check` on the modified Rust files and `git diff --check` also passed.

The first integration command stopped before fixtures because `clang` was absent from `PATH`. The recorded compiler settings resolved that environment issue. The [initial failed Phase 2 artifact](../results/ws/cargo-1795506795352918007-64867/phase-2/summary.json) retains its incomplete-gate status, and the [focused passing artifact](../results/ws/cargo-1795507246440860829-66070/phase-2/summary.json) remains available. Results directories are ignored by Git and are present in the integration checkout.

## Cleanup after qualification

Cleanup removed the direct upstream parser probes. The adapter's nonminimal-length and high-bit rejection cases remain. Busy rejection now runs once per partial-write quantum and once in the instrumented saturation scenario. Oversize and invalid-Close checks no longer repeat across unrelated memory scenarios. The test helper's unused lifetime annotation was removed.

The temporary design sketches and implementation worktree were removed after verifying that their implementation files were already in commit `a023f50`. Historical qualification artifacts remain in the main checkout. Tests were not rerun after cleanup, at the user's request. The passing results above describe the pre-cleanup suite.
