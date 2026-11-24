# Bend WebSocket design for Confluxion

Research date: November 17, 2026. This document consolidates the WebSocket feasibility and buffering assessments for the native connector in [confluxion-arch1.md](confluxion-arch1.md). It describes a proposed design, not an implemented connector.

Worker-model decision: November 23, 2026.

Transport-library decision: November 24, 2026. Use Rust/Tungstenite for the initial client; defer an all-Bend WebSocket implementation to optional Phase 7.

## Verdict

**The architecture is feasible, with runtime scheduling and binary-data representation as the main implementation risks.** Current Bend provides native binary TCP, timed polling, channels, and system entropy. SpacetimeDB accepts a plaintext WebSocket client that sends BSATN calls and receives replies and subscription updates on the same connection. [Bend Base][bend-base], [native TCP][bend-connect], [SpacetimeDB transport][stdb-sdk]

The selected design uses an independent Rust worker with Tungstenite for full-duplex socket IO, and a bounded number of computation workers rather than spawning one per request. Bounded native buffers connect transport to Bend BSATN codecs and a SpacetimeDB-specific session layer. Generated service bindings sit above those libraries. Application logic and authoritative schema declarations stay in Bend. React uses the existing SpacetimeDB SDK.

The initial target places SpacetimeDB and BendRT on the same machine, using IPv4 loopback, plaintext `ws://`, one pinned SpacetimeDB subprotocol, and disabled compression. Message throughput, latency under load, and progress during Bend computation are the performance priorities. No connector code was written, no live integration was exercised, and no performance improvement was measured. The `bend` executable was not found on this workspace's `PATH` during the research.

## Selected worker model

Each BendRT process has an independent native socket worker that owns its connections and drives both reads and writes. It receives messages while Bend computes and sends already completed results while later computations run. A result becomes eligible for sending only after its computation finishes. Socket progress does not wait for unrelated computations.

Independent computation workers run in a configured, bounded pool of BendRT processes. One process is a valid initial pool size. Requests enter bounded queues and reuse existing workers. Worker count, queued work, active jobs, and completed results all have explicit limits. A full queue propagates backpressure rather than creating another worker.

`IO.fork` interleaves tasks on a process's existing event loop. It does not create an independently scheduled computation worker. Bend parallel calls distribute computation within a worker across CPU or GPU resources. Process count and CPU threads per process are configured together so workers leave capacity for SpacetimeDB and socket IO. [Execution model][bend-guide]

Transport concurrency, concurrent jobs, and parallelism within a job have separate budgets. Completed jobs may arrive out of order; the coordinator correlates them with application job identities and preserves declared partition ordering. These identities are separate from connection-local WebSocket protocol request IDs.

The native socket worker handles bytes, descriptors, and transport state. Bend's runtime thread executes Bend code and constructs Bend values. A slow writer must not stop reads, and the socket worker must not wait for computation or queue capacity.

## Transport-library decision

### Caller experience and ownership

The application imports a generated Bend binding, receives typed subscription inputs, computes a proposal in Bend, and submits it through the binding. Rust implements transport mechanics. A WebSocket library supplies framing and connection IO, not SpacetimeDB schema generation, subscription-cache semantics, or durable commit recovery.

Conceptual usage and signatures below are not implemented APIs or checked Bend syntax:

```text
session = Binding.start(config, limits)
(session, inputs) = Binding.ready(session)
proposal = application_transition(inputs.values)
(session, admission) = Binding.submit(session, operation_id, inputs.versions, proposal)
Accepted(ticket) = admission
(session, outcome) = Binding.resolve(session, ticket)
Binding.close(session, drain_deadline)

Session = private affine capability
InputView = generated values + dependency versions + connection generation
Admission = NotAdmitted(proposal) | Accepted(ticket)
CommitOutcome = Confirmed | DeclaredRejection | HostFailure | Uncertain(operation_id)
```

The resolve call applies only after `Accepted`; non-admission retains the proposal for the coordinator. Accepted messages are not resubmitted because a socket write blocks. A confirmed outcome includes publication of any result-embedded update; an uncertain outcome requires durable reconciliation.

| Component | Ownership and interface |
| --- | --- |
| Bend application and coordinator | Business computation, ordering, job admission, and reconciliation through generated domain contracts |
| Bend session and generated codecs | SpacetimeDB envelopes, connection metadata, call correlation, schema conversion, BSATN, and complete cache publication |
| C effect adapter | Compiler-specific marshalling and runtime-thread transfer of bounded bytes through the private native ABI |
| Rust static library | One independent socket worker per BendRT process, Tungstenite state, packed buffers, bounded queues, deadlines, and shutdown |
| Generated Rust WASM module | Schema registrations and transactional checks/writes generated from Bend declarations; no dependency on the native transport library |

The private native ABI uses fixed-width status records and byte spans, not Rust objects or WebSocket enums. Conceptual `start`, `offer`, `take`, and `stop` operations remain unimplemented. `offer` copies on successful admission and retains nothing on rejection. `take` copies one complete message, consuming it only when the supplied capacity is sufficient. Each allocation is released by its owner. Rust panics must not unwind across the C boundary. The exact affine capability, generation checks, linker integration, and error paths require Phase 1 qualification.

### Packaging rationale

The embedded Rust static library preserves the independent-thread ownership model without adding an executable, IPC protocol, and supervision boundary. It keeps transport types private, byte transfers bounded, and generated shims independent.

The embedded worker accepts compiler-specific ABI coupling, initial byte copies, and shared-process failure in exchange for simpler deployment. A sidecar remains a possible redesign if linking or isolation requirements justify its extra IPC and lifecycle work. It is not a second production path. The initial dedicated-thread worker does not require Tokio; Tungstenite's nonblocking integration still needs fair read/write/flush scheduling.

### Deferred Bend implementation

Optional [Phase 7](ws-phased-plan.md#phase-7-revisit-an-all-bend-websocket-implementation) may replace the Rust transport after reliable SpacetimeDB integration. The public Bend bindings, application schemas, and generated Rust module shims retain the same contracts. This replacement needs independent scheduling and equivalent qualification; it is not achieved by replacing the Rust worker with `IO.fork` on Bend's computation loop.

The pinned Base TCP interface transfers `List<&2, U32>`, not packed byte arrays. Send and receive convert between those lists and native buffers. A future Bend transport must address this representation cost and the computation-blocked event loop; a separate IO-focused Bend process is a possible probe, not a qualified replacement. [Base TCP constraints](#constraints-that-affect-the-design)

## Architecture understood

The connector is an outbound client to SpacetimeDB. One logical service binding does not mean one socket or a Bend server exposed to the frontend. Several bindings can share one database connection. [Architecture, vocabulary and runtime](confluxion-arch1.md#runtime-architecture)

The connector supplies complete input updates and correlated call outcomes to the coordinator. Bend computes proposals outside database transactions. Generated module shims validate and commit state changes, checkpoints, and completion records atomically. Frontends independently subscribe to that committed state. [Architecture, coordination](confluxion-arch1.md#bend-owns-coordination-spacetimedb-owns-durable-commits)

This report covers the transport, BSATN codecs, and session machinery needed for that flow. Automatic module generation and the full event-sourcing coordinator are additional project work.

## Evidence and version target

The findings use these source revisions:

- Historical Bend research used `9c586f094f4fc88bef440dc8b4d77a2f92c257cc`. Implementation uses Bend 2.0.40 from the [qualified profile](../bench/ws/profiles/local-v1.json); its ABI and lifecycle still need Phase 1 checks. [Historical Bend revision][bend-revision]
- SpacetimeDB release `v2.15.0`, revision `07efb0c44c42288ed8e40dcece75659cf44ec80c`. [Release revision][stdb-revision]
- RFC 6455 for WebSocket handshake, framing, and closure. [WebSocket standard][rfc]

The recommended configuration is:

| Setting | Proposed target |
| --- | --- |
| Transport | Plaintext `ws://` over native TCP on IPv4 loopback |
| HTTP route | `/v1/database/NAME_OR_IDENTITY/subscribe` |
| WebSocket subprotocol | `v2.bsatn.spacetimedb` |
| Query parameters | `compression=None&confirmed=true` |
| Application schema metadata | `RawModuleDefV10` |

The released Rust SDK selects v2. V3 supports several logical messages per WebSocket payload, which adds parsing work without a capability required by this architecture. The `/v1/` route version is independent of the WebSocket subprotocol. [SDK][stdb-sdk], [v3 framing][stdb-v3], [upgrade handler][stdb-upgrade]

The website labels the reference as 2.0.0 and its HTTP page lists only v1. The pinned release source supports v2. Implementation must follow the pinned message declarations and server behavior rather than combine examples from different versions. These pins do not establish the version of an existing deployment. [HTTP reference][stdb-http], [message declarations][stdb-v2]

## BSATN and the commitlog are different layers

The linked [commitlog page][commitlog] describes the write-ahead log and its replication stream. Its segment headers, transaction offsets, epochs, CRC32C checksums, and mutation counts belong to that log format.

The public WebSocket client instead exchanges `ClientMessage` and `ServerMessage` envelopes. Both protocols contain BSATN values, but the WebSocket endpoint does not send commitlog records or expose a commitlog replay cursor. A commitlog reader is unnecessary for the proposed connector. [Commitlog][commitlog], [v2 envelopes][stdb-v2]

The actual boundary has these layers:

```text
	Bend domain values
	    ↕ generated BSATN application codecs
	SpacetimeDB v2 protocol envelopes
	    ↕ server compression-prefix handling
	WebSocket binary messages and control frames
	    ↕ RFC 6455 framing
	Native TCP byte stream
```

BSATN has no field names or general runtime type tags. Decoding needs the agreed schema. It cannot replace handshake negotiation, authentication, framing, correlation, or subscription semantics. [BSATN specification][bsatn]

## What Bend already supplies

| Requirement | Inspected capability | Consequence |
| --- | --- | --- |
| Outbound TCP | `TCP.connect` | Supplies a native implementation reference. The independent worker owns connection establishment and deadlines. |
| Binary traffic | `TCP.send_bytes`, `TCP.recv_bytes` | Arbitrary bytes do not pass through String decoding. |
| Bounded waits | `TCP.try_send_bytes`, `TCP.try_recv_bytes` | Reference implementations for readiness waits and deadlines. |
| Partial-send recovery | Exact unsent bytes returned on timeout or failure | Resume the already encoded suffix, preserving wire order. |
| Concurrent IO computations | `IO.fork`, `IO.spawn`, channels | Interleave Bend-side coordination tasks within one process, without providing independent computation scheduling. |
| Bounded channel operations | `Chan.new`, `Chan.try_send`, `Chan.try_recv` | Support Bend-side queues. The native socket worker uses separate native queues. |
| WebSocket randomness | `IO.random_u32` | Fresh handshake nonces and frame masks can use host entropy. |
| Custom native integration | Foreign C effects | Missing deployment facilities can be added without first changing the compiler. |

Sources: [Base signatures][bend-base], [native send][bend-send], [native receive][bend-recv], [entropy][bend-random], and [foreign-effect integration][bend-effects]. Linux entropy uses `getrandom`; the Apple implementation uses `arc4random_buf`.

The architecture's statement that low-level transport needs implementation remains correct for WebSocket. Current Base already implements the TCP portion. Its native sockets are nonblocking and suspend through the IO loop when readiness is unavailable. [Connect][bend-connect], [send][bend-send], [receive][bend-recv]

### Constraints that affect the design

- `Socket` is affine. An operation consumes it and returns it beside the result. Base has no socket split or clone operation. Two independent Bend tasks cannot both receive the same socket. [Base][bend-base]
- Native `TCP.connect` accepts IPv4 literals, matching the loopback deployment. DNS and IPv6 are outside the initial transport scope. [Connect implementation][bend-connect]
- Base byte operations use `List<&2, U32>`, one list cell per byte, with values constrained to 0 through 255. Native send converts the list to a C buffer; receive converts its buffer back to a list. This is not packed-byte or zero-copy transport. [Base][bend-base], [send][bend-send], [receive][bend-recv]
- Bend interleaves IO computations through one event loop. A pure computation runs until its next effect, even if it uses parallel CPU or GPU calls. Long mappings can delay network handling. `IO.fork` alone does not eliminate that delay. [Execution model][bend-guide]
- Persistent loops require an explicit unchecked recursion boundary or a finite lifetime/fuel policy. Parsers and codecs can remain terminating pure functions. Foreign effects and unchecked definitions are outside Bend's proof guarantees. [Bend guide][bend-guide]

## Recommended library design

### Caller experience

The coordinator starts the connector with deployment settings, resource limits, and generated contracts. Under the [trusted-local policy](confluxion-arch1.md#trusted-local-access-policy), it connects anonymously without client credentials. It establishes the declared input subscription and waits for a consistent ready snapshot.

For work in a partition, the coordinator obtains typed inputs and versions, computes a proposal, and submits a generated commit call. The connector returns a confirmed result, a declared rejection, a host failure, or an uncertain outcome after connection loss. It applies any result-embedded subscription update before reporting successful completion.

The coordinator handles durable reconciliation through operation IDs and checkpoints. It does not parse WebSocket frames, assign protocol request IDs, or mutate subscription-cache internals.

### Module boundaries

These are proposed responsibilities, not existing modules or APIs:

| Module | Owns |
| --- | --- |
| WebSocket | Rust/Tungstenite integration, configured limits, HTTP upgrade policy, control flushing, deadlines, and TCP progress |
| BSATN | Byte readers/writers, scalar and collection encoding, checked lengths, and codec failures |
| SpacetimeDB connector | Pinned envelopes, connection metadata, request correlation, subscriptions, cache publication, and connection generations |
| Generated bindings | Application codecs, typed operations, row keys, subscriptions, and schema compatibility metadata |

The WebSocket library handles complete binary messages without knowing SpacetimeDB or BSATN. The connector composes those libraries. Confluxion's public service interface exposes generated domain contracts rather than wire enums.

The conceptual interface needs start, subscription readiness, input-view access, typed call submission, and shutdown. Its state distinguishes connecting, synchronizing, ready, recovering, and closed. A ready input view carries its connection generation so a disconnected session cannot silently supply current-looking inputs.

### Independent transport progress

Bend already waits on socket readiness through its native IO loop. Pure computation occupies that loop until the next effect. A ring buffer serviced by the same loop cannot receive incoming traffic while the loop computes. [Execution model][bend-guide]

The kernel socket receive buffer continues operating during application computation. TCP provides an ordered, reliable byte stream with flow control and retransmission. Stopping application reads creates backpressure, but server queues can fill or the connection can time out. [TCP behavior][tcp], [SpacetimeDB output queue][stdb-outgoing]

```text
	SpacetimeDB WebSocket
	          ↕
	Native full-duplex socket worker
	  socket readiness, frames, Ping/Pong, partial sends
	          ↕
	Bounded inbound and outbound native buffers
	          ↕ custom effects at Bend IO boundaries
	Bend protocol session and BSATN codecs
	          ↕
	Bend coordinator and bounded job admission
	          ↕
	Computation in this BendRT worker process
```

Each process in the bounded computation pool has this transport path. The coordinator assigns work according to capacity and partition ownership.

One native thread per BendRT process exclusively owns that process's sockets. It drains readable bytes, assembles bounded messages in order, handles controls, and advances outgoing frames with exact partial-send offsets. Each turn bounds read and write work so either direction can progress under sustained traffic. It waits using `poll` for a few sockets or level-triggered `epoll` for a larger set. A wake descriptor notifies it of outbound work and shutdown. Linux `eventfd` supplies that notification mechanism. [Readiness][epoll], [wake descriptor][eventfd]

The inbound queue has one native producer and one Bend-session consumer. The outbound queue reverses those roles. Other Bend tasks submit through the session owner. Single-producer/single-consumer rings are possible, but a bounded queue with short mutex-protected operations is also valid. Lock-free queues are not required for independent IO progress.

Packed byte blocks, bounded message descriptors, and a ring or slab pool avoid one Bend list cell per buffered byte. Explicit buffer ownership and release remain necessary. Buffering alone does not establish zero-copy access from Bend.

### Native effect boundary and lifecycle

Bend permits custom C effects and supplies `io_work` for helper-thread native work. The selected Rust library connects through a small C effect adapter and manages its own independent worker lifecycle. The subprocess effect supplies a runtime integration reference, not the persistent transport implementation. [Foreign effects][bend-effects], [helper-thread example][bend-process]

A persistent transport service is new integration work. It needs startup, native-state lifetime, wakeups, queue access, error propagation, and deterministic shutdown. Its background thread operates on native bytes and metadata. It must not construct Bend heap terms or call arbitrary Bend functions or Base channel internals concurrently without a verified runtime interface.

The native worker implements connection, upgrade, partial-write progress, and close deadlines directly. Shutdown stops new job admission, settles active work within a deadline, then closes connections and joins native workers. Remaining durable work is recovered through the coordinator's operation and checkpoint protocol.

Bend-side effects transfer admitted bytes at IO boundaries and construct Bend values on the permitted runtime thread. Base TCP operations must not also read or close the service's descriptor. An affine session capability and native generation checks can protect access, but its exact representation needs validation because arbitrary opaque native handle types are restricted. The effect ABI is compiler-specific. [Handle and ABI constraints][bend-effects]

The thread preserves transport progress while Bend computes. Bend still decodes messages, updates its cache, and makes application decisions at IO boundaries. This design depends on OS scheduling and available resources, not a hard real-time guarantee.

### Frame ordering and bounded buffering

Outgoing messages use bounded frames. A partially transmitted frame must finish before another frame begins. Ping/Pong and Close receive priority at frame boundaries, including between fragments of a data message. A partial frame that exceeds its progress deadline closes the connection. Its suffix cannot be dropped and replaced with a new frame. [RFC sections 5.4 and 5.5][rfc]

Queues need both slot limits and retained-byte limits. The socket owner uses nonblocking native queue admission and reserves control capacity. Outbound admission distinguishes rejected messages from accepted messages awaiting transmission. Already accepted messages must not be resubmitted when a write encounters backpressure. If the connector cannot retain a complete subscription update within policy, it invalidates readiness and disconnects. Silent update loss would corrupt the coordinator's input state.

Job admission also bounds queued inputs, active computations, and completed results waiting for transmission. Outbound congestion stops admission of further jobs before completed-result storage is exhausted. The worker pool remains bounded under overload. Queue capacity absorbs bursts and computation pauses; it does not increase sustained computation capacity.

A ring must not overwrite unread messages. An approximate capacity requirement during a processing pause is:

$$B \geq B_0 + R_{in}T_{pause} + M_{max} + H$$

Here $B$ is the byte budget, $B_0$ is existing backlog, $R_{in}$ is an assumed incoming byte-rate bound, $T_{pause}$ is the maximum pause, $M_{max}$ is the maximum assembled message, and $H$ is descriptor and reserved headroom. This is a sizing model, not a measured workload or timing guarantee. Descriptor counts also need bounds.

Every finite buffer eventually fills if sustained production exceeds consumption. The options are to reduce incoming work, increase processing capacity, spool recoverable data, or disconnect and reconcile. Pausing reads propagates TCP backpressure. Controls share the byte stream and can sit behind application data, so reserved control capacity cannot guarantee heartbeat progress during indefinite saturation.

### WebSocket work still required

The Rust library supplies the core protocol implementation. Our integration must configure and verify these behaviors rather than write another framing engine:

- An HTTP/1.1 upgrade request and incremental response parser. Validate status 101, Upgrade and Connection headers, the selected subprotocol, and `Sec-WebSocket-Accept`.
- SHA-1 and Base64 for accept-value validation. Omitting SSL does not remove this handshake requirement. These can be finite pure Bend helpers using U32 operations.
- A fresh random 16-byte handshake key and fresh unpredictable 32-bit mask per client frame, using Base entropy.
- Incremental binary frame parsing across arbitrary TCP chunk boundaries. Preserve bytes received after the HTTP header terminator.
- Big-endian WebSocket extended-length parsing, with checked upper bounds before allocation. BSATN's little-endian lengths are separate.
- Continuation-frame assembly, interleaved control handling, mask/RSV/opcode validation, and close-code/reason validation.
- Bounded frame/message sizes, header bytes, fragment counts, wait times, and close-handshake duration.

These requirements come from [RFC 6455 sections 4 through 8][rfc]. The configured Tungstenite client supports text and binary messages for conformance testing; the application's initial traffic is binary. Text validation includes fragmented UTF-8. WebSocket compression extensions remain disabled. Comparing our worker with a Tungstenite reference is an integration comparison; uWebSockets and Autobahn provide independent interoperability evidence.

## Gated implementation plan

Implementation: when building a phase or validating its gate, read [ws-phased-plan.md](ws-phased-plan.md) for the ordered work, current statuses, benchmark workloads, and pass criteria.

## BSATN coverage and representation

Encoding is mechanically defined, so a schema-derived pure Bend codec is feasible. Products concatenate fields in schema order. Sums encode a one-byte variant index followed by the selected payload. Arrays use a little-endian U32 element count. Strings use a little-endian U32 UTF-8 byte count followed by those bytes. Integers are little-endian; signed integers use their signed bit representation. Floats transmit IEEE bits. [BSATN specification][bsatn]

For example, a product with `u32` value 42 and String value `"ok"` encodes as `2a 00 00 00 02 00 00 00 6f 6b`. There is no product header or extra array-length prefix around the String bytes.

### A bounded first type set

| BSATN type | Proposed Bend boundary representation |
| --- | --- |
| Bool | Base Bool with strict 0/1 decoding |
| u8, u16, u32 | U32 with range-checked narrower wrappers |
| u64 and i64 | Explicit two-U32-word values with exact wire bits |
| u128 and u256 | Fixed U32-word records for identities and other required protocol fields |
| String | Bend String through a strict UTF-8 codec and byte-length check |
| Product | Generated record codec in fixed schema field order |
| Sum | Generated variant codec in fixed schema variant order |
| Array and byte array | Bounded collection with explicit length and byte checks |
| Unit and optional/result values | Exact schema-defined product/sum representation |

Wide integers are required even if the application's first reducer uses only U32. Connection metadata includes identities and connection IDs; timestamps and row-offset wrappers include wider fields. A blanket U32-only connector cannot decode the protocol. [v2 types][stdb-v2], [common wire types][stdb-common]

Base has U32, F32, and `Word(n)`, not a complete built-in SATS scalar family. Use explicit words for wide values rather than lossy conversion through F32 or an assumed unbounded native Nat. Full signed arithmetic and F64 computation are additional libraries. [Base types][bend-base]

F32 support is plausible, and Base exposes its bits, but bit-exact decoding and edge cases need validation. F64 can initially be represented as opaque bits or rejected as an application boundary type. Neither choice establishes full numeric support. Unsupported application types must fail generation rather than silently narrow. [Base][bend-base], [BSATN][bsatn]

Functions, IO computations, handles, and erased proofs do not cross the boundary. Dependent types require an explicit concrete wire representation. A reusable `Data` declaration alone is insufficient. [Architecture, contracts](confluxion-arch1.md#contracts-come-from-the-bend-boundary-and-use-bsatn)

### Codec and generation obligations

Decoders reject truncation, invalid tags, invalid UTF-8, overflowing lengths, excessive nesting, and trailing bytes at complete-message boundaries. Limits apply before collection allocation. Encoders reject values outside narrower scalar ranges. Chunked accumulation avoids repeatedly appending the entire growing byte list.

Bend declarations remain authoritative, as the architecture requires. The generator emits application codecs and module shims from those declarations. After building the module, it compares extracted `RawModuleDefV10` metadata against the contracts. That check detects field-order, variant-order, and result-type disagreement.

SpacetimeDB supplies schema metadata and first-party code-generation machinery, but no Bend target in the inspected release. The v2 protocol schema is separately available as `ws_schema-2.json`. A fixed library owns those envelopes; application generation owns domain types. [Schema endpoint][stdb-schema], [schema definitions][stdb-raw], [generator][stdb-generator], [wire schema][stdb-wire-schema]

## SpacetimeDB session details that must be correct

### Anonymous local connection and framing

The initial loopback connection omits `Authorization` and follows the [trusted-local policy](confluxion-arch1.md#trusted-local-access-policy). The initial v2 message still returns identity, connection ID, and a server-issued token. Decode these fields as protocol metadata. The local profile does not require login, credential checks, or identity continuity across reconnects; durable work is correlated by operation IDs and checkpoints. [Upgrade handler][stdb-upgrade], [v2 schema][stdb-v2]

Client-to-server v2 traffic is one BSATN `ClientMessage` per binary WebSocket message, without a compression prefix. Server-to-client traffic has a prefix byte before the BSATN `ServerMessage`: 0 for none, 1 for Brotli, and 2 for gzip. `compression=None` still requires stripping byte 0. Omitted compression defaults to Brotli in this release. [SDK][stdb-sdk], [serialization][stdb-serialization], [common constants][stdb-common]

The first implementation explicitly selects `None` and rejects unexpected compressed payloads. This avoids a decompressor dependency while keeping the framing correct. Application compression and WebSocket extension compression are distinct.

### Calls and transaction updates

`CallReducer` contains a request ID, reserved flags, reducer name, and BSATN argument-product bytes. Request IDs correlate replies within a connection. They do not deduplicate database operations. [v2 declarations][stdb-v2]

The result distinguishes success with return bytes and an embedded update, empty success, structured declared error bytes, and internal error text. The caller's subscribed changes can arrive inside its successful reducer result. Other affected clients receive standalone transaction updates. Apply the embedded update once, before completing the call locally. [Result schema][stdb-v2], [broadcast implementation][stdb-broadcast]

`confirmed=true` gates transaction-associated messages on durability in the inspected release. It does not make a dropped reply proof that the operation failed. Durable operation identities and completion lookup remain necessary. [Durability gate][stdb-durability]

### Reducer write boundary and serialization

The reducer's declared argument product carries rows and any operation identities, versions, or checkpoints. For a typed row array, that product contains a U32 row count followed by each row's fields. The envelope wraps the whole product with an argument byte count. An opaque byte-array parameter instead includes its own byte count, and its contents need an explicit interpretation. [Protocol][stdb-v2], [BSATN][bsatn]

The public v2 client enum has no client-held transaction or arbitrary row-write messages. One reducer call can write several tables atomically inside its server-owned transaction. Separate calls do not share a client-opened transaction. [Protocol][stdb-v2], [reducer execution][stdb-tx]

The ordinary typed insertion path has these boundaries:

```text
	Bend values → BSATN encoding → binary WebSocket
	  → host envelope decode and argument schema checking
	  → original argument bytes reused for module dispatch
	  → module argument decoding into Rust or JavaScript values
	  → table API serializes rows as BSATN
	  → datastore validates and converts rows into storage layout
	  → commit and subscription evaluation
	  → matching changes encoded into BSATN subscription deltas
	  → binary WebSocket → Bend decoding
```

The host retains incoming BSATN argument bytes for dispatch. WASM still copies them into guest memory. The normal Rust and TypeScript table APIs encode typed rows for insertion. Storage converts row bytes into its layout and indexes. Subscription row-byte buffers can be shared among recipients. [Argument cache][stdb-args], [WASM transfer][stdb-wasm-copy], [Rust table API][stdb-insert], [TypeScript table API][stdb-ts-insert], [storage][stdb-storage], [shared deltas][stdb-delta]

Passing already encoded row bytes to the module-local insertion ABI is a distinct route from the typed table API. It does not expose direct table writes over WebSocket or bypass storage validation and transaction rules. The costs of either route require separate measurement. [Insertion ABI][stdb-raw-insert]

### Subscription state

The connector waits for the initial applied snapshot before publishing ready inputs. Related inputs need a common query set or the batch subscription mechanism that establishes them at one transaction offset. A partially successful subscription batch is not readiness for a service that needs every input. [v2 subscription messages][stdb-v2], [subscription actor][stdb-actor]

Each logical transaction update is staged and published as one cache transition. It can contain several tables and repeated table entries. Processing must not observe half an update. `BsatnRowList` also has its own size-hint and flattened-byte wrapper, including U64 row offsets for variable-sized rows. It is not an ordinary array of length-prefixed rows. [Common wire types][stdb-common], [v2 updates][stdb-v2]

Overlapping query sets need per-set row membership or equivalent counts. Removing one subscription must preserve a row still supplied by another. For the first Confluxion bindings, explicit table primary keys and bounded query sets reduce cache complexity. [Subscription behavior][stdb-subscriptions]

A fresh snapshot represents current query state, not every offline transition. The public v2 update schema has no durable replay cursor. Persisted domain events, sequence numbers, and checkpoints implement Confluxion recovery. Transient event-table notifications cannot be the only authoritative history. [v2 schema][stdb-v2], [subscription semantics][stdb-semantics], [architecture recovery](confluxion-arch1.md#closure-failure-and-recovery)

## Overload, delivery, and recovery

SpacetimeDB's output queue uses nonblocking admission. If it fills, the attempted message is not queued and the connection is aborted. Incoming queue overflow can also trigger closure. Some outstanding calls may already have executed; others remain unprocessed or are discarded. [Output overflow][stdb-outgoing], [input overflow][stdb-incoming]

| Item | Consequence of connection loss |
| --- | --- |
| Unprocessed reducer request | It may never execute. |
| Executed reducer without a received reply | Its outcome is uncertain to Bend. |
| Committed persistent rows | Lost output delivery does not undo the commit. |
| Subscription notification | It may be missed without historical replay. |
| Transient event-table rows | A reconnect snapshot does not restore their history. |

Execution commits before subscription output is delivered. `confirmed=true` waits for configured durability before transaction-associated messages. A database without persistence bypasses that gate; confirmation does not create persistence or guarantee delivery. [Commit ordering][stdb-commit], [confirmation][stdb-durability]

Persistent accepted events, checkpoints, and completion records support recovery. Bounded subscriptions, row batches, and in-flight call limits reduce pressure. Reconnection obtains fresh state and reconciles operation identities before resuming affected work.

## Work size and unresolved risks

This is a substantial client-library project. TCP is already available, but a usable connector includes WebSocket conformance, a schema-directed binary codec, transaction-aware subscriptions, and interrupted-call handling. A successful handshake alone demonstrates little of the required behavior.

The remaining work is concentrated in these areas:

- Tungstenite integration and duplex scheduling still need qualification. Slow peers, configured limits, partial writes, and control-frame flushing are the decisive cases.
- Protocol metadata needs exact wide integers, UTF-8 handling, and row-list decoding before simple application calls are sufficient.
- Packed native buffer transfer, resource lifetime, snapshot staging, and queue limits need validation against real workloads.
- Long Bend mappings delay application update processing even when the native thread continues socket receipt and Ping/Pong.
- Native connection and progress deadlines need validation under slow peers and shutdown.
- Worker count, CPU threads per worker, and queue budgets need validation with SpacetimeDB sharing the same machine.
- Schema extraction and Bend code generation are new work. Contract agreement must be verified against the built module, not assumed from type checking alone.
- Recovery depends on the generated module's durable operation/checkpoint protocol. The connector cannot provide exactly-once processing through WebSocket IDs.

A calendar estimate would depend on the required scalar family, workload sizes, and scheduling results. This report establishes a concrete build route rather than an untested delivery date.

## Validation gates before declaring the connector usable

These are proposed future checks, not checks completed by this research:

1. Compile with the selected Bend pin and establish a valid plaintext v2 handshake. Decode initial metadata and verify accept-key handling against RFC vectors.
2. Demonstrate independent incoming subscriptions and outgoing calls on one socket. Exercise slow readers, slow writers, partial sends, fragmented messages, and interleaved Ping/Pong.
3. Compare Bend encodings byte-for-byte with the pinned SpacetimeDB SATS implementation. Include nested values, Unicode, wide integer boundaries, and malformed length/tag rejection.
4. Receive a consistent initial snapshot and a multi-table transaction update. Prove that application work sees only the complete published update, including updates embedded in call results.
5. Drop the connection after submission but before reply. Reconnect, reconcile durable operation records, and show that retries do not accept duplicate projection effects.
6. Run realistic and deliberately long Bend mappings while incoming messages, already completed outgoing results, and server pings continue. Pause either peer's reads and verify progress in the opposite direction. Verify bounded queues and deterministic native-thread shutdown.
7. Generate a module and bindings from Bend declarations. Change a field order or return type and confirm that schema comparison rejects the disagreement.
8. Saturate the configured computation pool and verify that worker count, queued jobs, active jobs, and completed-result storage remain bounded. Exercise out-of-order completion and partition ordering. Measure receiver-validated messages per second in each direction, latency including queue waits, CPU use, and peak retained memory. Sweep process count and CPU threads per worker with SpacetimeDB on the same machine.

Pure-code proofs can establish codec and state-transition properties under declared assumptions. They cannot alone establish socket behavior, timing, native entropy behavior, or interoperability with the deployed server.

## Recommendation

Follow the [phase plan](ws-phased-plan.md) through Phase 4, then start the SpacetimeDB delivery track. Use Rust/Tungstenite, IPv4 loopback, and uncompressed traffic. Throughput tuning, the computation pool, and optional all-Bend WebSockets are separate from delivering the initial reliable connection.

The first decisive result is sustained bidirectional transport progress while Bend computes and peers apply backpressure. Native buffer ownership, deterministic shutdown, and uncertain-commit recovery must also pass before expanding generated service coverage.

[bend-revision]: https://github.com/bendlang/bend/commit/9c586f094f4fc88bef440dc8b4d77a2f92c257cc
[bend-base]: https://github.com/bendlang/bend/blob/9c586f094f4fc88bef440dc8b4d77a2f92c257cc/bend2/base.bend
[bend-guide]: https://github.com/bendlang/bend/blob/9c586f094f4fc88bef440dc8b4d77a2f92c257cc/guide/GUIDE.md
[bend-effects]: https://github.com/bendlang/bend/blob/9c586f094f4fc88bef440dc8b4d77a2f92c257cc/guide/EFFECTS.md
[bend-connect]: https://github.com/bendlang/bend/blob/9c586f094f4fc88bef440dc8b4d77a2f92c257cc/bend2/effs/tcp_connect.c
[bend-send]: https://github.com/bendlang/bend/blob/9c586f094f4fc88bef440dc8b4d77a2f92c257cc/bend2/effs/tcp_send.c
[bend-recv]: https://github.com/bendlang/bend/blob/9c586f094f4fc88bef440dc8b4d77a2f92c257cc/bend2/effs/tcp_recv.c
[bend-random]: https://github.com/bendlang/bend/blob/9c586f094f4fc88bef440dc8b4d77a2f92c257cc/bend2/effs/random_u32.c
[rfc]: https://www.rfc-editor.org/rfc/rfc6455
[bsatn]: https://spacetimedb.com/docs/bsatn
[commitlog]: https://spacetimedb.com/docs/reference/internals/commitlog
[stdb-revision]: https://github.com/clockworklabs/SpacetimeDB/commit/07efb0c44c42288ed8e40dcece75659cf44ec80c
[stdb-http]: https://spacetimedb.com/docs/http/database#get-v1databasename_or_identitysubscribe
[stdb-sdk]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/sdks/rust/src/websocket.rs
[stdb-v2]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/client-api-messages/src/websocket/v2.rs
[stdb-v3]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/client-api-messages/src/websocket/v3.rs
[stdb-upgrade]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/client-api/src/routes/subscribe.rs
[stdb-common]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/client-api-messages/src/websocket/common.rs
[stdb-serialization]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/core/src/client/messages.rs#L155-L240
[stdb-schema]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/client-api/src/routes/database.rs#L729-L778
[stdb-raw]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/lib/src/db/raw_def/v10.rs
[stdb-generator]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/cli/src/subcommands/generate.rs
[stdb-wire-schema]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/client-api-messages/ws_schema-2.json
[stdb-broadcast]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/core/src/subscription/module_subscription_manager.rs#L2101-L2142
[stdb-durability]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/core/src/client/client_connection.rs#L156-L258
[stdb-actor]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/core/src/subscription/module_subscription_actor.rs
[stdb-subscriptions]: https://spacetimedb.com/docs/clients/subscriptions
[stdb-semantics]: https://spacetimedb.com/docs/clients/subscriptions/semantics
[bend-process]: https://github.com/bendlang/bend/blob/9c586f094f4fc88bef440dc8b4d77a2f92c257cc/bend2/effs/process_run.c
[tcp]: https://man7.org/linux/man-pages/man7/tcp.7.html
[epoll]: https://man7.org/linux/man-pages/man7/epoll.7.html
[eventfd]: https://man7.org/linux/man-pages/man2/eventfd.2.html
[stdb-args]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/core/src/host/mod.rs#L72-L123
[stdb-tx]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/core/src/host/wasm_common/module_host_actor.rs#L1181-L1343
[stdb-wasm-copy]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/core/src/host/wasmtime/wasm_instance_env.rs#L1366-L1403
[stdb-insert]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/bindings/src/table.rs#L1855-L1877
[stdb-ts-insert]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/sdks/typescript/src/server/runtime.ts#L1308-L1323
[stdb-raw-insert]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/core/src/host/wasmtime/wasm_instance_env.rs#L980-L1007
[stdb-storage]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/table/src/table.rs#L696-L750
[stdb-delta]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/core/src/subscription/module_subscription_manager.rs#L1426-L1454
[stdb-outgoing]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/core/src/client/client_connection.rs#L411-L489
[stdb-incoming]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/client-api/src/routes/subscribe.rs#L1078-L1166
[stdb-commit]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/core/src/subscription/module_subscription_actor.rs#L2041-L2143
