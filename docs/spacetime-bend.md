# SpacetimeDB and native BendRT: connector feasibility

## Goal and evidence

Keep most application logic in native BendRT while accessing SpacetimeDB with fewer intermediate representations than the Node SDK and BendFlow codec path.

This report records read-only research on November 16, 2026. No connector was implemented, no live integration was tested, and no performance improvement was measured. SpacetimeDB source findings use revision `07e99d13506e55ba85872ab08931baec271e445a`. Local Bend findings use BendFlow's compiler pin `f0cf0a7c573145501981c44d9c33e326185f008e`.

The November 17 follow-up adds an external, event-sourced projection design. Follow-up protocol findings use SpacetimeDB revision `3a24691a352df64ffdd3834c439b88e9c07ce3ba` and the linked official documentation. The design combines supported database operations but has not been implemented or tested. Neither source revision establishes the version deployed by a particular service.

This research is separate from the [Phase 4 plan](bend-node-phase4.md). SpacetimeDB integration is not part of Phase 4.

## Desired feature: automatic build and publish

You can write Bend code, configure one build/publish script, and have it automatically generate, build, and publish the SpacetimeDB module and frontend bindings.

This is the desired developer experience for self-hosted SpacetimeDB, not an implemented capability. Bend source supplies the model and integration declarations. The generator emits table definitions, transactional shims, subscription helpers, and native coordinator bindings. The script invokes the standard SpacetimeDB tools and builds the frontend with its generated bindings.

After one-time deployment setup, routine changes require no separate manual table, reducer, or subscription registrations and no hand-maintained Rust or TypeScript module. The script also builds and deploys the native Bend worker beside SpacetimeDB. Publishing the database module alone does not deploy that worker. Incompatible event or projection changes still require an explicit migration or rebuild policy that the script can execute.

The [CQRS integration assessment](spacetime-bend-cqrs.md) describes platform reuse and the generation boundary. The native connector, coordinator, and shim generator remain to be implemented.

## Feasibility judgment

A native BendRT service can call SpacetimeDB reducers and procedures through the public BSATN WebSocket protocol without the Node SDK. A direct connector can remove intermediate JavaScript values and DB-related Node-to-worker payload crossings.

A BSATN codec is only part of that connector. Transport, authentication, response correlation, and recovery remain necessary. The connection still serializes values; direct access does not imply zero-copy.

Native BendRT can run beside self-hosted SpacetimeDB as a separate process or container. The inspected database module interfaces do not support embedding native BendRT or launching its executable.

An event-sourced pipeline can avoid the in-module BendRT limitation. Native Bend computes projections from durable events outside a transaction. A generated module operation then atomically validates the proposal, applies all state changes, and advances the projection checkpoint. React subscribes directly to the committed state. This preserves native Bend execution without porting its runtime into SpacetimeDB.

The remaining module contains schema definitions and transactional shims. The mapping algorithm can remain entirely in trusted Bend code. Computation and commit are separate stages, so correctness depends on complete dependency checks and retry-safe commits.

## What runs inside SpacetimeDB

TypeScript modules become browser-targeted JavaScript ES-module bundles at `dist/bundle.js` and run in V8. Rust modules compile to `wasm32-unknown-unknown` and run in Wasmtime. TypeScript does not compile to WASM in this execution path.

The host selects between `Js` and `Wasm`. The inspected interfaces provide no general native-library loader or subprocess capability for published modules.

Sources: [TypeScript build](https://github.com/ClockworkLabs/SpacetimeDB/blob/07e99d13506e55ba85872ab08931baec271e445a/crates/cli/src/tasks/javascript.rs), [Rust build](https://github.com/ClockworkLabs/SpacetimeDB/blob/07e99d13506e55ba85872ab08931baec271e445a/crates/cli/src/tasks/rust.rs), and [host dispatch](https://github.com/ClockworkLabs/SpacetimeDB/blob/07e99d13506e55ba85872ab08931baec271e445a/crates/core/src/host/host_controller.rs#L1063-L1081).

| Placement | Assessment |
| --- | --- |
| Native BendRT beside self-hosted SpacetimeDB | Feasible through ordinary networking; separate lifecycle and database transactions |
| External native BendRT connected to Maincloud | Supported client architecture; Bend compute needs separate hosting |
| Native sidecar uploaded into Maincloud | No supported facility or guaranteed co-location established by the inspected documentation |
| Native BendRT embedded in a database module | Unsupported by the inspected module interfaces |
| Pure Bend-generated `.mjs` bundled into a TypeScript module | Plausible but untested; runs JavaScript rather than native BendRT and loses native parallel execution |

Sources: [self-hosting](https://github.com/ClockworkLabs/SpacetimeDB/blob/07e99d13506e55ba85872ab08931baec271e445a/docs/docs/00250-how-to/00100-deploy/00200-self-hosting.md), [Maincloud](https://github.com/ClockworkLabs/SpacetimeDB/blob/07e99d13506e55ba85872ab08931baec271e445a/docs/docs/00250-how-to/00100-deploy/00100-maincloud.md), and [Bend JavaScript execution](https://github.com/bendlang/bend/blob/f0cf0a7c573145501981c44d9c33e326185f008e/guide/GUIDE.md).

## Why parallel BendRT cannot run inside the current SpacetimeDB module

BendRT here means Bend's emitted `.c` file, including its runtime. Compiling that C for WASM and linking it with a Rust module would place both inside one module. However, a Rust wrapper cannot supply the native execution services that BendRT requires. This assessment applies to the inspected revisions listed above.

BendRT turns independent parallel calls into tasks that CPU worker threads execute against a shared heap. The generated runtime creates workers with `pthread_create` and coordinates them with atomic operations, pthread mutexes, and condition variables. The inspected SpacetimeDB WASM linker registers `spacetime_10.*` module APIs, but no WASI services or guest-thread creation facility.

SpacetimeDB's source describes its WASM execution as a single executor backed by one OS thread, with synchronous reducers running inline. Async procedures can yield while waiting, but that does not provide simultaneous CPU execution of Bend tasks. The host also accesses the exported module memory as ordinary `wasmtime::Memory`. The inspected integration has no shared-memory execution path for guest workers.

| BendRT requirement | Constraint in the inspected module environment |
| --- | --- |
| CPU workers created with `pthread_create` | No registered guest-thread creation service |
| Shared heap and worker synchronization | No shared-memory worker integration or supplied pthread runtime |
| Native virtual-memory allocation and stack guards | WASM linear memory does not provide BendRT's POSIX mapping and signal model |
| Worker lifecycle and resource accounting | No BendRT-worker integration into module execution |

The allocator is a separate obstacle even for sequential execution. The CPU runtime initially reserves 8 GiB for its corpus and approximately 2 GiB per evaluator stack. These are virtual-address reservations, not necessarily resident RAM usage. A wasm32 linear memory has a 4 GiB address space. BendRT's mapping strategy, stack guards, and signal handlers therefore need replacement. Selecting `--threads 1` does not remove those dependencies.

The emitted C is also an executable with a `main`, rather than a supported library interface for reducer calls. Bend lists a native library target for pure definitions as planned. An in-module port needs callable definitions and explicit initialization, argument and result handling, cleanup, and failure behavior.

Threaded WebAssembly is possible in a host that supplies shared memory and worker execution. The limitation here is the inspected SpacetimeDB integration. Enabling a Wasmtime threading option alone would not provide the missing services. Full parallel BendRT inside WASM requires changes to both BendRT and the SpacetimeDB host. CUDA and Metal execution also have no supported path through the inspected module ABI.

A computation-only, single-threaded BendRT port is plausible but untested. Clang can compile adapted C into WASM objects for linking with Rust, but changing the compilation target alone does not adapt the runtime. Such a port could keep Bend computation inside a reducer while Rust owns database operations. It would not preserve BendRT's multicore parallelism.

Sources: [Bend worker pool](https://github.com/bendlang/bend/blob/f0cf0a7c573145501981c44d9c33e326185f008e/bend2/comp.ts#L4717-L4783), [stack allocation](https://github.com/bendlang/bend/blob/f0cf0a7c573145501981c44d9c33e326185f008e/bend2/comp.ts#L4686-L4712), [corpus allocation](https://github.com/bendlang/bend/blob/f0cf0a7c573145501981c44d9c33e326185f008e/bend2/comp.ts#L5234-L5302), [native library limitation](https://github.com/bendlang/bend/blob/f0cf0a7c573145501981c44d9c33e326185f008e/WONTFIX.txt#L152-L154), [SpacetimeDB execution model](https://github.com/ClockworkLabs/SpacetimeDB/blob/07e99d13506e55ba85872ab08931baec271e445a/crates/core/src/host/module_host.rs#L496-L508), [host linker and memory integration](https://github.com/ClockworkLabs/SpacetimeDB/blob/07e99d13506e55ba85872ab08931baec271e445a/crates/core/src/host/wasmtime/mod.rs), [registered module imports](https://github.com/ClockworkLabs/SpacetimeDB/blob/07e99d13506e55ba85872ab08931baec271e445a/crates/core/src/host/wasm_common.rs), and [LLVM WASM linking](https://lld.llvm.org/WebAssembly.html).

## Direct reducer and procedure calls

The public WebSocket endpoint is `/v1/database/{database}/subscribe`. Connecting does not require registering a SQL subscription. The connection supports reducer calls, procedure calls, one-off queries, and subscriptions.

`CallReducer` and `CallProcedure` carry a request ID, reserved flags, the function name, and a BSATN-encoded argument product matching the function's schema. Any compatible native client can construct these messages. Node is not required.

| Response | Meaning at the inspected revision |
| --- | --- |
| Reducer `Ok` | Committed transaction, return bytes, and subscribed transaction updates |
| Reducer `OkEmpty` | Committed transaction with empty return bytes and no subscribed updates |
| Reducer `Err` | Structured error bytes; transaction did not commit |
| Reducer `InternalError` | Diagnostic host failure |
| Procedure `Returned` | Application return bytes, possibly containing an application Result or optional value |
| Procedure `InternalError` | Host invocation failure |

V2 sends one application message per WebSocket message. V3 permits consecutive V2-schema messages in one WebSocket payload. Server binary payloads carry an application compression tag, including when uncompressed. WebSocket fragmentation is a separate transport concern.

The ordinary public HTTP `/call/{function}` endpoint requires JSON. The BSATN `call-from-database` endpoint has separate inter-database authentication and sequencing; it is not a general native-client shortcut.

Sources: [V2 protocol](https://github.com/ClockworkLabs/SpacetimeDB/blob/07e99d13506e55ba85872ab08931baec271e445a/crates/client-api-messages/src/websocket/v2.rs), [V3 framing](https://github.com/ClockworkLabs/SpacetimeDB/blob/07e99d13506e55ba85872ab08931baec271e445a/crates/client-api-messages/src/websocket/v3.rs), [WebSocket endpoint](https://github.com/ClockworkLabs/SpacetimeDB/blob/07e99d13506e55ba85872ab08931baec271e445a/crates/client-api/src/routes/subscribe.rs), and [HTTP handlers](https://github.com/ClockworkLabs/SpacetimeDB/blob/07e99d13506e55ba85872ab08931baec271e445a/crates/client-api/src/routes/database.rs).

## Serialization paths

The SDK-mediated payload path is:

```text
	SpacetimeDB BSATN
	  -> Node SDK decodes JavaScript values
	  -> BendFlow encodes its binary format
	  -> native worker decodes and computes
	  -> BendFlow encodes the reply
	  -> Node decodes JavaScript values
	  -> Node SDK encodes BSATN
	  -> SpacetimeDB
```

The direct payload path would be:

```text
	SpacetimeDB BSATN
	  -> native transport unwraps the protocol message
	  -> Bend BSATN decoder
	  -> Bend computation
	  -> Bend BSATN encoder
	  -> native transport wraps the protocol message
	  -> SpacetimeDB
```

The Node gateway calling the SDK is an in-process function call, not another network transfer. The direct path removes intermediate representations and worker crossings for database payloads. Public endpoint traffic can still use Node and the existing endpoint boundary.

BendFlow uses big-endian scalars and one-byte String/List lengths. BSATN uses little-endian scalars and U32 variable-length counts. The existing codecs cannot forward database bytes unchanged. The 248-byte BendFlow payload budget is not a BSATN limit, but any database values routed through that boundary remain subject to it.

Source: [BSATN specification](https://github.com/ClockworkLabs/SpacetimeDB/blob/07e99d13506e55ba85872ab08931baec271e445a/docs/docs/00300-resources/00300-internals/00300-bsatn.md), `src/protocol/framing.ts:1–35`, and `scripts/services/wire.ts:45–76`.

## Connector responsibilities and SDK functionality

A direct connector needs WebSocket negotiation, client masking, ping/pong, fragmentation, closure, DNS, TLS, bearer authentication, token handling, protocol envelopes, request correlation, and connection recovery. Subscriptions also require dispatching unsolicited updates and maintaining any desired local cache.

Removing the Node SDK does not remove server features. It transfers client responsibilities to the connector: generated bindings, authentication lifecycle, subscription-cache maintenance, callbacks, reconnect behavior, and interrupted-call handling. A call-only connector does not need a subscription cache.

The native Rust SDK is an established independent client. At the inspected revision its transport selects V2 and uses native networking. The inspected C++ client is the Unreal SDK; a standalone engine-free C++ library was not established.

Sources: [Rust SDK](https://github.com/ClockworkLabs/SpacetimeDB/blob/07e99d13506e55ba85872ab08931baec271e445a/sdks/rust/src/lib.rs), [Rust transport](https://github.com/ClockworkLabs/SpacetimeDB/blob/07e99d13506e55ba85872ab08931baec271e445a/sdks/rust/src/websocket.rs), and [TypeScript connection lifecycle](https://github.com/ClockworkLabs/SpacetimeDB/blob/07e99d13506e55ba85872ab08931baec271e445a/crates/bindings-typescript/src/sdk/db_connection_impl.ts).

## Native integration choices

| Route | Benefit | Cost |
| --- | --- | --- |
| Native transport adapter inside the worker, with Bend BSATN codecs | Removes the separate DB bridge process and keeps payload logic in Bend | Custom effects, native linking, protocol maintenance, and compiler-version coupling |
| Rust SDK companion | Reuses established client transport | Retains IPC, unless replaced by a linked C-ABI adapter |
| Node-owned WebSocket forwarding raw BSATN | Avoids JS payload decoding while retaining Node connection ownership | Retains IPC and requires a new raw-byte boundary |
| Bend WebSocket implementation over Base TCP | Keeps more protocol code in Bend | Substantial framing work and separate TLS/DNS integration |

The strongest candidate for native connection ownership is a small native transport adapter with schema-derived Bend BSATN codecs. This is a recommendation, not a demonstrated implementation.

Bend already supports custom C effects without compiler-source changes. BendFlow currently rejects non-Base foreign effects, so this route needs project generator/build support and a native link policy. The effect ABI has no stability promise. Base lacks DNS, TLS, crypto, and HTTP. The current compiler also has no implemented pure native-library target.

Sources: [Bend effects](https://github.com/bendlang/bend/blob/f0cf0a7c573145501981c44d9c33e326185f008e/guide/EFFECTS.md), `.toolchain/bend/WONTFIX.txt:74–78,92–97,152–154`, and `scripts/services/compiler.mjs:188`.

SpacetimeDB's schema endpoint and existing code-generation model provide ordered products, sums, function arguments, and return types. A Bend backend is feasible in principle but does not exist in the inspected generator. Wide integers, identities, timestamps, and other types need explicit mappings; full BSATN coverage must not be assumed.

Sources: [schema definitions](https://github.com/ClockworkLabs/SpacetimeDB/blob/07e99d13506e55ba85872ab08931baec271e445a/crates/lib/src/db/raw_def/v10.rs) and [code-generation interface](https://github.com/ClockworkLabs/SpacetimeDB/blob/07e99d13506e55ba85872ab08931baec271e445a/crates/codegen/src/lib.rs).

## Application logic, transactions, and connection ownership

Most computation can remain in Bend. Database modules still define the schema and expose transactional operations. A trusted, authorized Bend service can own business validation and mapping algorithms. The commit operation must enforce authorization and the concurrency conditions needed to accept Bend's result. Any invariant that must hold against current database state needs a database-enforced constraint or an atomic check.

A viable workflow reads a snapshot and versions, computes a proposal in Bend, and submits it to a reducer that validates current state and commits atomically. External Bend computation is not part of that transaction. Reducers cannot call an external sidecar. Procedures can make HTTP calls, but cannot hold a transaction open during those calls.

A lost response does not establish rollback. Stable operation IDs and durable result lookup can reconcile a call whose outcome became unknown. A database commit and an in-memory Bend session update are not one atomic operation.

Node can supply credentials and supervise workers while native code owns the database connection. It cannot independently own the same live protocol session. Connecting every existing session worker separately also creates separate connection identities, authentication state, and subscription caches. Pooling or sharing would require an explicit design.

Source: [procedures and transaction constraints](https://github.com/ClockworkLabs/SpacetimeDB/blob/07e99d13506e55ba85872ab08931baec271e445a/docs/docs/00200-core-concepts/00200-functions/00400-procedures.md).

## Reactivity and external Bend execution

A committed table change does not automatically invoke a reducer. SpacetimeDB evaluates subscription changes and sends matching row updates to clients. The client's update handlers execute in the client process. A native Bend service can subscribe and run its mapping logic when updates arrive. React can independently subscribe to the same database without routing updates through Bend.

Dependency-tracked views are a separate mechanism. Their read-only functions execute inside the module and refresh when their dependencies change. They do not provide an external transactional callback.

The inspected interfaces expose lifecycle and scheduled functions, but no general public table-change trigger that invokes external Bend. Specialized inter-database outboxes target other SpacetimeDB databases, not arbitrary native workers. A generated event append operation can create durable work explicitly instead of relying on a table trigger.

The public WebSocket protocol supports subscriptions, reads, and calls to published functions. It has no arbitrary row-mutation message or client-held transaction spanning external computation. Procedures can write inside explicit transaction blocks, but their HTTP calls must occur outside those blocks. Privileged HTTP SQL also supports writes. That management interface does not establish an external transactional trigger or a remote transaction spanning Bend computation.

Subscriptions replicate current query state. They are not a durable consumer log with replay cursors and acknowledgements. Reconnection requires a fresh snapshot and reconciliation. A snapshot cannot reconstruct an intermediate change that has already disappeared. The connector must apply each transaction's updates completely before Bend computes from its local cache.

Sources: [subscription semantics](https://spacetimedb.com/docs/clients/subscriptions/semantics/), [views](https://spacetimedb.com/docs/functions/views/), [lifecycle reducers](https://spacetimedb.com/docs/functions/reducers/lifecycle/), [schedule tables](https://spacetimedb.com/docs/tables/schedule-tables/), [inter-database communication](https://spacetimedb.com/docs/databases/inter-database-communication/), [procedures](https://spacetimedb.com/docs/functions/procedures/), [HTTP database API](https://spacetimedb.com/docs/http/database/), and [follow-up V2 protocol](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/client-api-messages/src/websocket/v2.rs).

## Event-sourced projections with generated transactional shims

### Separate event acceptance, computation, and projection commit

The proposed pipeline has three stages:

1. A generated append operation persists immutable events in an ordinary durable table. It assigns a stable stream sequence and deduplicates repeated submissions by event ID. Reuse of an event ID with different content must fail. Event acceptance commits independently of projection processing.
2. A native Bend worker reads pending events and the projection's input state. Subscriptions notify the worker that work exists. Bend computes all proposed state changes without holding a database transaction open.
3. A generated commit operation checks the proposal against current database state. In one transaction, it applies the state changes, advances the processing checkpoint, and records completion. React receives the resulting subscription updates.

The event table queues durable inputs. The state tables contain the committed projection. SpacetimeDB supplies subscription delivery, so a separate outgoing queue is unnecessary for ordinary reactive reads. A durable outgoing queue is needed if downstream consumers require acknowledged delivery of every output event.

The transaction covers acceptance of the event-to-state mapping, including its processing checkpoint. It does not cover the earlier event append or Bend's execution. Queued events can therefore exist before their state changes become visible.

### Commit a proposal against expected inputs

For example, a projection has processed sequence 40 and has revision 7. Bend reads events 41 through 45 and computes their combined state changes. Its proposal identifies:

- The projection and stream or partition.
- The expected checkpoint of 40 and projection revision of 7.
- The event range 41 through 45 and the relevant additional dependency versions.
- Every state insert, update, and deletion produced by the mapping.
- A stable operation ID and, if workers use claims, the current fencing token.

The generated commit operation executes these checks and writes in one transaction:

1. Authenticate the writer and verify its permission to update the projection.
2. Reconcile a repeated operation ID with its durable completion record. Reuse of an ID with a different proposal must fail.
3. Check the expected checkpoint, projection revision, and all other mutable dependencies. Check any worker claim or fencing token.
4. Verify that the specified events exist and form a valid next range under the stream's ordering policy.
5. Apply every proposed state change across the affected tables.
6. Advance the checkpoint to 45, increment the projection revision, and record completion.

If a check fails, no state change or checkpoint advancement commits. If another worker commits first, Bend reads the updated inputs and recomputes. One event can produce several state rows, and a batch can process several events, provided the whole proposal fits one supported transaction.

Bend submits a proposal to a module-side transaction. It does not transmit an open transaction or obtain direct access to the module's database context.

### Keep the mapping algorithm in Bend

Generated module code can define event, state, checkpoint, and completion tables. It can expose append, read-snapshot, commit, and optional worker-claim operations. The generator must describe which state each projection owns and which versions a commit checks. Generic row mutations alone do not encode those rules.

The shim can validate the transaction protocol without repeating the mapping algorithm. This assumes Bend is a trusted service whose credentials authorize the relevant writes. Version checks establish freshness, not that Bend computed the correct business result. Database constraints or additional checks remain necessary for invariants that must survive a faulty producer.

Calls made with Bend's service token carry Bend's authenticated identity. A user ID in an event or proposal is application data, not proof of that user's identity. The append and commit operations need explicit authorization for user attribution. React must receive only permitted rows through table visibility or server-side views. A client-side subscription filter is not access control.

### Build event-sourcing coordination in Bend

The integration needs a reusable event-sourcing coordination layer in Bend. SpacetimeDB supplies transactions and reactive subscriptions, but the subscription-to-reducer flow does not automatically provide ordered consumption, dependency validation, or retry-safe projection commits.

The Bend layer owns stream and partition selection, batch formation, mapping execution, retry reconciliation, recovery, and failed-event policy. It tracks processing positions and dependency versions through the durable checkpoint protocol. Generated module shims must enforce sequence allocation, expected checkpoints, dependency checks, deduplication, and any fencing conditions atomically with their writes. Checks performed only in Bend cannot prevent competing consumers from committing stale proposals.

Application developers define event and state types, the mapping function, partition keys, and additional dependencies. The runtime and generator manage the coordination protocol rather than requiring each handler to implement it. Database commit order alone does not establish application event order. Ordering metadata can advance once per batch or stream instead of requiring a separate transaction for each event.

The retained event log is the authoritative history. State tables are materialized projections, and Bend's local copies can be reconstructible caches. The event-to-state transaction accepts a transition and durably records its effects and processing position. A transaction is not persistent state by itself. Treating the event table as disposable requires another authoritative event store for replay and recovery.

SpacetimeDB's reactive subscriptions remain the frontend delivery mechanism. React subscribes directly to permitted state tables or views and receives committed projection changes through the existing SDK. Bend does not need to implement a second subscription server or relay those updates. Worker subscriptions notify the coordination layer of pending work, while frontend subscriptions expose completed state.

A local database such as SQLite can also atomically validate a checkpoint, apply projection changes, and record completion. The external event-sourcing protocol is required with either database. SpacetimeDB's main additional value in this design is incremental reactive queries, frontend synchronization, client bindings, and its client/server platform. With SQLite, those frontend delivery capabilities require an additional service or library. This design gives up automatic in-module read-compute-write isolation and must recover equivalent projection safety through the generated conditional-commit protocol.

### Preserve ordered, recoverable processing

Use an ordinary durable table for event storage. SpacetimeDB's specifically named event tables are ephemeral to subscribers and are unsuitable as the only recoverable work queue. Retaining immutable events permits replay and lets independent projections maintain separate checkpoints.

On startup or reconnect, Bend waits for a consistent subscription snapshot or obtains inputs through a generated read-snapshot operation. It then reads events after the checkpoint. Separate reads must not silently combine state and revisions from different transactions.

Sequence allocation and checkpoint advancement must prevent gaps or late arrivals from being skipped. Ordering is per stream or partition unless the application defines a global order. Competing workers can rely on optimistic commit checks. Optional leases reduce duplicate computation, but fencing checks must reject stale owners after lease reassignment.

The subscription is a wake-up signal. Pending durable events and checkpoints establish what still needs processing. Every mutation path that requires work must persist the corresponding event or job atomically. Subscription callbacks alone cannot recover work lost during an outage.

### Failure and visibility limits

The design has these shortcomings:

- Event acceptance and state publication are separate commits. React can observe pending events with the previous state. The frontend can subscribe only to projection outputs or display a processed checkpoint to distinguish accepted work from completed work.
- A successful projection transaction publishes its matching row changes atomically within a subscription update. Independently subscribed projections can still have different checkpoints. There is no global processing barrier unless the application adds one.
- Every mutable input must participate in validation, including configuration, other projections, and absence or range checks where relevant. A coarse revision works only if every relevant mutation advances it. Incomplete checks can accept stale results.
- High contention causes rejected proposals and repeated computation. A shared checkpoint can serialize commits even when Bend computes in parallel. Partitioning is useful only when the mapping's dependencies allow independent commits.
- A crash can cause Bend to compute the same events again. Atomic checkpoints and completion records can prevent duplicate committed database effects, but they do not guarantee exactly-once execution.
- Durability confirmation must match the pipeline's guarantees. A subscription update is not by itself proof that the observed state survives a database crash. The connector must negotiate confirmed reads where required and handle confirmation failures without assuming rollback.
- A lost response leaves the commit outcome unknown. Durable operation lookup is required before treating a retry as new work. A database commit is not atomic with Bend's in-memory state.
- Network calls, emails, and other external effects performed during mapping do not roll back with a rejected proposal. Deterministic mapping with durable outgoing work records avoids treating those effects as part of the projection transaction.
- Payload size, transaction execution limits, and memory use constrain batch size and one-to-many mappings. Splitting one logical projection update across commits loses its all-or-nothing guarantee. Actual deployment limits remain to be established.
- Slow workers create event backlog and frontend lag. Backpressure, worker recovery, retention, poison-event handling, and monitoring require an explicit policy. Skipping a failed event must not silently violate stream ordering.
- Replay requires retained events and a compatible mapping version. Schema migrations, projection rebuilds, and algorithm changes need versioned checkpoints and a publication policy for rebuilt state.
- Event cleanup must respect every consumer's checkpoint and replay requirements. Completion-record cleanup must preserve retry deduplication for the supported retry window. Otherwise, retention can remove pending work or make old operation IDs ambiguous.
- Subscribing Bend to its own output tables can create feedback loops. Input selection, event IDs, and checkpoints must distinguish new work from already processed output. Current V2 updates do not supply a durable consumer cursor or complete origin metadata.
- Native connection ownership, Bend bindings, BSATN codecs, module-shim generation, and the reusable Bend event-sourcing coordination layer remain unimplemented. Separate Bend hosting, authentication, recovery, and operational supervision are still required. No latency or throughput benefit has been measured.

This route is feasible in principle when asynchronous event acceptance is acceptable and one bounded transaction can publish the mapping's complete effects. It does not provide a synchronous trigger that runs external Bend inside the originating transaction. For this pipeline, the generated commit transaction supplies the required atomicity without embedding BendRT in the database.

Sources: [procedures and transaction behavior](https://spacetimedb.com/docs/functions/procedures/), [subscription semantics](https://spacetimedb.com/docs/clients/subscriptions/semantics/), [connection recovery](https://spacetimedb.com/docs/clients/connection/), [event tables](https://spacetimedb.com/docs/tables/event-tables/), [authentication](https://spacetimedb.com/docs/core-concepts/authentication/), [table access permissions](https://spacetimedb.com/docs/tables/access-permissions/), and [follow-up V2 messages](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/client-api-messages/src/websocket/v2.rs). The projection, checkpoint, claim, and completion protocol above is a proposed application design, not a built-in SpacetimeDB worker API.

## Remaining uncertainty

The direct protocol path exists. Its benefit over intermediate conversions has not been measured. A future investigation must establish deployed-version compatibility, supported Bend/BSATN mappings, native transport integration, connection scaling, and recovery behavior before treating the connector as production-ready.

The event-sourced route also needs a demonstrated consistent input snapshot, complete dependency validation, and atomic multi-table projection commits. Verification must cover competing workers, crashes before and after commit, lost responses, reconnect backlog, duplicate events, ordering gaps, and stale claims. Large mappings and rebuilds need deployment-specific limits and visibility checks. The architecture avoids the BendRT module port, but these integration and correctness requirements remain.

The [performance analysis](spacetime-bend-performance.md) traces the current transaction locks, durability batching, subscription evaluation, and transport costs. Moving mapping into Bend removes computation from the database's writer-excluding interval, but adds export and import work. Durability grouping and WebSocket coalescing do not merge reducer invocations into one transaction. The analysis distinguishes throughput opportunities from additional event-to-state latency without claiming a measured speedup.
