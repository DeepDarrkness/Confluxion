# External Bend projections: SpacetimeDB performance costs

## Scope and conclusion

This report follows the [native connector and event-sourced projection design](spacetime-bend.md). It records source research on November 17, 2026, against SpacetimeDB revision [`3a24691a352df64ffdd3834c439b88e9c07ce3ba`](https://github.com/ClockworkLabs/SpacetimeDB/commit/3a24691a352df64ffdd3834c439b88e9c07ce3ba), committed November 16, 2026. No connector was implemented and no benchmark was run. Source constants describe configuration, not measured latency or throughput.

The implementation has one mutable transaction at a time per database. It holds the exclusive state lock while a reducer computes. After commit, subscription delta evaluation retains a read lock that also excludes the next writer. External Bend can move expensive mapping out of that serialized interval and overlap it with subsequent database transactions.

Batching helps, but several different mechanisms exist. Local durability groups transaction records into a shared flush/sync cycle. WebSocket v3 groups protocol messages into a payload. Neither combines reducer invocations into one logical transaction. A generated reducer accepting an application batch is necessary to amortize transaction-level overhead across events.

External mapping adds event export, native decoding, result encoding, result import, and more scheduling. It retains module-side validation, table mutation, index maintenance, durability, and subscription work. These costs depend on payload size, query diversity, fanout, state dependencies, and batch size. The source cannot establish a percentage slowdown. Compute-heavy, partitionable mappings may increase throughput. Cheap mappings are more likely to pay extra latency and CPU work without enough serialized computation to remove.

The detailed host analysis follows the Wasmtime reducer path. The datastore lock applies to the shared database engine, including JavaScript modules, but JavaScript invocation and decoding costs need separate measurement. Local durability findings do not establish Maincloud's replication or storage behavior.

## The single writer includes reducer computation

`RelationalDB` owns a `Locking` datastore. `Locking::begin_mut_tx` calls `committed_state.write_arc()` and stores the resulting guard in `MutTxId`. Read transactions acquire the same lock in read mode. The inspected implementation uses exclusion to provide serializable isolation, rather than parallel MVCC writers.

The reducer host begins that transaction before invoking the reducer. It keeps the transaction through reducer execution, success hooks, view refresh, and commit. A pure, CPU-heavy mapping therefore delays unrelated reads and writes to the same database even if it changes few rows.

Wasm reducer jobs also run inline on a single module executor. The normal main instance is reused, so instance creation is not a normal per-call cost. Procedure instances can yield and interleave, but do not supply parallel reducer CPU execution on that executor. Separate databases have separate datastore locks. This is not a server-global single writer.

After merging writes into committed state, the host downgrades the write lock to a read lock. Subscription delta evaluation runs before releasing that read transaction. Encoding and sending the final per-client messages happen later on other tasks, but database-dependent subscription work still excludes the next writer.

This gives external Bend a stronger opportunity than merely using a different compute engine. Bend can compute from an immutable event prefix and captured projection state while SpacetimeDB accepts later events. The final projection commit still joins the serialized database path.

Sources:

- [`Locking::begin_mut_tx`](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/datastore/src/locking_tx_datastore/datastore.rs#L1001-L1032) and [read transactions](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/datastore/src/locking_tx_datastore/datastore.rs#L412-L447).
- [Reducer transaction lifetime](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/core/src/host/wasm_common/module_host_actor.rs#L1219-L1335).
- [Wasm executor and instance reuse](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/core/src/host/module_host.rs#L464-L507) and [synchronous job execution](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/core/src/util/jobs.rs#L307-L359).
- [Commit and subscription evaluation](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/core/src/subscription/module_subscription_actor.rs#L2049-L2143) and [`eval_updates_sequential`](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/core/src/subscription/module_subscription_manager.rs#L1338-L1395).

## What SpacetimeDB batches

### Local durability shares synchronization across transaction records

The local durability actor drains currently queued transactions with `recv_many`. It invokes `clog.commit([tx.into_transaction()])` separately for each transaction, then calls `flush_and_sync` for the drained group. With an empty queue, the receiver waits for one transaction. There is no minimum batch-fill requirement or intentional batch timer in this loop.

Consequently, busy traffic can share synchronization work. Sparse traffic may produce one transaction per sync cycle. The default `batch_capacity` of 4,096 reserves buffer capacity. It is neither a requirement to collect 4,096 transactions nor a hard maximum on the drained group.

This engine describes durability as an asynchronous write-behind log. In-memory commit can precede persistence. Durability payload preparation and disk work run outside the mutable transaction's ordinary execution path. However, the local provider's bounded queue uses `send_blocking`. A full durability queue can stall enqueue while the state write lock remains held.

Sources: [local durability loop](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/durability/src/imp/local.rs#L268-L359), [queue drain behavior](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/durability/src/imp/local.rs#L462-L484), [default capacities](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/durability/src/imp/local.rs#L32-L68), [durability preparation](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/engine/src/durability.rs#L16-L88), and [write-behind description](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/engine/src/relational_db.rs#L977-L992).

### WebSocket batching preserves logical transaction boundaries

V3 uses the V2 message schema and permits several messages in one WebSocket payload. The server coalesces available messages toward a 512 KiB uncompressed body target. One oversized logical message can be sent alone. V2 sends separate payloads for the individual logical messages.

Each incoming `CallReducer` still identifies one reducer invocation. The V3 handler decodes and dispatches the constituent messages separately. Putting ten calls into one payload does not turn them into one transaction. A generated projection commit reducer accepting ten events' combined output does.

The subscription send worker also has an adaptive linger. Its requested baseline is 25 microseconds and its maximum is 200 microseconds. These are timer settings, not measured delays or latency bounds. The WebSocket encoder coalesces the messages already available to it without a separate timed batch-fill window.

Sources: [V3 framing](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/client-api-messages/src/websocket/v3.rs#L1-L13), [V3 dispatch](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/core/src/client/message_handlers_v3.rs#L8-L24), [server payload grouping](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/client-api/src/routes/subscribe.rs#L1736-L1779), [send-worker linger settings](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/core/src/subscription/module_subscription_manager.rs#L1759-L1764), and [adaptive receiver](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/core/src/util/adaptive_recv.rs#L49-L98).

### Application batching can reduce both fixed work and intermediate changes

One projection transaction amortizes invocation, validation setup, checkpoint updates, and transaction setup across its event batch. The durability actor still persists the transaction's mutations, and subscriptions still evaluate its net changes.

If several events repeatedly update the same state row, a combined projection can publish one final result rather than expose every intermediate state. That can reduce database and subscription work as well as fixed overhead. The gain depends on actual generated mutations. It also changes observable intermediate-state semantics and is available to an in-module batch reducer too.

Batching does not eliminate per-byte input handling, per-row/index mutation, or fanout. Large batches can touch more query filters, create larger bursts, hold the commit lock longer, and add batch-formation delay.

## The serialization boundary has several stages

The external design adds a path that in-module computation does not need:

1. Evaluate the worker's event or input subscription and encode its rows.
2. Assemble, serialize, optionally compress, and send the worker's update.
3. Decode the update in native code and obtain Bend values.
4. Compute the mapping and encode the result proposal.
5. Receive and decode the proposal at SpacetimeDB.
6. Decode and validate reducer arguments, invoke the generated shim, and apply its mutations.

The source already avoids some copies. Transaction row arrays use `Arc`, encoded row lists use `Bytes`, and recipients share encoded rows for a common query result. Buffer pools reuse allocations. This makes direct BSATN more attractive than an additional JavaScript-value and IPC conversion path, but does not make delivery zero-copy.

Each receiving connection serializes its own outer message. With compression enabled, the connection first produces the uncompressed body and then writes compressed output into another buffer. The inspected compression policy applies above 1,024 body bytes. Compression is per connection, not one shared compressed broadcast to every recipient.

On the return path, V2 message decoding constructs owned argument bytes. Host schema decoding constructs a `ProductValue` while retaining the original BSATN for reuse. A Rust/Wasm shim receives argument bytes through a source copied into guest memory and performs typed decoding there. The generated shim therefore remains a real invocation and decoding cost even when the business algorithm is entirely in Bend.

These costs scale with exported input bytes and submitted result bytes. Sending a large input state and a large complete replacement can be expensive even when networking is local. Keeping a coherent worker cache, exporting only required rows, and submitting compact mutations reduces that work. Each choice must preserve complete dependency validation.

Sources: [shared row buffers](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/client-api-messages/src/websocket/common.rs#L67-L112), [row encoding](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/core/src/subscription/websocket_building.rs#L109-L168), [outer serialization](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/core/src/client/messages.rs#L221-L240), [compression buffers](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/core/src/client/messages.rs#L74-L118), [compression policy](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/core/src/subscription/websocket_building.rs#L200-L231), [owned byte decoding](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/sats/src/de/impls.rs#L1061-L1065), [host argument handling](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/core/src/host/mod.rs#L72-L97), [guest copy](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/core/src/host/wasmtime/wasm_instance_env.rs#L1366-L1394), and [typed reducer decode](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/bindings/src/rt.rs#L51-L59).

## Subscription costs remain after offloading

Incremental subscriptions operate on inserted and deleted rows rather than polling and rerunning a full snapshot on every change. The manager prunes and deduplicates affected query hashes. Identical query subscriptions can share evaluation and row encoding. Identity-dependent queries and distinct SQL texts can limit sharing.

Matching subscribers still add update records, per-client envelope assembly, queue operations, outer serialization, compression, and network output. A Bend subscriber to a query not already used by React can add another affected query evaluation. A subscriber to an already shared query still adds delivery work. Indexed joins and wide updated rows can be more expensive than the row count alone suggests.

At this revision, incremental evaluation is sequential and runs before releasing the datastore read transaction. Initial multi-query snapshot evaluation has a different, parallelized path. Steady-state performance and reconnection snapshots therefore need separate evaluation.

The first client output queue is bounded and disconnects an overflowing consumer. Later send-worker, encoder, and frame queues include unbounded stages. A slow worker can accumulate memory and lag downstream, not merely create immediate bounded backpressure. Reconnection then creates snapshot work. Group commit and coalescing do not solve sustained overload.

Sources: [affected-query selection](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/core/src/subscription/module_subscription_manager.rs#L1274-L1331), [V2 query sharing and fanout](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/core/src/subscription/module_subscription_manager.rs#L1457-L1515), [query hashes](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/core/src/subscription/execution_unit.rs#L32-L47), [per-client message assembly](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/core/src/subscription/module_subscription_manager.rs#L2053-L2127), [join delta work](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/core/src/subscription/delta.rs#L19-L124), [bounded client output](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/core/src/client/client_connection.rs#L450-L488), and [encoder and frame queues](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/client-api/src/routes/subscribe.rs#L1333-L1342).

## Latency and throughput can move in opposite directions

### Confirmed delivery introduces a causal durability boundary

V2 and V3 default to confirmed reads at this revision. The outgoing connection waits for the required durable offset before delivering affected messages. It can wait for the maximum offset in an available batch. That wait happens outside the database transaction lock.

A Bend worker consuming confirmed events cannot begin processing those events until their append is durable and delivered. The resulting projection transaction can then require another durability wait before React receives it. These are dependent stages for that event batch. Sharing synchronization across unrelated transactions does not collapse the causal sequence into one wait.

An in-module reducer that appends and maps in the same transaction has one final durability boundary. An asynchronous in-module projector also has two logical transactions, but need not wait for the append's confirmed network delivery before starting computation. This difference can matter even if durability and network work overlap across many batches.

Disabling confirmation changes the crash-consistency contract. It is not a like-for-like performance optimization for the durable pipeline.

Sources: [confirmed-read defaults](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/client-api/src/routes/subscribe.rs#L136-L143), [default constant](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/client-api/src/lib.rs#L32-L35), and [batched durability gating](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/core/src/client/client_connection.rs#L219-L267).

### Pipeline overlap helps capacity, not every event's response time

While Bend computes batch A, SpacetimeDB can append batch B or commit another independent projection. That overlap can increase sustained throughput. Batch A still experiences its own delivery, computation, return, commit, and publication delays.

Same-host deployment reduces network transit and can avoid a Node bridge, but retains encoding, decoding, memory traffic, queue handoffs, and shared CPU or memory-bandwidth contention. Remote deployment adds network distance and variability. Neither placement has a measured penalty in this report.

The following accounting model separates the constraints. It is not a benchmark prediction. For a fixed batch with the same final mutation semantics, let:

- $C_m$ be the in-module mapping time occupying the writer-excluding interval.
- $C_b$ be native Bend mapping CPU work for that batch, expressed in seconds.
- $D_i$ be the remaining database writer-excluding service time for the in-module design.
- $D_e$ be all database writer-excluding service time for the external design, including its append, commit checks, mutations, and input/output subscription evaluation.
- $P$ be the effective compute parallelism allowed by the mapping's dependencies.
- $N_e$ be the external transport and encoding capacity in batches per second.

Then optimistic capacity bounds are:

$$
X_i \leq \frac{1}{C_m + D_i}
$$

$$
X_e \leq \min\left(\frac{1}{D_e},\frac{P}{C_b},N_e\right).
$$

Other constraints, including disk, CPU contention, and consumer speed, can lower either bound. $D_e$ need not equal $D_i$. A second transaction, checkpoints, and an extra subscription can increase it. Batching or different net writes can reduce it. For one batch, external serialized-path demand is lower only if $D_e < C_m + D_i$.

State dependence limits $P$. For $s_{i+1}=f(s_i,e_i)$, a single ordered stream generally needs the prior state before computing the next state. Bend can parallelize independent work inside the mapping or process independent partitions. More workers do not automatically parallelize a dependent stream. Stale proposals and conflicting checkpoints also spend computation without advancing the projection.

## Workload-dependent assessment

| Workload | Assessment from the execution paths |
| --- | --- |
| Cheap counters or small row assignments | Little mapping work leaves the lock. Additional transport, decoding, and scheduling are difficult to amortize without application batching. |
| Expensive computation with compact outputs | Strong candidate for offloading. Native computation leaves the serialized path and can overlap database work. |
| Independent streams or partitions | Parallel Bend preparation can help if each partition's commit validates its own complete dependencies. |
| One globally dependent state machine | Native execution may help individual computations, but the dependency chain limits inter-event parallelism. |
| Wide inputs or large replacement outputs | Export/import bytes, memory traffic, and guest decoding can dominate. Compact deltas and coherent caches matter. |
| Many distinct queries or expensive joins | Subscription evaluation remains writer-excluding work and can limit throughput after mapping is offloaded. |
| Many subscribers to shared queries | Query sharing helps, but per-client serialization, compression, and egress remain. React's final delivery costs are shared by both architectures. |
| Sparse traffic with strict response-time goals | Few opportunities for batching. External delivery and causal confirmation stages remain visible. |
| Sustained load with bounded application batches | Fixed costs can be amortized and stages can overlap. Queue growth and completed projection rate must demonstrate stability. |

The strongest claim supported by the source is that external Bend can increase throughput when expensive mapping currently occupies the database's serialized execution interval. The claim that little performance is lost merely because SpacetimeDB batches transactions is not established. For cheap mappings, latency can increase even when sustained throughput remains acceptable.

## A fair measurement needs equivalent semantics

Three baselines separate architectural differences:

1. Append and map inside one reducer transaction. This has atomic event/state visibility and fewer stages.
2. Append events, then run an in-module batch projector in a second transaction. This matches queued event acceptance more closely and tests batching without externalization.
3. Append events, compute in native Bend, and submit one generated batch commit. This is the proposed architecture.

Use the same events, mapping results, mutation counts, indexes, authorization, subscriber queries, and durability contract. Repeat the batch-size choices in both projector designs. If batching removes intermediate states, record that semantic difference rather than crediting external Bend alone.

The first useful experiments vary cheap versus compute-heavy mapping, small versus wide payloads, a dependent stream versus independent partitions, shared versus distinct subscriptions, and same-host versus remote placement. Fix protocol and compression explicitly. At this pin, Rust requests V2 with default Brotli, while TypeScript prefers V3 and defaults to gzip. Those defaults can otherwise confound comparisons.

Measure accepted events and completed projections separately. Record event-acceptance-to-frontend latency distributions, completed events per second, backlog slope, rejected proposals, CPU by process, memory, input/output bytes, durability lag, subscription evaluation time, and reconnects. A high accepted-event rate with growing pending events is not sustainable projection throughput.

Sparse, bursty, and sustained load exercise different batching behavior. Sustained tests need enough duration to expose durability, worker, and socket backlog. A precomputed-result commit variant can isolate the commit and subscription path. A native mapping-only variant can isolate Bend computation while checking the same expected output. No percentage penalty or speedup is defensible until these boundaries are measured.

Sources for client defaults: [Rust protocol](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/sdks/rust/src/websocket.rs#L246-L251), [Rust compression selection](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/sdks/rust/src/websocket.rs#L184-L203), [TypeScript protocol preference](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/sdks/typescript/src/sdk/websocket_protocols.ts#L3-L17), and [TypeScript defaults](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/sdks/typescript/src/sdk/db_connection_builder.ts#L28-L37).
