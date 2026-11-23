# Bend CQRS coordination and SpacetimeDB platform reuse

## Scope and assessment

This report records November 17, 2026 research for the [external event-sourced connector design](spacetime-bend.md). SpacetimeDB findings use revision [`3a24691a352df64ffdd3834c439b88e9c07ce3ba`](https://github.com/ClockworkLabs/SpacetimeDB/commit/3a24691a352df64ffdd3834c439b88e9c07ce3ba). Bend findings use BendFlow's compiler pin [`f0cf0a7c573145501981c44d9c33e326185f008e`](https://github.com/bendlang/bend/tree/f0cf0a7c573145501981c44d9c33e326185f008e). No connector or generator was implemented. CLI commands below are source-verified, not execution-tested.

Native Bend can own business decisions, event evolution, projection computation, and the event-sourcing coordinator. A generated SpacetimeDB module supplies durable tables and conditional transactional operations. The standard TypeScript SDK and React integration can query and subscribe to the resulting read models without knowing that Bend computed them.

The event log is authoritative history. The event-to-state transaction is the authoritative transition. Materialized state and Bend's local copies can be reconstructible caches, but accepted transitions must durably record their checkpoint and effects.

SpacetimeDB supplies reactive query execution and client synchronization. It does not infer the coordinator, query catalogue, access rules, or external dependency checks from Bend code. The integration therefore needs one explicit model description that drives both Bend types and generated module/client artifacts.

## Bend types help with the domain model, not database freshness

The pinned Bend2 compiler supports dependent functions, dependent pairs, value-indexed type families, and affine use. These can connect a command variant to its payload, represent lifecycle-specific operations, and express preconditions or laws about event evolution. Application developers can keep decision and projection functions pure, with effects concentrated in the coordinator.

Affine use means at most once for the relevant binding. Dropping values is permitted, and `Data` values are reusable. Consuming an affine capability can prevent reuse within one checked execution. It does not establish exactly-once processing across crashes, retries, or separate workers. Capability constructors and the representation of state determine which values can be forged or copied.

A dependent type can tie a proposal to the revision supplied to Bend. It cannot establish that the live database still has that revision. Generated module operations must compare the expected revision, processing checkpoint, dependency versions, and any fencing token atomically with the writes. Commands arriving from React or database bytes also require validation before they inhabit the richer domain types.

Type and proof erasure means the wire representation must carry operational identifiers, tags, versions, and checkpoints explicitly. A TypeScript schema does not automatically preserve Bend's dependent relationships. This pin also distinguishes live-checked evidence from explicitly erased evidence. An erased proof field alone is not a trustworthy justification for a live invariant.

The compiler has no type classes or traits, and constructors cannot change a datatype's parameters through a conventional indexed constructor result. Computed type families, dependent pairs, and explicit evidence are the relevant mechanisms. Relational functional dependencies still need appropriate representations and database constraints where they concern persisted rows.

Sources: [dependent functions and pairs](https://github.com/bendlang/bend/blob/f0cf0a7c573145501981c44d9c33e326185f008e/guide/GUIDE.md#L594-L601), [affine quantities and reusable data](https://github.com/bendlang/bend/blob/f0cf0a7c573145501981c44d9c33e326185f008e/guide/GUIDE.md#L219-L255), [constructor limitation](https://github.com/bendlang/bend/blob/f0cf0a7c573145501981c44d9c33e326185f008e/bend2/bend.ts#L3982-L3991), [live and dead evidence](https://github.com/bendlang/bend/blob/f0cf0a7c573145501981c44d9c33e326185f008e/bend2/bend.ts#L163-L238), and [current language limitations](https://github.com/bendlang/bend/blob/f0cf0a7c573145501981c44d9c33e326185f008e/README.md).

## Reuse, generate, and implement

| Responsibility | Owner |
| --- | --- |
| Durable tables, indexes, atomic transactions, and database constraints | Reuse SpacetimeDB |
| SQL subscription evaluation, initial snapshots, and transaction updates | Reuse SpacetimeDB |
| TypeScript transport, local client cache, and React subscription hooks | Reuse the existing SDK |
| Supported module builds, publishing, schema bindings, and database inspection | Reuse the CLI |
| Table schemas and append, snapshot, claim, and conditional-commit operations | Generate a supported module from the integration model |
| Frontend read-model queries and visibility views | Generate from explicit query and authorization declarations |
| Business decisions, event evolution, and projections | Implement in Bend |
| Partitioning, batching, checkpoints, retries, recovery, and failed-event policy | Implement a reusable Bend coordinator |
| Native transport and Bend BSATN mappings | Implement or adapt a connector |
| Event/projection versions, rebuild publication, and coordinated migrations | Define in the integration and application model |
| Native Bend process build, hosting, and supervision | Supply Bend-specific orchestration |

The coordinator must not leave its safety checks solely in Bend. It submits expected state and operation identities, while generated module operations enforce those expectations at commit time. The generator needs a declaration of each projection's mutable dependencies. SpacetimeDB's view dependency tracking does not observe reads performed in an external process.

## Generate subscriptions from a read-model catalogue

SpacetimeDB code generation emits row types, table and view references, reducer/procedure methods, and connection bindings from the module schema. Its TypeScript query builder supplies typed query construction. It does not choose which data a screen needs or infer application subscriptions from Bend mapping functions.

Subscription generation can be automatic after the integration declares each read model's source table or view, parameters, access policy, and lifecycle. The generator can produce typed query helpers and readiness conventions. React then uses the normal SDK to establish subscriptions, receive initial rows, apply transaction changes, and release subscriptions when components stop using them.

At the inspected revision, `useTable(query)` owns subscription registration and cleanup. The package also exports `SpacetimeDBProvider`, `useSpacetimeDB`, `useReducer`, and `useProcedure`. The frontend need not maintain a custom WebSocket protocol or receive state changes relayed through Bend.

The useful boundary is explicit query generation, not indiscriminate subscription to every table. `subscribeToAllTables()` exists, but it subscribes to whole-row read models and skips parameterized views. It neither establishes a security policy nor controls a large cache's cost.

Subscription SQL has limits. It does not support `ORDER BY`, `LIMIT`, or `OFFSET`, and normal subscriptions return rows from a result table or view rather than arbitrary joined result shapes. Materializing the required frontend shape in a state table avoids depending on unrestricted SQL. At this pin, parameterized views need SQL invocation rather than the ordinary typed query-builder references and have additional composition and cache-provenance limits.

Private backing tables and caller-filtered views can enforce read visibility. A browser-side query filter over public rows is not authorization. Access predicates involving business policy must be declared or generated from a suitable policy model. They cannot be inferred from the table schema alone.

Sources: [client code generation](https://spacetimedb.com/docs/clients/codegen), [subscriptions](https://spacetimedb.com/docs/clients/subscriptions), [React exports](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/sdks/typescript/src/react/index.ts), [`useTable`](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/sdks/typescript/src/react/useTable.ts#L69-L150), [subscribe-all behavior](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/sdks/typescript/src/sdk/subscription_builder_impl.ts#L88-L160), [SQL reference](https://spacetimedb.com/docs/reference/sql), and [parameterized view limits](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/docs/docs/00200-core-concepts/00200-functions/00500-views.md#L866-L902).

## Frontend command completion remains an application concept

A generated ingress reducer can accept a command and durably enqueue work. Its successful response means acceptance at that boundary, not completion of the Bend business decision or projection.

The model must distinguish pending commands from accepted domain events. If Bend decides whether a command is valid, the ingress inbox is not itself authoritative business history. The accepted events and their required state effects must become durable through the declared transition protocol.

The read model needs explicit command IDs, outcomes, and projection checkpoints. React can subscribe to those through the standard SDK and distinguish accepted, completed, and failed work. These fields are application data, not automatic metadata on subscription transaction updates.

The SDK's reconnect mechanism restores subscriptions and reconciles current query state. It does not replay every intermediate offline event from a durable consumer cursor. Persisted command outcomes let the frontend reconcile interrupted calls and completed work after reconnecting. The Bend coordinator independently recovers pending events from its checkpoint.

Within a transaction update, the SDK applies the subscribed changes before row callbacks. Different projections can still be at different checkpoints. If a screen requires several read models at a common processing position, the integration must expose and enforce that readiness condition. Rendering and optimistic UI behavior also remain frontend concerns.

Sources: [connection and reconnect behavior](https://spacetimedb.com/docs/clients/connection), [SDK reconciliation](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/sdks/typescript/src/sdk/db_connection_impl.ts#L1122-L1240), and [protocol transaction and result fields](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/client-api-messages/src/websocket/v2.rs#L371-L531).

## Reuse the CLI around a generated supported module

The integration can generate a Rust or TypeScript shim project, then use the normal CLI for that project. Representative commands are:

| Task | Command |
| --- | --- |
| Start the local database server | `spacetime start --listen-addr 127.0.0.1:3000` |
| Build the generated module | `spacetime build --module-path ./spacetimedb` |
| Generate frontend bindings | `spacetime generate --lang typescript --module-path ./spacetimedb --out-dir ./src/module_bindings` |
| Publish or update the database module | `spacetime publish my-db --server local --module-path ./spacetimedb --delete-data=never` |
| Watch, rebuild, generate, and publish | `spacetime dev my-db --server local --project-path . --module-path ./spacetimedb --client-lang typescript --delete-data=never` |
| Inspect the database schema | `spacetime describe my-db --server local --json --no-config` |
| Follow module logs | `spacetime logs my-db --server local --follow --format json` |
| Inspect a generated checkpoint table | `spacetime sql my-db "SELECT * FROM projection_checkpoint" --server local --confirmed true --format json --no-config` |
| Observe checkpoint changes | `spacetime subscribe my-db "SELECT * FROM projection_checkpoint" --server local --print-initial-update --confirmed true --no-config` |

`projection_checkpoint` is a proposed generated table, not a built-in table. Login, server configuration, database listing, and endpoint checks are also reusable. Module logs do not contain the external Bend process's logs automatically.

The documented code-generation inputs are a supported module project, a compiled Wasm artifact through `--bin-path`, or a JavaScript bundle through `--js-path`. A positional database name selects local configuration targets. It does not fetch that published database's schema, and `generate` has no `--server` flag at this revision. A hidden schema-input hook exists, but normal project or artifact generation avoids depending on it.

The CLI does not build native Bend or accept Bend as a module language. Its build dispatch supports Rust, C#, JavaScript/TypeScript, and C++. It can publish the generated shim, not the native coordinator executable. Native compilation, linking, connector generation, and worker supervision need Bend-specific orchestration.

The development command has a configurable command runner, but that runner supplies no coordinator recovery or ownership protocol. The inspected `dev` implementation does not launch the local database server. Start the server explicitly for this revision. External Bend changes must also trigger schema/query generation where necessary, rather than assuming the module watcher observes all Bend inputs.

Publishing supports the platform's compatible schema changes. It cannot automatically migrate projection checkpoints, translate old event versions, rebuild state, or coordinate native worker versions. Those steps belong to the event-sourcing integration.

Sources: [CLI reference](https://spacetimedb.com/docs/cli-reference), [codegen arguments](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/cli/src/subcommands/generate.rs#L297-L418), [codegen implementation](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/cli/src/subcommands/generate.rs#L739-L827), [build dispatch](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/cli/src/tasks/mod.rs#L13-L35), and [development loop](https://github.com/ClockworkLabs/SpacetimeDB/blob/3a24691a352df64ffdd3834c439b88e9c07ce3ba/crates/cli/src/subcommands/dev.rs#L766-L899).

## Practical boundary

Bend supplies a typed domain model and reusable CQRS coordinator. Generated module code supplies atomic persistence and concurrency checks. SpacetimeDB supplies read-model subscriptions and the standard frontend SDK. The normal CLI operates on the generated module artifacts.

The frontend can reuse transport and synchronization without a custom client. Application-specific query scope, authorization, command completion, and projection readiness remain explicit. This separates reusable coordination infrastructure from domain handlers without assuming that dependent types or database serialization prove external state freshness.
