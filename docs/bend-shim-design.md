# Bend shim design for raw BSATN insertion

This design describes the write boundary between native Bend and a generated Rust SpacetimeDB module. It complements [bend-websocket-design.md](bend-websocket-design.md) and follows [confluxion-arch1.md](confluxion-arch1.md).

The source findings use SpacetimeDB `v2.15.0`, revision `07efb0c44c42288ed8e40dcece75659cf44ec80c`. The shim and Bend connector remain unimplemented. No live integration or performance measurement establishes their behavior yet.

## Design decision

Bend owns its internal domain representation. A generated boundary conversion produces rows matching the deployed SpacetimeDB schema and encodes those rows as BSATN. The reducer receives the encoded rows as bytes and submits mutable slices to `spacetimedb::sys::datastore_insert_bsatn`.

**The module does not need to reconstruct each row as a Rust struct and serialize it again.** The insertion API already accepts a BSATN row product. It is publicly exposed through `spacetimedb::sys` and registered in the module host ABI. No SpacetimeDB server modification is required for this path. [Public export][sys-export], [safe insertion wrapper][sys-insert], [host ABI registration][abi-registration]

This removes the typed table API's row encoding step. It does not make network transfer, module argument handling, storage conversion, or transaction commit zero-copy.

## Transport-independent module generation

The initial client transport is an embedded Rust/Tungstenite worker behind Bend effects. An all-Bend WebSocket implementation is deferred. Neither choice changes the generated SpacetimeDB shim's responsibility: the server host handles WebSocket messages and dispatches validated reducer arguments and caller context to the Rust WASM module. The shim does not see the WebSocket endpoint, frame masks, or the client's implementation language. [Client protocol][v2], [reducer dispatch][rust-dispatch], [host execution][reducer-execution]

The native Rust transport and generated Rust module are separate build artifacts. The module does not link the transport library. Explicit Bend declarations are the schema authority for both client bindings and module registrations; developers do not hand-maintain Rust table definitions alongside Bend declarations.

The generator implements the module boundary as follows:

1. Validate explicit Bend persistence, export, dependency, and invariant declarations and derive their concrete BSATN contracts. Use the [trusted-local access policy](confluxion-arch1.md#trusted-local-access-policy) for the initial deployment.
2. Emit Rust table, index, reducer, and result registrations from those contracts, alongside matching Bend codecs and bindings. Unsupported declarations fail generation rather than silently changing representation.
3. Emit the transactional commit shim that decodes proposal metadata and descriptors, checks current database versions, inserts the declared row slices, and writes checkpoints and operation outcomes atomically. Local shims do not check caller identity, roles, or auth claims.
4. Build the WASM module and compare its extracted schema with the generated contracts before deployment. Changes to application schema or policies regenerate both sides.

The row-write contract and raw insertion implementation described below apply identically with Rust or Bend WebSockets. Replacing only the transport should require no shim rewrite. A changed reducer signature, application schema, authorization policy, or SpacetimeDB ABI can require regeneration or migration independently of transport.

Application algorithms execute in native Bend outside database transactions. Generated Rust shims enforce declared invariants, freshness, and atomic persistence inside transactions, with allow-all local application access. These declarations originate in Bend, but arbitrary Bend code is not automatically executed inside a reducer. A required server-side rule needs a supported generated representation or an explicit database constraint; freshness checks alone do not validate business correctness.

## Internal Bend types and database contracts are separate

Bend business code can use records, variants, dependent relationships, and local resources suited to its computation. Those values do not have to mirror the database's row types.

Explicit persistence declarations define the database row schema and its conversion from Bend results. The generator produces:

- Conversion functions from domain results to concrete database rows.
- BSATN codecs with fixed field and variant order.
- Module table definitions and transactional reducer registrations.
- Binding identifiers, row descriptors, and the reducer argument contract.
- Compatibility metadata checked against the built module schema.

The generated boundary has one authoritative schema. Native rows must match the module's actual table schema even though the reducer argument declares the row payload as bytes. Unsupported conversions fail generation. Runtime values such as versions and checkpoints cross as explicit data rather than erased proofs or handles. [Architecture contracts](confluxion-arch1.md#contracts-come-from-the-bend-boundary-and-use-bsatn), [module schema][module-schema]

Encoding rows as an opaque transport payload does not make the stored table opaque. `datastore_insert_bsatn` interprets each payload using the table's schema. Stored columns remain available to indexes, queries, and subscriptions.

## Complete write path

```mermaid
sequenceDiagram
	participant B as Native Bend computation
	participant C as Bend conversion and codecs
	participant W as Native WebSocket connector
	participant H as SpacetimeDB host
	participant M as Generated Rust reducer shim
	participant D as Datastore
	B->>C: Domain proposal and dependency versions
	C->>C: Convert to database rows and encode BSATN
	C->>W: Encoded argument product and operation identity
	W->>H: Binary ClientMessage::CallReducer
	H->>H: Decode envelope and check argument schema
	H->>M: Execute reducer in server-owned transaction
	Note over H,M: Reuse incoming BSATN argument bytes for dispatch; copy into WASM memory
	M->>M: Decode metadata and byte buffer, validate commit conditions
	loop Each encoded row
		M->>D: datastore_insert_bsatn(table_id, mutable row slice)
		D->>D: Validate row schema, write storage layout, enforce constraints
		D-->>M: Generated column bytes or insertion error
	end
	M->>D: Write checkpoint and completion record
	M-->>H: Successful reducer completion
	H->>D: Commit transaction
	H-->>W: ReducerResult with matching subscription changes
	W->>W: Apply embedded update before successful completion
	W-->>B: Confirmed outcome
```

An execution error rolls the transaction back. The diagram's successful path assumes every write and commit condition succeeds. No transaction remains open across the external Bend computation. [Reducer execution][reducer-execution], [commit and broadcast][commit]

## `CallReducer` carries the argument product

The native connector targets `v2.bsatn.spacetimedb` at `/v1/database/NAME_OR_IDENTITY/subscribe`, with `compression=None&confirmed=true`.

The existing v2 `CallReducer` fields appear in this order:

1. `request_id: u32`, for connection-local result correlation.
2. `flags: u8`, with the reserved value zero.
3. `reducer: String`, identifying the generated reducer.
4. `args: Bytes`, containing the BSATN-encoded reducer argument product.

`CallReducer` is variant index 3 of `ClientMessage` in the pinned schema. The connector sends one encoded `ClientMessage` per binary WebSocket message. Client messages have no application compression-prefix byte. WebSocket masking and framing surround that payload. [v2 message schema][v2], [native SDK transport][sdk]

The conceptual wire layout is:

```text
	ClientMessage variant tag: u8 = 3
	request_id: u32 little-endian
	flags: u8 = 0
	reducer_name_byte_count: u32 little-endian
	reducer_name: UTF-8 bytes
	args_byte_count: u32 little-endian
	args: encoded reducer argument product
```

BSATN products concatenate their fields without names or a product header. Strings and byte arrays include U32 byte counts. Arrays of descriptors include a U32 element count. These wrappers must match the registered reducer schema. [BSATN specification][bsatn]

### Proposed generated commit contract

One proposed reducer parameter is a generated `CommitProposal` record. Its fields include contract version, durable operation identity, partition identity, expected and next checkpoints, dependency versions, row-write descriptors, and a contiguous row-byte buffer.

Each row-write descriptor identifies a generated table binding and an exclusive byte range `[start, end)` in the buffer. Binding identifiers are resolved to module-owned table names and IDs. The client does not choose unrestricted database table IDs.

The batch layout is conceptually:

```text
	args_byte_count
	  CommitProposal fields in generated schema order
	    metadata and dependency versions
	    descriptor_count
	      table_binding, start, end
	      table_binding, start, end
	      ...
	    row_bytes_byte_count
	      encoded row product | encoded row product | ...
```

The row buffer is a BSATN byte-array field, so its byte count is additional to the outer `args` byte count. Each range contains exactly one table-row BSATN product, without its descriptor, batch count, or byte-array wrapper. Variable-length fields inside that row retain their own BSATN lengths.

This contract is proposed generation output, not an existing SpacetimeDB message type. The argument metadata and descriptors are decoded normally. The row payload remains bytes until datastore validation.

## Host dispatch preserves the encoded arguments

The host decodes the client envelope and validates the reducer arguments against the registered signature. For BSATN input, `FunctionArgs::_into_tuple` retains the incoming bytes in `ArgsTuple`. `ArgsTuple::get_bsatn` returns those cached bytes instead of always re-encoding its decoded product. [Argument cache][args]

The WASM argument source copies bytes into guest memory. Standard Rust reducer dispatch then decodes the argument product. `Vec<u8>` decoding obtains an owned byte buffer through a specialized byte-slice path. These transfers still allocate or copy, but do not require decoding every row into a Rust row type. [WASM argument transfer][wasm-copy], [Rust reducer dispatch][rust-dispatch], [owned byte decoding][bytes-decode]

The generated shim can take mutable subslices of that owned buffer. There is no required fresh allocation or BSATN encoding for each row before insertion.

## `datastore_insert_bsatn` accepts the existing row bytes

The public Rust wrapper has this signature:

```rust
pub fn datastore_insert_bsatn(
	table_id: TableId,
	row: &mut [u8],
) -> Result<&[u8]>
```

Here `Result` is the syscall layer's result type, with an `Errno` error. A generated shim calls it through `spacetimedb::sys`. It can resolve a generated table name with `sys::table_id_from_name` inside the transaction. [Table lookup][table-lookup], [insertion wrapper][sys-insert]

The wrapper forwards the slice's pointer and length to the `spacetime_10.0` import. The host obtains a mutable view of the guest-memory range and calls `InstanceEnv::insert`. It does not reconstruct a typed module row or serialize that row into a second insertion buffer. [WASM insertion implementation][wasm-insert], [insertion environment][instance-insert]

For comparison, the normal Rust table API's `insert_with_backend` clears an encoding buffer and calls `buf.serialize_into(&row)` before insertion. The raw-byte shim bypasses that function for the proposed rows. Small checkpoint and completion writes may still use typed table APIs. [Typed insertion][typed-insert]

### Generated columns mutate the input slice

The host writes generated sequence-column values into the beginning of the supplied buffer. They form a BSATN product in generated-column order. The returned slice covers those generated values, not the entire original row. When no values are generated, the returned slice is empty. [Authoritative host behavior][wasm-insert], [column projection][instance-insert]

The shim therefore establishes all row boundaries before insertion and does not infer the next row's position from the returned length. Any proposal fingerprint is computed before destructive buffer writes. If the coordinator needs generated IDs, the shim records or returns them through a declared contract.

### Atomic validation and failures

The shim checks operation identity, contract agreement, expected checkpoints, dependency versions, and fencing conditions in the same transaction as writes. It performs no application authorization checks in the initial trusted-local profile. Descriptor ranges need checked arithmetic, bounds, non-overlap, and complete payload coverage. Batch row counts and byte sizes remain bounded.

The insertion path checks the actual row schema, table existence, uniqueness, and sequence behavior. Raw bytes do not bypass those checks. A malformed row, failed insertion, or failed final checkpoint write must cause reducer failure rather than leave an accepted partial batch. [Insertion errors][wasm-insert], [datastore constraints][mut-insert]

Freshness checks do not prove that Bend computed the correct business result. Required data invariants remain database constraints or explicit shim checks. A check that needs row fields can inspect those fields without requiring every row to be reconstructed and re-encoded.

## Remaining storage work

The raw insertion path removes module-side row reserialization. It does not remove these datastore boundaries:

- For eligible fixed-size row schemas, the datastore validates BSATN and copies segments into BFLATN storage, skipping padding. It need not first construct a general `ProductValue` for this physical insertion step. [Static insertion][physical-insert], [copy-based layout conversion][static-layout]
- For variable-size schemas, the fallback decodes a `ProductValue` and writes its fields into pages and blob storage. Array-valued columns are internally BSATN-encoded again for storage. [Variable-size insertion][variable-insert]
- During commit, the current implementation reconstructs product values from transaction rows and inserts them into committed storage. Whole-page transfer is identified in source as a future optimization. [Commit merge][commit-merge]
- Subscriptions encode matching committed query changes for delivery. These may differ from the input proposal and do not reuse it as an outgoing transaction payload. [Subscription encoding][subscription-encoding]

Fixed-size hot tables provide the closest existing physical insertion path to the intended low-conversion design. Bend's internal types remain independent. Variable-size tables are supported, but their storage path has additional work. True end-to-end zero-copy insertion requires datastore changes beyond this shim.

## Outcomes and recovery

The connector correlates `ReducerResult` by request ID and applies successful result-embedded subscription changes before reporting completion. With `confirmed=true`, transaction-associated output waits for configured durability. Databases without persistence bypass that gate. [v2 results][v2], [durability gate][durability]

A lost reply leaves the commit outcome uncertain. The module stores durable operation identities, checkpoints, and completion records so the coordinator can reconcile before retrying. Duplicate operation IDs resolve against the recorded outcome and proposal identity. Connection-local request IDs do not provide deduplication.

## Verification targets

Implementation must demonstrate byte-for-byte row codec agreement, valid raw insertion, malformed-row rejection, generated-column handling, atomic batch rollback, and uncertain-outcome reconciliation. Fixed and variable-size row schemas both need coverage. These checks have not been run for a Confluxion shim.

[sys-export]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/bindings/src/lib.rs#L37-L61
[sys-insert]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/bindings-sys/src/lib.rs#L1420-L1437
[abi-registration]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/core/src/host/wasm_common.rs#L407-L419
[module-schema]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/lib/src/db/raw_def/v10.rs
[v2]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/client-api-messages/src/websocket/v2.rs
[sdk]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/sdks/rust/src/websocket.rs
[bsatn]: https://spacetimedb.com/docs/bsatn
[args]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/core/src/host/mod.rs#L72-L123
[wasm-copy]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/core/src/host/wasmtime/wasm_instance_env.rs#L1366-L1403
[rust-dispatch]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/bindings/src/rt.rs#L47-L59
[bytes-decode]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/sats/src/de/impls.rs#L198-L285
[table-lookup]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/bindings-sys/src/lib.rs#L1380-L1389
[wasm-insert]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/core/src/host/wasmtime/wasm_instance_env.rs#L954-L1007
[instance-insert]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/core/src/host/instance_env.rs#L430-L500
[typed-insert]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/bindings/src/table.rs#L1855-L1877
[mut-insert]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/datastore/src/locking_tx_datastore/mut_tx.rs#L3868-L4055
[physical-insert]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/table/src/table.rs#L696-L750
[static-layout]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/table/src/static_layout.rs#L1-L22
[variable-insert]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/table/src/bflatn_to.rs
[commit-merge]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/datastore/src/locking_tx_datastore/committed_state.rs#L722-L803
[reducer-execution]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/core/src/host/wasm_common/module_host_actor.rs#L1181-L1343
[commit]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/core/src/subscription/module_subscription_actor.rs#L2041-L2143
[subscription-encoding]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/core/src/subscription/module_subscription_manager.rs#L1426-L1454
[durability]: https://github.com/clockworklabs/SpacetimeDB/blob/07efb0c44c42288ed8e40dcece75659cf44ec80c/crates/core/src/client/client_connection.rs#L187-L255
