# Confluxion

Confluxion is an event-sourced, reactive backend built on SpacetimeDB and Bend.

## Research and design

- [Desired architecture](confluxion-arch1.md) defines service bindings, coordination, transactions, and recovery.
- [WebSocket design](bend-websocket-design.md) describes the native transport, BSATN codecs, and SpacetimeDB session.
- [WebSocket phase plan](ws-phased-plan.md) tracks implementation status, phase gates, and local benchmark workloads.
- [Raw BSATN shim design](bend-shim-design.md) describes generated transactional writes without module-side row reserialization.
- [Connector feasibility](spacetime-bend.md) assesses external Bend execution and event-sourced projections.
- [CQRS and platform reuse](spacetime-bend-cqrs.md) covers subscriptions, generation, frontend integration, and CLI reuse.
- [Performance analysis](spacetime-bend-performance.md) traces costs and measurement requirements.
- [Agent guidelines](../AGENTS.md) records the project's Bend resources, phase-scoped testing policy, and Markdown conventions.

The reports record source research from November 17, 2026. They use different source revisions, so the WebSocket version target needs confirmation before implementation.

The initial deployment follows the [trusted-local access policy](confluxion-arch1.md#trusted-local-access-policy): loopback connections without client credentials or application auth checks. The current architecture and phase plan supersede broader auth requirements in historical research.

`architecture.md` and `bend-node-phase4.md`, referenced by earlier reports, are absent from this repository.
