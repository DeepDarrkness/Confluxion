# Agent guidelines

This project uses Bend and SpacetimeDB. Read these resources to understand the language before working on Bend code:
- https://bend2.dev/llms.txt
- https://github.com/bendlang/bend/blob/main/guide/GUIDE.md
- https://spacetimedb.com/llms.txt

Use `$...$` for inline math and `$$...$$` for block equations in Markdown.

## Minimalist testing

- Use Cargo's native Rust test runner: `cargo test --release --locked`. It runs the current phase's routine checks. Keep each phase feature independent and update the default when work advances.
- Select older suites only when changed code touches their tested boundaries. See [phase selection](docs/ws-phased-plan.md#phase-scoped-test-selection) for commands and dependency coverage. Report the phases selected and why. Full qualification is an explicit run, not a prerequisite for every edit.
- Use clang for native fixtures and prepared Bend/reference sources. Keep Python, extra test frameworks, and separate runners out of the test path.
- Follow YAGNI: remove no-op tests, redundant checks, unused modes, and speculative test infrastructure.
- Keep tests short and behavior-focused. Exercise real byte transfers, fault detection, resource bounds, and cleanup where relevant.
- Keep throughput benchmarks separate from routine correctness tests. Add long runs or CI configuration only when requested.
- Run the relevant suite once after meaningful changes; repeat only for new changes or unresolved failures. Honor requests to skip execution and report the suite as not rerun.
