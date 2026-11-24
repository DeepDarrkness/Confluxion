# Agent guidelines

This project uses Bend and SpacetimeDB. Read these resources to understand the language before working on Bend code:
- https://bend2.dev/llms.txt
- https://github.com/bendlang/bend/blob/main/guide/GUIDE.md
- https://spacetimedb.com/llms.txt

Use `$...$` for inline math and `$$...$$` for block equations in Markdown.

## Minimalist testing

- Use Cargo's native Rust test runner: `cargo test --release --locked`. It runs the current phase's routine checks. Keep each phase feature independent and update the default when work advances.
- Select the smallest Cargo target or named test covering the changed boundary. Select older phases only when their boundary changed, and run each selected suite once after the final relevant edit. See [phase selection](docs/ws-phased-plan.md#phase-scoped-test-selection). Record the selection and reason before execution. Previously passed unchanged phases stay out of the run.
- Use clang for native fixtures and prepared Bend/reference sources. Keep Python, extra test frameworks, and separate runners out of the test path.
- Follow YAGNI: remove no-op tests, redundant checks, unused modes, and speculative test infrastructure.
- Keep routine tests short and behavior-focused. Use one representative real transfer per path, focused fault controls, resource bounds, and cleanup. Run shared evidence-damage controls once. Put workload matrices, million-message campaigns, sanitizers, and soak in explicit qualification.
- Cache native fixtures by their actual source, dependency, toolchain, archive, and compiler-flag inputs. Validate cached artifact identity. Build each input set once across test cases and invocations; retain a build manifest in the evidence.
- Expose independent correctness checks as separate Cargo tests. Give each test its own ports, process groups, temporary files, and evidence directory. Bound concurrent builders and CPU-intensive fixtures; serialize only a real shared resource.
- Use distinct output files for parallel compilation units. Publish completed shared builds atomically under a per-input builder lock.
- Keep performance trials off correctness CPU slots. Reserve the same endpoint CPU budget for each paired comparison and prevent overlapping measurements on that budget. Parallel execution must preserve measurement validity.
- Keep throughput benchmarks separate from routine correctness tests. Add long runs or CI configuration only when requested.
- After a new failure or edit, rerun only affected checks. Report build time, test time, cache hits, and selected targets separately. Honor execution-skip requests; a skipped or interrupted gate is not a pass.
