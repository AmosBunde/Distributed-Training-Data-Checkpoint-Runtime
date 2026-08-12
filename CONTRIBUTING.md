# Contributing to Distributed Training Data & Checkpoint Runtime

Thanks for contributing! This document describes how changes flow into the repository.

## Workflow

1. **Open (or pick) an issue.** Every non-trivial change starts as a GitHub issue describing
   the problem, the proposed scope, and acceptance criteria.
2. **Create a linked branch.** Branches are attached to their issue
   (`gh issue develop <n> --name feat/<n>-<slug>`), so the issue's *Development* panel shows the branch.
3. **Implement with tests.** Rust changes need `cargo test` coverage; Python changes need `pytest`
   coverage. Performance-sensitive changes should include benchmark numbers.
4. **Run the quality gates locally** (the same gates CI enforces):

   ```bash
   # Rust (from runtime/)
   cargo fmt --all -- --check
   cargo clippy --all-targets -- -D warnings
   cargo test

   # Python (from repo root, with the dev extras installed)
   ruff check python/
   ruff format --check python/
   pytest -q python/tests
   ```

5. **Open a PR** that references its issue (`Closes #<n>`) and explains:
   - the problem statement
   - design notes / trade-offs considered
   - how it was tested
   - benchmarks, if performance-sensitive

## Branch & PR conventions

- Branch names: `feat/<issue>-<slug>`, `fix/<issue>-<slug>`, `docs/<issue>-<slug>`.
- Stacked PRs are welcome when changes depend on each other; set the PR base to the branch you
  stacked on and say so in the description.
- Keep commits focused; squash noisy WIP commits before review.

## Code style

- **Rust:** `rustfmt` defaults, `clippy` clean at `-D warnings`. Prefer `thiserror` for error
  types, `tracing` for logs (never `println!` in library code).
- **Python:** `ruff` (lint + format), type hints on the public API surface, `mypy` clean for `dtr/`.
- **Protos:** versioned packages (`dtr.<service>.v1`); never change field numbers of released
  messages — add new fields instead.

## Architecture decisions

Load-bearing design choices are recorded as ADRs in `docs/adr/`. If your PR changes a recorded
decision, add a superseding ADR rather than editing history.

## License

By contributing you agree that your contributions are licensed under the Apache License 2.0
(see `LICENSE`).
