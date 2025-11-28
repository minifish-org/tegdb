# AGENTS

This document defines the autonomous or semi-autonomous agents we rely on when working on TegDB. Each agent encapsulates a repeatable workflow so that contributors (human or AI) can take on a role, follow the playbook, and hand off artifacts with predictable quality.

## Shared Conventions

- Keep changes minimal and avoid introducing new dependencies unless a maintainer explicitly agrees.
- Default test command: `cargo test`. When in doubt, also run `cargo test --all-features --quiet` and `cargo test --target wasm32-unknown-unknown --all-features`.
- Skip benchmarks unless the task explicitly requires them; they take too long for normal CI.
- CI workflows must target the `main` branch only.
- Prefer absolute paths in scripts and documentation to reduce ambiguity on macOS/Linux runners.

## Code Quality & Behavior Constraints

**Core Principle:** All issues must be fixed, never worked around. Code quality checks exist to maintain standards, and bypassing them degrades code quality over time.

### Prohibited Workarounds

The following workarounds that bypass checks are **strictly forbidden**:

- `#[allow(dead_code)]` and similar `#[allow(...)]` attributes that suppress warnings
- `#[allow(unused_*)]` attributes for unused variables, imports, or functions
- `#[allow(clippy::*)]` attributes to silence clippy warnings
- Any other attribute or configuration that suppresses compiler or linter warnings

### Required Actions

1. **Fix the root cause:** If code is unused, remove it. If a warning indicates a real issue, address it properly.
2. **Pre-submission validation:** Before submitting any changes, `./ci_precheck.sh` must pass completely. This script enforces:
   - Formatting compliance (`cargo fmt --check`)
   - Zero clippy warnings (`cargo clippy --all-features -- -D warnings`)
   - Successful builds with all features
   - Documentation builds
   - All tests passing
   - Key examples running successfully

3. **No exceptions:** If `ci_precheck.sh` fails, the code is not ready for submission. Fix the issues, then re-run the script until it passes.

### Rationale

These constraints ensure:

- Code quality remains high over time
- Technical debt doesn't accumulate through suppressed warnings
- All contributors follow the same quality standards
- CI failures are caught locally before pushing

## Build & CI Agent

- **Goal:** Keep the repository buildable and the CI green.
- **Inputs:** Updated source tree, `.github/workflows/`, `ci_precheck.sh`, `run_all_tests.sh`.
- **Primary Tasks:**
  1. Validate formatting (`cargo fmt --check`) and linting (`cargo clippy --all-features -- -D warnings`).
  2. Run the shared test commands listed above and capture logs.
  3. Update workflow files when new targets/toolchains must be added.
  4. **Enforce pre-submission:** Ensure `./ci_precheck.sh` passes before any code is submitted. Reject any changes that include workarounds (see Code Quality & Behavior Constraints above).
- **Deliverables:** Test logs, workflow diffs, and a brief summary of any failures plus mitigations.

## QA & Regression Agent

- **Goal:** Catch behavioral regressions before they land on `main`.
- **Inputs:** PR diff, unit/integration test suites under `tests/`, and the `examples/` directory for reproduction.
- **Primary Tasks:**
  1. Map each change to existing coverage; add tests where gaps exist.
  2. Run targeted integration suites (e.g., `tests/integration_vector_features_test.rs`) related to the feature area.
  3. File reproducible bug reports when issues surface, including failing SQL snippets.
- **Deliverables:** New or updated tests, minimal repro scripts, and risk notes for reviewers.

## Documentation Agent

- **Goal:** Keep `README.md`, `examples/`, and supporting guides (including this file) accurate.
- **Inputs:** User-facing changes, CLI flag updates, or new features.
- **Primary Tasks:**
  1. Update relevant sections in `README.md`, `examples/`, or newly created guides.
  2. Provide copy-edited prose with command sequences tested on macOS (`/bin/zsh`) when possible.
  3. Cross-link new docs from `README.md` so discoverability stays high.
- **Deliverables:** Markdown diffs plus verification notes confirming commands were executed or, if not, why.

## Release & Packaging Agent

- **Goal:** Produce reproducible releases and communicate changes.
- **Inputs:** `Cargo.toml`, changelog material from merged PRs, release scripts, and Git tags.
- **Primary Tasks:**
  1. Bump versions consistently across crates and binaries.
  2. Regenerate release artifacts (binaries, archives) and verify signature/hashes.
  3. Draft release notes summarizing features, fixes, and known issues.
- **Deliverables:** Tagged release commit, published artifacts, and release notes ready for GitHub/Git.

## Security & Compliance Agent

- **Goal:** Surface security risks early and ensure data-handling policies remain intact.
- **Inputs:** Dependency graph (`cargo metadata`), persistence layers in `src/storage_*`, and cloud integrations (`tgstream`).
- **Primary Tasks:**
  1. Audit dependency updates for CVEs, ensuring no new optional features weaken defaults.
  2. Review file and network access patterns, especially around S3/MinIO configurations.
  3. Recommend mitigations (config hardening, validation layers) and track them to completion.
- **Deliverables:** Security advisories, follow-up issues, and proof of mitigation where applicable.

---

If you add a new agent, document its trigger conditions, responsibilities, and required outputs so contributors can adopt the role without additional onboarding.
