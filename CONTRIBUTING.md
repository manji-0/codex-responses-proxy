# Contributing Guide

Thanks for contributing to `codex-responses-proxy`.

## Before You Start

- Open an issue for non-trivial changes before implementation.
- Keep pull requests focused on one concern.
- Do not include secrets, credentials, or personal tokens in code, tests, or logs.

## Development Setup

```bash
git clone https://github.com/manji-0/codex-responses-proxy.git
cd codex-responses-proxy
cargo build
```

## Required Checks

Run all checks before opening a pull request:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

Optional live test (requires `codex` in `PATH`):

```bash
cargo test live_app_server_thread_start_reports_required_mcp_startup_failure -- --ignored
```

## Coding Standards

- Prefer small, composable functions with explicit error handling.
- Add or update tests for behavior changes.
- Keep public behavior documented in `README.md`.
- Preserve backward compatibility for API behavior whenever possible; document breaking changes clearly.

## Pull Request Requirements

- Link related issue(s).
- Explain behavior changes and compatibility impact.
- Include exact validation commands and outcomes.
- Update docs when flags, endpoints, or operational behavior change.

## Security

- Report vulnerabilities through `SECURITY.md`.
- Do not open public issues for security-sensitive findings.
