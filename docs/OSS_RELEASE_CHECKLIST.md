# OSS Release Checklist

Use this checklist before tagging or announcing a release.

## Repository Hygiene

- [ ] `LICENSE` is present and matches `Cargo.toml`.
- [ ] `README.md` reflects current CLI flags and API behavior.
- [ ] `CONTRIBUTING.md`, `SECURITY.md`, `SUPPORT.md`, and `CODE_OF_CONDUCT.md` exist and are current.
- [ ] No secrets or credentials are present in tracked files.

## Quality Gates

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --all-targets --all-features -- -D warnings`
- [ ] `cargo test --all-targets --all-features`

## Operational Constraints

- [ ] Default docs recommend localhost bind (`127.0.0.1`) unless explicitly required.
- [ ] Docs warn against exposing `~/.codex/auth.json` and access tokens.
- [ ] Security reporting path is private and tested.

## Release Notes

- [ ] Breaking changes are explicitly called out.
- [ ] Migration steps are documented.
- [ ] Known limitations and unsupported behavior are listed.
