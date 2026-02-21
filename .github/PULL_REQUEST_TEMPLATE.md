## Summary

Describe the change and why it is needed.

## Related Issues

- Closes #
- Related #

## Validation

List the exact commands you ran and outcomes.

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

## Compatibility Impact

- [ ] No behavior change for existing endpoints
- [ ] Breaking change (documented in README and PR summary)
- [ ] Docs updated

## Security Checklist

- [ ] No credentials/tokens added to code, tests, or logs
- [ ] Changes reviewed for auth, input validation, and error leakage
- [ ] Security-sensitive changes include test coverage
