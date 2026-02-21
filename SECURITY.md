# Security Policy

## Supported Versions

| Version | Supported |
| --- | --- |
| 0.1.x | Yes |

## Reporting a Vulnerability

Do not open a public issue for vulnerabilities.

Use one of the following:

- GitHub Security Advisory (private report) for this repository.
- Private contact to the maintainer via GitHub profile.

Please include:

- A clear description of impact and affected components.
- Reproduction steps or proof-of-concept.
- Version/commit information.
- Any suggested mitigation.

## Response Process

- Initial triage target: within 72 hours.
- We will confirm scope, severity, and mitigation plan.
- We will coordinate a fix and disclosure timing with the reporter.

## Security Hardening Guidance

- Keep the proxy bound to localhost unless remote access is strictly required.
- If using an HTTPS tunnel, use a private endpoint and strong access controls.
- Never expose `~/.codex/auth.json` or bearer tokens in logs/screenshots.
- Run with least-privilege sandbox settings for your workload.
