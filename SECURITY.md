# Security Policy

## Supported Versions

| Version | Supported          |
|---------|--------------------|
| latest  | :white_check_mark: |

## Reporting a Vulnerability

If you discover a security vulnerability, please report it responsibly:

1. **Do NOT open a public issue.**
2. Email the maintainer at the address listed in the repository profile, or use [GitHub's private vulnerability reporting](https://docs.github.com/en/code-security/security-advisories/guidance-on-reporting-and-writing-information-about-vulnerabilities/privately-reporting-a-security-vulnerability).
3. Include a description of the vulnerability, reproduction steps, and potential impact.
4. You will receive an acknowledgement within 72 hours.

## Security Measures

### Automated Auditing (CI)

ClickGraph runs security checks on every pull request and on a weekly schedule:

| Tool | What It Checks | Trigger |
|------|---------------|---------|
| [cargo audit](https://rustsec.org/) | Rust dependencies against the RustSec Advisory Database | PRs to `main` + weekly cron |
| [cargo deny](https://embarkstudios.github.io/cargo-deny/) | License compliance, banned crates, duplicate versions, source provenance | PRs to `main` + weekly cron |
| [Dependabot](https://docs.github.com/en/code-security/dependabot) | Automated dependency update PRs for Cargo crates | Weekly |

CI configuration: [`.github/workflows/security.yml`](.github/workflows/security.yml)

### Dependency Policy (`deny.toml`)

- **Advisories**: Known vulnerabilities are denied; exceptions are documented with rationale.
- **Licenses**: Only OSI-approved permissive licenses are allowed (MIT, Apache-2.0, BSD, ISC, etc.).
- **Sources**: Only crates from crates.io are permitted — unknown registries and git sources are denied.
- **Duplicates**: Multiple versions of the same crate trigger a warning.

### Go and Python Bindings

The Go (`clickgraph-go/`) and Python (`clickgraph-py/`) binding packages have **zero runtime dependencies** — they are thin wrappers over the Rust FFI shared library. All security-sensitive code lives in the Rust crate and is covered by the auditing above.

If third-party dependencies are added to these packages in the future, the corresponding ecosystem auditing tools should be integrated:

- **Go**: [`govulncheck`](https://pkg.go.dev/golang.org/x/vuln/cmd/govulncheck) + Dependabot for Go modules
- **Python**: [`pip-audit`](https://pypi.org/project/pip-audit/) + Dependabot for pip

### Architecture Security Notes

- **Read-only engine**: ClickGraph translates Cypher to SQL `SELECT` queries only. Write operations (`CREATE`, `SET`, `DELETE`, `MERGE`) are not supported, limiting the attack surface.
- **No credential storage**: Database credentials are passed via environment variables at runtime, never persisted in configuration files.
- **SQL injection mitigation**: Query parameters are handled through the Cypher parser AST, not string interpolation. String literals in Cypher are escaped before embedding in generated SQL.
