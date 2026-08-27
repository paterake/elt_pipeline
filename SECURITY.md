# Security Policy

## Reporting a vulnerability

**DO NOT file a public GitHub issue for security bugs.** Publicly disclosing a zero-day vulnerability puts every downstream user of `elt_pipeline` at risk before a fix or mitigation is available.

Instead, send a vulnerability report directly to the maintainers via:

- **Email:** `security@rakee.app` (preferred — monitored by both maintainers 24/7 PII-free)
- **Fallback private channel:** GitHub private security advisory — https://github.com/paterake/elt_pipeline/security/advisories/new

Include in your report, if you have them:
1. A short description of the vulnerability.
2. Exact reproduction steps (commands + input payloads + versions).
3. A minimal reproducer repository or pastebin of the triggering input.
4. Expected vs actual behavior.
5. Your assessment of the severity (CVSS score or descriptive label: Low/Medium/High/Critical).

## Disclosure window & timeline

This project follows a **90-day coordinated disclosure window** as the default operating mode:

| Day marker | Event |
|---|---|
| Day 0 | Maintainer acknowledges receipt of your report, assigns a tracking ID, confirms scope. |
| Day 14 | First classification response: "confirmed + working on fix" / "needs more info" / "out of scope — explain why". |
| Day 45 (±1 week) | Midpoint update: patch progress status, draft release plan. |
| Day 60–90 window close | Patch released as a new tagged PyPI version + GitHub Release with changelog + advisory. Public disclosure happens **after** the patch is released, or jointly at day 90 if the patch misses the window for any reason. |

If the vulnerability has **active exploitation in the wild** (credible evidence: CVE assigned, Twitter/X/Github mentions of in-the-wild abuse, vendor notifications), the 90-day window compresses to **14 days** and we will coordinate with you, any CNA (CVE Numbering Authority), and any known affected downstream deployers for an earlier coordinated drop.

## Scope

What IS in scope for a security report:
- Remote code execution (RCE) paths via untrusted input YAMLs, SQL connector DSNs, object-storage payloads.
- Secret leaks in logs, CLI tracebacks, Prometheus metrics, audit JSONL, or run artifacts.
- Authentication & TLS bypasses in the Trino serving path (M-4 subsystem).
- Bypasses of SECURITY DEFINER VIEWs, column-level masking, or DataClassification role checks (G-6 subsystem).
- Path traversal in the StorageBackend Protocol or connectors.

What is deliberately OUT of scope (use the normal issue tracker instead):
- Feature requests, capability additions (please open a feature RFC issue with concrete use case).
- False-positive scanner alerts on dependencies (e.g., "package X in `uv.lock` has a CVE against unused optional code paths") — open a regular issue and we'll triage via dependency updates.
- Denial of service that requires code execution access to the Spark driver host. Spark driver is a control plane; assume compromise of the driver host equals compromise of the pipeline.
- Security of downstream data platforms that `elt_pipeline` talks to (Unity Catalog access control, Glue IAM policies, etc.). These are owned by your cloud account.

## Supported versions for security patches

Security patches are backported to:
- **Latest tagged minor release (current).**
- **Previous tagged minor release (one back).**

Older tags: no backports. Users are expected to track current minus one.

## Acknowledgements

Reporters who follow this process and cooperate with maintainers on the disclosure window will be credited in the GitHub Release notes and the advisory text, unless you explicitly request anonymity.
