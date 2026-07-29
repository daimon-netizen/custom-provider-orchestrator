# Security

Custom Provider Orchestrator is a local process dispatcher. Treat provider
configuration, worker prompts, results, and local job state as sensitive.

## Credential boundary

- The MCP tools never accept API keys, tokens, passwords, or provider
  endpoints.
- Authentication stays in the user's existing Codex provider configuration.
- The dispatcher starts workers with a minimal allowlisted environment and
  excludes common provider-secret variables.
- Workspace writes are disabled unless the user explicitly configures
  `CUSTOM_PROVIDER_WORKSPACE_ROOTS`.

## Logs and reports

Local worker logs and results may contain task content. Do not attach raw logs,
job metadata, Codex configuration, or terminal screenshots to a public issue.
Before sharing a reproduction, reduce it to a synthetic task and remove local
paths, worker identifiers, thread identifiers, nonces, and provider details.

## Reporting a vulnerability

Use GitHub's private vulnerability reporting for this repository. Do not open a
public issue containing credentials, private task content, or exploitable
details.
