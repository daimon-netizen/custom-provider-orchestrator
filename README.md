# Custom Provider Orchestrator

> Experimental user-space workaround for coordinating Codex GPT with
> custom-provider workers. It is not a native subagent fix.

Custom Provider Orchestrator is a dependency-free local STDIO MCP dispatcher.
Codex remains the GPT root and launches bounded custom-provider work as
independent top-level `codex exec` processes.

```text
Codex GPT root
    |
    |  start / status / wait / followup / cancel / close
    v
local STDIO MCP dispatcher
    |
    |  four-field task envelope + receipt nonce
    v
independent codex exec worker
    |
    v
configured custom-provider profile
```

This route exists because native cross-provider children can select a custom
provider and model yet still lose the dynamic task or follow-up payload. The
dispatcher does not imitate a native child. It gives Codex a separate,
explicit lifecycle surface and preserves a verifiable delivery boundary.

## What it provides

- Stable opaque job handles.
- Nonblocking status and bounded waits.
- Follow-up turns after a recoverable Codex thread is available.
- Cancellation with process-tree cleanup.
- Explicit close and local job cleanup.
- Four required task fields: `delegation_id`, `need`, `boundaries`, and
  `deliverable`.
- A caller-preserved receipt nonce and exact receipt verification.
- Read-only workers by default.
- A profile allowlist and a cross-process active-worker limit.
- Local result, event, and error caps.
- Redacted error excerpts returned through MCP.

A valid receipt proves that the worker received the intended envelope. It does
not prove that the worker's answer is correct; the Codex root must still verify
and integrate the result.

## Install

```bash
codex plugin marketplace add daimon-netizen/custom-provider-orchestrator
codex plugin add custom-provider-orchestrator@daimon-locus
```

Start a new Codex task after installation so the skill and MCP tools load in a
fresh session.

The plugin uses profiles already configured in Codex. Provider authentication
remains in normal Codex configuration and is never accepted as an MCP tool
argument.

## Defaults

- Allowed profiles: `minimax`, `minimax-fast`
- Sandbox: `read-only`
- Workspace writes: disabled
- Worker native subagent tools: disabled
- Worker marketplace plugins: disabled
- Maximum active workers: 2
- Per-stream event/error cap: 5 MB
- Completed-job retention: 7 days
- Result returned to the root: at most 50,000 characters

Optional environment configuration:

| Variable | Purpose |
| --- | --- |
| `CUSTOM_PROVIDER_PROFILES` | Comma-separated profile allowlist |
| `CUSTOM_PROVIDER_WORKSPACE_ROOTS` | Platform-separated writable root allowlist |
| `CUSTOM_PROVIDER_MAX_ACTIVE_JOBS` | Active-worker ceiling, hard-capped at 8 |
| `CODEX_CLI_PATH` | Absolute Codex executable override |
| `CODEX_HOME` | Codex home; dispatcher jobs default to `${CODEX_HOME}/custom-provider-orchestrator/jobs` |

Setting writable roots does not itself authorize a write. The root must also
request `sandbox = "workspace-write"` for a bounded task whose working
directory is under an allowed root.

## MCP lifecycle

1. Call `provider_worker_start` with a fresh delegation ID, the bounded need,
   boundaries, deliverable, an absolute working directory, and a configured
   profile.
2. Preserve the returned `job_id` and `receipt_nonce`.
3. Poll with `provider_worker_status` or wait for at most 50 seconds per
   `provider_worker_wait` call.
4. Accept a result only when `receipt_verified` is true and the returned
   delegation ID and receipt nonce both match.
5. Use `provider_worker_followup` only after a completed run exposes a
   recoverable Codex thread.
6. Cancel work that is no longer needed or has left its boundary.
7. Close a job only after preserving any needed result; close deletes its
   local job directory.

## Tool and session boundary

Workers launch with native multi-agent features and marketplace plugins
disabled. They retain the built-in Codex CLI harness allowed by the selected
sandbox. Other MCP or App/Connector tools are configuration-dependent and must
be verified in the worker's actual tool surface before delegation; schema
visibility alone does not prove that a tool is callable.

In a clean MiniMax dogfood run, several App/Connector schemas were visible, but
the in-app browser was not exposed and the visible Node REPL bridge returned an
unsupported-call error. Therefore this release does not promise access to the
root task's signed-in browser session, browser tabs, or arbitrary MCP tools.
Browser-dependent external actions remain with the Codex root unless a worker
tool probe proves the required capability.

Long-context multi-turn work is supported as resumable turns, not as one
continuously resident process. `provider_worker_followup` starts a fresh
`codex exec resume` process against the completed worker's thread ID and uses a
new task envelope and receipt nonce.

## Development

```bash
cd plugins/custom-provider-orchestrator
python3 -m unittest discover -s tests -v
python3 scripts/run_canary.py --cwd /absolute/path/to/a/workspace
```

The unit suite is provider-free. The live canary uses the configured profiles
and may consume provider quota. Never publish its raw output: it can contain
local worker and thread identifiers.

## Tested boundary

Local end-to-end start, wait, receipt, and follow-up canaries succeeded with:

- MiniMax-M3
- MiniMax-M2.7-highspeed

The provider-free suite covers active-job limits, atomic reservation,
cancellation, descendant cleanup, restart recovery, result caps, secret
environment exclusion, receipt parsing, redaction, follow-up validation, MCP
negotiation, and workspace-write enforcement.

## Known limits

- This is an external worker path, not a repair or replacement for Codex
  native subagents.
- Workers are independent top-level processes and do not appear in the native
  subagent panel.
- Workers do not inherit native child-agent handles, context forks, or UI
  lifecycle state.
- Workers do not inherit the root task's signed-in browser state, and
  marketplace-plugin tools are disabled.
- Non-plugin MCP and App/Connector availability is runtime-dependent; verify
  both schema exposure and a safe read-only call before relying on a tool.
- Follow-up requires a recoverable Codex thread ID from the completed worker.
- The dispatcher requires a locally usable `codex exec` environment and
  correctly configured custom-provider profiles.
- Native custom-provider task delivery remains an upstream issue; see
  [openai/codex#35932](https://github.com/openai/codex/issues/35932) and the
  proposed external-backend contract in
  [openai/codex#33131](https://github.com/openai/codex/issues/33131).

## License

MIT © 2026 [Daimon Locus](https://github.com/daimon-netizen)
