---
name: custom-provider-orchestration
description: Coordinate Codex GPT with a custom-provider model through isolated top-level Codex workers. Use when a bounded task should be delegated to MiniMax or another configured Codex provider while avoiding OpenClaw and preserving Codex Harness tools.
---

# Custom Provider Orchestration

Codex is the owner and orchestrator. A custom-provider worker supplies a bounded
contribution; it does not become project authority and is not a native child
agent.

## Route

Use native collaboration tools for OpenAI-provider subagents. Use the
`custom-provider-orchestrator` MCP tools only when the requested role should run
through a configured custom-provider profile.

The MCP worker is an independent top-level `codex exec` session launched with
native multi-agent features and marketplace plugins disabled. It can use the
built-in Codex CLI harness allowed by its sandbox, but it does not appear in
the native subagent panel and cannot be controlled with `followup_task` or
`wait_agent`.

Do not assume that the worker inherits the root task's browser, Apps,
Connectors, or MCP tools. Inspect the actual worker tool surface and make one
safe read-only probe before relying on a configuration-dependent tool. Schema
visibility is not proof that a tool is callable. This release does not promise
access to the root task's signed-in browser session.

Multi-turn context is resumable rather than continuously resident.
`provider_worker_followup` launches a fresh `codex exec resume` process against
the completed worker's thread ID.

## Delegate

Call `provider_worker_start` with all four task fields:

- `delegation_id`: a fresh caller-generated identifier.
- `need`: the bounded contribution required.
- `boundaries`: exclusions, authority limits, and write scope.
- `deliverable`: the concrete result or artifact expected.

Also provide an absolute `cwd`. Default to `sandbox = "read-only"`; use
`workspace-write` only when the user has authorized implementation and the
worker has a disjoint write scope. Writable calls are rejected unless that
directory is under `CUSTOM_PROVIDER_WORKSPACE_ROOTS`.

The dispatcher returns a `job_id` and `receipt_nonce`. Preserve both. Accept a
result only when `receipt_verified` is true and the returned Delegation-ID and
receipt nonce match.

## Supervise

- Use `provider_worker_status` for a nonblocking snapshot.
- Use `provider_worker_wait` with at most 50 seconds per call.
- A wait timeout means only that the job is still running.
- Use `provider_worker_cancel` when the contribution is no longer needed or is
  outside scope.
- Use `provider_worker_followup` only after a completed run has a recoverable
  Codex thread ID.
- Use `provider_worker_close` only after preserving any needed result; it
  deletes the job's local logs.

Do not blindly retry a missing receipt, authentication failure, or provider
error. Inspect the job status and stderr excerpt, correct the route or task
envelope, then use a fresh Delegation-ID.

## Integrate

Treat worker output as candidate evidence. Codex must verify material claims,
inspect changed files, and synthesize the final answer. Never promote worker
state into shared memory, automation, project authority, or accepted evidence
without explicit verification.

Keep API keys out of prompts and results. Provider authentication belongs in
Codex provider configuration or environment variables, never in tool
arguments.
