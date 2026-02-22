# Codex Responses Proxy

`codex-responses-proxy` is an OpenAI-style HTTP proxy for `codex app-server`.
It is optimized for practical compatibility with Cline / VS Code extension workflows, not full OpenAI API parity.

## Overview

- Inbound API shapes:
  - Responses API style (`/responses`, `/v1/responses`)
  - Chat Completions API style (`/chat/completions`, `/v1/chat/completions`)
- Backend protocol:
  - Codex app-server JSON-RPC over WebSocket
- Thread continuity:
  - Reuses app-server threads by conversation key and `previous_response_id` mapping (in-memory)

## What This Proxy Actually Does

- Converts OpenAI-style request payloads into app-server calls (`initialize`, `thread/start` or `thread/resume`, `turn/start`).
- Runs a managed `codex app-server` by default (unless `--app-server-url` is provided).
- Handles managed app-server lifecycle:
  - startup
  - health/reachability checks
  - restart on exit/unreachable
  - shutdown on proxy exit
- Performs managed warmup (`initialize` + `thread/start`) and reuses it for the first compatible request.
- Exposes compatibility routes:
  - `GET /health`
  - `GET /models`, `GET /v1/models`
  - `POST /responses`, `POST /v1/responses`
  - `POST /chat/completions`, `POST /v1/chat/completions`
- Emits structured JSON logs for requests and lifecycle events.

## Current Compatibility Scope

### Responses API (`/responses`, `/v1/responses`)

- Requires:
  - `model` (non-empty)
  - `input` (must be present and not null)
- Supports fields used by this proxy path:
  - `instructions`, `stream`, `tools`, `tool_choice`, `max_output_tokens`
  - `store`, `temperature`, `top_p`, `truncation`, `text`
  - `parallel_tool_calls`, `reasoning`, `conversation`, `user`, `metadata`, `conversation_key`
  - `previous_response_id`
- Validation implemented:
  - `metadata` object type
  - range checks (`temperature`, `top_p`)
  - `max_output_tokens > 0`
  - `truncation` in `{auto, disabled}`

Conversation key resolution priority for Responses requests:

1. `conversation_key`
2. `metadata.conversation_key`
3. `conversation` (string or `conversation.id`)
4. `user`
5. `x-conversation-key` / `x-thread-key` header

`previous_response_id` handling:

- Resolved against in-memory response->conversation mapping.
- Unknown IDs return `400` with error code `previous_response_not_found`.

### Chat Completions (`/chat/completions`, `/v1/chat/completions`)

- Accepts:
  - `model`, `messages`, `temperature`, `max_tokens`, `stream`, `tools`, `tool_choice`
  - `user`, `metadata`, `conversation_key`

Conversation key resolution priority for Chat requests:

1. `conversation_key`
2. `metadata.conversation_key`
3. `user`
4. `x-conversation-key` / `x-thread-key` header

### Models (`/models`, `/v1/models`)

- Returns a static compatibility list (`gpt-4`, `gpt-5`).

## Streaming Behavior (Important)

### Responses streaming (`stream=true`)

- Returns SSE events, including:
  - `response.created`
  - `response.in_progress`
  - `response.output_item.added`
  - `response.output_text.delta`
  - `response.output_text.done`
  - `response.output_item.done`
  - `response.completed`
  - `response.failed`
  - `error`
  - final `data: [DONE]`

### Chat Completions streaming (`stream=true`)

- Returns OpenAI-style SSE chunks for compatibility.
- Current implementation emits compatibility chunks from the completed turn response (not true token-by-token passthrough).

## Known Constraints and Non-Goals

- Full OpenAI API parity is out of scope.
- Inbound API key authentication is not enforced by this proxy itself.
- Conversation and `previous_response_id` history is in-memory only:
  - lost on proxy restart
  - bounded by `--max-thread-history` (old entries are evicted)
- `/models` is static, not provider-discovered.
- Tool interoperability is focused on function tools.
- App-server approval-related requests are handled automatically (`accept` / empty answers), so deployment trust boundaries must be strict.

## Security Notes

- This is an unofficial community project and is not affiliated with or endorsed by OpenAI, Anthropic, or Cline.
- You are responsible for complying with Terms of Service, policies, and local laws.
- Default bind is `127.0.0.1`; keep it private unless you intentionally harden network exposure.
- Credentials are read from `~/.codex/auth.json`; never commit or expose token material.

## Quick Start

### 1. Build

```bash
git clone https://github.com/manji-0/codex-responses-proxy.git
cd codex-responses-proxy
cargo build --release
```

### 2. Run

```bash
./target/release/codex-responses-proxy --port 8888 --auth-path ~/.codex/auth.json
```

### 3. Smoke Test

```bash
curl http://127.0.0.1:8888/health

curl http://127.0.0.1:8888/v1/models

curl -X POST http://127.0.0.1:8888/v1/responses \
  -H "Content-Type: application/json" \
  -d '{"model":"gpt-5","input":"Hello"}'
```

### 4. Optional HTTPS Tunnel

```bash
ngrok http 8888 --domain=your-static-domain.ngrok-free.app
```

## Command Line Options

Current `--help` output:

```bash
Usage: codex-responses-proxy [OPTIONS]

Options:
  -p, --port <PORT>
          Port to listen on [default: 8080]
      --bind <BIND>
          Host/IP to bind to [default: 127.0.0.1]
      --auth-path <AUTH_PATH>
          Path to Codex auth.json file [default: ~/.codex/auth.json]
      --app-server-url <APP_SERVER_URL>
          Existing codex app-server WebSocket URL (optional)
      --managed-app-server-url <MANAGED_APP_SERVER_URL>
          WebSocket URL used by the managed background app-server [default: ws://127.0.0.1:39200]
      --codex-bin <CODEX_BIN>
          Codex executable path used when spawning app-server [default: codex]
      --app-server-cwd <APP_SERVER_CWD>
          Working directory for managed codex app-server (optional)
      --app-server-sandbox-mode <APP_SERVER_SANDBOX_MODE>
          Sandbox mode used for managed codex app-server and turn overrides [default: workspace-write] [possible values: read-only, workspace-write, danger-full-access]
      --max-concurrency <MAX_CONCURRENCY>
          Maximum number of concurrent in-flight chat requests [default: 8]
      --max-thread-history <MAX_THREAD_HISTORY>
          Maximum number of in-memory conversation/thread history mappings [default: 30]
      --request-timeout-secs <REQUEST_TIMEOUT_SECS>
          Per-request timeout in seconds [default: 300]
      --warmup-model <WARMUP_MODEL>
          Model used for managed app-server warmup thread/start [default: gpt-5]
  -h, --help
          Print help
  -V, --version
          Print version
```

Notes:

- Omitting `--app-server-url` enables managed app-server mode.
- `--app-server-sandbox-mode workspace-write` sets turn policy with `writableRoots=[effective_cwd]` and `networkAccess=true`.
- Setting `--warmup-model ""` effectively disables warmup.

## auth.json Shape

The proxy reads `--auth-path` (default: `~/.codex/auth.json`) and serves app-server token refresh requests from `tokens`.

```json
{
  "tokens": {
    "access_token": "...",
    "account_id": "...",
    "refresh_token": "..."
  },
  "OPENAI_API_KEY": "optional"
}
```

Notes:

- `tokens.access_token` and `tokens.account_id` are used for `account/chatgptAuthTokens/refresh`.
- `OPENAI_API_KEY` is parsed but not used for refresh handling.

## Development

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
```

Live app-server E2E tests (requires `codex` in `PATH`):

```bash
cargo test -- --ignored
```

## Project Policies

- Contribution guide: `CONTRIBUTING.md`
- Code of conduct: `CODE_OF_CONDUCT.md`
- Security policy: `SECURITY.md`
- Support policy: `SUPPORT.md`

## License

Licensed under Apache License 2.0. See `LICENSE`.
