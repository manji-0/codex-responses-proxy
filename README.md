# Codex Responses Proxy

A proxy server that allows CLINE (Claude Code) and other OpenAI-compatible extensions to use ChatGPT Plus tokens from Codex authentication instead of requiring separate OpenAI API keys.

## Overview

This proxy bridges the gap between:
- **Input (default)**: OpenAI Responses API format
- **Input (compatibility)**: OpenAI Chat Completions API format
- **Output**: Codex app-server JSON-RPC format

## OSS Scope and Constraints

- This is an unofficial community project and is not affiliated with or endorsed by OpenAI, Anthropic, or Cline.
- You are responsible for complying with all applicable Terms of Service, policies, and local laws when using this proxy.
- The proxy can read local credentials from `~/.codex/auth.json`; never commit, share, or expose credential files.
- Default binding is `127.0.0.1`. If you expose the proxy over the internet (for example with ngrok), you must apply strict access controls.
- This software is provided "as is", without warranty. See `LICENSE` for details.

## Project Policies

- Contribution guide: `CONTRIBUTING.md`
- Code of conduct: `CODE_OF_CONDUCT.md`
- Security policy: `SECURITY.md`
- Support policy: `SUPPORT.md`

## Features

- ✅ **OpenAI API Compatibility**: Supports Responses API (default) and Chat Completions (compatibility)
- ✅ **Codex app-server Integration**: Converts requests to Codex app-server JSON-RPC
- ✅ **Configurable Sandbox Execution**: Runs app-server with `approval_policy=never` and configurable `sandbox_mode` (default `workspace-write`)
- ✅ **Managed app-server Lifecycle**: Starts one background app-server at proxy startup and shuts it down with the proxy
- ✅ **Auto Restart**: Restarts managed app-server when connectivity is lost or the process exits
- ✅ **Managed Warmup**: Pre-starts one reusable thread at startup so the first compatible request avoids MCP startup latency
- ✅ **HTTPS Support**: Works with extensions requiring secure connections (via ngrok)
- ✅ **Streaming Responses**: Full streaming support for real-time responses
- ✅ **CLINE Compatible**: Tested extensively with CLINE VS Code extension
- ✅ **Array Content Support**: Handles both string and array message formats from OpenAI SDK
- ✅ **Conversation Continuity**: Reuses Codex thread IDs by conversation key headers/body, and maps `previous_response_id` for Responses API
- ✅ **Operational Controls**: Request concurrency limit, bounded in-memory thread history, and per-request timeout settings
- ✅ **Structured Logs & Health**: JSON logs with request IDs and enriched `/health` status payload
- ✅ **Universal Routing**: Bulletproof request routing that bypasses complex warp conflicts

## Quick Start

### 1. Build and Run

```bash
git clone https://github.com/manji-0/codex-responses-proxy.git
cd codex-responses-proxy
cargo build --release
./target/release/codex-responses-proxy --port 8888 --auth-path ~/.codex/auth.json
```

### 2. Setup HTTPS Tunnel (Required for CLINE)

Most VS Code extensions require HTTPS:

```bash
# Install ngrok and create your own static domain at https://dashboard.ngrok.com/domains
# Replace 'your-static-domain' with your unique domain name
ngrok http 8888 --domain=your-static-domain.ngrok-free.app
```

**Security Note**: Always use your own unique ngrok domain. Do not share your domain publicly to prevent unauthorized access to your proxy.

### 3. Configure CLINE Extension

In VS Code CLINE settings:
- **Base URL**: `https://your-static-domain.ngrok-free.app`
- **Model**: `gpt-5` (or `gpt-4`)
- **API Key**: Any value (not used, but required by extension)

### 4. Test Connection

```bash
# Health check
curl https://your-static-domain.ngrok-free.app/health

# Test completion (Responses API)
curl -X POST https://your-static-domain.ngrok-free.app/v1/responses \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer test-key" \
  -d '{
    "model": "gpt-5",
    "input": "Hello!"
  }'
```

## How It Works

### Request Flow

1. **Client** → Responses API (or Chat Completions) → **Proxy**
2. **Proxy** → Converts to app-server JSON-RPC (`initialize` / `thread/start` or `thread/resume` / `turn/start`) → **Codex app-server**
3. **Codex app-server** → Notifications / JSON-RPC responses → **Proxy**
4. **Proxy** → Converts to Responses API (or Chat Completions) → **Client**

When a request has a conversation key (`conversation_key`, `metadata.conversation_key`, `user`, `conversation.id`, or `x-conversation-key`), the proxy reuses the same app-server thread when possible.
For Responses API clients, `previous_response_id` is also mapped to the same internal conversation key.

### Format Conversion

**Responses API Request (default):**
```json
{
  "model": "gpt-5",
  "input": "Hello!"
}
```

**Codex app-server `turn/start` Request:**
```json
{
  "id": 3,
  "method": "turn/start",
  "params": {
    "threadId": "thread-123",
    "input": [
      {
        "type": "text",
        "text": "Hello!",
        "text_elements": []
      }
    ],
    "model": "gpt-5",
    "effort": "medium",
    "approvalPolicy": "never",
    "sandboxPolicy": {
      "type": "workspaceWrite",
      "writableRoots": ["/path/to/effective/cwd"],
      "networkAccess": true
    }
  }
}
```

## Configuration

### Command Line Options

```bash
codex-responses-proxy [OPTIONS]

Options:
  -p, --port <PORT>          Port to listen on [default: 8080]
      --bind <BIND>          Host/IP to bind to [default: 127.0.0.1]
      --auth-path <PATH>     Path to Codex auth.json [default: ~/.codex/auth.json]
      --app-server-url <URL> Existing codex app-server WebSocket URL (optional)
      --managed-app-server-url <URL>
                              WebSocket URL used by managed background app-server [default: ws://127.0.0.1:39200]
      --codex-bin <PATH>     codex executable path [default: codex]
      --app-server-cwd <PATH>
                              Working directory for managed codex app-server (optional)
      --app-server-sandbox-mode <APP_SERVER_SANDBOX_MODE>
                              Sandbox mode used for managed app-server and turn overrides
                              [possible values: read-only, workspace-write, danger-full-access]
                              [default: workspace-write]
      --max-concurrency <N>  Maximum number of concurrent chat requests [default: 8]
      --max-thread-history <N>
                              Maximum number of in-memory conversation/thread history mappings [default: 30]
      --request-timeout-secs <SECONDS>
                              Per-request timeout in seconds [default: 300]
      --warmup-model <MODEL>
                              Model used for managed app-server warmup thread/start [default: gpt-5]
  -h, --help                 Print help
  -v, --version              Print version
```

By default, the proxy starts one managed background `codex app-server` process on startup and reuses it for all requests.
If `--app-server-cwd` is omitted, the managed app-server inherits the proxy process working directory.
By default, `--app-server-sandbox-mode` is `workspace-write`; the proxy sets Responses/turn overrides so writable roots are limited to the effective CWD (`--app-server-cwd` if set, otherwise the proxy CWD).
By default, in-memory conversation continuity history is capped at 30 entries; this bound is configurable with `--max-thread-history`.
On proxy shutdown (for example, `Ctrl+C`), the managed app-server is stopped as well.
When managed mode is active, the proxy also performs a non-fatal startup warmup (`initialize` + `thread/start`) and keeps that connection as a one-shot prewarmed thread. The first compatible request consumes that thread, so MCP startup is paid during proxy boot instead of first user latency.

### Authentication

The proxy automatically reads authentication from your Codex `auth.json` file:

```json
{
  "access_token": "eyJ...",
  "account_id": "db1fc050-5df3-42c1-be65-9463d9d23f0b",
  "api_key": "sk-proj-..."
}
```

**Priority**: Uses `access_token` + `account_id` for ChatGPT Plus accounts, falls back to `api_key` for standard OpenAI accounts.

## API Endpoints

### Health Check
- **GET** `/health`
- Returns service status, app-server mode/status, restart counters, and concurrency availability

### Responses (Default)
- **POST** `/v1/responses` (also `/responses`)
- OpenAI-compatible Responses API endpoint
- Supports: `model`, `input`, `instructions`, `stream`, `tools`, `tool_choice`, `max_output_tokens`
- Continuity keys:
  - Request body: `conversation_key`
  - Request body metadata: `metadata.conversation_key`
  - Request body: `conversation.id`
  - Request body: `user`
  - Request body: `previous_response_id` (mapped to internal conversation key)
  - Header: `x-conversation-key` (or `x-thread-key`)

### Chat Completions (Compatibility)
- **POST** `/v1/chat/completions`
- OpenAI-compatible chat completions endpoint
- Supports: messages, model, temperature, max_tokens, stream, tools
- Optional conversation continuity keys:
  - Request body: `conversation_key`
  - Request body metadata: `metadata.conversation_key`
  - Header: `x-conversation-key` (or `x-thread-key`)

## Troubleshooting

### Common Issues

**Connection Refused:**
```bash
# Check if proxy is running
curl http://localhost:8080/health
```

**Authentication Errors:**
```bash
# Verify auth.json exists and has valid tokens
cat ~/.codex/auth.json | jq .
```

**Backend Errors:**
```bash
# Check proxy logs for detailed error messages
cargo run
```

### Structured Logging

```bash
# Run proxy (structured JSON logs are enabled by default)
cargo run -- --port 8080

# Test Responses endpoint
curl -v -X POST http://localhost:8080/v1/responses \
  -H "Content-Type: application/json" \
  -d '{"model": "gpt-5", "input": "Test"}'
```

## Development

### Building

```bash
cargo build
cargo test
cargo clippy
cargo fmt
```

### Live E2E

Run the live `codex app-server` startup check (requires installed `codex` in `PATH`):

```bash
cargo test live_app_server_thread_start_reports_required_mcp_startup_failure -- --ignored
```

### Adding Features

The proxy is designed to be extensible:

- **New endpoints**: Add routes in `main.rs`
- **Format conversion**: Modify conversion functions
- **Authentication**: Extend `AuthData` structure
- **Streaming**: Add SSE support for real-time responses

## Support and Security

- For general questions and bug reports, follow `SUPPORT.md`.
- For vulnerabilities, do not open a public issue. Follow `SECURITY.md`.

## License

This project is licensed under the Apache License 2.0. See `LICENSE`.
