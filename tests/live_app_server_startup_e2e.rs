use anyhow::{anyhow, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};
use tokio::process::{Child, Command};
use tokio::time::sleep;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use uuid::Uuid;

type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

#[tokio::test]
#[ignore = "live e2e: requires installed `codex` CLI in PATH"]
async fn live_app_server_thread_start_reports_required_mcp_startup_failure() -> Result<()> {
    ensure_codex_installed().await?;

    let codex_home = create_temp_codex_home()?;
    write_config_with_required_broken_mcp(&codex_home)?;
    let ws_url = format!("ws://127.0.0.1:{}", pick_free_port()?);

    let mut child = spawn_codex_app_server(&ws_url, &codex_home).await?;

    let test_result = async {
        let mut ws = connect_ws_with_retry(&ws_url, Duration::from_secs(15)).await?;

        send_ws_json(
            &mut ws,
            json!({
                "id": 1,
                "method": "initialize",
                "params": {
                    "clientInfo": {
                        "name": "codex-responses-proxy-live-e2e",
                        "version": env!("CARGO_PKG_VERSION")
                    },
                    "capabilities": {
                        "experimentalApi": true
                    }
                }
            }),
        )
        .await?;
        let _ = wait_for_rpc_response(&mut ws, 1).await?;

        send_ws_json(&mut ws, json!({ "method": "initialized" })).await?;

        send_ws_json(
            &mut ws,
            json!({
                "id": 2,
                "method": "thread/start",
                "params": {}
            }),
        )
        .await?;

        let response = wait_for_rpc_response(&mut ws, 2).await?;
        let message = response
            .pointer("/error/message")
            .and_then(Value::as_str)
            .unwrap_or_default();

        if message.is_empty() {
            return Err(anyhow!(
                "expected thread/start to fail with MCP startup error, got: {}",
                response
            ));
        }

        if !message.contains("required MCP servers failed to initialize") {
            return Err(anyhow!(
                "unexpected thread/start error message: {}",
                message
            ));
        }
        if !message.contains("required_broken") {
            return Err(anyhow!(
                "expected failing server name in error message: {}",
                message
            ));
        }

        let _ = ws.close(None).await;
        Ok(())
    }
    .await;

    terminate_child(&mut child).await;
    let _ = tokio::fs::remove_dir_all(&codex_home).await;

    test_result
}

#[tokio::test]
#[ignore = "live e2e: requires installed `codex` CLI in PATH"]
async fn live_app_server_thread_start_succeeds_with_mock_provider_config() -> Result<()> {
    ensure_codex_installed().await?;

    let codex_home = create_temp_codex_home()?;
    write_config_with_mock_provider_only(&codex_home)?;
    let ws_url = format!("ws://127.0.0.1:{}", pick_free_port()?);

    let mut child = spawn_codex_app_server(&ws_url, &codex_home).await?;

    let test_result = async {
        let mut ws = connect_ws_with_retry(&ws_url, Duration::from_secs(15)).await?;

        send_ws_json(
            &mut ws,
            json!({
                "id": 1,
                "method": "initialize",
                "params": {
                    "clientInfo": {
                        "name": "codex-responses-proxy-live-e2e",
                        "version": env!("CARGO_PKG_VERSION")
                    },
                    "capabilities": {
                        "experimentalApi": true
                    }
                }
            }),
        )
        .await?;
        let _ = wait_for_rpc_response(&mut ws, 1).await?;

        send_ws_json(&mut ws, json!({ "method": "initialized" })).await?;

        send_ws_json(
            &mut ws,
            json!({
                "id": 2,
                "method": "thread/start",
                "params": {}
            }),
        )
        .await?;

        let response = wait_for_rpc_response(&mut ws, 2).await?;
        let thread_id = response
            .pointer("/result/thread/id")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if thread_id.trim().is_empty() {
            return Err(anyhow!(
                "expected thread/start success with thread id, got: {}",
                response
            ));
        }

        let _ = ws.close(None).await;
        Ok(())
    }
    .await;

    terminate_child(&mut child).await;
    let _ = tokio::fs::remove_dir_all(&codex_home).await;

    test_result
}

#[tokio::test]
#[ignore = "live e2e: requires installed `codex` CLI in PATH"]
async fn live_app_server_thread_resume_missing_thread_returns_error() -> Result<()> {
    ensure_codex_installed().await?;

    let codex_home = create_temp_codex_home()?;
    write_config_with_mock_provider_only(&codex_home)?;
    let ws_url = format!("ws://127.0.0.1:{}", pick_free_port()?);

    let mut child = spawn_codex_app_server(&ws_url, &codex_home).await?;

    let test_result = async {
        let mut ws = connect_ws_with_retry(&ws_url, Duration::from_secs(15)).await?;

        send_ws_json(
            &mut ws,
            json!({
                "id": 1,
                "method": "initialize",
                "params": {
                    "clientInfo": {
                        "name": "codex-responses-proxy-live-e2e",
                        "version": env!("CARGO_PKG_VERSION")
                    },
                    "capabilities": {
                        "experimentalApi": true
                    }
                }
            }),
        )
        .await?;
        let _ = wait_for_rpc_response(&mut ws, 1).await?;

        send_ws_json(&mut ws, json!({ "method": "initialized" })).await?;

        send_ws_json(
            &mut ws,
            json!({
                "id": 2,
                "method": "thread/resume",
                "params": {
                    "threadId": format!("thread_missing_{}", Uuid::new_v4())
                }
            }),
        )
        .await?;

        let response = wait_for_rpc_response(&mut ws, 2).await?;
        let error_message = response
            .pointer("/error/message")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if error_message.trim().is_empty() {
            return Err(anyhow!(
                "expected thread/resume error for missing thread id, got: {}",
                response
            ));
        }

        let _ = ws.close(None).await;
        Ok(())
    }
    .await;

    terminate_child(&mut child).await;
    let _ = tokio::fs::remove_dir_all(&codex_home).await;

    test_result
}

async fn ensure_codex_installed() -> Result<()> {
    let output = Command::new("codex")
        .arg("--version")
        .output()
        .await
        .context("failed to execute `codex --version`")?;
    if !output.status.success() {
        return Err(anyhow!(
            "`codex --version` failed with status {}",
            output.status
        ));
    }
    Ok(())
}

fn create_temp_codex_home() -> Result<PathBuf> {
    let path =
        std::env::temp_dir().join(format!("codex-responses-proxy-live-e2e-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&path).with_context(|| {
        format!(
            "failed to create temporary CODEX_HOME at {}",
            path.display()
        )
    })?;
    Ok(path)
}

fn write_config_with_required_broken_mcp(codex_home: &Path) -> Result<()> {
    let config = r#"
model = "mock-model"
approval_policy = "never"
sandbox_mode = "read-only"

model_provider = "mock_provider"

[model_providers.mock_provider]
name = "Mock provider for test"
base_url = "http://127.0.0.1:65535/v1"
wire_api = "responses"
request_max_retries = 0
stream_max_retries = 0

[mcp_servers.required_broken]
command = "codex-definitely-not-a-real-binary"
required = true
"#;
    std::fs::write(codex_home.join("config.toml"), config)
        .context("failed to write config.toml")?;
    Ok(())
}

fn write_config_with_mock_provider_only(codex_home: &Path) -> Result<()> {
    let config = r#"
model = "mock-model"
approval_policy = "never"
sandbox_mode = "read-only"

model_provider = "mock_provider"

[model_providers.mock_provider]
name = "Mock provider for test"
base_url = "http://127.0.0.1:65535/v1"
wire_api = "responses"
request_max_retries = 0
stream_max_retries = 0
"#;
    std::fs::write(codex_home.join("config.toml"), config)
        .context("failed to write config.toml")?;
    Ok(())
}

fn pick_free_port() -> Result<u16> {
    let listener = std::net::TcpListener::bind("127.0.0.1:0")
        .context("binding ephemeral local port for websocket listen URL")?;
    Ok(listener.local_addr()?.port())
}

async fn spawn_codex_app_server(ws_url: &str, codex_home: &Path) -> Result<Child> {
    Command::new("codex")
        .arg("app-server")
        .arg("--listen")
        .arg(ws_url)
        .env("CODEX_HOME", codex_home)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("failed to spawn `codex app-server --listen {ws_url}`"))
}

async fn connect_ws_with_retry(ws_url: &str, timeout: Duration) -> Result<WsStream> {
    let deadline = Instant::now() + timeout;
    loop {
        match connect_async(ws_url).await {
            Ok((ws, _)) => return Ok(ws),
            Err(err) => {
                if Instant::now() >= deadline {
                    return Err(anyhow!("timed out connecting to {ws_url}: {err}"));
                }
                sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

async fn send_ws_json(ws: &mut WsStream, value: Value) -> Result<()> {
    ws.send(Message::Text(value.to_string().into()))
        .await
        .context("sending websocket json")
}

async fn read_ws_json(ws: &mut WsStream) -> Result<Value> {
    loop {
        let frame = ws
            .next()
            .await
            .ok_or_else(|| anyhow!("websocket stream closed"))?;
        match frame {
            Ok(Message::Text(text)) => {
                return serde_json::from_str(&text).context("parsing websocket text frame as json");
            }
            Ok(Message::Binary(bytes)) => {
                return serde_json::from_slice(&bytes)
                    .context("parsing websocket binary frame as json");
            }
            Ok(Message::Ping(payload)) => {
                ws.send(Message::Pong(payload))
                    .await
                    .context("replying to websocket ping")?;
            }
            Ok(Message::Pong(_)) => {}
            Ok(Message::Close(frame)) => {
                return Err(anyhow!("websocket closed: {:?}", frame));
            }
            Ok(_) => {}
            Err(err) => return Err(anyhow!(err).context("reading websocket frame")),
        }
    }
}

async fn wait_for_rpc_response(ws: &mut WsStream, request_id: i64) -> Result<Value> {
    loop {
        let msg = read_ws_json(ws).await?;

        if msg.get("id").and_then(Value::as_i64) == Some(request_id) {
            return Ok(msg);
        }

        let is_server_request = msg.get("id").is_some()
            && msg
                .get("method")
                .and_then(Value::as_str)
                .is_some_and(|m| !m.is_empty());
        if is_server_request {
            let id = msg.get("id").cloned().unwrap_or(Value::Null);
            send_ws_json(
                ws,
                json!({
                    "id": id,
                    "error": {
                        "code": -32601,
                        "message": "unsupported server request in live e2e test"
                    }
                }),
            )
            .await?;
        }
    }
}

async fn terminate_child(child: &mut Child) {
    let _ = child.kill().await;
    let _ = child.wait().await;
}
