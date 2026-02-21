use anyhow::{anyhow, Context, Result};
use clap::{Parser, ValueEnum};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, VecDeque};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::time::{sleep, Instant};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use uuid::Uuid;
use warp::{Filter, Reply};

#[path = "app/error_response.rs"]
mod error_response;
#[path = "app/request_context.rs"]
mod request_context;
#[path = "app/responses_defaults.rs"]
mod responses_defaults;
#[path = "app/streaming.rs"]
mod streaming;
#[path = "app/usecase.rs"]
mod usecase;
#[path = "app/validation.rs"]
mod validation;

use error_response::{
    json_error_response, openai_error_payload, parse_previous_response_not_found_error,
    previous_response_not_found_error, previous_response_not_found_message,
};
use request_context::{
    extract_conversation_key, extract_request_id, extract_responses_conversation_key, log_request,
    log_structured,
};
use responses_defaults::{
    normalize_responses_metadata, normalize_responses_reasoning, normalize_responses_text,
    normalize_responses_tool_choice,
};
use streaming::{
    build_live_sse_response, build_sse_done_chunk, build_sse_from_chat_response,
    send_responses_stream_event,
};
use validation::{
    responses_error_from_openai_error_payload, responses_request_validation_code,
    validate_responses_create_request, validate_responses_response_shape,
};

include!("app/domain.rs");
include!("app/infra.rs");

impl ProxyServer {
    async fn new(config: ProxyServerConfig) -> Result<Self> {
        let auth_path = expand_home_path(&config.auth_path)?;

        let auth_content = tokio::fs::read_to_string(&auth_path)
            .await
            .context("Failed to read auth.json")?;

        let auth_data: AuthData =
            serde_json::from_str(&auth_content).context("Failed to parse auth.json")?;

        let app_server_url = config
            .app_server_url
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());

        let managed_app_server_url = config.managed_app_server_url.trim().to_string();
        if managed_app_server_url.is_empty() {
            return Err(anyhow!("managed app-server url must not be empty"));
        }

        let app_server_cwd = config
            .app_server_cwd
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty())
            .map(|path| resolve_working_directory(&path))
            .transpose()?;

        let app_server = if let Some(url) = app_server_url {
            AppServerEndpoint::External { url }
        } else {
            let managed = ManagedAppServer::start(
                config.codex_bin,
                managed_app_server_url,
                app_server_cwd.clone(),
                config.app_server_sandbox_mode,
            )
            .await?;
            AppServerEndpoint::Managed {
                server: Arc::new(managed),
            }
        };

        let safe_concurrency = config.max_concurrency.max(1);
        let safe_thread_history = config.max_thread_history.max(1);
        let timeout_secs = config.request_timeout_secs.max(1);

        let server = Self {
            auth_data: Arc::new(Mutex::new(auth_data)),
            auth_path,
            app_server,
            app_server_cwd,
            app_server_sandbox_mode: config.app_server_sandbox_mode,
            thread_sessions: Arc::new(Mutex::new(BoundedHistoryMap::new(safe_thread_history))),
            response_conversation_keys: Arc::new(Mutex::new(BoundedHistoryMap::new(
                safe_thread_history,
            ))),
            warmup_connection: Arc::new(Mutex::new(None)),
            request_semaphore: Arc::new(Semaphore::new(safe_concurrency)),
            max_concurrency: safe_concurrency,
            max_thread_history: safe_thread_history,
            request_timeout: Duration::from_secs(timeout_secs),
            warmup_model: config.warmup_model.trim().to_string(),
        };

        if matches!(server.app_server, AppServerEndpoint::Managed { .. })
            && !server.warmup_model.is_empty()
        {
            let started = Instant::now();
            log_structured(
                "info",
                "app_server.warmup.started",
                None,
                json!({ "model": server.warmup_model }),
            );
            match server.warmup_managed_app_server().await {
                Ok(connection) => {
                    let thread_id = connection.thread_id.clone();
                    {
                        let mut slot = server.warmup_connection.lock().await;
                        *slot = Some(connection);
                    }
                    log_structured(
                        "info",
                        "app_server.warmup.completed",
                        None,
                        json!({
                            "thread_id": thread_id,
                            "latency_ms": started.elapsed().as_millis()
                        }),
                    );
                }
                Err(err) => {
                    log_structured(
                        "warn",
                        "app_server.warmup.failed",
                        None,
                        json!({
                            "error": err.to_string(),
                            "latency_ms": started.elapsed().as_millis()
                        }),
                    );
                }
            }
        }

        Ok(server)
    }

    async fn shutdown(&self) {
        let warmup_connection = {
            let mut warmup_slot = self.warmup_connection.lock().await;
            warmup_slot.take()
        };
        if let Some(mut connection) = warmup_connection {
            let _ = connection.ws.close(None).await;
        }

        if let AppServerEndpoint::Managed { server } = &self.app_server {
            if let Err(err) = server.shutdown().await {
                log_structured(
                    "error",
                    "app_server.shutdown.failed",
                    None,
                    json!({
                        "error": err.to_string()
                    }),
                );
            }
        }
    }

    fn app_server_description(&self) -> String {
        match &self.app_server {
            AppServerEndpoint::External { url } => {
                format!("Using existing codex app-server at {}", url)
            }
            AppServerEndpoint::Managed { server } => format!(
                "Started managed codex app-server at {}{} (approval_policy=never, sandbox_mode={})",
                server.ws_url,
                server
                    .app_server_cwd
                    .as_ref()
                    .map(|cwd| format!(", cwd={}", cwd.display()))
                    .unwrap_or_default(),
                server.app_server_sandbox_mode.as_config_value(),
            ),
        }
    }

    async fn proxy_request(
        &self,
        chat_req: ChatCompletionsRequest,
        ctx: RequestContext,
    ) -> Result<ChatCompletionsResponse> {
        self.proxy_request_with_responses_stream(chat_req, ctx, None)
            .await
    }

    async fn proxy_request_with_responses_stream(
        &self,
        chat_req: ChatCompletionsRequest,
        ctx: RequestContext,
        responses_stream_sender: Option<mpsc::UnboundedSender<String>>,
    ) -> Result<ChatCompletionsResponse> {
        let _permit = self
            .request_semaphore
            .acquire()
            .await
            .context("failed to acquire request semaphore")?;

        let start = Instant::now();
        let model = chat_req.model.clone();
        let app_result = tokio::time::timeout(
            self.request_timeout,
            self.run_app_server_turn(&chat_req, &ctx, responses_stream_sender),
        )
        .await
        .map_err(|_| {
            anyhow!(
                "request timed out after {}s",
                self.request_timeout.as_secs()
            )
        })??;

        let message = if let Some(tool_call) = app_result.tool_call {
            ChatResponseMessage {
                role: "assistant".to_string(),
                content: None,
                tool_calls: Some(vec![tool_call]),
            }
        } else {
            ChatResponseMessage {
                role: "assistant".to_string(),
                content: Some(app_result.content),
                tool_calls: None,
            }
        };

        let response = ChatCompletionsResponse {
            id: format!("chatcmpl-{}", Uuid::new_v4()),
            object: "chat.completion".to_string(),
            created: chrono::Utc::now().timestamp(),
            model: model.clone(),
            choices: vec![Choice {
                index: 0,
                message,
                finish_reason: Some(app_result.finish_reason),
            }],
            usage: app_result.usage,
        };

        log_structured(
            "info",
            "request.completed",
            Some(&ctx.request_id),
            json!({
                "model": model,
                "conversation_key": ctx.conversation_key,
                "latency_ms": start.elapsed().as_millis()
            }),
        );

        Ok(response)
    }

    async fn run_app_server_turn(
        &self,
        chat_req: &ChatCompletionsRequest,
        ctx: &RequestContext,
        responses_stream_sender: Option<mpsc::UnboundedSender<String>>,
    ) -> Result<AppServerTurnResult> {
        let (developer_instructions, turn_prompt) = Self::build_turn_input(&chat_req.messages);
        let turn_input = TurnInput {
            developer_instructions: &developer_instructions,
            turn_prompt: &turn_prompt,
        };

        if ctx.conversation_key.is_none() {
            if let Some(mut connection) = self
                .take_compatible_warmup_connection(chat_req, turn_input.developer_instructions)
                .await
            {
                let result = self
                    .run_warmup_turn_on_socket(
                        &mut connection.ws,
                        &connection.thread_id,
                        chat_req,
                        ctx,
                        turn_input,
                        responses_stream_sender.clone(),
                    )
                    .await;
                let _ = connection.ws.close(None).await;
                return result;
            }
        }

        let mut ws = self.connect_app_server().await?;
        let result = self
            .run_app_server_turn_on_socket(
                &mut ws,
                chat_req,
                ctx,
                turn_input,
                responses_stream_sender.clone(),
            )
            .await;

        let _ = ws.close(None).await;
        result
    }

    async fn get_thread_session(&self, key: &str) -> Option<ThreadSession> {
        self.thread_sessions.lock().await.get_cloned(key)
    }

    async fn set_thread_session(&self, key: String, session: ThreadSession) {
        self.thread_sessions.lock().await.insert(key, session);
    }

    async fn get_response_conversation_key(&self, response_id: &str) -> Option<String> {
        self.response_conversation_keys
            .lock()
            .await
            .get_cloned(response_id)
    }

    async fn set_response_conversation_key(&self, response_id: String, conversation_key: String) {
        self.response_conversation_keys
            .lock()
            .await
            .insert(response_id, conversation_key);
    }

    async fn refresh_auth_data(&self) -> Result<()> {
        let auth_content = tokio::fs::read_to_string(&self.auth_path)
            .await
            .context("failed to read auth.json during refresh")?;
        let new_auth: AuthData = serde_json::from_str(&auth_content)
            .context("failed to parse auth.json during refresh")?;
        *self.auth_data.lock().await = new_auth;
        Ok(())
    }

    async fn health_report(&self) -> Value {
        let (app_server_mode, app_server_status) = match &self.app_server {
            AppServerEndpoint::External { url } => {
                let reachable = match connect_ws_with_retry(url, Duration::from_millis(500)).await {
                    Ok(mut ws) => {
                        let _ = ws.close(None).await;
                        true
                    }
                    Err(_) => false,
                };
                (
                    "external",
                    json!({
                        "url": url,
                        "reachable": reachable
                    }),
                )
            }
            AppServerEndpoint::Managed { server } => {
                let status = server.status_snapshot().await;
                let reachable =
                    match connect_ws_with_retry(&status.ws_url, Duration::from_millis(500)).await {
                        Ok(mut ws) => {
                            let _ = ws.close(None).await;
                            true
                        }
                        Err(_) => false,
                    };
                (
                    "managed",
                    json!({
                        "url": status.ws_url,
                        "pid": status.pid,
                        "running": status.running,
                        "restart_count": status.restart_count,
                        "last_restart_at": status.last_restart_at,
                        "reachable": reachable
                    }),
                )
            }
        };

        let thread_count = self.thread_sessions.lock().await.len();
        let response_map_count = self.response_conversation_keys.lock().await.len();
        json!({
            "status": "ok",
            "service": "codex-responses-proxy",
            "app_server_mode": app_server_mode,
            "app_server": app_server_status,
            "thread_session_count": thread_count,
            "response_conversation_count": response_map_count,
            "max_concurrency": self.max_concurrency,
            "max_thread_history": self.max_thread_history,
            "available_permits": self.request_semaphore.available_permits(),
            "request_timeout_secs": self.request_timeout.as_secs()
        })
    }

    fn build_turn_input(messages: &[ChatMessage]) -> (String, String) {
        let mut system_parts: Vec<String> = Vec::new();
        let mut parts: Vec<String> = Vec::new();

        for msg in messages {
            let content = Self::extract_text_content(&msg.content);
            match msg.role.as_str() {
                "system" => {
                    if !content.trim().is_empty() {
                        system_parts.push(content.trim().to_string());
                    }
                }
                "user" => {
                    if !content.trim().is_empty() {
                        parts.push(content.trim().to_string());
                    }
                }
                "assistant" => {
                    if !content.trim().is_empty() {
                        parts.push(format!("Assistant: {}", content.trim()));
                    }
                    if let Some(tool_calls) = &msg.tool_calls {
                        for tool_call in tool_calls {
                            parts.push(Self::format_assistant_tool_call(tool_call));
                        }
                    }
                }
                "tool" => {
                    if !content.trim().is_empty() {
                        let tool_call_id = msg
                            .tool_call_id
                            .as_deref()
                            .filter(|v| !v.trim().is_empty())
                            .unwrap_or("unknown_tool_call");
                        parts.push(format!(
                            "[Tool Result for {}]: {}",
                            tool_call_id,
                            content.trim()
                        ));
                    }
                }
                _ => {
                    if !content.trim().is_empty() {
                        parts.push(format!("{}: {}", msg.role, content.trim()));
                    }
                }
            }
        }

        let system_prompt = system_parts.join("\n\n").trim().to_string();
        if parts.len() == 1 && system_prompt.is_empty() {
            return (system_prompt, parts[0].clone());
        }

        let turn_prompt = parts.join("\n").trim().to_string();
        if turn_prompt.is_empty() {
            (system_prompt, "Continue.".to_string())
        } else {
            (system_prompt, turn_prompt)
        }
    }

    fn extract_text_content(content: &Value) -> String {
        match content {
            Value::String(s) => s.clone(),
            Value::Array(arr) => arr
                .iter()
                .filter_map(|v| {
                    if let Some(obj) = v.as_object() {
                        obj.get("text").and_then(Value::as_str).map(str::to_string)
                    } else {
                        v.as_str().map(str::to_string)
                    }
                })
                .collect::<Vec<String>>()
                .join(" "),
            Value::Null => String::new(),
            _ => content.to_string(),
        }
    }

    fn format_assistant_tool_call(tool_call: &IncomingToolCall) -> String {
        let name = tool_call
            .function
            .as_ref()
            .and_then(|f| f.name.clone())
            .filter(|n| !n.trim().is_empty())
            .unwrap_or_else(|| "unknown_tool".to_string());

        let args = tool_call
            .function
            .as_ref()
            .and_then(|f| f.arguments.clone())
            .filter(|a| !a.trim().is_empty())
            .unwrap_or_else(|| "{}".to_string());

        if let Some(id) = tool_call.id.as_deref().filter(|id| !id.trim().is_empty()) {
            format!(
                "Assistant called tool {} (id={}) with arguments: {}",
                name, id, args
            )
        } else {
            format!("Assistant called tool {} with arguments: {}", name, args)
        }
    }

    fn translate_tools_for_app_server(tools: Option<&[Value]>) -> Vec<Value> {
        let Some(tools) = tools else {
            return Vec::new();
        };

        let mut out = Vec::new();
        for tool in tools {
            let tool_type = tool.get("type").and_then(Value::as_str).unwrap_or_default();
            if tool_type != "function" {
                continue;
            }

            let Some(function) = tool.get("function") else {
                continue;
            };
            let Some(name) = function.get("name").and_then(Value::as_str) else {
                continue;
            };

            let description = function
                .get("description")
                .and_then(Value::as_str)
                .filter(|v| !v.trim().is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| format!("Tool: {}", name));

            let input_schema = function
                .get("parameters")
                .cloned()
                .unwrap_or_else(|| json!({"type": "object", "properties": {}}));

            out.push(json!({
                "name": name,
                "description": description,
                "inputSchema": input_schema,
            }));
        }

        out
    }

    fn responses_to_chat_request(req: &ResponsesCreateRequest) -> ChatCompletionsRequest {
        let mut messages = Vec::new();
        if let Some(instructions) = req.instructions.as_ref() {
            let text = Self::extract_text_from_responses_value(instructions);
            if !text.trim().is_empty() {
                messages.push(ChatMessage {
                    role: "system".to_string(),
                    content: json!(text),
                    tool_calls: None,
                    tool_call_id: None,
                });
            }
        }

        messages.extend(Self::responses_input_to_messages(req.input.as_ref()));

        ChatCompletionsRequest {
            model: req.model.clone(),
            messages,
            temperature: req.temperature,
            max_tokens: req.max_output_tokens,
            stream: req.stream,
            tools: Self::normalize_responses_tools(req.tools.as_deref()),
            tool_choice: req.tool_choice.clone(),
            user: req.user.clone(),
            metadata: req.metadata.clone(),
            conversation_key: req.conversation_key.clone(),
        }
    }

    fn build_responses_response_context(
        req: &ResponsesCreateRequest,
        stream_output_item_id: Option<String>,
    ) -> ResponsesResponseContext {
        let instructions = req
            .instructions
            .as_ref()
            .map(Self::extract_text_from_responses_value)
            .filter(|v| !v.trim().is_empty());
        let truncation = req
            .truncation
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .unwrap_or("disabled")
            .to_string();

        ResponsesResponseContext {
            instructions,
            max_output_tokens: req.max_output_tokens,
            tools: Self::normalize_responses_tools_for_response(req.tools.as_deref()),
            tool_choice: normalize_responses_tool_choice(req.tool_choice.as_ref()),
            parallel_tool_calls: req.parallel_tool_calls.unwrap_or(true),
            previous_response_id: req.previous_response_id.clone(),
            reasoning: normalize_responses_reasoning(req.reasoning.as_ref()),
            store: req.store.unwrap_or(true),
            temperature: req.temperature,
            text: normalize_responses_text(req.text.as_ref()),
            top_p: req.top_p,
            truncation,
            user: req.user.clone(),
            metadata: normalize_responses_metadata(req.metadata.as_ref()),
            created_at: None,
            stream_output_item_id,
        }
    }

    fn responses_input_to_messages(input: Option<&Value>) -> Vec<ChatMessage> {
        let Some(input) = input else {
            return Vec::new();
        };

        match input {
            Value::Array(items) => items
                .iter()
                .filter_map(Self::responses_input_item_to_message)
                .collect(),
            _ => Self::responses_input_item_to_message(input)
                .map(|m| vec![m])
                .unwrap_or_default(),
        }
    }

    fn responses_input_item_to_message(item: &Value) -> Option<ChatMessage> {
        if let Some(obj) = item.as_object() {
            if let Some(item_type) = obj.get("type").and_then(Value::as_str) {
                match item_type {
                    "function_call" => {
                        let name = obj.get("name").and_then(Value::as_str)?.to_string();
                        let arguments = obj
                            .get("arguments")
                            .and_then(Value::as_str)
                            .map(str::to_string)
                            .unwrap_or_else(|| "{}".to_string());
                        let call_id = obj
                            .get("call_id")
                            .or_else(|| obj.get("id"))
                            .and_then(Value::as_str)
                            .map(str::to_string);
                        return Some(ChatMessage {
                            role: "assistant".to_string(),
                            content: Value::Null,
                            tool_calls: Some(vec![IncomingToolCall {
                                id: call_id,
                                _tool_type: Some("function".to_string()),
                                function: Some(IncomingToolFunction {
                                    name: Some(name),
                                    arguments: Some(arguments),
                                }),
                            }]),
                            tool_call_id: None,
                        });
                    }
                    "function_call_output" => {
                        let output = match obj.get("output") {
                            Some(Value::String(text)) => text.clone(),
                            Some(Value::Null) | None => String::new(),
                            Some(other) => other.to_string(),
                        };
                        let call_id = obj
                            .get("call_id")
                            .and_then(Value::as_str)
                            .map(str::to_string);
                        return Some(ChatMessage {
                            role: "tool".to_string(),
                            content: json!(output),
                            tool_calls: None,
                            tool_call_id: call_id,
                        });
                    }
                    "message" => {
                        let role = obj
                            .get("role")
                            .and_then(Value::as_str)
                            .map(Self::normalize_responses_input_role)
                            .unwrap_or_else(|| "user".to_string());
                        let content_source = obj.get("content").unwrap_or(item);
                        let text = Self::extract_text_from_responses_value(content_source);
                        if text.trim().is_empty() {
                            return None;
                        }
                        return Some(ChatMessage {
                            role,
                            content: json!(text),
                            tool_calls: None,
                            tool_call_id: None,
                        });
                    }
                    _ => {}
                }
            }

            let role = obj
                .get("role")
                .and_then(Value::as_str)
                .map(Self::normalize_responses_input_role)
                .unwrap_or_else(|| "user".to_string());
            let content_source = obj.get("content").unwrap_or(item);
            let text = Self::extract_text_from_responses_value(content_source);
            if text.trim().is_empty() {
                return None;
            }
            return Some(ChatMessage {
                role,
                content: json!(text),
                tool_calls: None,
                tool_call_id: None,
            });
        }

        let text = Self::extract_text_from_responses_value(item);
        if text.trim().is_empty() {
            return None;
        }
        Some(ChatMessage {
            role: "user".to_string(),
            content: json!(text),
            tool_calls: None,
            tool_call_id: None,
        })
    }

    fn normalize_responses_input_role(role: &str) -> String {
        match role.trim() {
            "developer" => "system".to_string(),
            "system" => "system".to_string(),
            "assistant" => "assistant".to_string(),
            "tool" => "tool".to_string(),
            _ => "user".to_string(),
        }
    }

    fn extract_text_from_responses_value(value: &Value) -> String {
        match value {
            Value::String(s) => s.clone(),
            Value::Array(items) => items
                .iter()
                .map(Self::extract_text_from_responses_value)
                .filter(|s| !s.trim().is_empty())
                .collect::<Vec<String>>()
                .join(" "),
            Value::Object(map) => {
                if let Some(text) = map.get("text").and_then(Value::as_str) {
                    return text.to_string();
                }
                if let Some(refusal) = map.get("refusal").and_then(Value::as_str) {
                    return refusal.to_string();
                }
                if let Some(content) = map.get("content") {
                    return Self::extract_text_from_responses_value(content);
                }
                String::new()
            }
            _ => String::new(),
        }
    }

    fn normalize_responses_tools(tools: Option<&[Value]>) -> Option<Vec<Value>> {
        let tools = tools?;

        let mut normalized = Vec::new();
        for tool in tools {
            let Some(obj) = tool.as_object() else {
                continue;
            };
            let tool_type = obj.get("type").and_then(Value::as_str).unwrap_or_default();
            if tool_type != "function" {
                continue;
            }

            if obj.get("function").is_some() {
                normalized.push(tool.clone());
                continue;
            }

            let Some(name) = obj.get("name").and_then(Value::as_str) else {
                continue;
            };

            let mut function = Map::new();
            function.insert("name".to_string(), json!(name));
            if let Some(description) = obj.get("description").and_then(Value::as_str) {
                function.insert("description".to_string(), json!(description));
            }
            if let Some(parameters) = obj.get("parameters") {
                function.insert("parameters".to_string(), parameters.clone());
            }

            normalized.push(json!({
                "type": "function",
                "function": Value::Object(function)
            }));
        }

        if normalized.is_empty() {
            None
        } else {
            Some(normalized)
        }
    }

    fn normalize_responses_tools_for_response(tools: Option<&[Value]>) -> Vec<Value> {
        let Some(tools) = tools else {
            return Vec::new();
        };

        let mut normalized = Vec::new();
        for tool in tools {
            let Some(obj) = tool.as_object() else {
                continue;
            };
            let tool_type = obj.get("type").and_then(Value::as_str).unwrap_or_default();

            if tool_type == "function" {
                if let Some(name) = obj.get("name").and_then(Value::as_str) {
                    normalized.push(json!({
                        "type": "function",
                        "name": name,
                        "description": obj.get("description").cloned().unwrap_or(Value::Null),
                        "parameters": obj
                            .get("parameters")
                            .cloned()
                            .unwrap_or_else(|| json!({"type":"object","properties":{}})),
                        "strict": obj.get("strict").cloned().unwrap_or(Value::Bool(true))
                    }));
                    continue;
                }

                if let Some(function) = obj.get("function").and_then(Value::as_object) {
                    if let Some(name) = function.get("name").and_then(Value::as_str) {
                        normalized.push(json!({
                            "type": "function",
                            "name": name,
                            "description": function.get("description").cloned().unwrap_or(Value::Null),
                            "parameters": function
                                .get("parameters")
                                .cloned()
                                .unwrap_or_else(|| json!({"type":"object","properties":{}})),
                            "strict": obj.get("strict").cloned().unwrap_or(Value::Bool(true))
                        }));
                    }
                    continue;
                }

                continue;
            }

            if !tool_type.is_empty() {
                normalized.push(tool.clone());
            }
        }

        normalized
    }

    fn responses_in_progress_response(
        response_id: String,
        created_at: i64,
        model: String,
        ctx: &ResponsesResponseContext,
    ) -> ResponsesCreateResponse {
        ResponsesCreateResponse {
            id: response_id,
            object: "response".to_string(),
            created_at,
            status: "in_progress".to_string(),
            error: None,
            incomplete_details: None,
            instructions: ctx.instructions.clone(),
            max_output_tokens: ctx.max_output_tokens,
            model,
            output: Vec::new(),
            output_text: None,
            parallel_tool_calls: ctx.parallel_tool_calls,
            previous_response_id: ctx.previous_response_id.clone(),
            reasoning: ctx.reasoning.clone(),
            store: ctx.store,
            temperature: ctx.temperature,
            text: ctx.text.clone(),
            tool_choice: ctx.tool_choice.clone(),
            tools: ctx.tools.clone(),
            top_p: ctx.top_p,
            truncation: ctx.truncation.clone(),
            usage: None,
            user: ctx.user.clone(),
            metadata: ctx.metadata.clone(),
        }
    }

    fn responses_failed_response(
        response_id: String,
        model: String,
        ctx: &ResponsesResponseContext,
        error: ResponsesError,
    ) -> ResponsesCreateResponse {
        ResponsesCreateResponse {
            id: response_id,
            object: "response".to_string(),
            created_at: ctx
                .created_at
                .unwrap_or_else(|| chrono::Utc::now().timestamp()),
            status: "failed".to_string(),
            error: Some(error),
            incomplete_details: None,
            instructions: ctx.instructions.clone(),
            max_output_tokens: ctx.max_output_tokens,
            model,
            output: Vec::new(),
            output_text: None,
            parallel_tool_calls: ctx.parallel_tool_calls,
            previous_response_id: ctx.previous_response_id.clone(),
            reasoning: ctx.reasoning.clone(),
            store: ctx.store,
            temperature: ctx.temperature,
            text: ctx.text.clone(),
            tool_choice: ctx.tool_choice.clone(),
            tools: ctx.tools.clone(),
            top_p: ctx.top_p,
            truncation: ctx.truncation.clone(),
            usage: None,
            user: ctx.user.clone(),
            metadata: ctx.metadata.clone(),
        }
    }

    fn chat_to_responses_response(
        chat_response: &ChatCompletionsResponse,
        response_id: String,
        ctx: &ResponsesResponseContext,
    ) -> ResponsesCreateResponse {
        let mut output_text: Option<String> = None;
        let mut output_items = Vec::new();

        if let Some(choice) = chat_response.choices.first() {
            if let Some(tool_calls) = &choice.message.tool_calls {
                for tool_call in tool_calls {
                    output_items.push(json!({
                        "id": format!("fc_{}", Uuid::new_v4()),
                        "type": "function_call",
                        "status": "completed",
                        "name": tool_call.function.name.clone(),
                        "arguments": tool_call.function.arguments.clone(),
                        "call_id": tool_call.id.clone()
                    }));
                }
            } else {
                let text = choice.message.content.clone().unwrap_or_default();
                if !text.is_empty() {
                    output_text = Some(text.clone());
                }
                let message_id = ctx
                    .stream_output_item_id
                    .clone()
                    .unwrap_or_else(|| format!("msg_{}", Uuid::new_v4()));
                output_items.push(json!({
                    "id": message_id,
                    "type": "message",
                    "status": "completed",
                    "role": "assistant",
                    "content": [{
                        "type": "output_text",
                        "text": text,
                        "annotations": []
                    }]
                }));
            }
        }

        ResponsesCreateResponse {
            id: response_id,
            object: "response".to_string(),
            created_at: ctx.created_at.unwrap_or(chat_response.created),
            status: "completed".to_string(),
            error: None,
            incomplete_details: None,
            instructions: ctx.instructions.clone(),
            max_output_tokens: ctx.max_output_tokens,
            model: chat_response.model.clone(),
            output: output_items,
            output_text,
            parallel_tool_calls: ctx.parallel_tool_calls,
            previous_response_id: ctx.previous_response_id.clone(),
            reasoning: ctx.reasoning.clone(),
            store: ctx.store,
            temperature: ctx.temperature,
            text: ctx.text.clone(),
            tool_choice: ctx.tool_choice.clone(),
            tools: ctx.tools.clone(),
            top_p: ctx.top_p,
            truncation: ctx.truncation.clone(),
            usage: chat_response.usage.as_ref().map(|usage| ResponsesUsage {
                input_tokens: usage.prompt_tokens,
                input_tokens_details: ResponsesInputTokensDetails { cached_tokens: 0 },
                output_tokens: usage.completion_tokens,
                output_tokens_details: ResponsesOutputTokensDetails {
                    reasoning_tokens: 0,
                },
                total_tokens: usage.total_tokens,
            }),
            user: ctx.user.clone(),
            metadata: ctx.metadata.clone(),
        }
    }
}

pub async fn run() -> Result<()> {
    let args = Args::parse();

    log_structured(
        "info",
        "proxy.startup.begin",
        None,
        json!({
            "bind": args.bind.to_string(),
            "port": args.port,
            "auth_path": args.auth_path,
            "managed_app_server_url": args.managed_app_server_url
        }),
    );

    let proxy = ProxyServer::new(ProxyServerConfig {
        auth_path: args.auth_path.clone(),
        app_server_url: args.app_server_url.clone(),
        managed_app_server_url: args.managed_app_server_url.clone(),
        codex_bin: args.codex_bin.clone(),
        app_server_cwd: args.app_server_cwd.clone(),
        app_server_sandbox_mode: args.app_server_sandbox_mode,
        max_concurrency: args.max_concurrency,
        max_thread_history: args.max_thread_history,
        request_timeout_secs: args.request_timeout_secs,
        warmup_model: args.warmup_model.clone(),
    })
    .await?;
    log_structured(
        "info",
        "proxy.startup.auth_loaded",
        None,
        json!({
            "auth_path": args.auth_path
        }),
    );
    log_structured(
        "info",
        "proxy.startup.app_server_ready",
        None,
        json!({
            "description": proxy.app_server_description()
        }),
    );

    let shutdown_proxy = proxy.clone();

    // Health check endpoint (removed unused variable warning)
    let _health = warp::path("health").and(warp::get()).map(|| {
        log_structured("info", "health.requested", None, json!({}));
        warp::reply::json(&json!({
            "status": "ok",
            "service": "codex-responses-proxy"
        }))
    });

    // Multiple endpoints for CLINE compatibility
    let proxy_for_filters = proxy.clone();
    let proxy_filter = warp::any().map(move || proxy_for_filters.clone());

    let _chat_completions_v1 = warp::path!("v1" / "chat" / "completions")
        .and(warp::post())
        .and(warp::header::headers_cloned())
        .and(warp::body::json())
        .and(proxy_filter.clone())
        .and_then(handle_chat_completions);

    let _chat_completions_direct = warp::path("chat")
        .and(warp::path("completions"))
        .and(warp::path::end())
        .and(warp::post())
        .and(warp::header::headers_cloned())
        .and(warp::body::json())
        .and(proxy_filter.clone())
        .and_then(handle_chat_completions);

    // Models endpoints
    let _models_v1 = warp::path!("v1" / "models")
        .and(warp::get())
        .and(warp::header::headers_cloned())
        .and_then(handle_models);

    let _models_direct = warp::path("models")
        .and(warp::get())
        .and(warp::header::headers_cloned())
        .and_then(handle_models);

    // CORS headers - allow all headers to fix CLINE issues
    let cors = warp::cors()
        .allow_any_origin()
        .allow_headers(vec![
            "authorization",
            "content-type",
            "accept",
            "accept-encoding",
            "x-stainless-arch",
            "x-stainless-lang",
            "x-stainless-os",
            "x-stainless-package-version",
            "x-stainless-retry-count",
            "x-stainless-runtime",
            "x-stainless-runtime-version",
            "x-stainless-timeout",
        ])
        .allow_methods(vec!["GET", "POST", "PUT", "DELETE", "OPTIONS"]);

    // Single universal handler
    let universal_handler = warp::any()
        .and(warp::method())
        .and(warp::path::full())
        .and(warp::header::headers_cloned())
        .and(warp::body::bytes())
        .and(proxy_filter.clone())
        .and_then(universal_request_handler);

    let routes = universal_handler.with(cors);

    let base_url = format!("http://{}:{}", args.bind, args.port);
    log_structured(
        "info",
        "proxy.listening",
        None,
        json!({
            "base_url": base_url,
            "health_url": format!("http://{}:{}/health", args.bind, args.port),
            "responses_url": format!("http://{}:{}/v1/responses", args.bind, args.port),
            "chat_completions_url": format!("http://{}:{}/v1/chat/completions", args.bind, args.port),
            "cline_config": {
                "base_url": format!("http://{}:{}", args.bind, args.port),
                "model": "gpt-5",
                "api_key": "any value"
            }
        }),
    );

    let (_addr, server) =
        warp::serve(routes).bind_with_graceful_shutdown((args.bind, args.port), async {
            let _ = tokio::signal::ctrl_c().await;
            log_structured("info", "proxy.shutdown.signal_received", None, json!({}));
        });

    server.await;
    shutdown_proxy.shutdown().await;
    log_structured("info", "proxy.shutdown.completed", None, json!({}));

    Ok(())
}

// Universal handler that routes based on path and method
async fn universal_request_handler(
    method: warp::http::Method,
    path: warp::path::FullPath,
    headers: warp::http::HeaderMap,
    body: bytes::Bytes,
    proxy: ProxyServer,
) -> Result<impl warp::Reply, warp::Rejection> {
    let path_str = path.as_str();
    let request_id = extract_request_id(&headers);

    log_request(&method, path_str, &headers, Some(&request_id));
    log_structured(
        "info",
        "request.received",
        Some(&request_id),
        json!({
            "method": method.as_str(),
            "path": path_str
        }),
    );

    match (method.as_str(), path_str) {
        ("GET", "/health") => Ok(usecase::handle_health(&proxy).await),
        ("GET", "/models") | ("GET", "/v1/models") => Ok(usecase::handle_models()),
        ("POST", "/responses") | ("POST", "/v1/responses") => {
            usecase::handle_responses_create(path_str, headers, body, proxy, request_id).await
        }
        ("POST", "/chat/completions") | ("POST", "/v1/chat/completions") => {
            usecase::handle_chat_completions_create(path_str, headers, body, proxy, request_id)
                .await
        }
        _ => {
            log_structured(
                "warn",
                "route.unmatched",
                Some(&request_id),
                json!({
                    "method": method.as_str(),
                    "path": path_str
                }),
            );
            Ok(
                warp::reply::with_status("Not found", warp::http::StatusCode::NOT_FOUND)
                    .into_response(),
            )
        }
    }
}

async fn handle_models(
    headers: warp::http::HeaderMap,
) -> Result<impl warp::Reply, warp::Rejection> {
    let request_id = extract_request_id(&headers);
    log_request(
        &warp::http::Method::GET,
        "/models",
        &headers,
        Some(&request_id),
    );
    Ok(usecase::handle_models())
}

async fn handle_chat_completions(
    headers: warp::http::HeaderMap,
    req: ChatCompletionsRequest,
    proxy: ProxyServer,
) -> Result<impl warp::Reply, warp::Rejection> {
    let request_id = extract_request_id(&headers);
    log_request(
        &warp::http::Method::POST,
        "/chat/completions",
        &headers,
        Some(&request_id),
    );
    log_structured(
        "info",
        "request.chat_completions.matched",
        Some(&request_id),
        json!({
            "path": "/chat/completions",
            "model": req.model,
            "message_count": req.messages.len()
        }),
    );
    let conversation_key = extract_conversation_key(&req, &headers);

    let request_ctx = RequestContext {
        request_id: request_id.clone(),
        conversation_key: conversation_key.clone(),
        previous_response_id: None,
        require_existing_thread: false,
        responses_stream_output_item_id: None,
        responses_stream_output_item_added: None,
    };

    match proxy.proxy_request(req, request_ctx).await {
        Ok(response) => Ok(warp::reply::json(&response)),
        Err(e) => {
            log_structured(
                "error",
                "request.failed",
                Some(&request_id),
                json!({
                    "conversation_key": conversation_key,
                    "error": e.to_string()
                }),
            );
            Ok(warp::reply::json(&json!({
                "error": {
                    "message": format!("Proxy error: {}", e),
                    "type": "proxy_error",
                    "code": "internal_error"
                }
            })))
        }
    }
}

impl Clone for ProxyServer {
    fn clone(&self) -> Self {
        Self {
            auth_data: Arc::clone(&self.auth_data),
            auth_path: self.auth_path.clone(),
            app_server: match &self.app_server {
                AppServerEndpoint::External { url } => {
                    AppServerEndpoint::External { url: url.clone() }
                }
                AppServerEndpoint::Managed { server } => AppServerEndpoint::Managed {
                    server: Arc::clone(server),
                },
            },
            app_server_cwd: self.app_server_cwd.clone(),
            app_server_sandbox_mode: self.app_server_sandbox_mode,
            thread_sessions: Arc::clone(&self.thread_sessions),
            response_conversation_keys: Arc::clone(&self.response_conversation_keys),
            warmup_connection: Arc::clone(&self.warmup_connection),
            request_semaphore: Arc::clone(&self.request_semaphore),
            max_concurrency: self.max_concurrency,
            max_thread_history: self.max_thread_history,
            request_timeout: self.request_timeout,
            warmup_model: self.warmup_model.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::validation::validate_responses_stream_event_shape;
    use super::*;
    use warp::http::{HeaderMap, HeaderValue};

    fn sample_request() -> ChatCompletionsRequest {
        ChatCompletionsRequest {
            model: "gpt-5".to_string(),
            messages: vec![ChatMessage {
                role: "user".to_string(),
                content: json!("hello"),
                tool_calls: None,
                tool_call_id: None,
            }],
            temperature: None,
            max_tokens: None,
            stream: None,
            tools: None,
            tool_choice: None,
            user: None,
            metadata: None,
            conversation_key: None,
        }
    }

    fn sample_responses_context() -> ResponsesResponseContext {
        ResponsesResponseContext {
            instructions: None,
            max_output_tokens: None,
            tools: vec![],
            tool_choice: json!("auto"),
            parallel_tool_calls: true,
            previous_response_id: None,
            reasoning: json!({
                "effort": Value::Null,
                "summary": Value::Null
            }),
            store: true,
            temperature: Some(1.0),
            text: json!({"format":{"type":"text"}}),
            top_p: Some(1.0),
            truncation: "disabled".to_string(),
            user: None,
            metadata: json!({}),
            created_at: None,
            stream_output_item_id: None,
        }
    }

    #[test]
    fn extract_conversation_key_priority_order() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-conversation-key",
            HeaderValue::from_static("from-header"),
        );

        let mut req = sample_request();
        req.user = Some("from-user".to_string());
        req.metadata = Some(json!({"conversation_key": "from-metadata"}));
        req.conversation_key = Some("from-request".to_string());
        assert_eq!(
            extract_conversation_key(&req, &headers),
            Some("from-request".to_string())
        );

        req.conversation_key = None;
        assert_eq!(
            extract_conversation_key(&req, &headers),
            Some("from-metadata".to_string())
        );

        req.metadata = None;
        assert_eq!(
            extract_conversation_key(&req, &headers),
            Some("from-user".to_string())
        );

        req.user = None;
        assert_eq!(
            extract_conversation_key(&req, &headers),
            Some("from-header".to_string())
        );
    }

    #[test]
    fn parse_usage_uses_sum_when_total_missing() {
        let params = json!({
            "tokenUsage": {
                "last": {
                    "inputTokens": 10,
                    "cachedInputTokens": 3,
                    "outputTokens": 7
                }
            }
        });

        let usage = ProxyServer::parse_usage_from_token_update(&params).expect("usage");
        assert_eq!(usage.prompt_tokens, 13);
        assert_eq!(usage.completion_tokens, 7);
        assert_eq!(usage.total_tokens, 20);
    }

    #[test]
    fn build_turn_input_formats_tool_exchange() {
        let messages = vec![
            ChatMessage {
                role: "system".to_string(),
                content: json!("system-rules"),
                tool_calls: None,
                tool_call_id: None,
            },
            ChatMessage {
                role: "user".to_string(),
                content: json!("first-question"),
                tool_calls: None,
                tool_call_id: None,
            },
            ChatMessage {
                role: "assistant".to_string(),
                content: json!(""),
                tool_calls: Some(vec![IncomingToolCall {
                    id: Some("call-1".to_string()),
                    _tool_type: Some("function".to_string()),
                    function: Some(IncomingToolFunction {
                        name: Some("read_file".to_string()),
                        arguments: Some("{\"path\":\"README.md\"}".to_string()),
                    }),
                }]),
                tool_call_id: None,
            },
            ChatMessage {
                role: "tool".to_string(),
                content: json!("file-content"),
                tool_calls: None,
                tool_call_id: Some("call-1".to_string()),
            },
        ];

        let (developer_instructions, prompt) = ProxyServer::build_turn_input(&messages);
        assert_eq!(developer_instructions, "system-rules");
        assert!(prompt.contains("first-question"));
        assert!(prompt.contains("Assistant called tool read_file (id=call-1)"));
        assert!(prompt.contains("[Tool Result for call-1]: file-content"));
    }

    #[test]
    fn parse_message_without_content_defaults_to_null() {
        let req: ChatCompletionsRequest = serde_json::from_value(json!({
            "model": "gpt-5",
            "messages": [
                {"role": "assistant", "tool_calls": [{"id": "call-1", "type": "function", "function": {"name": "read_file", "arguments": "{}"}}]},
                {"role": "user", "content": "next"}
            ]
        }))
        .expect("request should parse");

        assert_eq!(req.messages.len(), 2);
        assert_eq!(req.messages[0].content, Value::Null);
    }

    #[test]
    fn responses_to_chat_request_maps_input_instructions_and_tools() {
        let req: ResponsesCreateRequest = serde_json::from_value(json!({
            "model": "gpt-5",
            "instructions": "system-rule",
            "input": [
                {
                    "role": "user",
                    "content": [
                        {"type": "input_text", "text": "hello"}
                    ]
                }
            ],
            "tools": [
                {
                    "type": "function",
                    "name": "read_file",
                    "description": "Read a file",
                    "parameters": {"type": "object", "properties": {"path": {"type": "string"}}},
                    "strict": true
                }
            ]
        }))
        .expect("responses request should parse");

        let chat = ProxyServer::responses_to_chat_request(&req);
        assert_eq!(chat.model, "gpt-5");
        assert_eq!(chat.messages.len(), 2);
        assert_eq!(chat.messages[0].role, "system");
        assert_eq!(chat.messages[0].content, json!("system-rule"));
        assert_eq!(chat.messages[1].role, "user");
        assert_eq!(chat.messages[1].content, json!("hello"));

        let tools = chat.tools.expect("tools should be normalized");
        assert_eq!(tools.len(), 1);
        assert_eq!(
            tools[0]
                .pointer("/function/name")
                .and_then(Value::as_str)
                .unwrap_or_default(),
            "read_file"
        );
    }

    #[test]
    fn responses_input_maps_function_call_and_output_items() {
        let input = json!([
            {
                "type": "function_call",
                "call_id": "call_123",
                "name": "read_file",
                "arguments": "{\"path\":\"README.md\"}"
            },
            {
                "type": "function_call_output",
                "call_id": "call_123",
                "output": "file contents"
            }
        ]);

        let messages = ProxyServer::responses_input_to_messages(Some(&input));
        assert_eq!(messages.len(), 2);

        assert_eq!(messages[0].role, "assistant");
        let tool_calls = messages[0]
            .tool_calls
            .as_ref()
            .expect("assistant tool call should be mapped");
        assert_eq!(tool_calls.len(), 1);
        assert_eq!(tool_calls[0].id.as_deref(), Some("call_123"));
        assert_eq!(
            tool_calls[0]
                .function
                .as_ref()
                .and_then(|f| f.name.as_deref()),
            Some("read_file")
        );

        assert_eq!(messages[1].role, "tool");
        assert_eq!(messages[1].tool_call_id.as_deref(), Some("call_123"));
        assert_eq!(messages[1].content, json!("file contents"));
    }

    #[test]
    fn extract_responses_conversation_key_priority_order() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-conversation-key",
            HeaderValue::from_static("from-header"),
        );

        let mut req: ResponsesCreateRequest = serde_json::from_value(json!({
            "model": "gpt-5",
            "conversation_key": "from-request",
            "metadata": {"conversation_key": "from-metadata"},
            "conversation": {"id": "from-conversation"},
            "user": "from-user"
        }))
        .expect("responses request should parse");

        assert_eq!(
            extract_responses_conversation_key(&req, &headers),
            Some("from-request".to_string())
        );

        req.conversation_key = None;
        assert_eq!(
            extract_responses_conversation_key(&req, &headers),
            Some("from-metadata".to_string())
        );

        req.metadata = None;
        assert_eq!(
            extract_responses_conversation_key(&req, &headers),
            Some("from-conversation".to_string())
        );

        req.conversation = None;
        assert_eq!(
            extract_responses_conversation_key(&req, &headers),
            Some("from-user".to_string())
        );

        req.user = None;
        assert_eq!(
            extract_responses_conversation_key(&req, &headers),
            Some("from-header".to_string())
        );
    }

    #[test]
    fn chat_to_responses_response_maps_message_output() {
        let chat = ChatCompletionsResponse {
            id: "chatcmpl-1".to_string(),
            object: "chat.completion".to_string(),
            created: 123,
            model: "gpt-5".to_string(),
            choices: vec![Choice {
                index: 0,
                message: ChatResponseMessage {
                    role: "assistant".to_string(),
                    content: Some("hello".to_string()),
                    tool_calls: None,
                },
                finish_reason: Some("stop".to_string()),
            }],
            usage: Some(Usage {
                prompt_tokens: 10,
                completion_tokens: 5,
                total_tokens: 15,
            }),
        };

        let response = ProxyServer::chat_to_responses_response(
            &chat,
            "resp_1".to_string(),
            &sample_responses_context(),
        );
        assert_eq!(response.id, "resp_1");
        assert_eq!(response.object, "response");
        assert_eq!(response.output_text.as_deref(), Some("hello"));
        assert_eq!(
            response
                .output
                .first()
                .and_then(|v| v.get("type"))
                .and_then(Value::as_str),
            Some("message")
        );
        assert_eq!(
            response.usage.as_ref().map(|u| u.total_tokens),
            Some(15),
            "usage should map to responses usage"
        );
        validate_responses_response_shape(&response).expect("response shape should be valid");

        let serialized = serde_json::to_value(&response).expect("serialization should succeed");
        assert_eq!(
            serialized.get("object").and_then(Value::as_str),
            Some("response")
        );
        assert!(
            serialized.get("error").is_some(),
            "responses payload should include required error field"
        );
        assert!(
            serialized.get("tools").is_some(),
            "responses payload should include required tools field"
        );
        assert!(
            serialized.get("choices").is_none(),
            "responses payload must not contain chat.completions fields"
        );
    }

    #[test]
    fn chat_to_responses_response_uses_context_created_at_when_present() {
        let chat = ChatCompletionsResponse {
            id: "chatcmpl-1".to_string(),
            object: "chat.completion".to_string(),
            created: 999,
            model: "gpt-5".to_string(),
            choices: vec![Choice {
                index: 0,
                message: ChatResponseMessage {
                    role: "assistant".to_string(),
                    content: Some("hello".to_string()),
                    tool_calls: None,
                },
                finish_reason: Some("stop".to_string()),
            }],
            usage: None,
        };

        let mut ctx = sample_responses_context();
        ctx.created_at = Some(123);
        let response = ProxyServer::chat_to_responses_response(&chat, "resp_1".to_string(), &ctx);
        assert_eq!(response.created_at, 123);
    }

    #[test]
    fn validate_responses_response_shape_rejects_unknown_output_type() {
        let mut response = ProxyServer::responses_in_progress_response(
            "resp_test".to_string(),
            123,
            "gpt-5".to_string(),
            &sample_responses_context(),
        );
        response.status = "completed".to_string();
        response.output = vec![json!({
            "id": "bad_1",
            "type": "unknown_type"
        })];

        let err = validate_responses_response_shape(&response).expect_err("shape should fail");
        assert!(
            err.to_string().contains("unsupported value"),
            "unexpected validation error: {err}"
        );
    }

    #[test]
    fn validate_responses_create_request_requires_input() {
        let req: ResponsesCreateRequest = serde_json::from_value(json!({
            "model": "gpt-5"
        }))
        .expect("request should parse");
        let err = validate_responses_create_request(&req).expect_err("request should fail");
        assert!(
            err.to_string()
                .contains("Missing required parameter: 'input'"),
            "unexpected validation error: {err}"
        );
    }

    #[test]
    fn validate_responses_stream_event_shape_checks_required_fields() {
        let invalid_delta = json!({
            "type": "response.output_text.delta",
            "delta": "hi"
        });
        let err = validate_responses_stream_event_shape(&invalid_delta)
            .expect_err("delta event without indexes should fail");
        assert!(
            err.to_string().contains("item_id"),
            "unexpected validation error: {err}"
        );

        let valid_done = json!({
            "type": "response.output_text.done",
            "item_id": "msg_1",
            "output_index": 0,
            "content_index": 0,
            "text": "hello"
        });
        validate_responses_stream_event_shape(&valid_done)
            .expect("done event with required fields should pass");

        let valid_fn_delta = json!({
            "type": "response.function_call_arguments.delta",
            "item_id": "fc_1",
            "output_index": 0,
            "delta": "{\"path\":"
        });
        validate_responses_stream_event_shape(&valid_fn_delta)
            .expect("function call delta with required fields should pass");

        let valid_fn_done = json!({
            "type": "response.function_call_arguments.done",
            "item_id": "fc_1",
            "output_index": 0,
            "arguments": "{\"path\":\"README.md\"}"
        });
        validate_responses_stream_event_shape(&valid_fn_done)
            .expect("function call done with required fields should pass");
    }

    #[test]
    fn validate_responses_response_shape_requires_function_tool_fields() {
        let mut response = ProxyServer::responses_in_progress_response(
            "resp_test".to_string(),
            123,
            "gpt-5".to_string(),
            &sample_responses_context(),
        );
        response.status = "completed".to_string();
        response.tools = vec![json!({
            "type": "function",
            "name": "read_file",
            "parameters": {"type":"object","properties":{}}
        })];
        response.output = vec![json!({
            "id": "msg_1",
            "type": "message",
            "status": "completed",
            "role": "assistant",
            "content": [{
                "type": "output_text",
                "text": "ok",
                "annotations": []
            }]
        })];

        let err = validate_responses_response_shape(&response)
            .expect_err("missing strict field should fail validation");
        assert!(
            err.to_string().contains(".strict"),
            "unexpected validation error: {err}"
        );
    }

    #[test]
    fn validate_responses_response_shape_requires_error_when_failed() {
        let mut response = ProxyServer::responses_in_progress_response(
            "resp_test".to_string(),
            123,
            "gpt-5".to_string(),
            &sample_responses_context(),
        );
        response.status = "failed".to_string();
        response.error = None;

        let err = validate_responses_response_shape(&response)
            .expect_err("failed response without error should fail validation");
        assert!(
            err.to_string().contains("response.error must be present"),
            "unexpected validation error: {err}"
        );
    }

    #[test]
    fn validate_responses_stream_event_shape_rejects_failed_event_without_failed_status() {
        let response = ProxyServer::responses_in_progress_response(
            "resp_test".to_string(),
            123,
            "gpt-5".to_string(),
            &sample_responses_context(),
        );
        let event = json!({
            "type": "response.failed",
            "response": response
        });

        let err = validate_responses_stream_event_shape(&event)
            .expect_err("response.failed event with non-failed status should fail");
        assert!(
            err.to_string()
                .contains("response.failed event must include response.status='failed'"),
            "unexpected validation error: {err}"
        );
    }

    #[test]
    fn bounded_history_map_evicts_oldest_entry() {
        let mut map = BoundedHistoryMap::new(2);
        map.insert("conv-1".to_string(), "thread-1".to_string());
        map.insert("conv-2".to_string(), "thread-2".to_string());
        map.insert("conv-3".to_string(), "thread-3".to_string());

        assert_eq!(map.len(), 2);
        assert_eq!(map.get_cloned("conv-1"), None);
        assert_eq!(map.get_cloned("conv-2"), Some("thread-2".to_string()));
        assert_eq!(map.get_cloned("conv-3"), Some("thread-3".to_string()));
    }

    #[test]
    fn previous_response_not_found_error_round_trip() {
        let err = previous_response_not_found_error(Some("resp_missing"));
        let parsed =
            parse_previous_response_not_found_error(&err).expect("error marker should parse");
        assert!(parsed.contains("resp_missing"));
    }

    #[test]
    fn openai_error_payload_matches_expected_shape() {
        let payload = openai_error_payload("bad request", "invalid_request_error", "invalid_json");
        assert_eq!(
            payload.pointer("/error/message").and_then(Value::as_str),
            Some("bad request")
        );
        assert_eq!(
            payload.pointer("/error/type").and_then(Value::as_str),
            Some("invalid_request_error")
        );
        assert_eq!(
            payload.pointer("/error/code").and_then(Value::as_str),
            Some("invalid_json")
        );
    }
}
