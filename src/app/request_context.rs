use serde_json::{json, Value};
use uuid::Uuid;

use super::{ChatCompletionsRequest, ResponsesCreateRequest};

pub(super) fn log_request(
    method: &warp::http::Method,
    path: &str,
    headers: &warp::http::HeaderMap,
    request_id: Option<&str>,
) {
    let mut header_entries = Vec::with_capacity(headers.len());
    for (name, value) in headers.iter() {
        let header_name = name.as_str().to_lowercase();
        let value_str = sanitize_header_value(&header_name, value);
        let category = if header_name.contains("user-agent")
            || header_name.contains("client")
            || header_name.contains("cline")
        {
            "client"
        } else if is_sensitive_header(&header_name) {
            "secret"
        } else {
            "general"
        };

        header_entries.push(json!({
            "name": name.as_str(),
            "value": value_str,
            "category": category
        }));
    }

    let user_agent = headers
        .get("user-agent")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("none")
        .to_lowercase();

    let mut clients = Vec::new();
    if user_agent.contains("vscode") {
        clients.push("vscode");
    }
    if user_agent.contains("cline") {
        clients.push("cline");
    }

    log_structured(
        "info",
        "http.request",
        request_id,
        json!({
            "method": method.as_str(),
            "path": path,
            "header_count": headers.len(),
            "headers": header_entries,
            "detected_clients": clients
        }),
    );
}

fn is_sensitive_header(header_name: &str) -> bool {
    matches!(
        header_name,
        "authorization" | "proxy-authorization" | "cookie" | "set-cookie" | "x-api-key" | "api-key"
    ) || header_name.contains("token")
        || header_name.contains("secret")
}

fn sanitize_header_value(header_name: &str, value: &warp::http::HeaderValue) -> String {
    if is_sensitive_header(header_name) {
        return "[REDACTED]".to_string();
    }

    let raw = match value.to_str() {
        Ok(v) => v,
        Err(_) => return "[INVALID UTF-8]".to_string(),
    };

    let max_chars = 120;
    let char_count = raw.chars().count();
    let truncated: String = raw.chars().take(max_chars).collect();
    if char_count > max_chars {
        format!("{}...", truncated)
    } else {
        truncated
    }
}

pub(super) fn extract_request_id(headers: &warp::http::HeaderMap) -> String {
    headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| Uuid::new_v4().to_string())
}

pub(super) fn extract_conversation_key(
    req: &ChatCompletionsRequest,
    headers: &warp::http::HeaderMap,
) -> Option<String> {
    if let Some(v) = req
        .conversation_key
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        return Some(v.to_string());
    }

    if let Some(v) = req
        .metadata
        .as_ref()
        .and_then(|m| m.get("conversation_key"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        return Some(v.to_string());
    }

    if let Some(v) = req.user.as_deref().map(str::trim).filter(|v| !v.is_empty()) {
        return Some(v.to_string());
    }

    headers
        .get("x-conversation-key")
        .or_else(|| headers.get("x-thread-key"))
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string)
}

pub(super) fn extract_responses_conversation_key(
    req: &ResponsesCreateRequest,
    headers: &warp::http::HeaderMap,
) -> Option<String> {
    if let Some(v) = req
        .conversation_key
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        return Some(v.to_string());
    }

    if let Some(v) = req
        .metadata
        .as_ref()
        .and_then(|m| m.get("conversation_key"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        return Some(v.to_string());
    }

    if let Some(v) = req
        .conversation
        .as_ref()
        .and_then(|conversation| match conversation {
            Value::String(s) => Some(s.as_str()),
            Value::Object(obj) => obj.get("id").and_then(Value::as_str),
            _ => None,
        })
        .map(str::trim)
        .filter(|v| !v.is_empty())
    {
        return Some(v.to_string());
    }

    if let Some(v) = req.user.as_deref().map(str::trim).filter(|v| !v.is_empty()) {
        return Some(v.to_string());
    }

    headers
        .get("x-conversation-key")
        .or_else(|| headers.get("x-thread-key"))
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string)
}

pub(super) fn log_structured(level: &str, event: &str, request_id: Option<&str>, details: Value) {
    println!(
        "{}",
        json!({
            "ts": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            "level": level,
            "event": event,
            "request_id": request_id,
            "details": details
        })
    );
}
