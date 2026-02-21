use bytes::Bytes;
use serde_json::{json, Value};
use std::convert::Infallible;
use tokio::sync::mpsc;
use uuid::Uuid;

use super::error_response::openai_error_payload;
use super::log_structured;
use super::validation::validate_responses_stream_event_shape;
use super::ChatCompletionsResponse;

pub(super) fn build_sse_from_chat_response(response: &ChatCompletionsResponse) -> String {
    let created = chrono::Utc::now().timestamp();
    let chunk_id = format!("chatcmpl-{}", Uuid::new_v4());
    let model = response.model.clone();

    let choice = response.choices.first();
    let content = choice
        .and_then(|c| c.message.content.as_deref())
        .unwrap_or_default();
    let finish_reason = choice
        .and_then(|c| c.finish_reason.as_deref())
        .unwrap_or("stop");

    let mut chunks = Vec::new();
    chunks.push(format!(
        "data: {}\n\n",
        json!({
            "id": chunk_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{
                "index": 0,
                "delta": { "role": "assistant" },
                "finish_reason": Value::Null
            }]
        })
    ));

    if !content.is_empty() {
        chunks.push(format!(
            "data: {}\n\n",
            json!({
                "id": chunk_id,
                "object": "chat.completion.chunk",
                "created": created,
                "model": model,
                "choices": [{
                    "index": 0,
                    "delta": { "content": content },
                    "finish_reason": Value::Null
                }]
            })
        ));
    }

    chunks.push(format!(
        "data: {}\n\n",
        json!({
            "id": chunk_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": model,
            "choices": [{
                "index": 0,
                "delta": {},
                "finish_reason": finish_reason
            }]
        })
    ));
    chunks.push("data: [DONE]\n\n".to_string());

    chunks.join("")
}

pub(super) fn build_responses_sse_chunk(payload: &Value) -> String {
    let event_name = payload
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("message");
    format!("event: {}\ndata: {}\n\n", event_name, payload)
}

pub(super) fn build_sse_done_chunk() -> String {
    "data: [DONE]\n\n".to_string()
}

pub(super) fn send_responses_stream_event(sender: &mpsc::UnboundedSender<String>, payload: Value) {
    if let Err(err) = validate_responses_stream_event_shape(&payload) {
        log_structured(
            "error",
            "stream.invalid_event_generated",
            None,
            json!({
                "error": err.to_string()
            }),
        );
        let fallback = json!({
            "type": "error",
            "error": openai_error_payload(
                "Proxy generated an invalid Responses stream event",
                "server_error",
                "internal_error"
            )
            .get("error")
            .cloned()
            .unwrap_or_else(|| json!({"message":"internal error"}))
        });
        let _ = sender.send(build_responses_sse_chunk(&fallback));
        return;
    }

    let _ = sender.send(build_responses_sse_chunk(&payload));
}

pub(super) fn build_live_sse_response(
    stream_rx: mpsc::UnboundedReceiver<String>,
) -> warp::reply::Response {
    let stream = futures_util::stream::unfold(stream_rx, |mut rx| async move {
        rx.recv()
            .await
            .map(|chunk| (Ok::<Bytes, Infallible>(Bytes::from(chunk)), rx))
    });

    let body = warp::hyper::Body::wrap_stream(stream);
    let mut response = warp::http::Response::new(body);
    let headers = response.headers_mut();
    headers.insert(
        "content-type",
        warp::http::HeaderValue::from_static("text/event-stream"),
    );
    headers.insert(
        "cache-control",
        warp::http::HeaderValue::from_static("no-cache"),
    );
    headers.insert(
        "connection",
        warp::http::HeaderValue::from_static("keep-alive"),
    );
    headers.insert(
        "access-control-allow-origin",
        warp::http::HeaderValue::from_static("*"),
    );
    response
}
