use anyhow::anyhow;
use serde_json::{json, Value};
use warp::Reply;

const PREVIOUS_RESPONSE_NOT_FOUND_PREFIX: &str = "previous_response_not_found:";

pub(super) fn openai_error_payload(
    message: impl Into<String>,
    error_type: &str,
    code: &str,
) -> Value {
    json!({
        "error": {
            "message": message.into(),
            "type": error_type,
            "param": Value::Null,
            "code": code
        }
    })
}

pub(super) fn previous_response_not_found_message(previous_response_id: Option<&str>) -> String {
    if let Some(id) = previous_response_id
        .map(str::trim)
        .filter(|id| !id.is_empty())
    {
        format!("No response found with id '{}'.", id)
    } else {
        "No response found for the provided previous_response_id.".to_string()
    }
}

pub(super) fn previous_response_not_found_error(
    previous_response_id: Option<&str>,
) -> anyhow::Error {
    anyhow!(
        "{} {}",
        PREVIOUS_RESPONSE_NOT_FOUND_PREFIX,
        previous_response_not_found_message(previous_response_id)
    )
}

pub(super) fn parse_previous_response_not_found_error(error: &anyhow::Error) -> Option<String> {
    let text = error.to_string();
    text.strip_prefix(PREVIOUS_RESPONSE_NOT_FOUND_PREFIX)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string)
}

pub(super) fn json_error_response(
    status: warp::http::StatusCode,
    payload: Value,
) -> warp::reply::Response {
    let reply = warp::reply::with_status(warp::reply::json(&payload), status);
    let reply = warp::reply::with_header(reply, "access-control-allow-origin", "*");
    reply.into_response()
}
