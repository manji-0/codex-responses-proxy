use anyhow::{anyhow, Context, Result};
use serde_json::Value;

use super::{ResponsesCreateRequest, ResponsesCreateResponse, ResponsesError};

pub(super) fn validate_responses_create_request(req: &ResponsesCreateRequest) -> Result<()> {
    if req.model.trim().is_empty() {
        return Err(anyhow!("'model' must be a non-empty string."));
    }

    match req.input.as_ref() {
        Some(Value::Null) | None => return Err(anyhow!("Missing required parameter: 'input'.")),
        _ => {}
    }

    if let Some(metadata) = req.metadata.as_ref() {
        if !metadata.is_object() {
            return Err(anyhow!("Invalid type for 'metadata': expected object."));
        }
    }

    if let Some(max_output_tokens) = req.max_output_tokens {
        if max_output_tokens <= 0 {
            return Err(anyhow!("'max_output_tokens' must be greater than 0."));
        }
    }

    if let Some(temperature) = req.temperature {
        if !(0.0..=2.0).contains(&temperature) {
            return Err(anyhow!(
                "'temperature' must be between 0.0 and 2.0 inclusive."
            ));
        }
    }

    if let Some(top_p) = req.top_p {
        if !(0.0..=1.0).contains(&top_p) {
            return Err(anyhow!("'top_p' must be between 0.0 and 1.0 inclusive."));
        }
    }

    if let Some(truncation) = req.truncation.as_deref() {
        let truncation = truncation.trim();
        if !truncation.is_empty() && truncation != "auto" && truncation != "disabled" {
            return Err(anyhow!("'truncation' must be either 'auto' or 'disabled'."));
        }
    }

    if let Some(text) = req.text.as_ref() {
        if !text.is_object() {
            return Err(anyhow!("Invalid type for 'text': expected object."));
        }
    }

    Ok(())
}

pub(super) fn responses_request_validation_code(message: &str) -> &'static str {
    if message.contains("Missing required parameter") {
        "missing_required_parameter"
    } else {
        "invalid_request"
    }
}

pub(super) fn responses_error_from_openai_error_payload(payload: &Value) -> ResponsesError {
    let code = payload
        .pointer("/error/code")
        .and_then(Value::as_str)
        .unwrap_or("internal_error")
        .to_string();
    let message = payload
        .pointer("/error/message")
        .and_then(Value::as_str)
        .unwrap_or("internal error")
        .to_string();
    ResponsesError { code, message }
}

pub(super) fn validate_responses_response_shape(response: &ResponsesCreateResponse) -> Result<()> {
    if response.id.trim().is_empty() {
        return Err(anyhow!("response.id must not be empty"));
    }
    if response.object != "response" {
        return Err(anyhow!(
            "response.object must be 'response', got '{}'",
            response.object
        ));
    }
    if response.status.trim().is_empty() {
        return Err(anyhow!("response.status must not be empty"));
    }
    if !matches!(
        response.status.as_str(),
        "completed" | "failed" | "in_progress" | "incomplete"
    ) {
        return Err(anyhow!(
            "response.status has unsupported value '{}'",
            response.status
        ));
    }
    if response.created_at <= 0 {
        return Err(anyhow!(
            "response.created_at must be a positive unix timestamp"
        ));
    }
    if response.model.trim().is_empty() {
        return Err(anyhow!("response.model must not be empty"));
    }
    if response.status == "failed" && response.error.is_none() {
        return Err(anyhow!(
            "response.error must be present when response.status is 'failed'"
        ));
    }
    if response.status != "failed" && response.error.is_some() {
        return Err(anyhow!(
            "response.error must be null unless response.status is 'failed'"
        ));
    }
    if !response.metadata.is_object() {
        return Err(anyhow!("response.metadata must be an object"));
    }
    if !response.reasoning.is_object() {
        return Err(anyhow!("response.reasoning must be an object"));
    }
    if response
        .text
        .pointer("/format/type")
        .and_then(Value::as_str)
        .is_none()
    {
        return Err(anyhow!(
            "response.text.format.type must be present and a string"
        ));
    }
    if response.truncation != "auto" && response.truncation != "disabled" {
        return Err(anyhow!(
            "response.truncation must be 'auto' or 'disabled', got '{}'",
            response.truncation
        ));
    }
    if let Some(temperature) = response.temperature {
        if !(0.0..=2.0).contains(&temperature) {
            return Err(anyhow!("response.temperature must be within [0.0, 2.0]"));
        }
    }
    if let Some(top_p) = response.top_p {
        if !(0.0..=1.0).contains(&top_p) {
            return Err(anyhow!("response.top_p must be within [0.0, 1.0]"));
        }
    }

    match &response.tool_choice {
        Value::String(value) if !value.trim().is_empty() => {}
        Value::Object(_) => {}
        _ => {
            return Err(anyhow!(
                "response.tool_choice must be a non-empty string or object"
            ))
        }
    }

    for (index, tool) in response.tools.iter().enumerate() {
        let Some(tool_type) = tool.get("type").and_then(Value::as_str) else {
            return Err(anyhow!(
                "response.tools[{index}].type must be a non-empty string"
            ));
        };
        if tool_type.trim().is_empty() {
            return Err(anyhow!(
                "response.tools[{index}].type must be a non-empty string"
            ));
        }
        if tool_type == "function" {
            let name = tool
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("response.tools[{index}].name must be a string"))?;
            if name.trim().is_empty() {
                return Err(anyhow!(
                    "response.tools[{index}].name must be a non-empty string"
                ));
            }

            if !tool
                .get("parameters")
                .is_some_and(|v| v.is_object() || v.is_null())
            {
                return Err(anyhow!(
                    "response.tools[{index}].parameters must be an object or null"
                ));
            }

            if !tool
                .get("strict")
                .is_some_and(|v| v.is_boolean() || v.is_null())
            {
                return Err(anyhow!(
                    "response.tools[{index}].strict must be a boolean or null"
                ));
            }
        }
    }

    if let Some(usage) = response.usage.as_ref() {
        if usage.input_tokens < 0 || usage.output_tokens < 0 || usage.total_tokens < 0 {
            return Err(anyhow!(
                "response.usage token counters must be non-negative integers"
            ));
        }
        if usage.input_tokens_details.cached_tokens < 0 {
            return Err(anyhow!(
                "response.usage.input_tokens_details.cached_tokens must be non-negative"
            ));
        }
        if usage.output_tokens_details.reasoning_tokens < 0 {
            return Err(anyhow!(
                "response.usage.output_tokens_details.reasoning_tokens must be non-negative"
            ));
        }
    }

    for (index, item) in response.output.iter().enumerate() {
        if item.get("id").and_then(Value::as_str).is_none() {
            return Err(anyhow!("response.output[{index}].id must be a string"));
        }
        let Some(item_type) = item.get("type").and_then(Value::as_str) else {
            return Err(anyhow!(
                "response.output[{index}] must include string field 'type'"
            ));
        };

        match item_type {
            "message" => {
                let role = item
                    .get("role")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("response.output[{index}].role must be a string"))?;
                if role != "assistant" {
                    return Err(anyhow!(
                        "response.output[{index}].role must be 'assistant', got '{role}'"
                    ));
                }

                let content = item
                    .get("content")
                    .and_then(Value::as_array)
                    .ok_or_else(|| anyhow!("response.output[{index}].content must be an array"))?;
                for (content_index, chunk) in content.iter().enumerate() {
                    let Some(content_type) = chunk.get("type").and_then(Value::as_str) else {
                        return Err(anyhow!(
                            "response.output[{index}].content[{content_index}].type must be a string"
                        ));
                    };
                    if content_type != "output_text" && content_type != "refusal" {
                        return Err(anyhow!(
                            "response.output[{index}].content[{content_index}].type must be 'output_text' or 'refusal', got '{content_type}'"
                        ));
                    }
                    if content_type == "output_text"
                        && chunk.get("text").and_then(Value::as_str).is_none()
                    {
                        return Err(anyhow!(
                            "response.output[{index}].content[{content_index}].text must be a string"
                        ));
                    }
                    if content_type == "refusal"
                        && chunk.get("refusal").and_then(Value::as_str).is_none()
                    {
                        return Err(anyhow!(
                            "response.output[{index}].content[{content_index}].refusal must be a string"
                        ));
                    }
                }
            }
            "function_call" => {
                if item.get("name").and_then(Value::as_str).is_none() {
                    return Err(anyhow!(
                        "response.output[{index}].name must be a string for function_call"
                    ));
                }
                if item.get("arguments").and_then(Value::as_str).is_none() {
                    return Err(anyhow!(
                        "response.output[{index}].arguments must be a string for function_call"
                    ));
                }
                if item.get("call_id").and_then(Value::as_str).is_none() {
                    return Err(anyhow!(
                        "response.output[{index}].call_id must be a string for function_call"
                    ));
                }
            }
            "function_call_output" => {
                if item.get("call_id").and_then(Value::as_str).is_none() {
                    return Err(anyhow!(
                        "response.output[{index}].call_id must be a string for function_call_output"
                    ));
                }
                if item.get("output").and_then(Value::as_str).is_none() {
                    return Err(anyhow!(
                        "response.output[{index}].output must be a string for function_call_output"
                    ));
                }
            }
            other => {
                return Err(anyhow!(
                    "response.output[{index}].type has unsupported value '{other}'"
                ));
            }
        }
    }

    Ok(())
}

pub(super) fn validate_responses_stream_event_shape(event: &Value) -> Result<()> {
    let Some(event_type) = event.get("type").and_then(Value::as_str) else {
        return Err(anyhow!("stream event must include string field 'type'"));
    };

    match event_type {
        "response.created" | "response.in_progress" | "response.completed" | "response.failed" => {
            let response_value = event
                .get("response")
                .cloned()
                .ok_or_else(|| anyhow!("{event_type} event must include 'response'"))?;
            let response: ResponsesCreateResponse = serde_json::from_value(response_value)
                .with_context(|| format!("{event_type} event contains invalid response object"))?;
            validate_responses_response_shape(&response)?;

            if event_type == "response.completed" && response.status != "completed" {
                return Err(anyhow!(
                    "response.completed event must include response.status='completed'"
                ));
            }
            if event_type == "response.failed" && response.status != "failed" {
                return Err(anyhow!(
                    "response.failed event must include response.status='failed'"
                ));
            }
            if (event_type == "response.created" || event_type == "response.in_progress")
                && response.status != "in_progress"
            {
                return Err(anyhow!(
                    "{event_type} event must include response.status='in_progress'"
                ));
            }
        }
        "response.output_text.delta" => {
            if event.get("item_id").and_then(Value::as_str).is_none() {
                return Err(anyhow!(
                    "response.output_text.delta must include string field 'item_id'"
                ));
            }
            if event.get("output_index").and_then(Value::as_i64).is_none() {
                return Err(anyhow!(
                    "response.output_text.delta must include integer field 'output_index'"
                ));
            }
            if event.get("content_index").and_then(Value::as_i64).is_none() {
                return Err(anyhow!(
                    "response.output_text.delta must include integer field 'content_index'"
                ));
            }
            if event.get("delta").and_then(Value::as_str).is_none() {
                return Err(anyhow!(
                    "response.output_text.delta must include string field 'delta'"
                ));
            }
        }
        "response.output_text.done" => {
            if event.get("item_id").and_then(Value::as_str).is_none() {
                return Err(anyhow!(
                    "response.output_text.done must include string field 'item_id'"
                ));
            }
            if event.get("output_index").and_then(Value::as_i64).is_none() {
                return Err(anyhow!(
                    "response.output_text.done must include integer field 'output_index'"
                ));
            }
            if event.get("content_index").and_then(Value::as_i64).is_none() {
                return Err(anyhow!(
                    "response.output_text.done must include integer field 'content_index'"
                ));
            }
            if event.get("text").and_then(Value::as_str).is_none() {
                return Err(anyhow!(
                    "response.output_text.done must include string field 'text'"
                ));
            }
        }
        "response.output_item.added" | "response.output_item.done" => {
            if event.get("output_index").and_then(Value::as_i64).is_none() {
                return Err(anyhow!(
                    "{event_type} must include integer field 'output_index'"
                ));
            }
            let item = event
                .get("item")
                .and_then(Value::as_object)
                .ok_or_else(|| anyhow!("{event_type} must include object field 'item'"))?;
            if item.get("type").and_then(Value::as_str).is_none() {
                return Err(anyhow!("{event_type}.item.type must be a string"));
            }
        }
        "response.function_call_arguments.delta" => {
            if event.get("item_id").and_then(Value::as_str).is_none() {
                return Err(anyhow!(
                    "response.function_call_arguments.delta must include string field 'item_id'"
                ));
            }
            if event.get("output_index").and_then(Value::as_i64).is_none() {
                return Err(anyhow!(
                    "response.function_call_arguments.delta must include integer field 'output_index'"
                ));
            }
            if event.get("delta").and_then(Value::as_str).is_none() {
                return Err(anyhow!(
                    "response.function_call_arguments.delta must include string field 'delta'"
                ));
            }
        }
        "response.function_call_arguments.done" => {
            if event.get("item_id").and_then(Value::as_str).is_none() {
                return Err(anyhow!(
                    "response.function_call_arguments.done must include string field 'item_id'"
                ));
            }
            if event.get("output_index").and_then(Value::as_i64).is_none() {
                return Err(anyhow!(
                    "response.function_call_arguments.done must include integer field 'output_index'"
                ));
            }
            if event.get("arguments").and_then(Value::as_str).is_none() {
                return Err(anyhow!(
                    "response.function_call_arguments.done must include string field 'arguments'"
                ));
            }
        }
        "error" => {
            let error = event
                .get("error")
                .and_then(Value::as_object)
                .ok_or_else(|| anyhow!("error event must include object field 'error'"))?;
            if error.get("message").and_then(Value::as_str).is_none() {
                return Err(anyhow!("error event must include error.message as string"));
            }
        }
        other => {
            return Err(anyhow!("unsupported Responses stream event type '{other}'"));
        }
    }

    Ok(())
}
