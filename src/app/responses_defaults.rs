use serde_json::{json, Value};

pub(super) fn normalize_responses_metadata(metadata: Option<&Value>) -> Value {
    match metadata {
        Some(Value::Object(map)) => Value::Object(map.clone()),
        _ => json!({}),
    }
}

pub(super) fn normalize_responses_tool_choice(tool_choice: Option<&Value>) -> Value {
    let Some(tool_choice) = tool_choice else {
        return json!("auto");
    };

    match tool_choice {
        Value::String(value) if !value.trim().is_empty() => tool_choice.clone(),
        Value::Object(_) => tool_choice.clone(),
        _ => json!("auto"),
    }
}

pub(super) fn normalize_responses_text(text: Option<&Value>) -> Value {
    match text {
        Some(Value::Object(_)) => text
            .cloned()
            .unwrap_or_else(|| json!({"format":{"type":"text"}})),
        _ => json!({"format":{"type":"text"}}),
    }
}

pub(super) fn normalize_responses_reasoning(reasoning: Option<&Value>) -> Value {
    let mut normalized = reasoning
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    normalized
        .entry("effort".to_string())
        .or_insert(Value::Null);
    normalized
        .entry("summary".to_string())
        .or_insert(Value::Null);
    Value::Object(normalized)
}
