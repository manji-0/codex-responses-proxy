use super::*;

pub(super) async fn handle_health(proxy: &ProxyServer) -> warp::reply::Response {
    log_structured("info", "health.requested", None, json!({}));
    let health = proxy.health_report().await;
    warp::reply::json(&health).into_response()
}

pub(super) fn handle_models() -> warp::reply::Response {
    let models_response = json!({
        "object": "list",
        "data": [
            {
                "id": "gpt-4",
                "object": "model",
                "created": 1687882411,
                "owned_by": "openai"
            },
            {
                "id": "gpt-5",
                "object": "model",
                "created": 1687882411,
                "owned_by": "openai"
            }
        ]
    });

    warp::reply::json(&models_response).into_response()
}

pub(super) async fn handle_responses_create(
    path_str: &str,
    headers: warp::http::HeaderMap,
    body: bytes::Bytes,
    proxy: ProxyServer,
    request_id: String,
) -> Result<warp::reply::Response, warp::Rejection> {
    log_structured(
        "info",
        "request.responses.matched",
        Some(&request_id),
        json!({
            "path": path_str,
            "body_size": body.len()
        }),
    );

    let responses_req: ResponsesCreateRequest = match serde_json::from_slice(&body) {
        Ok(req) => req,
        Err(e) => {
            log_structured(
                "warn",
                "request.parse_error",
                Some(&request_id),
                json!({
                    "path": path_str,
                    "error": e.to_string()
                }),
            );
            let payload = openai_error_payload(
                format!("Invalid JSON: {e}"),
                "invalid_request_error",
                "invalid_json",
            );
            return Ok(json_error_response(
                warp::http::StatusCode::BAD_REQUEST,
                payload,
            ));
        }
    };

    log_structured(
        "info",
        "request.responses.parsed",
        Some(&request_id),
        json!({
            "path": path_str,
            "model": responses_req.model,
            "stream": responses_req.stream.unwrap_or(false)
        }),
    );
    if let Err(validation_error) = validate_responses_create_request(&responses_req) {
        let payload = openai_error_payload(
            validation_error.to_string(),
            "invalid_request_error",
            responses_request_validation_code(&validation_error.to_string()),
        );
        return Ok(json_error_response(
            warp::http::StatusCode::BAD_REQUEST,
            payload,
        ));
    }

    let explicit_conversation_key = extract_responses_conversation_key(&responses_req, &headers);
    let previous_response_id = responses_req.previous_response_id.clone();
    let prior_conversation_key = match previous_response_id.as_deref() {
        Some(prev_id) => proxy.get_response_conversation_key(prev_id).await,
        None => None,
    };
    if previous_response_id.is_some() && prior_conversation_key.is_none() {
        let payload = openai_error_payload(
            previous_response_not_found_message(previous_response_id.as_deref()),
            "invalid_request_error",
            "previous_response_not_found",
        );
        return Ok(json_error_response(
            warp::http::StatusCode::BAD_REQUEST,
            payload,
        ));
    }
    let conversation_key = explicit_conversation_key
        .or(prior_conversation_key)
        .unwrap_or_else(|| format!("resp-conv-{}", Uuid::new_v4()));
    let stream = responses_req.stream.unwrap_or(false);
    let chat_req = ProxyServer::responses_to_chat_request(&responses_req);
    let stream_output_item_id = if stream {
        Some(format!("msg_{}", Uuid::new_v4()))
    } else {
        None
    };
    let stream_output_item_added = if stream {
        Some(Arc::new(AtomicBool::new(false)))
    } else {
        None
    };
    let responses_ctx = ProxyServer::build_responses_response_context(
        &responses_req,
        stream_output_item_id.clone(),
    );
    let request_ctx = RequestContext {
        request_id: request_id.clone(),
        conversation_key: Some(conversation_key.clone()),
        previous_response_id: previous_response_id.clone(),
        require_existing_thread: previous_response_id.is_some(),
        responses_stream_output_item_id: stream_output_item_id,
        responses_stream_output_item_added: stream_output_item_added.clone(),
    };

    if stream {
        let response_id = format!("resp_{}", Uuid::new_v4());
        let created_at = chrono::Utc::now().timestamp();
        let response_id_for_task = response_id.clone();
        let model_for_task = chat_req.model.clone();
        let conversation_key_for_task = conversation_key.clone();
        let request_id_for_task = request_id.clone();
        let previous_response_id_for_task = previous_response_id.clone();
        let mut responses_ctx_for_task = responses_ctx.clone();
        responses_ctx_for_task.created_at = Some(created_at);
        let proxy_for_task = proxy.clone();
        let request_ctx_for_task = request_ctx;
        let stream_output_item_added_for_task = stream_output_item_added;
        let (stream_tx, stream_rx) = mpsc::unbounded_channel::<String>();

        tokio::spawn(async move {
            let response_started = ProxyServer::responses_in_progress_response(
                response_id_for_task.clone(),
                created_at,
                model_for_task,
                &responses_ctx_for_task,
            );
            let model_for_failures = response_started.model.clone();
            if let Err(validation_error) = validate_responses_response_shape(&response_started) {
                log_structured(
                    "error",
                    "request.invalid_responses_payload",
                    Some(&request_id_for_task),
                    json!({
                        "conversation_key": conversation_key_for_task,
                        "error": validation_error.to_string()
                    }),
                );
                send_responses_stream_event(
                    &stream_tx,
                    json!({
                        "type": "error",
                        "error": openai_error_payload(
                            "Proxy generated a non-compliant Responses API payload",
                            "server_error",
                            "internal_error"
                        )
                        .get("error")
                        .cloned()
                        .unwrap_or_else(|| json!({"message":"internal error"}))
                    }),
                );
                let _ = stream_tx.send(build_sse_done_chunk());
                return;
            }
            send_responses_stream_event(
                &stream_tx,
                json!({
                    "type": "response.created",
                    "response": response_started.clone()
                }),
            );
            send_responses_stream_event(
                &stream_tx,
                json!({
                    "type": "response.in_progress",
                    "response": response_started
                }),
            );

            match proxy_for_task
                .proxy_request_with_responses_stream(
                    chat_req,
                    request_ctx_for_task,
                    Some(stream_tx.clone()),
                )
                .await
            {
                Ok(chat_response) => {
                    let response = ProxyServer::chat_to_responses_response(
                        &chat_response,
                        response_id.clone(),
                        &responses_ctx_for_task,
                    );
                    if let Err(validation_error) = validate_responses_response_shape(&response) {
                        log_structured(
                            "error",
                            "request.invalid_responses_payload",
                            Some(&request_id_for_task),
                            json!({
                                "conversation_key": conversation_key_for_task,
                                "error": validation_error.to_string()
                            }),
                        );
                        let payload = openai_error_payload(
                            "Proxy generated a non-compliant Responses API payload",
                            "server_error",
                            "internal_error",
                        );
                        let failed_response = ProxyServer::responses_failed_response(
                            response_id.clone(),
                            model_for_failures.clone(),
                            &responses_ctx_for_task,
                            responses_error_from_openai_error_payload(&payload),
                        );
                        send_responses_stream_event(
                            &stream_tx,
                            json!({
                                "type": "response.failed",
                                "response": failed_response
                            }),
                        );
                        let error_obj = payload
                            .get("error")
                            .cloned()
                            .unwrap_or_else(|| json!({"message":"internal error"}));
                        send_responses_stream_event(
                            &stream_tx,
                            json!({
                                "type": "error",
                                "error": error_obj
                            }),
                        );
                    } else {
                        for (output_index, item) in response.output.iter().enumerate() {
                            let item_type =
                                item.get("type").and_then(Value::as_str).unwrap_or_default();
                            let item_id = item.get("id").and_then(Value::as_str);

                            if item_type == "message" {
                                let added_already = stream_output_item_added_for_task
                                    .as_ref()
                                    .map(|flag| flag.load(Ordering::Relaxed))
                                    .unwrap_or(false);
                                if !added_already {
                                    if let Some(output_item_id) = item_id {
                                        send_responses_stream_event(
                                            &stream_tx,
                                            json!({
                                                "type": "response.output_item.added",
                                                "output_index": output_index,
                                                "item": {
                                                    "id": output_item_id,
                                                    "type": "message",
                                                    "status": "in_progress",
                                                    "role": "assistant",
                                                    "content": []
                                                }
                                            }),
                                        );
                                        if let Some(flag) = &stream_output_item_added_for_task {
                                            flag.store(true, Ordering::Relaxed);
                                        }
                                    }
                                }

                                if let (Some(output_item_id), Some(content)) =
                                    (item_id, item.get("content").and_then(Value::as_array))
                                {
                                    for (content_index, part) in content.iter().enumerate() {
                                        if part.get("type").and_then(Value::as_str)
                                            == Some("output_text")
                                        {
                                            if let Some(text) =
                                                part.get("text").and_then(Value::as_str)
                                            {
                                                send_responses_stream_event(
                                                    &stream_tx,
                                                    json!({
                                                        "type": "response.output_text.done",
                                                        "item_id": output_item_id,
                                                        "output_index": output_index,
                                                        "content_index": content_index,
                                                        "text": text
                                                    }),
                                                );
                                            }
                                        }
                                    }
                                }

                                send_responses_stream_event(
                                    &stream_tx,
                                    json!({
                                        "type": "response.output_item.done",
                                        "output_index": output_index,
                                        "item": item
                                    }),
                                );
                                continue;
                            }

                            if item_type == "function_call" {
                                send_responses_stream_event(
                                    &stream_tx,
                                    json!({
                                        "type": "response.output_item.added",
                                        "output_index": output_index,
                                        "item": item
                                    }),
                                );
                                if let (Some(output_item_id), Some(arguments)) =
                                    (item_id, item.get("arguments").and_then(Value::as_str))
                                {
                                    if !arguments.is_empty() {
                                        send_responses_stream_event(
                                            &stream_tx,
                                            json!({
                                                "type": "response.function_call_arguments.delta",
                                                "item_id": output_item_id,
                                                "output_index": output_index,
                                                "delta": arguments
                                            }),
                                        );
                                    }
                                    send_responses_stream_event(
                                        &stream_tx,
                                        json!({
                                            "type": "response.function_call_arguments.done",
                                            "item_id": output_item_id,
                                            "output_index": output_index,
                                            "arguments": arguments
                                        }),
                                    );
                                }
                                send_responses_stream_event(
                                    &stream_tx,
                                    json!({
                                        "type": "response.output_item.done",
                                        "output_index": output_index,
                                        "item": item
                                    }),
                                );
                                continue;
                            }

                            send_responses_stream_event(
                                &stream_tx,
                                json!({
                                    "type": "response.output_item.done",
                                    "output_index": output_index,
                                    "item": item
                                }),
                            );
                        }
                        send_responses_stream_event(
                            &stream_tx,
                            json!({
                                "type": "response.completed",
                                "response": response
                            }),
                        );
                        proxy_for_task
                            .set_response_conversation_key(
                                response_id.clone(),
                                conversation_key_for_task.clone(),
                            )
                            .await;
                    }
                }
                Err(e) => {
                    log_structured(
                        "error",
                        "request.failed",
                        Some(&request_id_for_task),
                        json!({
                            "conversation_key": conversation_key_for_task,
                            "previous_response_id": previous_response_id_for_task,
                            "error": e.to_string()
                        }),
                    );
                    let payload = if let Some(not_found_message) =
                        parse_previous_response_not_found_error(&e)
                    {
                        openai_error_payload(
                            not_found_message,
                            "invalid_request_error",
                            "previous_response_not_found",
                        )
                    } else {
                        openai_error_payload(
                            format!("Proxy error: {e}"),
                            "server_error",
                            "internal_error",
                        )
                    };
                    let failed_response = ProxyServer::responses_failed_response(
                        response_id.clone(),
                        model_for_failures.clone(),
                        &responses_ctx_for_task,
                        responses_error_from_openai_error_payload(&payload),
                    );
                    send_responses_stream_event(
                        &stream_tx,
                        json!({
                            "type": "response.failed",
                            "response": failed_response
                        }),
                    );
                    let error_obj = payload
                        .get("error")
                        .cloned()
                        .unwrap_or_else(|| json!({"message":"internal error"}));
                    send_responses_stream_event(
                        &stream_tx,
                        json!({
                            "type": "error",
                            "error": error_obj
                        }),
                    );
                }
            }

            let _ = stream_tx.send(build_sse_done_chunk());
        });

        return Ok(build_live_sse_response(stream_rx));
    }

    match proxy.proxy_request(chat_req, request_ctx).await {
        Ok(chat_response) => {
            let response_id = format!("resp_{}", Uuid::new_v4());
            let response = ProxyServer::chat_to_responses_response(
                &chat_response,
                response_id.clone(),
                &responses_ctx,
            );
            if let Err(validation_error) = validate_responses_response_shape(&response) {
                log_structured(
                    "error",
                    "request.invalid_responses_payload",
                    Some(&request_id),
                    json!({
                        "conversation_key": conversation_key,
                        "error": validation_error.to_string()
                    }),
                );
                let payload = openai_error_payload(
                    "Proxy generated a non-compliant Responses API payload",
                    "server_error",
                    "internal_error",
                );
                return Ok(json_error_response(
                    warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                    payload,
                ));
            }
            proxy
                .set_response_conversation_key(response_id, conversation_key.clone())
                .await;

            let reply = warp::reply::json(&response);
            let reply = warp::reply::with_header(reply, "content-type", "application/json");
            let reply = warp::reply::with_header(reply, "access-control-allow-origin", "*");
            Ok(reply.into_response())
        }
        Err(e) => {
            log_structured(
                "error",
                "request.failed",
                Some(&request_id),
                json!({
                    "conversation_key": conversation_key,
                    "previous_response_id": previous_response_id,
                    "error": e.to_string()
                }),
            );
            let (status, payload) =
                if let Some(not_found_message) = parse_previous_response_not_found_error(&e) {
                    (
                        warp::http::StatusCode::BAD_REQUEST,
                        openai_error_payload(
                            not_found_message,
                            "invalid_request_error",
                            "previous_response_not_found",
                        ),
                    )
                } else {
                    (
                        warp::http::StatusCode::INTERNAL_SERVER_ERROR,
                        openai_error_payload(
                            format!("Proxy error: {e}"),
                            "server_error",
                            "internal_error",
                        ),
                    )
                };
            Ok(json_error_response(status, payload))
        }
    }
}

pub(super) async fn handle_chat_completions_create(
    path_str: &str,
    headers: warp::http::HeaderMap,
    body: bytes::Bytes,
    proxy: ProxyServer,
    request_id: String,
) -> Result<warp::reply::Response, warp::Rejection> {
    log_structured(
        "info",
        "request.chat_completions.matched",
        Some(&request_id),
        json!({
            "path": path_str,
            "body_size": body.len()
        }),
    );

    let chat_req: ChatCompletionsRequest = match serde_json::from_slice(&body) {
        Ok(req) => req,
        Err(e) => {
            log_structured(
                "warn",
                "request.parse_error",
                Some(&request_id),
                json!({
                    "path": path_str,
                    "error": e.to_string()
                }),
            );
            return Ok(warp::reply::with_status(
                "Invalid JSON",
                warp::http::StatusCode::BAD_REQUEST,
            )
            .into_response());
        }
    };

    log_structured(
        "info",
        "request.chat_completions.parsed",
        Some(&request_id),
        json!({
            "path": path_str,
            "model": chat_req.model,
            "message_count": chat_req.messages.len(),
            "stream": chat_req.stream.unwrap_or(false)
        }),
    );
    let conversation_key = extract_conversation_key(&chat_req, &headers);
    let request_ctx = RequestContext {
        request_id: request_id.clone(),
        conversation_key: conversation_key.clone(),
        previous_response_id: None,
        require_existing_thread: false,
        responses_stream_output_item_id: None,
        responses_stream_output_item_added: None,
    };

    if chat_req.stream.unwrap_or(false) {
        match proxy.proxy_request(chat_req, request_ctx).await {
            Ok(response) => {
                let sse_response = build_sse_from_chat_response(&response);
                let reply =
                    warp::reply::with_header(sse_response, "content-type", "text/event-stream");
                let reply = warp::reply::with_header(reply, "cache-control", "no-cache");
                let reply = warp::reply::with_header(reply, "connection", "keep-alive");
                let reply = warp::reply::with_header(reply, "access-control-allow-origin", "*");
                Ok(reply.into_response())
            }
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
                Ok(proxy_error_response(e))
            }
        }
    } else {
        match proxy.proxy_request(chat_req, request_ctx).await {
            Ok(response) => {
                let reply = warp::reply::json(&response);
                let reply = warp::reply::with_header(reply, "content-type", "application/json");
                let reply = warp::reply::with_header(reply, "access-control-allow-origin", "*");
                Ok(reply.into_response())
            }
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
                Ok(proxy_error_response(e))
            }
        }
    }
}

fn proxy_error_response(error: anyhow::Error) -> warp::reply::Response {
    let reply = warp::reply::json(&json!({
        "error": {
            "message": format!("Proxy error: {}", error),
            "type": "proxy_error",
            "code": "internal_error"
        }
    }));
    let reply = warp::reply::with_header(reply, "content-type", "application/json");
    let reply = warp::reply::with_header(reply, "access-control-allow-origin", "*");
    reply.into_response()
}
