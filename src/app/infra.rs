impl ProxyServer {
    async fn run_app_server_turn_on_socket(
        &self,
        ws: &mut WsStream,
        chat_req: &ChatCompletionsRequest,
        ctx: &RequestContext,
        turn_input: TurnInput<'_>,
        responses_stream_sender: Option<mpsc::UnboundedSender<String>>,
    ) -> Result<AppServerTurnResult> {
        self.initialize_app_server(ws).await?;

        if let Some(conversation_key) = ctx.conversation_key.as_deref() {
            if let Some(session) = self.get_thread_session(conversation_key).await {
                let should_resume_existing_thread = ctx.require_existing_thread
                    || (session.model == chat_req.model
                        && session.developer_instructions == turn_input.developer_instructions);
                if should_resume_existing_thread {
                    match self
                        .resume_thread(
                            ws,
                            &session.thread_id,
                            chat_req,
                            turn_input.developer_instructions,
                        )
                        .await
                    {
                        Ok(resumed_thread_id) => match self
                            .start_turn(
                                ws,
                                &resumed_thread_id,
                                chat_req,
                                turn_input.turn_prompt,
                                StartTurnOptions {
                                    request_id: &ctx.request_id,
                                    responses_stream_sender: responses_stream_sender.clone(),
                                    responses_stream_output_item_id: ctx
                                        .responses_stream_output_item_id
                                        .as_deref(),
                                    responses_stream_output_item_added: ctx
                                        .responses_stream_output_item_added
                                        .as_ref(),
                                },
                            )
                            .await
                        {
                            Ok(result) => {
                                self.set_thread_session(
                                    conversation_key.to_string(),
                                    ThreadSession {
                                        thread_id: resumed_thread_id.clone(),
                                        model: chat_req.model.clone(),
                                        developer_instructions: turn_input
                                            .developer_instructions
                                            .to_string(),
                                    },
                                )
                                .await;
                                log_structured(
                                    "info",
                                    "thread.reused",
                                    Some(&ctx.request_id),
                                    json!({
                                        "conversation_key": conversation_key,
                                        "thread_id": resumed_thread_id
                                    }),
                                );
                                return Ok(result);
                            }
                            Err(err) => {
                                log_structured(
                                    "warn",
                                    "thread.reuse_failed",
                                    Some(&ctx.request_id),
                                    json!({
                                        "conversation_key": conversation_key,
                                        "thread_id": session.thread_id,
                                        "error": err.to_string()
                                    }),
                                );
                                if ctx.require_existing_thread {
                                    return Err(previous_response_not_found_error(
                                        ctx.previous_response_id.as_deref(),
                                    ));
                                }
                            }
                        },
                        Err(err) => {
                            log_structured(
                                "warn",
                                "thread.resume_failed",
                                Some(&ctx.request_id),
                                json!({
                                    "conversation_key": conversation_key,
                                    "thread_id": session.thread_id,
                                    "error": err.to_string()
                                }),
                            );
                            if ctx.require_existing_thread {
                                return Err(previous_response_not_found_error(
                                    ctx.previous_response_id.as_deref(),
                                ));
                            }
                        }
                    }
                }
            } else if ctx.require_existing_thread {
                return Err(previous_response_not_found_error(
                    ctx.previous_response_id.as_deref(),
                ));
            }
        }

        let thread_id = self
            .start_thread(ws, chat_req, turn_input.developer_instructions, false)
            .await
            .context("starting new app-server thread")?;
        log_structured(
            "info",
            "thread.started",
            Some(&ctx.request_id),
            json!({
                "conversation_key": ctx.conversation_key.clone(),
                "thread_id": thread_id
            }),
        );
        let result = self
            .start_turn(
                ws,
                &thread_id,
                chat_req,
                turn_input.turn_prompt,
                StartTurnOptions {
                    request_id: &ctx.request_id,
                    responses_stream_sender,
                    responses_stream_output_item_id: ctx.responses_stream_output_item_id.as_deref(),
                    responses_stream_output_item_added: ctx
                        .responses_stream_output_item_added
                        .as_ref(),
                },
            )
            .await?;

        if let Some(conversation_key) = ctx.conversation_key.as_deref() {
            self.set_thread_session(
                conversation_key.to_string(),
                ThreadSession {
                    thread_id,
                    model: chat_req.model.clone(),
                    developer_instructions: turn_input.developer_instructions.to_string(),
                },
            )
            .await;
        }

        Ok(result)
    }

    async fn run_warmup_turn_on_socket(
        &self,
        ws: &mut WsStream,
        thread_id: &str,
        chat_req: &ChatCompletionsRequest,
        ctx: &RequestContext,
        turn_input: TurnInput<'_>,
        responses_stream_sender: Option<mpsc::UnboundedSender<String>>,
    ) -> Result<AppServerTurnResult> {
        let result = self
            .start_turn(
                ws,
                thread_id,
                chat_req,
                turn_input.turn_prompt,
                StartTurnOptions {
                    request_id: &ctx.request_id,
                    responses_stream_sender,
                    responses_stream_output_item_id: ctx.responses_stream_output_item_id.as_deref(),
                    responses_stream_output_item_added: ctx
                        .responses_stream_output_item_added
                        .as_ref(),
                },
            )
            .await?;

        if let Some(conversation_key) = ctx.conversation_key.as_deref() {
            self.set_thread_session(
                conversation_key.to_string(),
                ThreadSession {
                    thread_id: thread_id.to_string(),
                    model: chat_req.model.clone(),
                    developer_instructions: turn_input.developer_instructions.to_string(),
                },
            )
            .await;
        }

        log_structured(
            "info",
            "thread.warmup_reused",
            Some(&ctx.request_id),
            json!({
                "thread_id": thread_id
            }),
        );

        Ok(result)
    }

    async fn take_compatible_warmup_connection(
        &self,
        chat_req: &ChatCompletionsRequest,
        developer_instructions: &str,
    ) -> Option<WarmupConnection> {
        if !developer_instructions.trim().is_empty() {
            return None;
        }
        if chat_req
            .tools
            .as_ref()
            .is_some_and(|tools| !tools.is_empty())
        {
            return None;
        }

        let mut slot = self.warmup_connection.lock().await;
        let is_compatible = slot
            .as_ref()
            .map(|connection| connection.model == chat_req.model)
            .unwrap_or(false);
        if !is_compatible {
            return None;
        }
        slot.take()
    }

    async fn start_thread(
        &self,
        ws: &mut WsStream,
        chat_req: &ChatCompletionsRequest,
        developer_instructions: &str,
        ephemeral: bool,
    ) -> Result<String> {
        let effective_cwd = self.effective_app_server_cwd()?;
        let mut thread_params = Map::new();
        thread_params.insert("approvalPolicy".to_string(), json!("never"));
        thread_params.insert(
            "sandbox".to_string(),
            json!(self.app_server_sandbox_mode.as_config_value()),
        );
        thread_params.insert("model".to_string(), json!(chat_req.model));
        thread_params.insert("ephemeral".to_string(), json!(ephemeral));
        thread_params.insert(
            "cwd".to_string(),
            json!(effective_cwd.display().to_string()),
        );
        if !developer_instructions.trim().is_empty() {
            thread_params.insert(
                "developerInstructions".to_string(),
                json!(developer_instructions),
            );
        }

        let dynamic_tools = Self::translate_tools_for_app_server(chat_req.tools.as_deref());
        if !dynamic_tools.is_empty() {
            thread_params.insert("dynamicTools".to_string(), Value::Array(dynamic_tools));
        }

        send_ws_json(
            ws,
            json!({
                "id": 2,
                "method": "thread/start",
                "params": Value::Object(thread_params),
            }),
        )
        .await?;

        let thread_response = self.wait_for_rpc_response(ws, 2).await?;
        thread_response
            .pointer("/result/thread/id")
            .and_then(Value::as_str)
            .filter(|v| !v.trim().is_empty())
            .map(str::to_string)
            .ok_or_else(|| anyhow!("thread/start response missing result.thread.id"))
    }

    async fn warmup_managed_app_server(&self) -> Result<WarmupConnection> {
        if !matches!(self.app_server, AppServerEndpoint::Managed { .. }) {
            return Err(anyhow!("warmup is only supported for managed app-server"));
        }

        let mut ws = self
            .connect_app_server()
            .await
            .context("connecting app-server for warmup")?;

        let warmup_request = ChatCompletionsRequest {
            model: self.warmup_model.clone(),
            messages: Vec::new(),
            temperature: None,
            max_tokens: None,
            stream: None,
            tools: None,
            tool_choice: None,
            user: None,
            metadata: None,
            conversation_key: None,
        };

        self.initialize_app_server(&mut ws).await?;
        let thread_id = self
            .start_thread(&mut ws, &warmup_request, "", false)
            .await?;
        log_structured(
            "info",
            "app_server.warmup.thread_started",
            None,
            json!({
                "thread_id": thread_id,
                "model": self.warmup_model
            }),
        );

        Ok(WarmupConnection {
            thread_id,
            model: self.warmup_model.clone(),
            ws,
        })
    }

    async fn resume_thread(
        &self,
        ws: &mut WsStream,
        thread_id: &str,
        chat_req: &ChatCompletionsRequest,
        developer_instructions: &str,
    ) -> Result<String> {
        let effective_cwd = self.effective_app_server_cwd()?;
        let mut resume_params = Map::new();
        resume_params.insert("threadId".to_string(), json!(thread_id));
        resume_params.insert("approvalPolicy".to_string(), json!("never"));
        resume_params.insert(
            "sandbox".to_string(),
            json!(self.app_server_sandbox_mode.as_config_value()),
        );
        resume_params.insert("model".to_string(), json!(chat_req.model));
        resume_params.insert(
            "cwd".to_string(),
            json!(effective_cwd.display().to_string()),
        );
        if !developer_instructions.trim().is_empty() {
            resume_params.insert(
                "developerInstructions".to_string(),
                json!(developer_instructions),
            );
        }

        let dynamic_tools = Self::translate_tools_for_app_server(chat_req.tools.as_deref());
        if !dynamic_tools.is_empty() {
            resume_params.insert("dynamicTools".to_string(), Value::Array(dynamic_tools));
        }

        send_ws_json(
            ws,
            json!({
                "id": 2,
                "method": "thread/resume",
                "params": Value::Object(resume_params),
            }),
        )
        .await?;

        let thread_response = self.wait_for_rpc_response(ws, 2).await?;
        thread_response
            .pointer("/result/thread/id")
            .and_then(Value::as_str)
            .filter(|v| !v.trim().is_empty())
            .map(str::to_string)
            .ok_or_else(|| anyhow!("thread/resume response missing result.thread.id"))
    }

    async fn start_turn(
        &self,
        ws: &mut WsStream,
        thread_id: &str,
        chat_req: &ChatCompletionsRequest,
        turn_prompt: &str,
        options: StartTurnOptions<'_>,
    ) -> Result<AppServerTurnResult> {
        let effective_cwd = self.effective_app_server_cwd()?;
        let sandbox_policy = self.build_turn_sandbox_policy(&effective_cwd);
        send_ws_json(
            ws,
            json!({
                "id": 3,
                "method": "turn/start",
                "params": {
                    "threadId": thread_id,
                    "input": [{
                        "type": "text",
                        "text": turn_prompt,
                        "text_elements": []
                    }],
                    "model": chat_req.model,
                    "effort": "medium",
                    "cwd": effective_cwd.display().to_string(),
                    "approvalPolicy": "never",
                    "sandboxPolicy": sandbox_policy
                },
            }),
        )
        .await?;

        let mut response_content = String::new();
        let mut last_agent_text = String::new();
        let mut usage: Option<Usage> = None;
        let mut last_error_message = String::new();
        let mut streamed_output_item_announced = false;

        loop {
            let msg = read_ws_json(ws).await?;

            if json_id_matches(msg.get("id"), 3) {
                if let Some(err_obj) = msg.get("error") {
                    return Err(anyhow!(
                        "turn/start RPC error: {}",
                        rpc_error_message(err_obj)
                    ));
                }
                continue;
            }

            let method = msg.get("method").and_then(Value::as_str).unwrap_or("");

            // Server-initiated requests include both id and method.
            if !method.is_empty() && msg.get("id").is_some() {
                if let Some(tool_call) = self.handle_server_request(ws, &msg).await? {
                    return Ok(AppServerTurnResult {
                        content: String::new(),
                        usage,
                        tool_call: Some(tool_call),
                        finish_reason: "tool_calls".to_string(),
                    });
                }
                continue;
            }

            if method.is_empty() {
                continue;
            }

            match method {
                "item/agentMessage/delta" => {
                    if let Some(delta) = msg.pointer("/params/delta").and_then(Value::as_str) {
                        response_content.push_str(delta);
                        if let Some(sender) = &options.responses_stream_sender {
                            if let Some(item_id) = options.responses_stream_output_item_id {
                                if !streamed_output_item_announced {
                                    send_responses_stream_event(
                                        sender,
                                        json!({
                                            "type": "response.output_item.added",
                                            "output_index": 0,
                                            "item": {
                                                "id": item_id,
                                                "type": "message",
                                                "status": "in_progress",
                                                "role": "assistant",
                                                "content": []
                                            }
                                        }),
                                    );
                                    streamed_output_item_announced = true;
                                    if let Some(flag) = options.responses_stream_output_item_added {
                                        flag.store(true, Ordering::Relaxed);
                                    }
                                }
                                send_responses_stream_event(
                                    sender,
                                    json!({
                                        "type": "response.output_text.delta",
                                        "item_id": item_id,
                                        "output_index": 0,
                                        "content_index": 0,
                                        "delta": delta
                                    }),
                                );
                            }
                        }
                    }
                }
                "item/completed" => {
                    if let Some(item_type) =
                        msg.pointer("/params/item/type").and_then(Value::as_str)
                    {
                        if item_type == "agentMessage" {
                            if let Some(text) =
                                msg.pointer("/params/item/text").and_then(Value::as_str)
                            {
                                last_agent_text = text.to_string();
                            }
                        }
                    }
                }
                "thread/tokenUsage/updated" => {
                    if let Some(u) = Self::parse_usage_from_token_update(
                        msg.get("params").unwrap_or(&Value::Null),
                    ) {
                        usage = Some(u);
                    }
                }
                "error" => {
                    if let Some(err_message) = msg
                        .pointer("/params/error/message")
                        .and_then(Value::as_str)
                        .or_else(|| msg.pointer("/params/message").and_then(Value::as_str))
                    {
                        last_error_message = err_message.to_string();
                    }
                }
                "turn/completed" => {
                    let status = msg
                        .pointer("/params/turn/status")
                        .and_then(Value::as_str)
                        .unwrap_or_default();

                    if status.eq_ignore_ascii_case("failed") {
                        let message = msg
                            .pointer("/params/turn/error/message")
                            .and_then(Value::as_str)
                            .map(str::to_string)
                            .filter(|s| !s.trim().is_empty())
                            .or_else(|| {
                                if last_error_message.trim().is_empty() {
                                    None
                                } else {
                                    Some(last_error_message.clone())
                                }
                            })
                            .unwrap_or_else(|| "turn failed".to_string());
                        log_structured(
                            "error",
                            "turn.failed",
                            Some(options.request_id),
                            json!({
                                "thread_id": thread_id,
                                "message": message
                            }),
                        );
                        return Err(anyhow!("codex app-server turn failed: {}", message));
                    }

                    let content = if response_content.trim().is_empty() {
                        last_agent_text.trim().to_string()
                    } else {
                        response_content.trim().to_string()
                    };

                    return Ok(AppServerTurnResult {
                        content,
                        usage,
                        tool_call: None,
                        finish_reason: "stop".to_string(),
                    });
                }
                _ => {}
            }
        }
    }

    async fn initialize_app_server(&self, ws: &mut WsStream) -> Result<()> {
        send_ws_json(
            ws,
            json!({
                "id": 1,
                "method": "initialize",
                "params": {
                    "clientInfo": {
                        "name": "codex-responses-proxy",
                        "version": env!("CARGO_PKG_VERSION")
                    },
                    "capabilities": {
                        "experimentalApi": true
                    }
                }
            }),
        )
        .await?;

        self.wait_for_rpc_response(ws, 1).await?;
        send_ws_json(ws, json!({ "method": "initialized" })).await?;

        Ok(())
    }

    async fn wait_for_rpc_response(&self, ws: &mut WsStream, request_id: i64) -> Result<Value> {
        loop {
            let msg = read_ws_json(ws).await?;
            if json_id_matches(msg.get("id"), request_id) {
                if let Some(err_obj) = msg.get("error") {
                    return Err(anyhow!("rpc error: {}", rpc_error_message(err_obj)));
                }
                return Ok(msg);
            }

            let method = msg.get("method").and_then(Value::as_str).unwrap_or("");
            if !method.is_empty()
                && msg.get("id").is_some()
                && self.handle_server_request(ws, &msg).await?.is_some()
            {
                return Err(anyhow!(
                    "unexpected dynamic tool call while waiting for rpc response"
                ));
            }
        }
    }

    async fn handle_server_request(
        &self,
        ws: &mut WsStream,
        msg: &Value,
    ) -> Result<Option<ChatToolCall>> {
        let method = msg.get("method").and_then(Value::as_str).unwrap_or("");
        let id = msg
            .get("id")
            .cloned()
            .ok_or_else(|| anyhow!("server request missing id"))?;

        match method {
            "account/chatgptAuthTokens/refresh" => {
                let mut tokens = { self.auth_data.lock().await.tokens.clone() };
                if tokens.is_none() {
                    let _ = self.refresh_auth_data().await;
                    tokens = self.auth_data.lock().await.tokens.clone();
                }

                if let Some(tokens) = tokens {
                    send_ws_json(
                        ws,
                        json!({
                            "id": id,
                            "result": {
                                "accessToken": tokens.access_token,
                                "chatgptAccountId": tokens.account_id,
                                "chatgptPlanType": Value::Null
                            }
                        }),
                    )
                    .await?;
                } else {
                    send_ws_json(
                        ws,
                        json!({
                            "id": id,
                            "error": {
                                "code": -32000,
                                "message": "auth tokens unavailable after refresh"
                            }
                        }),
                    )
                    .await?;
                }
                Ok(None)
            }
            "item/commandExecution/requestApproval" => {
                send_ws_json(
                    ws,
                    json!({
                        "id": id,
                        "result": { "decision": "accept" }
                    }),
                )
                .await?;
                Ok(None)
            }
            "item/fileChange/requestApproval" => {
                send_ws_json(
                    ws,
                    json!({
                        "id": id,
                        "result": { "decision": "accept" }
                    }),
                )
                .await?;
                Ok(None)
            }
            "item/tool/requestUserInput" => {
                send_ws_json(
                    ws,
                    json!({
                        "id": id,
                        "result": { "answers": {} }
                    }),
                )
                .await?;
                Ok(None)
            }
            "item/tool/call" => {
                let params = msg.get("params").cloned().unwrap_or(Value::Null);
                let call_id = params
                    .get("callId")
                    .and_then(Value::as_str)
                    .unwrap_or("call-unknown")
                    .to_string();
                let tool_name = params
                    .get("tool")
                    .and_then(Value::as_str)
                    .unwrap_or("unknown_tool")
                    .to_string();
                let arguments_value = params
                    .get("arguments")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                let arguments_json =
                    serde_json::to_string(&arguments_value).unwrap_or_else(|_| "{}".to_string());

                Ok(Some(ChatToolCall {
                    id: call_id,
                    tool_type: "function".to_string(),
                    function: ChatToolCallFunction {
                        name: tool_name,
                        arguments: arguments_json,
                    },
                }))
            }
            _ => {
                send_ws_json(
                    ws,
                    json!({
                        "id": id,
                        "error": {
                            "code": -32601,
                            "message": format!("unsupported server request method: {}", method)
                        }
                    }),
                )
                .await?;
                Ok(None)
            }
        }
    }

    async fn connect_app_server(&self) -> Result<WsStream> {
        match &self.app_server {
            AppServerEndpoint::External { url } => {
                connect_ws_with_retry(url, Duration::from_secs(10))
                    .await
                    .with_context(|| format!("connecting to existing codex app-server at {}", url))
            }
            AppServerEndpoint::Managed { server } => server.connect().await,
        }
    }
    fn parse_usage_from_token_update(params: &Value) -> Option<Usage> {
        let last = params.pointer("/tokenUsage/last")?;

        let input = value_to_i32(last.get("inputTokens"));
        let cached = value_to_i32(last.get("cachedInputTokens"));
        let output = value_to_i32(last.get("outputTokens"));
        let mut total = value_to_i32(last.get("totalTokens"));

        if total == 0 && (input != 0 || cached != 0 || output != 0) {
            total = input + cached + output;
        }
        if total == 0 {
            return None;
        }

        Some(Usage {
            prompt_tokens: input + cached,
            completion_tokens: output,
            total_tokens: total,
        })
    }

    fn effective_app_server_cwd(&self) -> Result<PathBuf> {
        if let Some(cwd) = self.app_server_cwd.clone() {
            return Ok(cwd);
        }
        std::env::current_dir().context("failed to resolve process working directory")
    }

    fn build_turn_sandbox_policy(&self, effective_cwd: &Path) -> Value {
        match self.app_server_sandbox_mode {
            AppServerSandboxMode::ReadOnly => {
                json!({ "type": self.app_server_sandbox_mode.as_turn_policy_value() })
            }
            AppServerSandboxMode::WorkspaceWrite => json!({
                "type": self.app_server_sandbox_mode.as_turn_policy_value(),
                "writableRoots": [effective_cwd.display().to_string()],
                "networkAccess": true
            }),
            AppServerSandboxMode::DangerFullAccess => {
                json!({ "type": self.app_server_sandbox_mode.as_turn_policy_value() })
            }
        }
    }
}

impl ManagedAppServer {
    async fn start(
        codex_bin: String,
        ws_url: String,
        app_server_cwd: Option<PathBuf>,
        app_server_sandbox_mode: AppServerSandboxMode,
    ) -> Result<Self> {
        let mut child = spawn_app_server_child(
            &codex_bin,
            &ws_url,
            app_server_cwd.as_deref(),
            app_server_sandbox_mode,
        )?;
        let ws = connect_ws_with_retry(&ws_url, Duration::from_secs(10))
            .await
            .with_context(|| format!("waiting for spawned codex app-server at {}", ws_url))?;
        drop(ws);

        if let Some(status) = child
            .try_wait()
            .context("checking spawned codex app-server process status")?
        {
            return Err(anyhow!(
                "spawned codex app-server exited early with status {}",
                status
            ));
        }

        Ok(Self {
            ws_url,
            codex_bin,
            app_server_cwd,
            app_server_sandbox_mode,
            state: Mutex::new(ManagedAppServerState {
                child,
                restart_count: 0,
                last_restart_at: chrono::Utc::now().timestamp(),
            }),
        })
    }

    async fn ensure_running(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        if let Some(status) = state
            .child
            .try_wait()
            .context("checking managed codex app-server process status")?
        {
            log_structured(
                "warn",
                "app_server.managed.exited",
                None,
                json!({
                    "status": status.to_string(),
                    "action": "restart"
                }),
            );
            state.child = spawn_app_server_child(
                &self.codex_bin,
                &self.ws_url,
                self.app_server_cwd.as_deref(),
                self.app_server_sandbox_mode,
            )?;
            state.restart_count += 1;
            state.last_restart_at = chrono::Utc::now().timestamp();
        }
        Ok(())
    }

    async fn connect(&self) -> Result<WsStream> {
        self.ensure_running().await?;
        match connect_ws_with_retry(&self.ws_url, Duration::from_secs(2)).await {
            Ok(ws) => Ok(ws),
            Err(initial_err) => {
                log_structured(
                    "warn",
                    "app_server.managed.unreachable",
                    None,
                    json!({
                        "url": self.ws_url,
                        "error": initial_err.to_string(),
                        "action": "restart"
                    }),
                );
                self.restart().await?;
                connect_ws_with_retry(&self.ws_url, Duration::from_secs(10))
                    .await
                    .with_context(|| {
                        format!(
                            "reconnecting to restarted codex app-server at {}",
                            self.ws_url
                        )
                    })
            }
        }
    }

    async fn restart(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        terminate_child(&mut state.child).await;
        state.child = spawn_app_server_child(
            &self.codex_bin,
            &self.ws_url,
            self.app_server_cwd.as_deref(),
            self.app_server_sandbox_mode,
        )?;
        state.restart_count += 1;
        state.last_restart_at = chrono::Utc::now().timestamp();
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        let mut state = self.state.lock().await;
        terminate_child(&mut state.child).await;
        state.last_restart_at = chrono::Utc::now().timestamp();
        Ok(())
    }

    async fn status_snapshot(&self) -> ManagedAppServerStatus {
        let mut state = self.state.lock().await;
        let running = match state.child.try_wait() {
            Ok(None) => true,
            Ok(Some(_)) => false,
            Err(_) => false,
        };
        ManagedAppServerStatus {
            ws_url: self.ws_url.clone(),
            pid: if running { state.child.id() } else { None },
            running,
            restart_count: state.restart_count,
            last_restart_at: state.last_restart_at,
        }
    }
}

fn expand_home_path(path: &str) -> Result<String> {
    if path.starts_with("~/") {
        let home = std::env::var("HOME").context("HOME environment variable not set")?;
        Ok(path.replacen('~', &home, 1))
    } else {
        Ok(path.to_string())
    }
}

fn spawn_app_server_child(
    codex_bin: &str,
    ws_url: &str,
    app_server_cwd: Option<&Path>,
    app_server_sandbox_mode: AppServerSandboxMode,
) -> Result<Child> {
    let mut command = Command::new(codex_bin);
    command
        .arg("app-server")
        .arg("-c")
        .arg("approval_policy=\"never\"")
        .arg("-c")
        .arg(format!(
            "sandbox_mode=\"{}\"",
            app_server_sandbox_mode.as_config_value()
        ))
        .arg("--listen")
        .arg(ws_url)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);

    if let Some(cwd) = app_server_cwd {
        command.current_dir(cwd);
    }

    command.spawn().with_context(|| {
        let cwd_text = app_server_cwd
            .map(|p| p.display().to_string())
            .unwrap_or_else(|| "(inherited)".to_string());
        format!(
            "failed to spawn {} app-server (cwd: {}, sandbox_mode: {})",
            codex_bin,
            cwd_text,
            app_server_sandbox_mode.as_config_value()
        )
    })
}

fn resolve_working_directory(path: &str) -> Result<PathBuf> {
    let expanded = expand_home_path(path)?;
    let parsed = PathBuf::from(expanded);
    let resolved = if parsed.is_absolute() {
        parsed
    } else {
        std::env::current_dir()
            .context("failed to resolve process working directory for --app-server-cwd")?
            .join(parsed)
    };

    let metadata = std::fs::metadata(&resolved)
        .with_context(|| format!("failed to stat --app-server-cwd {}", resolved.display()))?;
    if !metadata.is_dir() {
        return Err(anyhow!(
            "--app-server-cwd must point to a directory, got {}",
            resolved.display()
        ));
    }

    Ok(resolved)
}

async fn connect_ws_with_retry(ws_url: &str, timeout: Duration) -> Result<WsStream> {
    let deadline = Instant::now() + timeout;

    loop {
        match connect_async(ws_url).await {
            Ok((ws, _)) => return Ok(ws),
            Err(err) => {
                let err_text = err.to_string();
                if Instant::now() >= deadline {
                    return Err(anyhow!("timed out connecting to {}: {}", ws_url, err_text));
                }
                sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

async fn signal_child_descendants(parent_pid: u32, signal: &str) {
    let signal_flag = format!("-{}", signal);
    let _ = Command::new("pkill")
        .arg(signal_flag)
        .arg("-P")
        .arg(parent_pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await;
}

async fn terminate_child(child: &mut Child) {
    if let Some(pid) = child.id() {
        signal_child_descendants(pid, "TERM").await;
        sleep(Duration::from_millis(150)).await;
        signal_child_descendants(pid, "KILL").await;
    }
    let _ = child.kill().await;
    let _ = child.wait().await;
}

fn value_to_i32(value: Option<&Value>) -> i32 {
    match value {
        Some(Value::Number(n)) => n.as_i64().unwrap_or(0) as i32,
        _ => 0,
    }
}

fn json_id_matches(id: Option<&Value>, expected: i64) -> bool {
    match id {
        Some(Value::Number(n)) => n.as_i64() == Some(expected),
        Some(Value::String(s)) => s.parse::<i64>().ok() == Some(expected),
        _ => false,
    }
}

fn rpc_error_message(error_obj: &Value) -> String {
    error_obj
        .get("message")
        .and_then(Value::as_str)
        .filter(|v| !v.trim().is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| "unknown rpc error".to_string())
}

async fn send_ws_json(ws: &mut WsStream, value: Value) -> Result<()> {
    ws.send(Message::Text(value.to_string().into()))
        .await
        .context("sending websocket message")
}

async fn read_ws_json(ws: &mut WsStream) -> Result<Value> {
    loop {
        let next = ws
            .next()
            .await
            .ok_or_else(|| anyhow!("websocket stream closed"))?;
        match next {
            Ok(Message::Text(text)) => {
                let value: Value =
                    serde_json::from_str(&text).context("parsing websocket text frame as json")?;
                return Ok(value);
            }
            Ok(Message::Binary(bytes)) => {
                let value: Value = serde_json::from_slice(&bytes)
                    .context("parsing websocket binary frame as json")?;
                return Ok(value);
            }
            Ok(Message::Ping(payload)) => {
                ws.send(Message::Pong(payload))
                    .await
                    .context("replying websocket pong")?;
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
