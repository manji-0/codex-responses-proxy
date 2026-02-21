type WsStream = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

#[derive(Clone, Copy, Debug, ValueEnum)]
#[value(rename_all = "kebab-case")]
enum AppServerSandboxMode {
    ReadOnly,
    WorkspaceWrite,
    DangerFullAccess,
}

impl AppServerSandboxMode {
    fn as_config_value(self) -> &'static str {
        match self {
            AppServerSandboxMode::ReadOnly => "read-only",
            AppServerSandboxMode::WorkspaceWrite => "workspace-write",
            AppServerSandboxMode::DangerFullAccess => "danger-full-access",
        }
    }

    fn as_turn_policy_value(self) -> &'static str {
        match self {
            AppServerSandboxMode::ReadOnly => "readOnly",
            AppServerSandboxMode::WorkspaceWrite => "workspaceWrite",
            AppServerSandboxMode::DangerFullAccess => "dangerFullAccess",
        }
    }
}

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Port to listen on
    #[arg(short, long, default_value = "8080")]
    port: u16,

    /// Host/IP to bind to
    #[arg(long, default_value = "127.0.0.1")]
    bind: IpAddr,

    /// Path to Codex auth.json file
    #[arg(long, default_value = "~/.codex/auth.json")]
    auth_path: String,

    /// Existing codex app-server WebSocket URL (optional)
    #[arg(long)]
    app_server_url: Option<String>,

    /// WebSocket URL used by the managed background app-server
    #[arg(long, default_value = "ws://127.0.0.1:39200")]
    managed_app_server_url: String,

    /// Codex executable path used when spawning app-server
    #[arg(long, default_value = "codex")]
    codex_bin: String,

    /// Working directory for managed codex app-server (optional)
    #[arg(long)]
    app_server_cwd: Option<String>,

    /// Sandbox mode used for managed codex app-server and turn overrides
    #[arg(long, value_enum, default_value_t = AppServerSandboxMode::WorkspaceWrite)]
    app_server_sandbox_mode: AppServerSandboxMode,

    /// Maximum number of concurrent in-flight chat requests
    #[arg(long, default_value = "8")]
    max_concurrency: usize,

    /// Maximum number of in-memory conversation/thread history mappings
    #[arg(long, default_value = "30")]
    max_thread_history: usize,

    /// Per-request timeout in seconds
    #[arg(long, default_value = "300")]
    request_timeout_secs: u64,

    /// Model used for managed app-server warmup thread/start
    #[arg(long, default_value = "gpt-5")]
    warmup_model: String,
}

/// Chat Completions API format (what CLINE sends)
#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct ChatCompletionsRequest {
    model: String,
    messages: Vec<ChatMessage>,
    temperature: Option<f32>,
    max_tokens: Option<i32>,
    stream: Option<bool>,
    tools: Option<Vec<Value>>,
    tool_choice: Option<Value>,
    #[serde(default)]
    user: Option<String>,
    #[serde(default)]
    metadata: Option<Value>,
    #[serde(default)]
    conversation_key: Option<String>,
}

/// Responses API format (default interface)
#[derive(Deserialize, Debug)]
#[allow(dead_code)]
struct ResponsesCreateRequest {
    model: String,
    #[serde(default)]
    input: Option<Value>,
    #[serde(default)]
    instructions: Option<Value>,
    #[serde(default)]
    stream: Option<bool>,
    #[serde(default)]
    tools: Option<Vec<Value>>,
    #[serde(default)]
    tool_choice: Option<Value>,
    #[serde(default)]
    max_output_tokens: Option<i32>,
    #[serde(default)]
    store: Option<bool>,
    #[serde(default)]
    temperature: Option<f32>,
    #[serde(default)]
    top_p: Option<f32>,
    #[serde(default)]
    truncation: Option<String>,
    #[serde(default)]
    text: Option<Value>,
    #[serde(default)]
    parallel_tool_calls: Option<bool>,
    #[serde(default)]
    reasoning: Option<Value>,
    #[serde(default)]
    previous_response_id: Option<String>,
    #[serde(default)]
    conversation: Option<Value>,
    #[serde(default)]
    user: Option<String>,
    #[serde(default)]
    metadata: Option<Value>,
    #[serde(default)]
    conversation_key: Option<String>,
}

#[derive(Deserialize, Debug)]
struct ChatMessage {
    role: String,
    #[serde(default)]
    content: Value,
    #[serde(default)]
    tool_calls: Option<Vec<IncomingToolCall>>,
    #[serde(default)]
    tool_call_id: Option<String>,
}

#[derive(Deserialize, Debug)]
struct IncomingToolCall {
    #[serde(default)]
    id: Option<String>,
    #[serde(rename = "type", default)]
    _tool_type: Option<String>,
    #[serde(default)]
    function: Option<IncomingToolFunction>,
}

#[derive(Deserialize, Debug)]
struct IncomingToolFunction {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    arguments: Option<String>,
}

/// Chat Completions API response format (what CLINE expects)
#[derive(Serialize, Debug)]
struct ChatCompletionsResponse {
    id: String,
    object: String,
    created: i64,
    model: String,
    choices: Vec<Choice>,
    usage: Option<Usage>,
}

#[derive(Serialize, Debug)]
struct Choice {
    index: i32,
    message: ChatResponseMessage,
    finish_reason: Option<String>,
}

#[derive(Serialize, Debug)]
struct ChatResponseMessage {
    role: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    content: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_calls: Option<Vec<ChatToolCall>>,
}

#[derive(Serialize, Debug)]
struct ChatToolCall {
    id: String,
    #[serde(rename = "type")]
    tool_type: String,
    function: ChatToolCallFunction,
}

#[derive(Serialize, Debug)]
struct ChatToolCallFunction {
    name: String,
    arguments: String,
}

#[derive(Serialize, Debug, Clone)]
struct Usage {
    prompt_tokens: i32,
    completion_tokens: i32,
    total_tokens: i32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ResponsesInputTokensDetails {
    cached_tokens: i32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ResponsesOutputTokensDetails {
    reasoning_tokens: i32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ResponsesUsage {
    input_tokens: i32,
    input_tokens_details: ResponsesInputTokensDetails,
    output_tokens: i32,
    output_tokens_details: ResponsesOutputTokensDetails,
    total_tokens: i32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ResponsesError {
    code: String,
    message: String,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct ResponsesCreateResponse {
    id: String,
    object: String,
    created_at: i64,
    status: String,
    error: Option<ResponsesError>,
    incomplete_details: Option<Value>,
    instructions: Option<String>,
    max_output_tokens: Option<i32>,
    model: String,
    output: Vec<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_text: Option<String>,
    parallel_tool_calls: bool,
    previous_response_id: Option<String>,
    reasoning: Value,
    store: bool,
    temperature: Option<f32>,
    text: Value,
    tool_choice: Value,
    tools: Vec<Value>,
    top_p: Option<f32>,
    truncation: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    usage: Option<ResponsesUsage>,
    user: Option<String>,
    metadata: Value,
}

#[derive(Debug, Clone)]
struct ResponsesResponseContext {
    instructions: Option<String>,
    max_output_tokens: Option<i32>,
    tools: Vec<Value>,
    tool_choice: Value,
    parallel_tool_calls: bool,
    previous_response_id: Option<String>,
    reasoning: Value,
    store: bool,
    temperature: Option<f32>,
    text: Value,
    top_p: Option<f32>,
    truncation: String,
    user: Option<String>,
    metadata: Value,
    created_at: Option<i64>,
    stream_output_item_id: Option<String>,
}

/// Codex auth.json structure
#[derive(Deserialize, Debug, Clone)]
struct AuthData {
    #[serde(rename = "OPENAI_API_KEY")]
    #[allow(dead_code)]
    api_key: Option<String>,
    tokens: Option<TokenData>,
}

#[derive(Deserialize, Debug, Clone)]
struct TokenData {
    access_token: String,
    account_id: String,
    #[allow(dead_code)]
    refresh_token: Option<String>,
}

struct BoundedHistoryMap<T> {
    entries: HashMap<String, T>,
    order: VecDeque<String>,
    max_entries: usize,
}

impl<T> BoundedHistoryMap<T> {
    fn new(max_entries: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            max_entries: max_entries.max(1),
        }
    }

    fn len(&self) -> usize {
        self.entries.len()
    }

    fn insert(&mut self, key: String, value: T) {
        self.entries.insert(key.clone(), value);
        self.touch(&key);
        self.evict_if_needed();
    }

    fn get_cloned(&mut self, key: &str) -> Option<T>
    where
        T: Clone,
    {
        let value = self.entries.get(key).cloned();
        if value.is_some() {
            self.touch(key);
        }
        value
    }

    fn touch(&mut self, key: &str) {
        self.order.retain(|existing| existing != key);
        self.order.push_back(key.to_string());
    }

    fn evict_if_needed(&mut self) {
        while self.entries.len() > self.max_entries {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };
            self.entries.remove(&oldest);
        }
    }
}

struct ProxyServer {
    auth_data: Arc<Mutex<AuthData>>,
    auth_path: String,
    app_server: AppServerEndpoint,
    app_server_cwd: Option<PathBuf>,
    app_server_sandbox_mode: AppServerSandboxMode,
    thread_sessions: Arc<Mutex<BoundedHistoryMap<ThreadSession>>>,
    response_conversation_keys: Arc<Mutex<BoundedHistoryMap<String>>>,
    warmup_connection: Arc<Mutex<Option<WarmupConnection>>>,
    request_semaphore: Arc<Semaphore>,
    max_concurrency: usize,
    max_thread_history: usize,
    request_timeout: Duration,
    warmup_model: String,
}

enum AppServerEndpoint {
    External { url: String },
    Managed { server: Arc<ManagedAppServer> },
}

struct ManagedAppServer {
    ws_url: String,
    codex_bin: String,
    app_server_cwd: Option<PathBuf>,
    app_server_sandbox_mode: AppServerSandboxMode,
    state: Mutex<ManagedAppServerState>,
}

struct ManagedAppServerState {
    child: Child,
    restart_count: u64,
    last_restart_at: i64,
}

struct ManagedAppServerStatus {
    ws_url: String,
    pid: Option<u32>,
    running: bool,
    restart_count: u64,
    last_restart_at: i64,
}

#[derive(Clone)]
struct ThreadSession {
    thread_id: String,
    model: String,
    developer_instructions: String,
}

struct WarmupConnection {
    thread_id: String,
    model: String,
    ws: WsStream,
}

#[derive(Clone)]
struct RequestContext {
    request_id: String,
    conversation_key: Option<String>,
    previous_response_id: Option<String>,
    require_existing_thread: bool,
    responses_stream_output_item_id: Option<String>,
    responses_stream_output_item_added: Option<Arc<AtomicBool>>,
}

struct AppServerTurnResult {
    content: String,
    usage: Option<Usage>,
    tool_call: Option<ChatToolCall>,
    finish_reason: String,
}

struct ProxyServerConfig {
    auth_path: String,
    app_server_url: Option<String>,
    managed_app_server_url: String,
    codex_bin: String,
    app_server_cwd: Option<String>,
    app_server_sandbox_mode: AppServerSandboxMode,
    max_concurrency: usize,
    max_thread_history: usize,
    request_timeout_secs: u64,
    warmup_model: String,
}

#[derive(Clone, Copy)]
struct TurnInput<'a> {
    developer_instructions: &'a str,
    turn_prompt: &'a str,
}

#[derive(Clone)]
struct StartTurnOptions<'a> {
    request_id: &'a str,
    responses_stream_sender: Option<mpsc::UnboundedSender<String>>,
    responses_stream_output_item_id: Option<&'a str>,
    responses_stream_output_item_added: Option<&'a Arc<AtomicBool>>,
}

