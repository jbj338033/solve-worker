use anyhow::{anyhow, Result};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, Mutex, Semaphore};
use tracing::{debug, error, info, warn};

static RUN_COUNTER: AtomicU64 = AtomicU64::new(0);

use crate::language::Language;

const SANDBOX_ROOT: &str = "/var/sandbox";
const CGROUP_ROOT: &str = "/sys/fs/cgroup";

pub struct Sandbox {
    semaphore: Arc<Semaphore>,
    box_pool: Arc<Mutex<Vec<u32>>>,
}

#[derive(Debug)]
pub struct RunResult {
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
    #[allow(dead_code)]
    pub exit_code: i32,
    pub time: u32,
    pub memory: u32,
    pub status: RunStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStatus {
    Ok,
    TimeLimitExceeded,
    MemoryLimitExceeded,
    RuntimeError,
    #[allow(dead_code)]
    InternalError,
}

pub struct CompileResult {
    pub success: bool,
    pub error: Option<String>,
    pub box_id: Option<u32>,
}

pub struct ExecuteHandle {
    pub stdin_tx: mpsc::Sender<String>,
    pub kill_tx: mpsc::Sender<()>,
}

pub enum ExecuteOutput {
    Stdout(String),
    Stderr(String),
    Complete { exit_code: i32, time: u32, memory: u32 },
}

struct ExecuteIoContext {
    stdin: tokio::process::ChildStdin,
    stdout: tokio::process::ChildStdout,
    stderr: tokio::process::ChildStderr,
    stdin_rx: mpsc::Receiver<String>,
    kill_rx: mpsc::Receiver<()>,
    child: Child,
    start_time: Instant,
    event_tx: mpsc::Sender<ExecuteOutput>,
    cgroup_path: PathBuf,
}

impl Sandbox {
    pub fn new(max_boxes: usize) -> Self {
        info!(max_boxes = %max_boxes, "[Sandbox] Initialized");
        Self {
            semaphore: Arc::new(Semaphore::new(max_boxes)),
            box_pool: Arc::new(Mutex::new((0..max_boxes as u32).collect())),
        }
    }

    pub async fn compile(&self, language: Language, code: &str) -> Result<CompileResult> {
        debug!(language = ?language, code_len = code.len(), "[Sandbox] Acquiring box for compile");
        let box_id = self.acquire_box().await?;
        debug!(box_id = %box_id, "[Sandbox] Box acquired");

        match self.compile_in_box(box_id, language, code).await {
            Ok(result) => {
                if !result.success {
                    debug!(box_id = %box_id, "[Sandbox] Compile failed, releasing box");
                    self.release_box(box_id).await;
                } else {
                    debug!(box_id = %box_id, "[Sandbox] Compile success");
                }
                Ok(result)
            }
            Err(e) => {
                error!(box_id = %box_id, error = %e, "[Sandbox] Compile error, releasing box");
                self.release_box(box_id).await;
                Err(e)
            }
        }
    }

    pub async fn run(
        &self,
        box_id: u32,
        language: Language,
        input: &str,
        time_limit: u32,
        memory_limit: u32,
    ) -> Result<RunResult> {
        debug!(
            box_id = %box_id,
            language = ?language,
            input_len = input.len(),
            time_limit = %time_limit,
            memory_limit = %memory_limit,
            "[Sandbox] Running code"
        );

        let config = language.config();
        let box_dir = self.box_dir(box_id);

        tokio::fs::write(box_dir.join("input.txt"), input).await?;

        let result = self
            .run_in_box(
                box_id,
                config.execute_command,
                time_limit,
                memory_limit,
                Some("input.txt"),
            )
            .await?;

        debug!(
            box_id = %box_id,
            status = ?result.status,
            time = %result.time,
            memory = %result.memory,
            exit_code = %result.exit_code,
            stdout_len = result.stdout.len(),
            stderr_len = result.stderr.len(),
            "[Sandbox] Run complete"
        );

        Ok(result)
    }

    pub async fn run_execute(
        &self,
        box_id: u32,
        language: Language,
        time_limit: u32,
        memory_limit: u32,
        event_tx: mpsc::Sender<ExecuteOutput>,
    ) -> Result<ExecuteHandle> {
        // Generate unique cgroup for this execution
        let run_id = RUN_COUNTER.fetch_add(1, Ordering::SeqCst);
        let cgroup_path = PathBuf::from(format!("{}/nsjail-{}-{}", CGROUP_ROOT, box_id, run_id));
        tokio::fs::create_dir_all(&cgroup_path).await?;

        let config = language.config();
        let box_dir = self.box_dir(box_id);

        let time_limit_sec = (time_limit as f64 / 1000.0).max(1.0);
        let memory_mb = memory_limit;

        let mut args = self.base_nsjail_args(box_id, &cgroup_path, time_limit_sec, memory_mb);
        args.push("--".to_string());
        args.extend(config.execute_command.iter().map(|s| s.to_string()));

        let mut child = Command::new("nsjail")
            .args(&args)
            .current_dir(&box_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        let stdin = child.stdin.take().unwrap();

        let (stdin_tx, stdin_rx) = mpsc::channel::<String>(32);
        let (kill_tx, kill_rx) = mpsc::channel::<()>(1);

        let ctx = ExecuteIoContext {
            stdin,
            stdout,
            stderr,
            stdin_rx,
            kill_rx,
            child,
            start_time: Instant::now(),
            event_tx,
            cgroup_path,
        };
        tokio::spawn(Self::execute_io_task(ctx));

        Ok(ExecuteHandle { stdin_tx, kill_tx })
    }

    async fn execute_io_task(mut ctx: ExecuteIoContext) {
        let (stdout_done_tx, mut stdout_done_rx) = mpsc::channel::<()>(1);
        let (stderr_done_tx, mut stderr_done_rx) = mpsc::channel::<()>(1);

        let event_tx_stdout = ctx.event_tx.clone();
        tokio::spawn(async move {
            let mut reader = BufReader::new(ctx.stdout);
            let mut line = String::new();
            while reader.read_line(&mut line).await.unwrap_or(0) > 0 {
                let _ = event_tx_stdout
                    .send(ExecuteOutput::Stdout(std::mem::take(&mut line)))
                    .await;
            }
            let _ = stdout_done_tx.send(()).await;
        });

        let event_tx_stderr = ctx.event_tx.clone();
        tokio::spawn(async move {
            let mut reader = BufReader::new(ctx.stderr);
            let mut line = String::new();
            while reader.read_line(&mut line).await.unwrap_or(0) > 0 {
                let _ = event_tx_stderr
                    .send(ExecuteOutput::Stderr(std::mem::take(&mut line)))
                    .await;
            }
            let _ = stderr_done_tx.send(()).await;
        });

        loop {
            tokio::select! {
                biased;
                result = ctx.child.wait() => {
                    let exit_code = result.map(|s| s.code().unwrap_or(-1)).unwrap_or(-1);
                    let _ = stdout_done_rx.recv().await;
                    let _ = stderr_done_rx.recv().await;
                    let elapsed = ctx.start_time.elapsed().as_millis() as u32;

                    let memory = Self::read_memory_peak_from(&ctx.cgroup_path).await;

                    // Cleanup cgroup asynchronously
                    let cgroup_path = ctx.cgroup_path.clone();
                    tokio::spawn(Self::cleanup_cgroup_dir(cgroup_path));

                    let _ = ctx.event_tx.send(ExecuteOutput::Complete {
                        exit_code,
                        time: elapsed,
                        memory,
                    }).await;
                    return;
                }
                kill = ctx.kill_rx.recv() => {
                    if kill.is_some() {
                        let _ = ctx.child.kill().await;
                    }
                }
                data = ctx.stdin_rx.recv() => {
                    if let Some(data) = data {
                        if ctx.stdin.write_all(data.as_bytes()).await.is_err() {
                            continue;
                        }
                        let _ = ctx.stdin.flush().await;
                    }
                }
            }
        }
    }

    pub async fn release(&self, box_id: u32) {
        self.release_box(box_id).await;
    }

    async fn acquire_box(&self) -> Result<u32> {
        debug!("[Sandbox] Waiting for available box");
        let permit = self.semaphore.clone().acquire_owned().await?;
        std::mem::forget(permit);

        let mut pool = self.box_pool.lock().await;
        let box_id = pool.pop().ok_or_else(|| anyhow!("No available boxes"))?;
        debug!(box_id = %box_id, available = pool.len(), "[Sandbox] Box acquired from pool");
        Ok(box_id)
    }

    async fn release_box(&self, box_id: u32) {
        debug!(box_id = %box_id, "[Sandbox] Releasing box");
        self.cleanup_box(box_id).await;
        let mut pool = self.box_pool.lock().await;
        pool.push(box_id);
        self.semaphore.add_permits(1);
        debug!(box_id = %box_id, available = pool.len(), "[Sandbox] Box returned to pool");
    }

    async fn cleanup_box(&self, box_id: u32) {
        let box_dir = self.box_dir(box_id);
        let _ = tokio::fs::remove_dir_all(&box_dir).await;
    }

    async fn init_box(&self, box_id: u32) -> Result<PathBuf> {
        let box_dir = self.box_dir(box_id);

        // Clean up any existing directory
        let _ = tokio::fs::remove_dir_all(&box_dir).await;

        // Create fresh directory
        tokio::fs::create_dir_all(&box_dir).await?;

        Ok(box_dir)
    }

    fn box_dir(&self, box_id: u32) -> PathBuf {
        PathBuf::from(format!("{}/box-{}", SANDBOX_ROOT, box_id))
    }

    async fn compile_in_box(
        &self,
        box_id: u32,
        language: Language,
        code: &str,
    ) -> Result<CompileResult> {
        let config = language.config();
        debug!(
            box_id = %box_id,
            language = ?language,
            source_file = %config.source_file,
            "[Sandbox] Compile in box"
        );

        let box_dir = self.init_box(box_id).await?;

        tokio::fs::write(box_dir.join(config.source_file), code).await?;
        debug!(box_id = %box_id, "[Sandbox] Source file written");

        let Some(compile_cmd) = config.compile_command else {
            debug!(box_id = %box_id, "[Sandbox] No compile step needed (interpreted language)");
            return Ok(CompileResult {
                success: true,
                error: None,
                box_id: Some(box_id),
            });
        };

        debug!(box_id = %box_id, cmd = ?compile_cmd, "[Sandbox] Running compile command");
        let result = self
            .run_in_box(box_id, compile_cmd, 30000, 512, None)
            .await?;

        if result.success {
            debug!(box_id = %box_id, "[Sandbox] Compile successful");
            Ok(CompileResult {
                success: true,
                error: None,
                box_id: Some(box_id),
            })
        } else {
            warn!(box_id = %box_id, stderr_len = result.stderr.len(), "[Sandbox] Compile failed");
            Ok(CompileResult {
                success: false,
                error: Some(result.stderr),
                box_id: None,
            })
        }
    }

    fn base_nsjail_args(
        &self,
        box_id: u32,
        cgroup_dir: &std::path::Path,
        time_limit_sec: f64,
        memory_mb: u32,
    ) -> Vec<String> {
        let box_dir = self.box_dir(box_id);
        let memory_bytes = (memory_mb as u64) * 1024 * 1024;

        let mut args = vec![
            // Mode: one-shot
            "-Mo".to_string(),
            // Quiet mode - suppress nsjail logs
            "--really_quiet".to_string(),
            // Time limit
            "-t".to_string(),
            format!("{}", time_limit_sec.ceil() as u32),
            // Resource limits
            "--rlimit_cpu".to_string(),
            format!("{}", (time_limit_sec * 2.0).ceil() as u32),
            "--rlimit_as".to_string(),
            "soft".to_string(), // Use soft limit for address space
            "--rlimit_nproc".to_string(),
            "64".to_string(),
            "--rlimit_nofile".to_string(),
            "64".to_string(),
            "--rlimit_fsize".to_string(),
            "10".to_string(),
            "--rlimit_stack".to_string(),
            "soft".to_string(),
            // User/Group
            "--user".to_string(),
            "99999".to_string(),
            "--group".to_string(),
            "99999".to_string(),
            "--hostname".to_string(),
            "sandbox".to_string(),
            // Cgroups v2 for memory limiting
            "--use_cgroupv2".to_string(),
            "--cgroupv2_mount".to_string(),
            cgroup_dir.to_string_lossy().to_string(),
            "--cgroup_mem_max".to_string(),
            format!("{}", memory_bytes),
            // Filesystem mounts - read-only system directories
            "-R".to_string(),
            "/bin".to_string(),
            "-R".to_string(),
            "/lib".to_string(),
        ];

        // /lib64 only exists on x86_64
        if std::path::Path::new("/lib64").exists() {
            args.extend(["-R".to_string(), "/lib64".to_string()]);
        }

        args.extend([
            "-R".to_string(),
            "/usr".to_string(),
            "-R".to_string(),
            "/etc".to_string(),
            // /proc - essential for many programs
            "--proc_rw".to_string(),
            // /dev - tmpfs with basic devices
            "--mount".to_string(),
            "none:/dev:tmpfs:size=4m".to_string(),
            "--symlink".to_string(),
            "/dev/null:/dev/null".to_string(),
            "--symlink".to_string(),
            "/dev/zero:/dev/zero".to_string(),
            "--symlink".to_string(),
            "/dev/urandom:/dev/urandom".to_string(),
            // /tmp - writable temp directory
            "--mount".to_string(),
            "none:/tmp:tmpfs:size=16m".to_string(),
            // Working directory
            "-B".to_string(),
            format!("{}:/box", box_dir.display()),
            "--cwd".to_string(),
            "/box".to_string(),
            // Network: use host network (already sandboxed in Docker)
            "--disable_clone_newnet".to_string(),
            // Environment variables
            "--env".to_string(),
            "PATH=/usr/local/bin:/usr/bin:/bin".to_string(),
            "--env".to_string(),
            "HOME=/box".to_string(),
            "--env".to_string(),
            "LANG=C.UTF-8".to_string(),
            "--env".to_string(),
            "LC_ALL=C.UTF-8".to_string(),
        ]);

        args
    }

    async fn run_in_box(
        &self,
        box_id: u32,
        command: &[&str],
        time_limit_ms: u32,
        memory_limit_mb: u32,
        stdin_file: Option<&str>,
    ) -> Result<RunResult> {
        // Generate unique cgroup for this run
        let run_id = RUN_COUNTER.fetch_add(1, Ordering::SeqCst);
        let cgroup_dir = PathBuf::from(format!("{}/nsjail-{}-{}", CGROUP_ROOT, box_id, run_id));

        debug!(
            box_id = %box_id,
            run_id = %run_id,
            cmd = ?command,
            time_limit_ms = %time_limit_ms,
            memory_limit_mb = %memory_limit_mb,
            stdin_file = ?stdin_file,
            "[Sandbox] run_in_box start"
        );

        // Create fresh cgroup directory
        tokio::fs::create_dir_all(&cgroup_dir).await?;

        let box_dir = self.box_dir(box_id);
        let time_limit_sec = (time_limit_ms as f64 / 1000.0).max(1.0);

        let mut args = self.base_nsjail_args(box_id, &cgroup_dir, time_limit_sec, memory_limit_mb);
        args.push("--".to_string());
        args.extend(command.iter().map(|s| s.to_string()));

        debug!(box_id = %box_id, run_id = %run_id, "[Sandbox] nsjail args: {:?}", args);

        let start_time = Instant::now();

        // Read input file content if specified
        let input_content = if let Some(input_file) = stdin_file {
            let input_path = box_dir.join(input_file);
            tokio::fs::read(&input_path).await.unwrap_or_default()
        } else {
            Vec::new()
        };

        // Spawn nsjail with piped I/O so we can capture everything
        let mut child = Command::new("nsjail")
            .args(&args)
            .current_dir(&box_dir)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        // Write input to stdin
        if let Some(mut stdin) = child.stdin.take() {
            let _ = stdin.write_all(&input_content).await;
            drop(stdin); // Close stdin to signal EOF
        }

        // Capture stdout and stderr
        let mut stdout_buf = Vec::new();
        let mut stderr_buf = Vec::new();

        if let Some(mut stdout) = child.stdout.take() {
            let _ = stdout.read_to_end(&mut stdout_buf).await;
        }
        if let Some(mut stderr) = child.stderr.take() {
            let _ = stderr.read_to_end(&mut stderr_buf).await;
        }

        let status = child.wait().await?;
        let elapsed = start_time.elapsed().as_millis() as u32;
        let exit_code = status.code().unwrap_or(-1);

        let stdout = String::from_utf8_lossy(&stdout_buf).to_string();
        let stderr = String::from_utf8_lossy(&stderr_buf).to_string();

        // Read memory peak from this run's unique cgroup
        let memory = Self::read_memory_peak_from(&cgroup_dir).await;

        // Cleanup cgroup asynchronously (fire and forget)
        tokio::spawn(Self::cleanup_cgroup_dir(cgroup_dir));

        let result = self.build_run_result(exit_code, elapsed, time_limit_ms, memory, stdout, stderr.clone());

        if exit_code != 0 {
            warn!(
                box_id = %box_id,
                run_id = %run_id,
                exit_code = %exit_code,
                elapsed_ms = %elapsed,
                memory_kb = %memory,
                status = ?result.status,
                stderr = %stderr.chars().take(1000).collect::<String>(),
                "[Sandbox] run_in_box failed"
            );
        } else {
            debug!(
                box_id = %box_id,
                run_id = %run_id,
                exit_code = %exit_code,
                elapsed_ms = %elapsed,
                memory_kb = %memory,
                status = ?result.status,
                "[Sandbox] run_in_box complete"
            );
        }
        Ok(result)
    }

    async fn read_memory_peak_from(cgroup_dir: &std::path::Path) -> u32 {
        let peak_file = cgroup_dir.join("memory.peak");
        tokio::fs::read_to_string(&peak_file)
            .await
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .map(|bytes| (bytes / 1024) as u32)
            .unwrap_or(0)
    }

    async fn cleanup_cgroup_dir(cgroup_dir: PathBuf) {
        for _ in 0..20 {
            if tokio::fs::remove_dir(&cgroup_dir).await.is_ok() {
                return;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
        }
        // If still can't remove, it will be cleaned up on container restart
    }

    fn build_run_result(
        &self,
        exit_code: i32,
        elapsed_ms: u32,
        time_limit_ms: u32,
        memory_kb: u32,
        stdout: String,
        stderr: String,
    ) -> RunResult {
        let status = if elapsed_ms > time_limit_ms {
            RunStatus::TimeLimitExceeded
        } else if exit_code == 137 || exit_code == 9 {
            RunStatus::MemoryLimitExceeded
        } else if exit_code != 0 {
            RunStatus::RuntimeError
        } else {
            RunStatus::Ok
        };

        RunResult {
            success: status == RunStatus::Ok,
            stdout,
            stderr,
            exit_code,
            time: elapsed_ms,
            memory: memory_kb,
            status,
        }
    }
}
