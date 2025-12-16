use anyhow::{anyhow, Result};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, Mutex, Semaphore};
use tracing::{debug, error, warn};

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

        let result = self.run_in_box(
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
        self.init_cgroup(box_id).await?;

        let config = language.config();
        let box_dir = self.box_dir(box_id);
        let cgroup_path = self.cgroup_dir(box_id);

        let time_limit_sec = (time_limit as f64 / 1000.0).max(1.0);
        let memory_mb = memory_limit;

        let mut args = self.base_nsjail_args(box_id, time_limit_sec, memory_mb);
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

                    let memory = tokio::fs::read_to_string(ctx.cgroup_path.join("memory.peak"))
                        .await
                        .ok()
                        .and_then(|s| s.trim().parse::<u64>().ok())
                        .map(|bytes| (bytes / 1024) as u32)
                        .unwrap_or(0);
                    let _ = tokio::fs::remove_dir(&ctx.cgroup_path).await;

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

    fn cgroup_dir(&self, box_id: u32) -> PathBuf {
        PathBuf::from(format!("{}/nsjail-{}", CGROUP_ROOT, box_id))
    }

    async fn init_cgroup(&self, box_id: u32) -> Result<()> {
        let cgroup_dir = self.cgroup_dir(box_id);
        debug!(box_id = %box_id, cgroup_dir = %cgroup_dir.display(), "[Sandbox] Initializing cgroup");
        let _ = tokio::fs::remove_dir(&cgroup_dir).await;
        tokio::fs::create_dir_all(&cgroup_dir).await?;
        Ok(())
    }

    async fn cleanup_cgroup(&self, box_id: u32) {
        let cgroup_dir = self.cgroup_dir(box_id);
        debug!(box_id = %box_id, "[Sandbox] Cleaning up cgroup");
        let _ = tokio::fs::remove_dir(&cgroup_dir).await;
    }

    async fn read_memory_peak(&self, box_id: u32) -> u32 {
        let peak_file = self.cgroup_dir(box_id).join("memory.peak");
        let memory = tokio::fs::read_to_string(&peak_file)
            .await
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .map(|bytes| (bytes / 1024) as u32) // Convert to KB
            .unwrap_or(0);
        debug!(box_id = %box_id, memory_kb = %memory, peak_file = %peak_file.display(), "[Sandbox] Read memory peak");
        memory
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

    fn base_nsjail_args(&self, box_id: u32, time_limit_sec: f64, memory_mb: u32) -> Vec<String> {
        let box_dir = self.box_dir(box_id);
        let cgroup_dir = self.cgroup_dir(box_id);
        let memory_bytes = (memory_mb as u64) * 1024 * 1024;

        let mut args = vec![
            "-Mo".to_string(),
            "-Q".to_string(),
            "-t".to_string(),
            format!("{}", time_limit_sec.ceil() as u32),
            "--rlimit_cpu".to_string(),
            format!("{}", (time_limit_sec * 2.0).ceil() as u32),
            "--rlimit_as".to_string(),
            format!("{}", memory_mb),
            "--rlimit_nproc".to_string(),
            "64".to_string(),
            "--rlimit_nofile".to_string(),
            "64".to_string(),
            "--rlimit_fsize".to_string(),
            "10".to_string(),
            "--rlimit_stack".to_string(),
            "soft".to_string(),
            "--user".to_string(),
            "99999".to_string(),
            "--group".to_string(),
            "99999".to_string(),
            "--hostname".to_string(),
            "sandbox".to_string(),
            "--use_cgroupv2".to_string(),
            "--cgroupv2_mount".to_string(),
            cgroup_dir.to_string_lossy().to_string(),
            "--cgroup_mem_max".to_string(),
            format!("{}", memory_bytes),
            "-R".to_string(),
            "/bin".to_string(),
            "-R".to_string(),
            "/lib".to_string(),
        ];

        if std::path::Path::new("/lib64").exists() {
            args.extend(["-R".to_string(), "/lib64".to_string()]);
        }

        args.extend([
            "-R".to_string(),
            "/usr".to_string(),
            "-R".to_string(),
            "/etc/alternatives".to_string(),
            "-B".to_string(),
            format!("{}:/box", box_dir.display()),
            "--cwd".to_string(),
            "/box".to_string(),
            "--disable_clone_newnet".to_string(),
            "--env".to_string(),
            "PATH=/usr/local/bin:/usr/bin:/bin".to_string(),
            "--env".to_string(),
            "HOME=/box".to_string(),
            "--env".to_string(),
            "LANG=C.UTF-8".to_string(),
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
        debug!(
            box_id = %box_id,
            cmd = ?command,
            time_limit_ms = %time_limit_ms,
            memory_limit_mb = %memory_limit_mb,
            stdin_file = ?stdin_file,
            "[Sandbox] run_in_box start"
        );

        self.init_cgroup(box_id).await?;

        let box_dir = self.box_dir(box_id);
        let stdout_file = box_dir.join("stdout.txt");
        let stderr_file = box_dir.join("stderr.txt");

        let time_limit_sec = (time_limit_ms as f64 / 1000.0).max(1.0);

        let mut args = self.base_nsjail_args(box_id, time_limit_sec, memory_limit_mb);
        args.push("--".to_string());
        args.extend(command.iter().map(|s| s.to_string()));

        let start_time = Instant::now();

        let mut cmd = Command::new("nsjail");
        cmd.args(&args).current_dir(&box_dir);

        let (exit_code, elapsed, stdout, stderr) = if let Some(input_file) = stdin_file {
            let input_path = box_dir.join(input_file);
            let input_content = tokio::fs::read(&input_path).await.unwrap_or_default();
            cmd.stdin(Stdio::piped());

            let stdout_f = std::fs::File::create(&stdout_file)?;
            let stderr_f = std::fs::File::create(&stderr_file)?;
            cmd.stdout(stdout_f);
            cmd.stderr(stderr_f);

            let mut child = cmd.spawn()?;
            if let Some(mut stdin) = child.stdin.take() {
                let _ = stdin.write_all(&input_content).await;
            }
            let status = child.wait().await?;

            let elapsed = start_time.elapsed().as_millis() as u32;
            let exit_code = status.code().unwrap_or(-1);

            let stdout = tokio::fs::read_to_string(&stdout_file).await.unwrap_or_default();
            let stderr = tokio::fs::read_to_string(&stderr_file).await.unwrap_or_default();

            (exit_code, elapsed, stdout, stderr)
        } else {
            let stdout_f = std::fs::File::create(&stdout_file)?;
            let stderr_f = std::fs::File::create(&stderr_file)?;
            cmd.stdin(Stdio::null());
            cmd.stdout(stdout_f);
            cmd.stderr(stderr_f);

            let status = cmd.status().await?;
            let elapsed = start_time.elapsed().as_millis() as u32;
            let exit_code = status.code().unwrap_or(-1);

            let stdout = tokio::fs::read_to_string(&stdout_file).await.unwrap_or_default();
            let stderr = tokio::fs::read_to_string(&stderr_file).await.unwrap_or_default();

            (exit_code, elapsed, stdout, stderr)
        };

        let memory = self.read_memory_peak(box_id).await;
        self.cleanup_cgroup(box_id).await;

        let result = self.build_run_result(exit_code, elapsed, time_limit_ms, memory, stdout, stderr);
        debug!(
            box_id = %box_id,
            exit_code = %exit_code,
            elapsed_ms = %elapsed,
            memory_kb = %memory,
            status = ?result.status,
            "[Sandbox] run_in_box complete"
        );
        Ok(result)
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
