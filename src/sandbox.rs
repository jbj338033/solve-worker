use anyhow::{anyhow, Result};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, Mutex, Semaphore};

use crate::language::Language;

const SANDBOX_ROOT: &str = "/var/sandbox";

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
}

impl Sandbox {
    pub fn new(max_boxes: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_boxes)),
            box_pool: Arc::new(Mutex::new((0..max_boxes as u32).collect())),
        }
    }

    pub async fn compile(&self, language: Language, code: &str) -> Result<CompileResult> {
        let box_id = self.acquire_box().await?;

        match self.compile_in_box(box_id, language, code).await {
            Ok(result) => {
                if !result.success {
                    self.release_box(box_id).await;
                }
                Ok(result)
            }
            Err(e) => {
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
        let config = language.config();
        let box_dir = self.box_dir(box_id);

        tokio::fs::write(box_dir.join("input.txt"), input).await?;

        self.run_in_box(
            box_id,
            config.execute_command,
            time_limit,
            memory_limit,
            Some("input.txt"),
        )
        .await
    }

    pub async fn run_execute(
        &self,
        box_id: u32,
        language: Language,
        time_limit: u32,
        memory_limit: u32,
        event_tx: mpsc::Sender<ExecuteOutput>,
    ) -> Result<ExecuteHandle> {
        let config = language.config();
        let box_dir = self.box_dir(box_id);

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
                    let _ = ctx.event_tx.send(ExecuteOutput::Complete {
                        exit_code,
                        time: elapsed,
                        memory: 0,
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
        let permit = self.semaphore.clone().acquire_owned().await?;
        std::mem::forget(permit);

        let mut pool = self.box_pool.lock().await;
        pool.pop().ok_or_else(|| anyhow!("No available boxes"))
    }

    async fn release_box(&self, box_id: u32) {
        self.cleanup_box(box_id).await;
        let mut pool = self.box_pool.lock().await;
        pool.push(box_id);
        self.semaphore.add_permits(1);
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
        let box_dir = self.init_box(box_id).await?;

        tokio::fs::write(box_dir.join(config.source_file), code).await?;

        let Some(compile_cmd) = config.compile_command else {
            return Ok(CompileResult {
                success: true,
                error: None,
                box_id: Some(box_id),
            });
        };

        let result = self
            .run_in_box(box_id, compile_cmd, 30000, 512, None)
            .await?;

        if result.success {
            Ok(CompileResult {
                success: true,
                error: None,
                box_id: Some(box_id),
            })
        } else {
            Ok(CompileResult {
                success: false,
                error: Some(result.stderr),
                box_id: None,
            })
        }
    }

    fn base_nsjail_args(&self, box_id: u32, time_limit_sec: f64, memory_mb: u32) -> Vec<String> {
        let box_dir = self.box_dir(box_id);

        vec![
            // Mode: execute once
            "-Mo".to_string(),
            // Quiet mode (no nsjail logs to stderr)
            "-Q".to_string(),
            // Time limit (wall time)
            "-t".to_string(),
            format!("{}", time_limit_sec.ceil() as u32),
            // CPU time limit
            "--rlimit_cpu".to_string(),
            format!("{}", (time_limit_sec * 2.0).ceil() as u32),
            // Memory limit (address space in MB)
            "--rlimit_as".to_string(),
            format!("{}", memory_mb),
            // Max processes
            "--rlimit_nproc".to_string(),
            "64".to_string(),
            // Max open files
            "--rlimit_nofile".to_string(),
            "64".to_string(),
            // Max file size (10MB)
            "--rlimit_fsize".to_string(),
            "10".to_string(),
            // Stack size (unlimited for Java)
            "--rlimit_stack".to_string(),
            "soft".to_string(),
            // User/Group (run as nobody)
            "--user".to_string(),
            "99999".to_string(),
            "--group".to_string(),
            "99999".to_string(),
            // Hostname
            "--hostname".to_string(),
            "sandbox".to_string(),
            // Mount system directories read-only
            "-R".to_string(),
            "/bin".to_string(),
            "-R".to_string(),
            "/lib".to_string(),
            "-R".to_string(),
            "/lib64".to_string(),
            "-R".to_string(),
            "/usr".to_string(),
            "-R".to_string(),
            "/etc/alternatives".to_string(),
            // Mount box directory read-write
            "-B".to_string(),
            format!("{}:/box", box_dir.display()),
            // Working directory
            "--cwd".to_string(),
            "/box".to_string(),
            // Disable network
            "--disable_clone_newnet".to_string(),
            // Environment variables
            "--env".to_string(),
            "PATH=/usr/local/bin:/usr/bin:/bin".to_string(),
            "--env".to_string(),
            "HOME=/box".to_string(),
            "--env".to_string(),
            "LANG=C.UTF-8".to_string(),
        ]
    }

    async fn run_in_box(
        &self,
        box_id: u32,
        command: &[&str],
        time_limit_ms: u32,
        memory_limit_mb: u32,
        stdin_file: Option<&str>,
    ) -> Result<RunResult> {
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

        // Handle stdin
        if let Some(input_file) = stdin_file {
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

            let stdout = tokio::fs::read_to_string(&stdout_file)
                .await
                .unwrap_or_default();
            let stderr = tokio::fs::read_to_string(&stderr_file)
                .await
                .unwrap_or_default();

            return Ok(self.build_run_result(exit_code, elapsed, time_limit_ms, stdout, stderr));
        }

        // No stdin
        let stdout_f = std::fs::File::create(&stdout_file)?;
        let stderr_f = std::fs::File::create(&stderr_file)?;
        cmd.stdin(Stdio::null());
        cmd.stdout(stdout_f);
        cmd.stderr(stderr_f);

        let status = cmd.status().await?;
        let elapsed = start_time.elapsed().as_millis() as u32;
        let exit_code = status.code().unwrap_or(-1);

        let stdout = tokio::fs::read_to_string(&stdout_file)
            .await
            .unwrap_or_default();
        let stderr = tokio::fs::read_to_string(&stderr_file)
            .await
            .unwrap_or_default();

        Ok(self.build_run_result(exit_code, elapsed, time_limit_ms, stdout, stderr))
    }

    fn build_run_result(
        &self,
        exit_code: i32,
        elapsed_ms: u32,
        time_limit_ms: u32,
        stdout: String,
        stderr: String,
    ) -> RunResult {
        // Determine status based on exit code and time
        let status = if elapsed_ms > time_limit_ms {
            RunStatus::TimeLimitExceeded
        } else if exit_code == 137 || exit_code == 9 {
            // SIGKILL - usually memory limit
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
            memory: 0, // nsjail doesn't provide memory stats easily without cgroups
            status,
        }
    }
}
