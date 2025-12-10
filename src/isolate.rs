use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, Command};
use tokio::sync::{mpsc, Mutex, Semaphore};

use crate::language::Language;

const ISOLATE_PATH: &str = "/var/local/lib/isolate";

pub struct Isolate {
    semaphore: Arc<Semaphore>,
    box_pool: Arc<Mutex<Vec<u32>>>,
}

#[derive(Debug)]
pub struct RunResult {
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
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

impl Isolate {
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
        let box_dir = PathBuf::from(format!("{}/{}/box", ISOLATE_PATH, box_id));

        tokio::fs::write(box_dir.join("input.txt"), input).await?;

        self.run_in_box(
            box_id,
            config.execute_command,
            time_limit as f64 / 1000.0,
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
        let meta_file = tempfile::NamedTempFile::new()?;
        let meta_path = meta_file.path().to_string_lossy().to_string();

        let time_limit_sec = time_limit as f64 / 1000.0;
        let mut args = vec![
            "--cg".to_string(),
            "-b".to_string(),
            box_id.to_string(),
            "-M".to_string(),
            meta_path.clone(),
            "-t".to_string(),
            format!("{:.1}", time_limit_sec),
            "-w".to_string(),
            format!("{:.1}", time_limit_sec * 2.0 + 1.0),
            "-x".to_string(),
            "0.5".to_string(),
            format!("--cg-mem={}", memory_limit * 1024),
            "-p".to_string(),
            "--run".to_string(),
            "--".to_string(),
        ];
        args.extend(config.execute_command.iter().map(|s| s.to_string()));

        let mut child = Command::new("isolate")
            .args(&args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        let stdin = child.stdin.take().unwrap();

        let (stdin_tx, stdin_rx) = mpsc::channel::<String>(32);
        let (kill_tx, kill_rx) = mpsc::channel::<()>(1);

        tokio::spawn(Self::execute_io_task(
            stdin,
            stdout,
            stderr,
            stdin_rx,
            kill_rx,
            child,
            meta_path,
            event_tx,
        ));

        std::mem::forget(meta_file);

        Ok(ExecuteHandle { stdin_tx, kill_tx })
    }

    async fn execute_io_task(
        mut stdin: tokio::process::ChildStdin,
        stdout: tokio::process::ChildStdout,
        stderr: tokio::process::ChildStderr,
        mut stdin_rx: mpsc::Receiver<String>,
        mut kill_rx: mpsc::Receiver<()>,
        mut child: Child,
        meta_path: String,
        event_tx: mpsc::Sender<ExecuteOutput>,
    ) {
        let (stdout_done_tx, mut stdout_done_rx) = mpsc::channel::<()>(1);
        let (stderr_done_tx, mut stderr_done_rx) = mpsc::channel::<()>(1);

        let event_tx_stdout = event_tx.clone();
        tokio::spawn(async move {
            let mut reader = BufReader::new(stdout);
            let mut line = String::new();
            while reader.read_line(&mut line).await.unwrap_or(0) > 0 {
                let _ = event_tx_stdout.send(ExecuteOutput::Stdout(std::mem::take(&mut line))).await;
            }
            let _ = stdout_done_tx.send(()).await;
        });

        let event_tx_stderr = event_tx.clone();
        tokio::spawn(async move {
            let mut reader = BufReader::new(stderr);
            let mut line = String::new();
            while reader.read_line(&mut line).await.unwrap_or(0) > 0 {
                let _ = event_tx_stderr.send(ExecuteOutput::Stderr(std::mem::take(&mut line))).await;
            }
            let _ = stderr_done_tx.send(()).await;
        });

        loop {
            tokio::select! {
                biased;
                result = child.wait() => {
                    let exit_code = result.map(|s| s.code().unwrap_or(-1)).unwrap_or(-1);
                    let _ = stdout_done_rx.recv().await;
                    let _ = stderr_done_rx.recv().await;
                    let (time, memory) = Self::parse_meta_file(&meta_path).await;
                    let _ = event_tx.send(ExecuteOutput::Complete { exit_code, time, memory }).await;
                    let _ = std::fs::remove_file(&meta_path);
                    return;
                }
                kill = kill_rx.recv() => {
                    if kill.is_some() {
                        let _ = child.kill().await;
                    }
                }
                data = stdin_rx.recv() => {
                    if let Some(data) = data {
                        if stdin.write_all(data.as_bytes()).await.is_err() {
                            continue;
                        }
                        let _ = stdin.flush().await;
                    }
                }
            }
        }
    }

    async fn parse_meta_file(meta_path: &str) -> (u32, u32) {
        let content = tokio::fs::read_to_string(meta_path)
            .await
            .unwrap_or_default();

        let meta: HashMap<String, String> = content
            .lines()
            .filter_map(|line| {
                let mut parts = line.splitn(2, ':');
                Some((parts.next()?.to_string(), parts.next()?.to_string()))
            })
            .collect();

        let time: u32 = meta
            .get("time")
            .and_then(|s| s.parse::<f64>().ok())
            .map(|t| (t * 1000.0) as u32)
            .unwrap_or(0);
        let memory: u32 = meta
            .get("cg-mem")
            .or_else(|| meta.get("max-rss"))
            .and_then(|s| s.parse().ok())
            .map(|m: u32| m / 1024)
            .unwrap_or(0);

        (time, memory)
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
        let _ = self.cleanup_box(box_id).await;
        let mut pool = self.box_pool.lock().await;
        pool.push(box_id);
        self.semaphore.add_permits(1);
    }

    async fn cleanup_box(&self, box_id: u32) -> Result<()> {
        Command::new("isolate")
            .args(["--cg", "-b", &box_id.to_string(), "--cleanup"])
            .output()
            .await?;
        Ok(())
    }

    async fn init_box(&self, box_id: u32) -> Result<PathBuf> {
        self.cleanup_box(box_id).await?;

        let output = Command::new("isolate")
            .args(["--cg", "-b", &box_id.to_string(), "--init"])
            .output()
            .await?;

        if !output.status.success() {
            return Err(anyhow!(
                "Failed to init box {}: {}",
                box_id,
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        Ok(PathBuf::from(
            String::from_utf8_lossy(&output.stdout).trim(),
        ))
    }

    async fn compile_in_box(
        &self,
        box_id: u32,
        language: Language,
        code: &str,
    ) -> Result<CompileResult> {
        let config = language.config();
        let box_path = self.init_box(box_id).await?;
        let box_dir = box_path.join("box");

        tokio::fs::write(box_dir.join(config.source_file), code).await?;

        let Some(compile_cmd) = config.compile_command else {
            return Ok(CompileResult {
                success: true,
                error: None,
                box_id: Some(box_id),
            });
        };

        let result = self
            .run_in_box(box_id, compile_cmd, 30.0, 512 * 1024, None)
            .await?;

        if result.success {
            Ok(CompileResult {
                success: true,
                error: None,
                box_id: Some(box_id),
            })
        } else {
            self.cleanup_box(box_id).await?;
            Ok(CompileResult {
                success: false,
                error: Some(result.stderr),
                box_id: None,
            })
        }
    }

    async fn run_in_box(
        &self,
        box_id: u32,
        command: &[&str],
        time_limit_sec: f64,
        memory_limit: u32,
        stdin: Option<&str>,
    ) -> Result<RunResult> {
        let meta_file = tempfile::NamedTempFile::new()?;
        let meta_path = meta_file.path().to_string_lossy().to_string();

        let mut args = vec![
            "--cg".to_string(),
            "-b".to_string(),
            box_id.to_string(),
            "-M".to_string(),
            meta_path.clone(),
            "-t".to_string(),
            format!("{:.1}", time_limit_sec),
            "-w".to_string(),
            format!("{:.1}", time_limit_sec * 2.0 + 1.0),
            "-x".to_string(),
            "0.5".to_string(),
            format!("--cg-mem={}", memory_limit * 1024),
            "-p".to_string(),
            "-o".to_string(),
            "stdout.txt".to_string(),
            "-r".to_string(),
            "stderr.txt".to_string(),
        ];

        if let Some(input) = stdin {
            args.push("-i".to_string());
            args.push(input.to_string());
        }

        args.push("--run".to_string());
        args.push("--".to_string());
        args.extend(command.iter().map(|s| s.to_string()));

        Command::new("isolate").args(&args).output().await?;

        let box_dir = PathBuf::from(format!("{}/{}/box", ISOLATE_PATH, box_id));
        let stdout = tokio::fs::read_to_string(box_dir.join("stdout.txt"))
            .await
            .unwrap_or_default();
        let stderr = tokio::fs::read_to_string(box_dir.join("stderr.txt"))
            .await
            .unwrap_or_default();

        self.parse_meta(&meta_path, stdout, stderr).await
    }

    async fn parse_meta(
        &self,
        meta_path: &str,
        stdout: String,
        stderr: String,
    ) -> Result<RunResult> {
        let content = tokio::fs::read_to_string(meta_path)
            .await
            .unwrap_or_default();

        let meta: HashMap<String, String> = content
            .lines()
            .filter_map(|line| {
                let mut parts = line.splitn(2, ':');
                Some((parts.next()?.to_string(), parts.next()?.to_string()))
            })
            .collect();

        let status_code = meta.get("status").map(|s| s.as_str());
        let exit_code: i32 = meta
            .get("exitcode")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let time: u32 = meta
            .get("time")
            .and_then(|s| s.parse::<f64>().ok())
            .map(|t| (t * 1000.0) as u32)
            .unwrap_or(0);
        let memory: u32 = meta
            .get("cg-mem")
            .or_else(|| meta.get("max-rss"))
            .and_then(|s| s.parse().ok())
            .map(|m: u32| m / 1024)
            .unwrap_or(0);

        let status = match status_code {
            Some("TO") => RunStatus::TimeLimitExceeded,
            Some("SG") if meta.get("exitsig").map(|s| s.as_str()) == Some("9") => {
                RunStatus::MemoryLimitExceeded
            }
            Some("SG") | Some("RE") => RunStatus::RuntimeError,
            Some("XX") => RunStatus::InternalError,
            None if exit_code == 0 => RunStatus::Ok,
            None => RunStatus::RuntimeError,
            _ => RunStatus::RuntimeError,
        };

        Ok(RunResult {
            success: status == RunStatus::Ok && exit_code == 0,
            stdout,
            stderr,
            exit_code,
            time,
            memory,
            status,
        })
    }
}
