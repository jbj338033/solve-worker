use anyhow::Result;
use futures::StreamExt;
use redis::AsyncCommands;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex};
use tracing::{debug, error, info};

use crate::job::{ExecuteCommand, ExecuteEvent, ExecuteJob, JudgeEvent, JudgeJob};
use crate::service::WorkerService;

const JUDGE_QUEUE: &str = "solve:jobs:judge";
const EXECUTE_QUEUE: &str = "solve:jobs:execute";
const EXECUTE_COMMAND_CHANNEL: &str = "solve:execute:commands";

fn judge_stream_key(submission_id: &str) -> String {
    format!("solve:judge:stream:{}", submission_id)
}

fn execute_stream_key(execution_id: &str) -> String {
    format!("solve:execute:stream:{}", execution_id)
}

type ExecuteSessions = Arc<Mutex<HashMap<String, mpsc::Sender<ExecuteCommand>>>>;

pub struct Worker {
    redis_url: String,
    redis: redis::aio::ConnectionManager,
    service: Arc<WorkerService>,
    execute_sessions: ExecuteSessions,
}

impl Worker {
    pub async fn new(redis_url: &str, max_boxes: usize) -> Result<Self> {
        let client = redis::Client::open(redis_url)?;
        let redis = redis::aio::ConnectionManager::new(client).await?;

        Ok(Self {
            redis_url: redis_url.to_string(),
            redis,
            service: Arc::new(WorkerService::new(max_boxes)),
            execute_sessions: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub async fn run(&self) -> Result<()> {
        info!("Worker started, waiting for jobs...");

        let redis_url = self.redis_url.clone();
        let sessions = self.execute_sessions.clone();
        tokio::spawn(async move {
            if let Err(e) = Self::listen_execute_commands(&redis_url, sessions).await {
                error!("Execute command listener error: {}", e);
            }
        });

        loop {
            if let Err(e) = self.process_next_job().await {
                error!("Error processing job: {}", e);
            }
        }
    }

    async fn listen_execute_commands(redis_url: &str, sessions: ExecuteSessions) -> Result<()> {
        let client = redis::Client::open(redis_url)?;
        let mut pubsub = client.get_async_pubsub().await?;
        pubsub
            .psubscribe(format!("{}:*", EXECUTE_COMMAND_CHANNEL))
            .await?;

        let mut stream = pubsub.on_message();
        while let Some(msg) = stream.next().await {
            let channel: String = msg.get_channel()?;
            let payload: String = msg.get_payload()?;

            let execution_id = channel
                .strip_prefix(&format!("{}:", EXECUTE_COMMAND_CHANNEL))
                .unwrap_or("");

            if let Ok(cmd) = serde_json::from_str::<ExecuteCommand>(&payload) {
                let sessions = sessions.lock().await;
                if let Some(tx) = sessions.get(execution_id) {
                    let _ = tx.send(cmd).await;
                }
            }
        }

        Ok(())
    }

    async fn process_next_job(&self) -> Result<()> {
        let mut conn = self.redis.clone();

        let result: Option<(String, String)> =
            conn.blpop(&[JUDGE_QUEUE, EXECUTE_QUEUE], 0.0).await?;

        let Some((queue, payload)) = result else {
            return Ok(());
        };

        match queue.as_str() {
            JUDGE_QUEUE => self.handle_judge(&payload).await?,
            EXECUTE_QUEUE => self.handle_execute(&payload).await?,
            _ => error!("Unknown queue: {}", queue),
        }

        Ok(())
    }

    async fn handle_judge(&self, payload: &str) -> Result<()> {
        let job: JudgeJob = serde_json::from_str(payload)?;
        let submission_id = job.submission_id.to_string();
        info!(
            submission_id = %submission_id,
            language = ?job.language,
            testcases = job.testcases.len(),
            time_limit = job.time_limit,
            memory_limit = job.memory_limit,
            code_len = job.code.len(),
            "[Judge] Job received"
        );

        let client = redis::Client::open(self.redis_url.as_str())?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        let stream_key = judge_stream_key(&submission_id);
        debug!(submission_id = %submission_id, stream_key = %stream_key, "[Judge] Stream key created");

        let (event_tx, mut event_rx) = mpsc::channel::<JudgeEvent>(32);

        let service = self.service.clone();
        let sid = submission_id.clone();
        tokio::spawn(async move {
            debug!(submission_id = %sid, "[Judge] Starting judge task");
            service.judge(job, event_tx).await;
            debug!(submission_id = %sid, "[Judge] Judge task finished");
        });

        let mut event_count = 0u32;
        while let Some(event) = event_rx.recv().await {
            event_count += 1;
            match &event {
                JudgeEvent::Progress { testcase_id, result, time, memory, score, progress } => {
                    debug!(
                        submission_id = %submission_id,
                        testcase_id = %testcase_id,
                        result = ?result,
                        time = %time,
                        memory = %memory,
                        score = %score,
                        progress = %progress,
                        "[Judge] Progress event"
                    );
                }
                JudgeEvent::Complete { result, score, time, memory, error } => {
                    info!(
                        submission_id = %submission_id,
                        result = ?result,
                        score = %score,
                        time = %time,
                        memory = %memory,
                        error = ?error,
                        "[Judge] Complete event"
                    );
                }
            }

            if let Ok(data) = serde_json::to_string(&event) {
                let _: Result<String, _> = conn.xadd(&stream_key, "*", &[("data", &data)]).await;
            }

            if matches!(event, JudgeEvent::Complete { .. }) {
                let _: Result<(), _> = conn.expire(&stream_key, 60).await;
                break;
            }
        }

        info!(submission_id = %submission_id, event_count = %event_count, "[Judge] Completed");
        Ok(())
    }

    async fn handle_execute(&self, payload: &str) -> Result<()> {
        let job: ExecuteJob = serde_json::from_str(payload)?;
        let execution_id = job.execution_id.to_string();
        info!("Processing execute job: {}", execution_id);

        let (event_tx, mut event_rx) = mpsc::channel::<ExecuteEvent>(32);
        let (command_tx, command_rx) = mpsc::channel::<ExecuteCommand>(32);

        {
            let mut sessions = self.execute_sessions.lock().await;
            sessions.insert(execution_id.clone(), command_tx);
        }

        let client = redis::Client::open(self.redis_url.as_str())?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        let stream_key = execute_stream_key(&execution_id);

        let service = self.service.clone();
        tokio::spawn(async move {
            service.execute(job, event_tx, command_rx).await;
        });

        let sessions = self.execute_sessions.clone();
        let exec_id = execution_id;
        tokio::spawn(async move {
            while let Some(event) = event_rx.recv().await {
                if let Ok(data) = serde_json::to_string(&event) {
                    let _: Result<String, _> = conn.xadd(&stream_key, "*", &[("data", &data)]).await;
                }

                if matches!(event, ExecuteEvent::Complete { .. } | ExecuteEvent::Error { .. }) {
                    let _: Result<(), _> = conn.expire(&stream_key, 60).await;
                    break;
                }
            }

            let mut sessions = sessions.lock().await;
            sessions.remove(&exec_id);
        });

        Ok(())
    }
}
