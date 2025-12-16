use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::job::{ExecuteCommand, ExecuteEvent, ExecuteJob, JudgeEvent, JudgeJob, JudgeResult};
use crate::sandbox::{ExecuteOutput, RunStatus, Sandbox};

pub struct WorkerService {
    sandbox: Arc<Sandbox>,
}

impl WorkerService {
    pub fn new(max_boxes: usize) -> Self {
        Self {
            sandbox: Arc::new(Sandbox::new(max_boxes)),
        }
    }

    pub async fn judge(&self, job: JudgeJob, event_tx: mpsc::Sender<JudgeEvent>) {
        let submission_id = job.submission_id;
        info!(
            submission_id = %submission_id,
            language = ?job.language,
            "[Service] Starting judge"
        );

        debug!(submission_id = %submission_id, "[Service] Compiling code");
        let compile_result = match self.sandbox.compile(job.language, &job.code).await {
            Ok(r) => r,
            Err(e) => {
                error!(submission_id = %submission_id, error = %e, "[Service] Compile internal error");
                let _ = event_tx
                    .send(JudgeEvent::Complete {
                        result: JudgeResult::InternalError,
                        score: 0,
                        time: 0,
                        memory: 0,
                        error: Some(e.to_string()),
                    })
                    .await;
                return;
            }
        };

        if !compile_result.success {
            warn!(
                submission_id = %submission_id,
                error = ?compile_result.error,
                "[Service] Compile error"
            );
            let _ = event_tx
                .send(JudgeEvent::Complete {
                    result: JudgeResult::CompileError,
                    score: 0,
                    time: 0,
                    memory: 0,
                    error: compile_result.error,
                })
                .await;
            return;
        }

        let box_id = compile_result.box_id.unwrap();
        info!(submission_id = %submission_id, box_id = %box_id, "[Service] Compile success");

        let total = job.testcases.len();
        let mut accepted = 0u32;
        let mut overall_result = JudgeResult::Accepted;
        let mut max_time = 0u32;
        let mut max_memory = 0u32;

        let mut testcases = job.testcases.clone();
        testcases.sort_by_key(|tc| tc.order);

        for (i, testcase) in testcases.iter().enumerate() {
            debug!(
                submission_id = %submission_id,
                testcase_id = %testcase.id,
                testcase_order = %testcase.order,
                "[Service] Running testcase {}/{}",
                i + 1,
                total
            );

            let run_result = match self
                .sandbox
                .run(
                    box_id,
                    job.language,
                    &testcase.input,
                    job.time_limit,
                    job.memory_limit,
                )
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    error!(
                        submission_id = %submission_id,
                        testcase_id = %testcase.id,
                        error = %e,
                        "[Service] Run internal error"
                    );
                    self.sandbox.release(box_id).await;
                    let _ = event_tx
                        .send(JudgeEvent::Complete {
                            result: JudgeResult::InternalError,
                            score: (accepted * 100 / total as u32),
                            time: max_time,
                            memory: max_memory,
                            error: Some(e.to_string()),
                        })
                        .await;
                    return;
                }
            };

            let judge_result = match run_result.status {
                RunStatus::Ok => {
                    if compare_output(&run_result.stdout, &testcase.output) {
                        JudgeResult::Accepted
                    } else {
                        JudgeResult::WrongAnswer
                    }
                }
                RunStatus::TimeLimitExceeded => JudgeResult::TimeLimitExceeded,
                RunStatus::MemoryLimitExceeded => JudgeResult::MemoryLimitExceeded,
                RunStatus::RuntimeError => JudgeResult::RuntimeError,
                RunStatus::InternalError => JudgeResult::InternalError,
            };

            debug!(
                submission_id = %submission_id,
                testcase_id = %testcase.id,
                result = ?judge_result,
                status = ?run_result.status,
                time = %run_result.time,
                memory = %run_result.memory,
                exit_code = %run_result.exit_code,
                "[Service] Testcase result"
            );

            max_time = max_time.max(run_result.time);
            max_memory = max_memory.max(run_result.memory);

            if judge_result == JudgeResult::Accepted {
                accepted += 1;
            } else if overall_result == JudgeResult::Accepted {
                overall_result = judge_result.clone();
            }

            let score = (accepted * 100) / total as u32;
            let progress = ((i + 1) * 100 / total) as u32;

            let _ = event_tx
                .send(JudgeEvent::Progress {
                    testcase_id: testcase.id,
                    result: judge_result,
                    time: run_result.time,
                    memory: run_result.memory,
                    score,
                    progress,
                })
                .await;
        }

        debug!(submission_id = %submission_id, box_id = %box_id, "[Service] Releasing sandbox");
        self.sandbox.release(box_id).await;

        let final_score = if total == 0 {
            0
        } else {
            (accepted * 100) / total as u32
        };

        info!(
            submission_id = %submission_id,
            result = ?overall_result,
            score = %final_score,
            time = %max_time,
            memory = %max_memory,
            accepted = %accepted,
            total = %total,
            "[Service] Judge complete"
        );

        let _ = event_tx
            .send(JudgeEvent::Complete {
                result: overall_result,
                score: final_score,
                time: max_time,
                memory: max_memory,
                error: None,
            })
            .await;
    }

    pub async fn execute(
        &self,
        job: ExecuteJob,
        event_tx: mpsc::Sender<ExecuteEvent>,
        mut command_rx: mpsc::Receiver<ExecuteCommand>,
    ) {
        let compile_result = match self.sandbox.compile(job.language, &job.code).await {
            Ok(r) => r,
            Err(e) => {
                let _ = event_tx
                    .send(ExecuteEvent::Error {
                        message: e.to_string(),
                    })
                    .await;
                return;
            }
        };

        if !compile_result.success {
            let _ = event_tx
                .send(ExecuteEvent::Error {
                    message: compile_result
                        .error
                        .unwrap_or_else(|| "Compile Error".to_string()),
                })
                .await;
            return;
        }

        let box_id = compile_result.box_id.unwrap();
        let sandbox = self.sandbox.clone();

        let (output_tx, mut output_rx) = mpsc::channel::<ExecuteOutput>(32);

        let handle = match self
            .sandbox
            .run_execute(box_id, job.language, job.time_limit, job.memory_limit, output_tx)
            .await
        {
            Ok(h) => h,
            Err(e) => {
                let _ = event_tx
                    .send(ExecuteEvent::Error {
                        message: e.to_string(),
                    })
                    .await;
                sandbox.release(box_id).await;
                return;
            }
        };

        if event_tx.send(ExecuteEvent::Ready).await.is_err() {
            sandbox.release(box_id).await;
            return;
        }

        loop {
            tokio::select! {
                biased;
                output = output_rx.recv() => {
                    match output {
                        Some(ExecuteOutput::Stdout(data)) => {
                            let _ = event_tx.send(ExecuteEvent::Stdout { data }).await;
                        }
                        Some(ExecuteOutput::Stderr(data)) => {
                            let _ = event_tx.send(ExecuteEvent::Stderr { data }).await;
                        }
                        Some(ExecuteOutput::Complete { exit_code, time, memory }) => {
                            let _ = event_tx.send(ExecuteEvent::Complete { exit_code, time, memory }).await;
                            break;
                        }
                        None => break,
                    }
                }
                cmd = command_rx.recv() => {
                    match cmd {
                        Some(ExecuteCommand::Stdin { data }) => {
                            let _ = handle.stdin_tx.send(data).await;
                        }
                        Some(ExecuteCommand::Kill) => {
                            let _ = handle.kill_tx.send(()).await;
                        }
                        None => {}
                    }
                }
            }
        }

        sandbox.release(box_id).await;
    }
}

fn compare_output(actual: &str, expected: &str) -> bool {
    let actual_lines: Vec<&str> = actual.trim_end().lines().map(|l| l.trim_end()).collect();
    let expected_lines: Vec<&str> = expected.trim_end().lines().map(|l| l.trim_end()).collect();
    actual_lines == expected_lines
}
