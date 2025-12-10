use std::sync::Arc;
use tokio::sync::mpsc;

use crate::isolate::{ExecuteOutput, Isolate, RunStatus};
use crate::job::{ExecuteCommand, ExecuteEvent, ExecuteJob, JudgeEvent, JudgeJob, JudgeResult};

pub struct WorkerService {
    isolate: Arc<Isolate>,
}

impl WorkerService {
    pub fn new(max_boxes: usize) -> Self {
        Self {
            isolate: Arc::new(Isolate::new(max_boxes)),
        }
    }

    pub async fn judge(&self, job: JudgeJob, event_tx: mpsc::Sender<JudgeEvent>) {
        let compile_result = match self.isolate.compile(job.language, &job.code).await {
            Ok(r) => r,
            Err(e) => {
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
        let total = job.testcases.len();
        let mut accepted = 0u32;
        let mut overall_result = JudgeResult::Accepted;
        let mut max_time = 0u32;
        let mut max_memory = 0u32;

        let mut testcases = job.testcases.clone();
        testcases.sort_by_key(|tc| tc.order);

        for (i, testcase) in testcases.iter().enumerate() {
            let run_result = match self
                .isolate
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
                    self.isolate.release(box_id).await;
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

        self.isolate.release(box_id).await;

        let final_score = if total == 0 {
            0
        } else {
            (accepted * 100) / total as u32
        };

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
        let compile_result = match self.isolate.compile(job.language, &job.code).await {
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
        let isolate = self.isolate.clone();

        let (output_tx, mut output_rx) = mpsc::channel::<ExecuteOutput>(32);

        let handle = match self
            .isolate
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
                isolate.release(box_id).await;
                return;
            }
        };

        if event_tx.send(ExecuteEvent::Ready).await.is_err() {
            isolate.release(box_id).await;
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

        isolate.release(box_id).await;
    }
}

fn compare_output(actual: &str, expected: &str) -> bool {
    let actual_lines: Vec<&str> = actual.trim_end().lines().map(|l| l.trim_end()).collect();
    let expected_lines: Vec<&str> = expected.trim_end().lines().map(|l| l.trim_end()).collect();
    actual_lines == expected_lines
}
