use serde::{Deserialize, Serialize};

use crate::language::Language;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JudgeJob {
    pub submission_id: i64,
    pub language: Language,
    pub code: String,
    pub time_limit: u32,
    pub memory_limit: u32,
    pub testcases: Vec<TestCase>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TestCase {
    pub id: i64,
    pub input: String,
    pub output: String,
    pub order: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ExecuteJob {
    pub execution_id: String,
    pub language: Language,
    pub code: String,
    pub time_limit: u32,
    pub memory_limit: u32,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JudgeResult {
    Accepted,
    WrongAnswer,
    TimeLimitExceeded,
    MemoryLimitExceeded,
    RuntimeError,
    CompileError,
    InternalError,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum JudgeEvent {
    Progress {
        #[serde(rename = "testcaseId")]
        testcase_id: i64,
        result: JudgeResult,
        time: u32,
        memory: u32,
        score: u32,
        progress: u32,
    },
    Complete {
        result: JudgeResult,
        score: u32,
        time: u32,
        memory: u32,
        error: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecuteEvent {
    Ready,
    Stdout { data: String },
    Stderr { data: String },
    Complete {
        #[serde(rename = "exitCode")]
        exit_code: i32,
        time: u32,
        memory: u32,
    },
    Error { message: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ExecuteCommand {
    Stdin { data: String },
    Kill,
}
