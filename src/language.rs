use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Language {
    C,
    Cpp,
    Java,
    Python,
    Javascript,
    Kotlin,
    Go,
    Rust,
}

pub struct LanguageConfig {
    pub source_file: &'static str,
    pub compile_command: Option<&'static [&'static str]>,
    pub execute_command: &'static [&'static str],
}

impl Language {
    pub fn config(self) -> LanguageConfig {
        match self {
            Language::C => LanguageConfig {
                source_file: "main.c",
                compile_command: Some(&["/usr/bin/gcc", "-O2", "-o", "main", "main.c", "-lm"]),
                execute_command: &["./main"],
            },
            Language::Cpp => LanguageConfig {
                source_file: "main.cpp",
                compile_command: Some(&[
                    "/usr/bin/g++",
                    "-O2",
                    "-std=c++17",
                    "-o",
                    "main",
                    "main.cpp",
                ]),
                execute_command: &["./main"],
            },
            Language::Java => LanguageConfig {
                source_file: "Main.java",
                compile_command: Some(&["/usr/bin/javac", "Main.java"]),
                execute_command: &["/usr/bin/java", "Main"],
            },
            Language::Python => LanguageConfig {
                source_file: "main.py",
                compile_command: None,
                execute_command: &["/usr/bin/python3", "main.py"],
            },
            Language::Javascript => LanguageConfig {
                source_file: "main.js",
                compile_command: None,
                execute_command: &["/usr/bin/node", "main.js"],
            },
            Language::Kotlin => LanguageConfig {
                source_file: "Main.kt",
                compile_command: Some(&[
                    "/usr/bin/kotlinc",
                    "Main.kt",
                    "-include-runtime",
                    "-d",
                    "Main.jar",
                ]),
                execute_command: &["/usr/bin/java", "-jar", "Main.jar"],
            },
            Language::Go => LanguageConfig {
                source_file: "main.go",
                compile_command: Some(&["/usr/bin/go", "build", "-o", "main", "main.go"]),
                execute_command: &["./main"],
            },
            Language::Rust => LanguageConfig {
                source_file: "main.rs",
                compile_command: Some(&["/usr/bin/rustc", "-O", "-o", "main", "main.rs"]),
                execute_command: &["./main"],
            },
        }
    }
}
