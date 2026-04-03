//! LLM client for schema discovery and NL→Cypher translation.
//!
//! Supports Anthropic (default) and any OpenAI-compatible API.
//!
//! Configuration priority:
//!   CG_LLM_PROVIDER / CLICKGRAPH_LLM_PROVIDER → provider selection
//!   CG_LLM_API_KEY / ANTHROPIC_API_KEY / OPENAI_API_KEY → credentials
//!   CG_LLM_MODEL / CLICKGRAPH_LLM_MODEL → model override
//!   CG_LLM_BASE_URL / CLICKGRAPH_LLM_API_URL → endpoint override (for OpenAI-compat)
//!   CG_LLM_MAX_TOKENS / CLICKGRAPH_LLM_MAX_TOKENS → token limit (default 8192)

use anyhow::{anyhow, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::config::LlmConfig as CgLlmConfig;

const LLM_REQUEST_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Debug, Clone, PartialEq)]
pub enum LlmProvider {
    Anthropic,
    OpenAI,
}

#[derive(Debug, Clone)]
pub struct LlmClient {
    pub api_key: String,
    pub model: String,
    pub api_url: String,
    pub max_tokens: u32,
    pub provider: LlmProvider,
    client: Client,
}

impl LlmClient {
    /// Build from CgConfig's LlmConfig, falling back to environment variables.
    pub fn from_config(cfg: &CgLlmConfig) -> Result<Self> {
        let provider_str = cfg
            .provider
            .as_deref()
            .or_else(|| None)
            .unwrap_or("anthropic")
            .to_lowercase();

        let (provider, api_key, default_model, default_url) = match provider_str.as_str() {
            "openai" => {
                let key = cfg
                    .api_key
                    .clone()
                    .or_else(|| std::env::var("OPENAI_API_KEY").ok())
                    .or_else(|| std::env::var("ANTHROPIC_API_KEY").ok())
                    .ok_or_else(|| anyhow!("No API key found. Set CG_LLM_API_KEY, OPENAI_API_KEY, or ANTHROPIC_API_KEY."))?;
                (
                    LlmProvider::OpenAI,
                    key,
                    "gpt-4o".to_string(),
                    "https://api.openai.com/v1/chat/completions".to_string(),
                )
            }
            _ => {
                let key = cfg
                    .api_key
                    .clone()
                    .or_else(|| std::env::var("ANTHROPIC_API_KEY").ok())
                    .ok_or_else(|| {
                        anyhow!("No API key found. Set CG_LLM_API_KEY or ANTHROPIC_API_KEY.")
                    })?;
                (
                    LlmProvider::Anthropic,
                    key,
                    "claude-sonnet-4-6".to_string(),
                    "https://api.anthropic.com/v1/messages".to_string(),
                )
            }
        };

        let model = cfg.model.clone().unwrap_or(default_model);
        let api_url = cfg.base_url.clone().unwrap_or(default_url);
        let max_tokens = cfg.max_tokens.unwrap_or(8192);

        let client = Client::builder().timeout(LLM_REQUEST_TIMEOUT).build()?;

        Ok(LlmClient {
            api_key,
            model,
            api_url,
            max_tokens,
            provider,
            client,
        })
    }

    /// Call the LLM with a system prompt and user message, return the text response.
    pub async fn call(&self, system_prompt: &str, user_prompt: &str) -> Result<String> {
        match self.provider {
            LlmProvider::Anthropic => self.call_anthropic(system_prompt, user_prompt).await,
            LlmProvider::OpenAI => self.call_openai(system_prompt, user_prompt).await,
        }
    }

    async fn call_anthropic(&self, system_prompt: &str, user_prompt: &str) -> Result<String> {
        #[derive(Serialize)]
        struct Req<'a> {
            model: &'a str,
            max_tokens: u32,
            system: &'a str,
            messages: Vec<Msg<'a>>,
        }
        #[derive(Serialize)]
        struct Msg<'a> {
            role: &'a str,
            content: &'a str,
        }
        #[derive(Deserialize)]
        struct Resp {
            content: Vec<Block>,
        }
        #[derive(Deserialize)]
        struct Block {
            text: Option<String>,
        }

        let body = Req {
            model: &self.model,
            max_tokens: self.max_tokens,
            system: system_prompt,
            messages: vec![Msg {
                role: "user",
                content: user_prompt,
            }],
        };

        let resp = self
            .client
            .post(&self.api_url)
            .header("x-api-key", &self.api_key)
            .header("anthropic-version", "2023-06-01")
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("LLM API error {}: {}", status, body));
        }

        let resp: Resp = resp.json().await?;
        resp.content
            .into_iter()
            .find_map(|b| b.text)
            .ok_or_else(|| anyhow!("Empty LLM response"))
    }

    async fn call_openai(&self, system_prompt: &str, user_prompt: &str) -> Result<String> {
        #[derive(Serialize)]
        struct Req<'a> {
            model: &'a str,
            max_tokens: u32,
            messages: Vec<Msg<'a>>,
        }
        #[derive(Serialize)]
        struct Msg<'a> {
            role: &'a str,
            content: &'a str,
        }
        #[derive(Deserialize)]
        struct Resp {
            choices: Vec<Choice>,
        }
        #[derive(Deserialize)]
        struct Choice {
            message: MsgOut,
        }
        #[derive(Deserialize)]
        struct MsgOut {
            content: String,
        }

        let body = Req {
            model: &self.model,
            max_tokens: self.max_tokens,
            messages: vec![
                Msg {
                    role: "system",
                    content: system_prompt,
                },
                Msg {
                    role: "user",
                    content: user_prompt,
                },
            ],
        };

        let resp = self
            .client
            .post(&self.api_url)
            .bearer_auth(&self.api_key)
            .json(&body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(anyhow!("LLM API error {}: {}", status, body));
        }

        let resp: Resp = resp.json().await?;
        resp.choices
            .into_iter()
            .next()
            .map(|c| c.message.content)
            .ok_or_else(|| anyhow!("Empty LLM response"))
    }
}

/// Strip markdown code fences from an LLM response.
pub fn extract_code_block(response: &str, lang_hint: &str) -> String {
    let fence = format!("```{}", lang_hint);
    if let Some(start) = response.find(&fence).or_else(|| response.find("```")) {
        let after = &response[start..];
        let content_start = after.find('\n').map(|i| start + i + 1).unwrap_or(start);
        let content = &response[content_start..];
        if let Some(end) = content.find("```") {
            return content[..end].trim().to_string();
        }
    }
    response.trim().to_string()
}

/// Extract YAML content from an LLM response (strips ``` fences).
pub fn extract_yaml(response: &str) -> String {
    extract_code_block(response, "yaml")
}
