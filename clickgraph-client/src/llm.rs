//! LLM client for schema discovery
//!
//! Supports two API formats:
//! - **Anthropic** (default): Claude API with `x-api-key` auth
//! - **OpenAI-compatible**: Works with OpenAI, Ollama, vLLM, LiteLLM, Together, Groq, etc.
//!
//! Set `CLICKGRAPH_LLM_PROVIDER=openai` to switch to OpenAI-compatible mode.

use reqwest::Client;
use serde::{Deserialize, Serialize};

/// Supported API providers
#[derive(Debug, Clone, PartialEq)]
pub enum LlmProvider {
    Anthropic,
    OpenAI,
}

/// LLM configuration loaded from environment variables
#[derive(Debug, Clone)]
pub struct LlmConfig {
    pub api_key: String,
    pub model: String,
    pub api_url: String,
    pub max_tokens: u32,
    pub provider: LlmProvider,
}

impl LlmConfig {
    /// Load config from environment. Returns None if no API key is set.
    ///
    /// Checks `CLICKGRAPH_LLM_PROVIDER` to determine the provider:
    /// - `"openai"` → OpenAI-compatible mode (checks `OPENAI_API_KEY` then `ANTHROPIC_API_KEY`)
    /// - `"anthropic"` or unset → Anthropic mode (checks `ANTHROPIC_API_KEY`)
    pub fn from_env() -> Option<Self> {
        let provider_str = std::env::var("CLICKGRAPH_LLM_PROVIDER")
            .unwrap_or_default()
            .to_lowercase();

        let (provider, api_key, default_model, default_url) = match provider_str.as_str() {
            "openai" => {
                let key = std::env::var("OPENAI_API_KEY")
                    .or_else(|_| std::env::var("ANTHROPIC_API_KEY"))
                    .ok()?;
                (
                    LlmProvider::OpenAI,
                    key,
                    "gpt-4o".to_string(),
                    "https://api.openai.com/v1/chat/completions".to_string(),
                )
            }
            _ => {
                let key = std::env::var("ANTHROPIC_API_KEY").ok()?;
                (
                    LlmProvider::Anthropic,
                    key,
                    "claude-sonnet-4-20250514".to_string(),
                    "https://api.anthropic.com/v1/messages".to_string(),
                )
            }
        };

        if api_key.is_empty() {
            return None;
        }

        Some(Self {
            api_key,
            model: std::env::var("CLICKGRAPH_LLM_MODEL").unwrap_or(default_model),
            api_url: std::env::var("CLICKGRAPH_LLM_API_URL").unwrap_or(default_url),
            max_tokens: std::env::var("CLICKGRAPH_LLM_MAX_TOKENS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(8192),
            provider,
        })
    }
}

// ── Anthropic API types ──

#[derive(Debug, Serialize)]
struct AnthropicRequest {
    model: String,
    max_tokens: u32,
    system: String,
    messages: Vec<ChatMessage>,
}

#[derive(Debug, Deserialize)]
struct AnthropicResponse {
    content: Vec<AnthropicContentBlock>,
}

#[derive(Debug, Deserialize)]
struct AnthropicContentBlock {
    text: Option<String>,
}

// ── OpenAI-compatible API types ──

#[derive(Debug, Serialize)]
struct OpenAIRequest {
    model: String,
    max_tokens: u32,
    messages: Vec<ChatMessage>,
}

#[derive(Debug, Deserialize)]
struct OpenAIResponse {
    choices: Vec<OpenAIChoice>,
}

#[derive(Debug, Deserialize)]
struct OpenAIChoice {
    message: OpenAIMessage,
}

#[derive(Debug, Deserialize)]
struct OpenAIMessage {
    content: Option<String>,
}

// ── Shared types ──

#[derive(Debug, Serialize, Deserialize)]
struct ChatMessage {
    role: String,
    content: String,
}

/// Call the LLM API with the given system and user prompts.
/// Dispatches to Anthropic or OpenAI-compatible format based on config.
pub async fn call_llm(
    client: &Client,
    config: &LlmConfig,
    system_prompt: &str,
    user_prompt: &str,
) -> Result<String, String> {
    match config.provider {
        LlmProvider::Anthropic => call_anthropic(client, config, system_prompt, user_prompt).await,
        LlmProvider::OpenAI => call_openai(client, config, system_prompt, user_prompt).await,
    }
}

async fn call_anthropic(
    client: &Client,
    config: &LlmConfig,
    system_prompt: &str,
    user_prompt: &str,
) -> Result<String, String> {
    let request = AnthropicRequest {
        model: config.model.clone(),
        max_tokens: config.max_tokens,
        system: system_prompt.to_string(),
        messages: vec![ChatMessage {
            role: "user".to_string(),
            content: user_prompt.to_string(),
        }],
    };

    let response = client
        .post(&config.api_url)
        .header("x-api-key", &config.api_key)
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .json(&request)
        .send()
        .await
        .map_err(|e| format!("Anthropic API request failed: {}", e))?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(format!("Anthropic API error ({}): {}", status, body));
    }

    let msg: AnthropicResponse = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse Anthropic response: {}", e))?;

    let text = msg
        .content
        .into_iter()
        .filter_map(|b| b.text)
        .collect::<Vec<_>>()
        .join("");

    if text.is_empty() {
        return Err("Anthropic returned empty response".to_string());
    }

    Ok(text)
}

async fn call_openai(
    client: &Client,
    config: &LlmConfig,
    system_prompt: &str,
    user_prompt: &str,
) -> Result<String, String> {
    let request = OpenAIRequest {
        model: config.model.clone(),
        max_tokens: config.max_tokens,
        messages: vec![
            ChatMessage {
                role: "system".to_string(),
                content: system_prompt.to_string(),
            },
            ChatMessage {
                role: "user".to_string(),
                content: user_prompt.to_string(),
            },
        ],
    };

    let response = client
        .post(&config.api_url)
        .header("authorization", format!("Bearer {}", config.api_key))
        .header("content-type", "application/json")
        .json(&request)
        .send()
        .await
        .map_err(|e| format!("OpenAI API request failed: {}", e))?;

    let status = response.status();
    if !status.is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(format!("OpenAI API error ({}): {}", status, body));
    }

    let msg: OpenAIResponse = response
        .json()
        .await
        .map_err(|e| format!("Failed to parse OpenAI response: {}", e))?;

    let text = msg
        .choices
        .into_iter()
        .filter_map(|c| c.message.content)
        .collect::<Vec<_>>()
        .join("");

    if text.is_empty() {
        return Err("OpenAI returned empty response".to_string());
    }

    Ok(text)
}

/// Extract YAML from LLM response, stripping markdown code fences if present.
pub fn extract_yaml(response: &str) -> String {
    let trimmed = response.trim();

    // Check for ```yaml ... ``` or ``` ... ```
    if let Some(rest) = trimmed.strip_prefix("```yaml") {
        if let Some(yaml) = rest.strip_suffix("```") {
            return yaml.trim().to_string();
        }
    }
    if let Some(rest) = trimmed.strip_prefix("```") {
        if let Some(yaml) = rest.strip_suffix("```") {
            return yaml.trim().to_string();
        }
    }

    trimmed.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_yaml_plain() {
        let input = "name: test\nversion: \"1.0\"";
        assert_eq!(extract_yaml(input), input);
    }

    #[test]
    fn test_extract_yaml_fenced() {
        let input = "```yaml\nname: test\nversion: \"1.0\"\n```";
        assert_eq!(extract_yaml(input), "name: test\nversion: \"1.0\"");
    }

    #[test]
    fn test_extract_yaml_generic_fence() {
        let input = "```\nname: test\n```";
        assert_eq!(extract_yaml(input), "name: test");
    }

    #[test]
    fn test_extract_yaml_with_whitespace() {
        let input = "  ```yaml\n  name: test\n  ```  ";
        assert_eq!(extract_yaml(input), "name: test");
    }

    #[test]
    fn test_config_anthropic_default() {
        std::env::remove_var("ANTHROPIC_API_KEY");
        std::env::remove_var("CLICKGRAPH_LLM_PROVIDER");
        assert!(LlmConfig::from_env().is_none());
    }

    #[test]
    fn test_config_openai_provider() {
        // Save and clear existing env
        let saved_anthropic = std::env::var("ANTHROPIC_API_KEY").ok();
        let saved_openai = std::env::var("OPENAI_API_KEY").ok();
        let saved_provider = std::env::var("CLICKGRAPH_LLM_PROVIDER").ok();
        let saved_model = std::env::var("CLICKGRAPH_LLM_MODEL").ok();
        let saved_url = std::env::var("CLICKGRAPH_LLM_API_URL").ok();

        std::env::remove_var("ANTHROPIC_API_KEY");
        std::env::remove_var("CLICKGRAPH_LLM_MODEL");
        std::env::remove_var("CLICKGRAPH_LLM_API_URL");
        std::env::set_var("CLICKGRAPH_LLM_PROVIDER", "openai");
        std::env::set_var("OPENAI_API_KEY", "sk-test-key");

        let config = LlmConfig::from_env().expect("should load openai config");
        assert_eq!(config.provider, LlmProvider::OpenAI);
        assert_eq!(config.api_key, "sk-test-key");
        assert_eq!(config.model, "gpt-4o");
        assert!(config.api_url.contains("openai.com"));

        // Restore env
        std::env::remove_var("CLICKGRAPH_LLM_PROVIDER");
        std::env::remove_var("OPENAI_API_KEY");
        if let Some(v) = saved_anthropic {
            std::env::set_var("ANTHROPIC_API_KEY", v);
        }
        if let Some(v) = saved_openai {
            std::env::set_var("OPENAI_API_KEY", v);
        }
        if let Some(v) = saved_provider {
            std::env::set_var("CLICKGRAPH_LLM_PROVIDER", v);
        }
        if let Some(v) = saved_model {
            std::env::set_var("CLICKGRAPH_LLM_MODEL", v);
        }
        if let Some(v) = saved_url {
            std::env::set_var("CLICKGRAPH_LLM_API_URL", v);
        }
    }
}
