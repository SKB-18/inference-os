use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatMessage {
    pub role: String,
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatCompletionRequest {
    pub model: String,
    pub messages: Vec<ChatMessage>,
    #[serde(default)]
    pub stream: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idempotency_key: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_tier: Option<String>, // "interactive", "batch", "default"
}

impl ChatCompletionRequest {
    /// Fast heuristic estimation of prompt tokens (approx 4 chars per token)
    pub fn estimate_prompt_tokens(&self) -> u32 {
        let mut total_chars: usize = 0;
        for msg in &self.messages {
            total_chars += msg.content.len();
        }
        let est = (total_chars / 4).max(1) as u32;
        est
    }

    /// Total estimated tokens = estimated prompt + max requested completion tokens
    pub fn estimate_total_tokens(&self) -> u32 {
        let prompt_est = self.estimate_prompt_tokens();
        let completion_est = self.max_tokens.unwrap_or(256);
        prompt_est + completion_est
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Usage {
    pub prompt_tokens: u32,
    pub completion_tokens: u32,
    pub total_tokens: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatChoice {
    pub index: u32,
    pub message: ChatMessage,
    pub finish_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatCompletionResponse {
    pub id: String,
    pub object: String,
    pub created: i64,
    pub model: String,
    pub choices: Vec<ChatChoice>,
    pub usage: Option<Usage>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkDelta {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkChoice {
    pub index: u32,
    pub delta: ChunkDelta,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChatCompletionChunk {
    pub id: String,
    pub object: String,
    pub created: i64,
    pub model: String,
    pub choices: Vec<ChunkChoice>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_estimation_heuristics() {
        let req = ChatCompletionRequest {
            model: "gpt-4o-mini".to_string(),
            messages: vec![
                ChatMessage {
                    role: "system".to_string(),
                    content: "You are a helpful assistant.".to_string(),
                    name: None,
                },
                ChatMessage {
                    role: "user".to_string(),
                    content: "Hello world!".to_string(),
                    name: None,
                },
            ],
            stream: false,
            max_tokens: Some(100),
            temperature: None,
            top_p: None,
            idempotency_key: None,
            latency_tier: None,
        };

        let prompt_est = req.estimate_prompt_tokens();
        assert!(prompt_est > 0);
        let total_est = req.estimate_total_tokens();
        assert_eq!(total_est, prompt_est + 100);
    }

    #[test]
    fn test_chat_serialization_roundtrip() {
        let req = ChatCompletionRequest {
            model: "llama-3-8b".to_string(),
            messages: vec![ChatMessage {
                role: "user".to_string(),
                content: "ping".to_string(),
                name: None,
            }],
            stream: true,
            max_tokens: Some(50),
            temperature: Some(0.7),
            top_p: Some(0.9),
            idempotency_key: Some("key-123".to_string()),
            latency_tier: Some("interactive".to_string()),
        };

        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("llama-3-8b"));
        let parsed: ChatCompletionRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.idempotency_key, Some("key-123".to_string()));
    }
}

