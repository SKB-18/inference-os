use crate::models::ChatCompletionRequest;

pub struct TokenEstimator;

impl TokenEstimator {
    /// Heuristic estimation: characters / 4 + requested max_tokens
    pub fn estimate(req: &ChatCompletionRequest) -> (u32, u32) {
        let prompt_est = req.estimate_prompt_tokens();
        let total_est = req.estimate_total_tokens();
        (prompt_est, total_est)
    }
}
