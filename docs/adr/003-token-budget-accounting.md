# ADR-003: Two-Phase Token Budget Accounting

## Status
Accepted

## Context
When admitting requests, the exact number of completion tokens is unknown in advance because generation length depends on the model's output distribution, stop tokens, and temperature. However, admitting requests without accounting for expected token usage risks queue saturation and tenant budget overruns.

Furthermore, different models and engines use different tokenizers (Llama vs Mistral vs Qwen), making exact prompt tokenization on the gateway computationally prohibitive and slow.

## Decision
InferenceOS employs a **Two-Phase Token Accounting** mechanism:
1. **Phase 1: Soft Pre-Dispatch Estimation & Reservation**:
   - Fast heuristic: Prompt characters divided by 4 plus requested `max_tokens` (capped by tenant default limit).
   - If `(reserved_tokens + tenant_used_budget) > tenant_budget`, the request is immediately rejected with `429 Too Many Requests (BudgetExceeded)`.
   - The estimated cost is temporarily held against the tenant's inflight budget.
2. **Phase 2: Post-Execution True-Up**:
   - As the response stream finishes or returns the OpenAI `usage` block (`prompt_tokens`, `completion_tokens`, `total_tokens`), the gateway releases the soft reservation and records the exact measured usage.
   - If the backend crashes mid-stream, actual flushed tokens are counted and reconciled so the tenant is never over-billed.

## Consequences
- Protects the cluster from catastrophic resource starvation caused by abusive tenants.
- Eliminates heavy tokenizer dependencies on the critical gateway path.
- Provides sub-millisecond admission decision latencies.
