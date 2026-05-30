---

# Replacing the finance research feature with MLE-Beast's OpenRouter LLM pattern

## Part 1 — The MLE-Beast LLM pattern

**Underlying library: the official OpenAI Python SDK wrapped by `instructor`.** No litellm (it's declared in `pyproject.toml` but unused), no `anthropic`, no raw httpx for calls. Every provider — OpenAI, OpenRouter, and local OpenAI-compatible servers — is reached through one `openai.OpenAI` client differentiated only by `base_url` + `api_key`. `httpx` is used solely to build an explicit `httpx.Timeout`.

**Client abstraction (`src/mle_beast/llm.py`).**
- `_make_openai_client(*, base_url=None, api_key)` → `OpenAI(api_key=..., timeout=_HTTP_TIMEOUT, max_retries=2[, base_url=...])`, where `_HTTP_TIMEOUT = httpx.Timeout(connect=10, read=180, write=60, pool=10)` (per-phase timeouts; a scalar timeout reportedly failed to reach the socket read and caused a 14-min hang).
- `get_client(settings=None)` → `instructor.from_openai(oai, mode=instructor.Mode.JSON)` for **all** providers, returning an `instructor.Instructor`.
- `call_llm(response_model, messages, **kwargs)` is the single-shot helper → `client.chat.completions.create_with_completion(response_model=..., messages=..., model=MODEL, **max_tokens_kwarg(...), **kwargs)`, returning `(validated_pydantic_instance, raw_completion)`. Hand-rolled retry `LLM_CALL_RETRIES=3` with exponential backoff (`2**attempt`), re-raises on exhaustion.

**Provider/model/key config (auto-detected at import → module globals `PROVIDER, MODEL`).** Resolution order in `_detect_provider()`: (1) `MLE_BEAST_PROVIDER` pin (only if its key is also present), (2) `LOCAL_LLM_BASE_URL`→local, (3) `OPENROUTER_API_KEY`→openrouter, (4) `OPENAI_API_KEY`→openai, (5) sentinel `'none'` so import never fails (error deferred to `get_client()`). Model from `MLE_BEAST_MODEL` or per-provider defaults (openrouter `google/gemini-3-flash-preview`, openai `gpt-5-mini`, local auto-detected via `/v1/models`). A SQLite `Settings` layer (`model_provider`, `model_name`) overrides env. `max_tokens_kwarg()` switches `max_completion_tokens` (openai) vs `max_tokens` (others).

**OpenRouter is a first-class, in fact the recommended, provider** — `.env.example` leads with it. Config is just `base_url="https://openrouter.ai/api/v1"` + `api_key=OPENROUTER_API_KEY`. **No `openrouter/` model prefix** (that's a litellm convention); the model is passed as a bare OpenRouter slug (`vendor/model`). It's the only provider that returns real per-call cost (`usage.cost`, USD); OpenAI/local return tokens only.

**Structured outputs** are driven entirely by instructor in `Mode.JSON`: the Pydantic `response_model`'s JSON schema is injected into the system prompt, the model emits JSON, instructor validates into the typed instance. No native function-calling / `tools=` API is used (this is what keeps it portable to local models).

**Tool-calling** is *emulated* on top of structured output, not via the OpenAI tools API. Per agent role a discriminated-union Pydantic model (`Field(discriminator="tool")`) lists exactly the allowed tools ("eliminates tool hallucination"); the actor loop asks the LLM to return one union member, then dispatches the validated args through a `TOOL_REGISTRY` dict (`execute_tool` returns strings; errors become observations, never raised).

**Node/agent unit pattern** (pocketflow `Node`, `prep→exec→post`): `BaseActorNode` runs an inner multi-iteration tool loop (cap `MAX_TOOL_ITERATIONS=30`, context trimming at `MAX_CONTEXT_CHARS=40000`, cancellation, deep-copies messages because instructor mutates `messages[0]`); `BaseCriticNode` judges with `_evaluate`/`_is_pass`, owns retry/feedback. State flows through a shared dict. **This whole graph machinery is overkill for simple research.**

**Prompts** (`prompts/` package): plain UPPER_SNAKE string constants, one module per stage, `_SYSTEM_PROMPT` / `_CRITIC_PROMPT` suffixes, static (no interpolation — runtime data goes in the user message), shared fragments composed by string concatenation.

**Retries/cost (three layers):** SDK `max_retries=2`; app-level `call_llm` retry×3 backoff; (in nodes) a third loop that fails gracefully. `extract_usage()` pulls prompt/completion/reasoning/cached tokens + OpenRouter cost; `emit_llm_call_event()` rolls it into per-run aggregates.

---

## Part 2 — Adaptation plan for the finance research feature (via OpenRouter)

### Current state (what we're replacing)
`backend/finance/research.py` calls the Anthropic SDK directly (`_generate_with_claude`) with Claude's native `web_search_20250305` tool, gated on `ANTHROPIC_API_KEY`. `config.py` `Settings` has `anthropic_api_key` + `research_model="claude-sonnet-4-6"` and a `research_enabled` property. Cache + handoff fallback live in `get_or_generate` via `BarStore.get_research/save_research` (DuckDB). `anthropic>=0.40` is the only LLM dep. **The cache layer, handoff fallback, `build_prompt` context injection, and the `get_or_generate` contract are all worth keeping verbatim** — only the generation call and config change.

### Adopt vs. skip
**Adopt (the lean subset):**
- The OpenAI-SDK-via-`base_url` + `instructor` client pattern (`_make_openai_client`, `get_client`, `call_llm`, `max_tokens_kwarg`, `_HTTP_TIMEOUT`, retry/backoff).
- `instructor.Mode.JSON` + a Pydantic `response_model` for validated structured research output.
- OpenRouter config via `base_url` + `OPENROUTER_API_KEY` + bare model slug; `extract_usage` for optional cost tracking.

**Skip (not needed for single-company research):**
- The pocketflow node graph, `BaseActorNode`/`BaseCriticNode`, actor/critic retry loops.
- The discriminated-union tool-calling machinery and `TOOL_REGISTRY` (unless we want multi-step web-search tools — see grounding note).
- Per-run DB usage rollup / event bus (`emit_llm_call_event`, `db.add_run_usage`) — the finance app has no run concept; cost tracking is optional.
- Local-server auto-detection and the SQLite settings override layer (finance uses pydantic-settings `.env`, not a DB settings table).

### New deps and env vars
- **`pyproject.toml`:** add `openai>=1.40`, `instructor>=1.5`; **remove `anthropic>=0.40`** (or keep temporarily during transition). `httpx` and `pydantic` are already transitively present.
- **`.env` (repo root):** add `OPENROUTER_API_KEY=...`. Optionally `RESEARCH_MODEL=<openrouter-slug>`.
- **Config change in `config.py` `Settings`:** add `openrouter_api_key: str = ""`; change `research_model` default from `"claude-sonnet-4-6"` to an OpenRouter slug (**confirm with user** — e.g. `anthropic/claude-sonnet-4-6` to preserve current behavior, or a cheap default like `google/gemini-3-flash-preview`). Update `research_enabled` to `bool(self.openrouter_api_key)`. Keep `anthropic_api_key` only if a fallback path is desired.

### Web-search grounding — the key tradeoff
The current code relies on Anthropic's **native** `web_search` server-tool, which MLE-Beast's JSON-mode pattern does **not** use. Options, in order of effort:
1. **OpenRouter `:online` suffix / web plugin (recommended, simplest).** OpenRouter supports web search by appending `:online` to the model slug (e.g. `anthropic/claude-sonnet-4-6:online`) or via the `web` plugin in the request body. This keeps grounding with **zero tool-loop code** — it stays a single-shot `call_llm`. **Caveat:** `:online` returns prose, and instructor JSON-mode + web plugin interaction should be verified; if it fights the JSON schema, fall back to plain markdown output (option 3).
2. **Native model tool-calling** for models that support it via OpenRouter (`tools=`) — more faithful but reintroduces a tool loop; not worth it for one report.
3. **No live web search** — drop grounding, return model knowledge only. Lowest fidelity; only if `:online` proves unreliable.

Because the existing output is **free markdown** (not structured), the **simplest faithful port is: keep markdown output (no `response_model`), use a single-shot OpenAI-SDK call with `model=<slug>:online`** — i.e. we don't even strictly need instructor for this feature. Adopting instructor + a `CompanyResearch` Pydantic model is a *nice-to-have upgrade* (typed fields, source list) but conflicts most with web-search grounding. **Confirm with user: keep free-form markdown, or move to structured `CompanyResearch`?**

### Vendor vs. thin equivalent
**Recommendation: write a thin equivalent**, not vendor the whole `llm.py`. MLE-Beast's `llm.py` carries local-detection, settings-DB override, event emission, and module-global import-time detection that don't fit finance's config model. A ~40-line `backend/finance/llm.py` capturing just `_make_openai_client` + `get_client` + a `call_llm`-style helper (or even a direct `client.chat.completions.create`) is cleaner and dependency-light. **Confirm with user** if they'd rather vendor for cross-project consistency.

### File-by-file changes
1. **`backend/pyproject.toml`** — add `openai`, `instructor`; remove/retire `anthropic`.
2. **`backend/finance/config.py`** — `Settings`: add `openrouter_api_key`, retype/redefault `research_model` to an OpenRouter slug, point `research_enabled` at the OpenRouter key. (`anthropic_api_key` optional/legacy.)
3. **`backend/finance/llm.py` (new, ~40 lines)** — `OPENROUTER_BASE_URL="https://openrouter.ai/api/v1"`; `get_client(settings)` building `OpenAI(api_key=settings.openrouter_api_key, base_url=..., timeout=httpx.Timeout(connect=10, read=180, write=60, pool=10), max_retries=2)`; a `call_llm(...)` helper with retry×3 + `2**attempt` backoff. Wrap with `instructor.from_openai(..., Mode.JSON)` **only if** going structured.
4. **`backend/finance/research.py`** — replace `_generate_with_claude` with `_generate_with_openrouter(prompt, settings)` that calls the new client. Append `:online` (or web plugin) to `settings.research_model` for grounding. Keep `SYSTEM`, `build_prompt`, `get_or_generate`, the cache check, and the handoff fallback **unchanged**. The `mode` values (`cached`/`generated`/`handoff`) and return shape stay identical so API callers and `store.save_research/get_research` need no change.
5. **`.env` / `.env.example`** — add `OPENROUTER_API_KEY` and optional `RESEARCH_MODEL`.
6. **Tests** — update any test that monkeypatches `_generate_with_claude` / `anthropic`; the `get_or_generate` cache+handoff contract tests should keep passing untouched.

### Tradeoffs & confirmations to surface
- **Default model?** Preserve Claude quality (`anthropic/claude-sonnet-4-6` via OpenRouter) vs. cheaper default (`google/gemini-3-flash-preview`, `deepseek/...`).
- **Markdown vs. structured `CompanyResearch`?** Markdown is the minimal faithful port and plays best with `:online` web search; structured output is the bigger MLE-Beast-flavored upgrade but complicates grounding.
- **Web search via `:online`** needs a quick live verification against the chosen model (support + cost + whether it coexists with JSON mode). Fallback is plain (ungrounded) generation.
- **Vendor vs. thin client** (recommend thin).
- **Cost tracking** (`usage.cost`) is available from OpenRouter — optional to surface in the UI; finance has no run-aggregate concept so it'd be per-report only.
- The handoff fallback's value drops once a key is configured, but keeping it costs nothing and preserves the free no-key path.

Relevant files: `/home/cpadwick/code/finance/backend/finance/research.py`, `/home/cpadwick/code/finance/backend/finance/config.py`, `/home/cpadwick/code/finance/backend/finance/store.py` (lines 40, 139-152), `/home/cpadwick/code/finance/backend/pyproject.toml`.