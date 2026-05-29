"""Per-company AI research reports.

Given a stock's screener context (sector, momentum stats), produce a short
"what they do / why it's moving" report. Two modes:
  - generated: if ANTHROPIC_API_KEY is set, call Claude (with web search) and
    cache the markdown in DuckDB.
  - handoff:   otherwise return a ready-to-paste prompt the user can drop into
    any LLM (free, no key).

The key differentiator vs. a generic "tell me about TICKER" prompt: we feed the
model the screener's quantitative context so the report explains *these* numbers.
"""

from __future__ import annotations

from finance.config import Settings, get_settings
from finance.store import BarStore

SYSTEM = (
    "You are an equity research analyst. Write a concise, factual research note "
    "for a momentum-screener UI. Be specific and current; use web search to find "
    "recent catalysts. Structure with short markdown sections: a one-line summary, "
    "**What they do**, **Why it's moving** (recent catalysts / why now), "
    "**Market context**, and **Key risks**. Keep it skimmable (~250-350 words). "
    "End with one line noting this is research context, not investment advice."
)


def build_prompt(ctx: dict) -> str:
    """Human-readable prompt embedding the screener's quantitative context."""

    def pct(x):
        return f"{x * 100:.0f}%" if isinstance(x, (int, float)) else "n/a"

    return (
        f"Research the US-listed company {ctx.get('name') or ctx['symbol']} "
        f"(ticker {ctx['symbol']}).\n\n"
        f"It appears on a momentum screener with this context (as of {ctx.get('as_of')}):\n"
        f"- Sector / industry: {ctx.get('sector', 'n/a')} / {ctx.get('industry', 'n/a')}\n"
        f"- Recent price: ${ctx.get('last_close', 'n/a')}\n"
        f"- 12-1 momentum: {pct(ctx.get('mom_12_1'))}\n"
        f"- Risk-adjusted momentum: {ctx.get('risk_adjusted_mom', 'n/a')}\n"
        f"- Proximity to 52-week high: {pct(ctx.get('high_52w_proximity'))}\n"
        f"- Annualized volatility: {pct(ctx.get('volatility'))}\n\n"
        "Explain what the company does and the most plausible reasons it has "
        "strong (or weak) price momentum right now, grounded in recent, real "
        "developments. If the momentum figure looks implausibly large, treat it "
        "as a dataset quirk and focus on the real company."
    )


def _generate_with_claude(prompt: str, settings: Settings) -> str:
    from anthropic import Anthropic

    client = Anthropic(api_key=settings.anthropic_api_key)
    resp = client.messages.create(
        model=settings.research_model,
        max_tokens=1500,
        system=[{"type": "text", "text": SYSTEM, "cache_control": {"type": "ephemeral"}}],
        messages=[{"role": "user", "content": prompt}],
        tools=[{"type": "web_search_20250305", "name": "web_search", "max_uses": 5}],
    )
    parts = [b.text for b in resp.content if getattr(b, "type", None) == "text"]
    return "\n".join(p for p in parts if p).strip()


def get_or_generate(
    ctx: dict,
    store: BarStore,
    generate: bool = False,
    settings: Settings | None = None,
    now: str = "",
) -> dict:
    """Return a research result for a stock.

    - Always returns the cached report if present (mode='cached').
    - If generate=True and a key is configured, generate + cache (mode='generated').
    - Otherwise return the hand-off prompt (mode='handoff').
    """
    settings = settings or get_settings()
    symbol = ctx["symbol"]
    as_of = ctx.get("as_of") or "latest"

    cached = store.get_research(symbol, as_of)
    if cached:
        return {"mode": "cached", "report": cached["report"], "generated_at": cached["generated_at"]}

    prompt = build_prompt(ctx)

    if generate and settings.research_enabled:
        report = _generate_with_claude(prompt, settings)
        store.save_research(symbol, as_of, report, now)
        return {"mode": "generated", "report": report, "generated_at": now}

    return {
        "mode": "handoff",
        "report": None,
        "prompt": prompt,
        "research_enabled": settings.research_enabled,
    }
