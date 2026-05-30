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


def _resolve_model(settings: Settings, model_override: str | None) -> str:
    """Pick the model slug and append ':online' for web grounding when on."""
    model = model_override or settings.research_model
    if settings.research_web_search and ":online" not in model:
        model = f"{model}:online"
    return model


def _generate(prompt: str, settings: Settings, model_override: str | None = None) -> dict:
    """Generate markdown via OpenRouter; return {report, model, cost, tokens}."""
    from finance.llm import complete_with_usage

    messages = [
        {"role": "system", "content": SYSTEM},
        {"role": "user", "content": prompt},
    ]
    res = complete_with_usage(
        messages, model=_resolve_model(settings, model_override), settings=settings
    )
    return {
        "report": res["text"],
        "model": res["model"],
        "cost": res["cost"],
        "tokens": res["tokens"],
    }


def _cached_result(cached: dict) -> dict:
    return {
        "mode": "cached",
        "report": cached["report"],
        "generated_at": cached.get("generated_at"),
        "model": cached.get("model"),
        "cost": cached.get("cost"),
        "tokens": cached.get("tokens"),
    }


def get_or_generate(
    ctx: dict,
    store: BarStore,
    generate: bool = False,
    settings: Settings | None = None,
    now: str = "",
    force: bool = False,
    model: str | None = None,
) -> dict:
    """Return a research result for a stock.

    - Returns the cached report if present (unless force=True).
    - If generate=True and a key is configured, generate + cache (mode='generated').
    - Otherwise return the hand-off prompt (mode='handoff').
    """
    settings = settings or get_settings()
    symbol = ctx["symbol"]
    as_of = ctx.get("as_of") or "latest"

    if not force:
        cached = store.get_research(symbol, as_of)
        if cached:
            return _cached_result(cached)

    prompt = build_prompt(ctx)

    if generate and settings.research_enabled:
        gen = _generate(prompt, settings, model_override=model)
        store.save_research(
            symbol, as_of, gen["report"], now, gen["model"], gen["cost"], gen["tokens"]
        )
        return {"mode": "generated", "generated_at": now, **gen}

    return {
        "mode": "handoff",
        "report": None,
        "prompt": prompt,
        "research_enabled": settings.research_enabled,
    }


# --------------------------- cohort briefing ---------------------------

COHORT_SYSTEM = (
    "You are a portfolio strategist writing a brief momentum-cohort note for a "
    "screener UI. Identify the dominant THEMES driving the group (group names by "
    "theme/sector), the common root cause(s) of the momentum, and the shared "
    "risks. Use web search for recent context. Output skimmable markdown: a "
    "one-line takeaway, **Themes** (grouped), **What's driving it**, and "
    "**Cross-cutting risks**. ~250-350 words. End noting this is research "
    "context, not investment advice."
)


def build_cohort_prompt(view: str, rows: list[dict], as_of: str) -> str:
    lines = []
    for r in rows:
        mom = r.get("mom_12_1")
        mom_s = f"{mom * 100:.0f}%" if isinstance(mom, (int, float)) else "n/a"
        lines.append(
            f"- {r['symbol']} ({r.get('sector', 'n/a')}): score {r.get('score'):.2f}, "
            f"12-1 mom {mom_s}"
        )
    body = "\n".join(lines)
    return (
        f"These are the current momentum {view} from a stock screener "
        f"(as of {as_of}):\n\n{body}\n\n"
        "Explain what themes connect them and why this group is moving now. "
        "Treat any implausibly large momentum figures as dataset quirks."
    )


def get_or_generate_cohort(
    view: str,
    rows: list[dict],
    as_of: str,
    store: BarStore,
    generate: bool = False,
    settings: Settings | None = None,
    now: str = "",
    force: bool = False,
    model: str | None = None,
) -> dict:
    """Cohort-level briefing, cached under a synthetic symbol key."""
    settings = settings or get_settings()
    key = f"__cohort_{view}"

    if not force:
        cached = store.get_research(key, as_of)
        if cached:
            return _cached_result(cached)

    prompt = build_cohort_prompt(view, rows, as_of)

    if generate and settings.research_enabled:
        from finance.llm import complete_with_usage

        messages = [
            {"role": "system", "content": COHORT_SYSTEM},
            {"role": "user", "content": prompt},
        ]
        res = complete_with_usage(
            messages, model=_resolve_model(settings, model), settings=settings
        )
        store.save_research(
            key, as_of, res["text"], now, res["model"], res["cost"], res["tokens"]
        )
        return {
            "mode": "generated",
            "report": res["text"],
            "generated_at": now,
            "model": res["model"],
            "cost": res["cost"],
            "tokens": res["tokens"],
        }

    return {
        "mode": "handoff",
        "report": None,
        "prompt": prompt,
        "research_enabled": settings.research_enabled,
    }
