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
    "You are an equity research analyst writing for a NON-EXPERT investor. Write a "
    "concise, factual note for a momentum-screener UI. Be specific and current; use "
    "web search for recent catalysts. CRITICAL: explain any technical term in plain "
    "English the first time you use it — e.g. 'a golden cross (the 50-day average "
    "price rising above the 200-day, a classic uptrend signal)'. Never use bare "
    "jargon (RSI, MACD, drawdown) without a short plain-English gloss. "
    "Structure with short markdown sections: a one-line plain-English summary, "
    "**What they do**, **Why it's moving** (recent catalysts / why now), "
    "**The technical picture** (translate the computed signals into plain language), "
    "and **Key risks**. Keep it skimmable (~250-350 words). End with one line "
    "noting this is research context, not investment advice."
)


def build_context_block(ctx: dict) -> str:
    """The shared, grounded context (company + screener + computed technicals)."""

    def pct(x):
        return f"{x * 100:.0f}%" if isinstance(x, (int, float)) else "n/a"

    return (
        f"Company: {ctx.get('name') or ctx['symbol']} (ticker {ctx['symbol']}).\n"
        f"Screener context (as of {ctx.get('as_of')}):\n"
        f"- Sector / industry: {ctx.get('sector', 'n/a')} / {ctx.get('industry', 'n/a')}\n"
        f"- Recent price: ${ctx.get('last_close', 'n/a')}\n"
        f"- 12-1 momentum: {pct(ctx.get('mom_12_1'))}\n"
        f"- Risk-adjusted momentum: {ctx.get('risk_adjusted_mom', 'n/a')}\n"
        f"- Proximity to 52-week high: {pct(ctx.get('high_52w_proximity'))}\n"
        f"- Annualized volatility: {pct(ctx.get('volatility'))}\n\n"
        + (f"{ctx['technicals_text']}\n\n" if ctx.get("technicals_text") else "")
    )


def build_prompt(ctx: dict) -> str:
    """Single-report prompt (the original 'fact-based' note)."""
    return build_context_block(ctx) + (
        "Write the report. Explain what the company does and the most plausible "
        "reasons it has strong (or weak) price momentum right now, grounded in "
        "recent, real developments AND the computed technicals above (cite the "
        "real figures — do not invent numbers). If a momentum figure looks "
        "implausibly large, treat it as a dataset quirk and focus on the real company."
    )


# --------- bull / bear / facts analyst panel ---------

_PANEL_COMMON = (
    " Use web search for recent, real developments. Write for a non-expert and "
    "explain any technical term in plain English. Ground every claim in the "
    "computed figures above — never invent numbers. ~150-220 words, markdown."
)
PANEL_PERSONAS = {
    "facts": (
        "You are a strictly NEUTRAL research analyst. State only the facts: what "
        "the company does, its business/market, and what the computed technicals "
        "show — in plain language, with NO opinion, recommendation, or bull/bear "
        "spin." + _PANEL_COMMON
    ),
    "bull": (
        "You are a BULLISH analyst making the BULL CASE — the strongest honest "
        "argument FOR owning this stock: momentum, catalysts, technical strength, "
        "the opportunity. Be persuasive but factual." + _PANEL_COMMON
    ),
    "bear": (
        "You are a SKEPTICAL analyst making the BEAR CASE — the strongest honest "
        "argument for CAUTION: overbought conditions, drawdown/volatility risk, "
        "what could break the trend, competition/valuation concerns." + _PANEL_COMMON
    ),
}


def generate_panel(ctx: dict, settings: Settings, model_override: str | None = None) -> dict:
    """Run the three analysts concurrently, all grounded in the same context."""
    from concurrent.futures import ThreadPoolExecutor

    from finance.llm import complete_with_usage

    model = _resolve_model(settings, model_override)
    context = build_context_block(ctx)

    def run(persona_system: str) -> dict:
        return complete_with_usage(
            [
                {"role": "system", "content": persona_system},
                {"role": "user", "content": context + "Write only your assigned section."},
            ],
            model=model,
            settings=settings,
        )

    with ThreadPoolExecutor(max_workers=3) as ex:
        futures = {k: ex.submit(run, sysp) for k, sysp in PANEL_PERSONAS.items()}
        res = {k: f.result() for k, f in futures.items()}

    return {
        "facts": res["facts"]["text"],
        "bull": res["bull"]["text"],
        "bear": res["bear"]["text"],
        "model": res["facts"]["model"],
        "cost": round(sum((res[k]["cost"] or 0) for k in res), 6),
        "tokens": sum((res[k]["tokens"] or 0) for k in res),
    }


def get_or_generate_panel(
    ctx: dict,
    store: BarStore,
    generate: bool = False,
    settings: Settings | None = None,
    now: str = "",
    force: bool = False,
    model: str | None = None,
) -> dict:
    """Bull/bear/facts panel for a stock, cached as JSON under '<as_of>|panel'."""
    import json

    settings = settings or get_settings()
    symbol = ctx["symbol"]
    key = f"{ctx.get('as_of') or 'latest'}|panel"

    if not force:
        cached = store.get_research(symbol, key)
        if cached:
            parts = json.loads(cached["report"])
            return {"mode": "cached", **parts, "generated_at": cached.get("generated_at"),
                    "model": cached.get("model"), "cost": cached.get("cost"),
                    "tokens": cached.get("tokens")}

    if generate and settings.research_enabled:
        panel = generate_panel(ctx, settings, model_override=model)
        store.save_research(
            symbol, key, json.dumps({k: panel[k] for k in ("facts", "bull", "bear")}),
            now, panel["model"], panel["cost"], panel["tokens"],
        )
        return {"mode": "generated", "generated_at": now, **panel}

    return {
        "mode": "handoff",
        "prompt": build_prompt(ctx),
        "research_enabled": settings.research_enabled,
    }


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
    "You are a portfolio strategist writing a brief note for a NON-EXPERT reader. "
    "Identify the dominant THEMES driving the group (group names by theme/sector), "
    "the common root cause(s) of the momentum, and the shared risks. Use web search "
    "for recent context. Explain any technical term in plain English. Output "
    "skimmable markdown: a one-line plain takeaway, **Themes** (grouped), "
    "**What's driving it**, and **Cross-cutting risks**. ~250-350 words. End "
    "noting this is research context, not investment advice."
)


def build_cohort_prompt(
    label: str, rows: list[dict], as_of: str, tech_text: str = ""
) -> str:
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
        f"These are {label} from a stock screener (as of {as_of}):\n\n{body}\n\n"
        + (f"{tech_text}\n\n" if tech_text else "")
        + "Explain what themes connect them and why this group is moving now, "
        "citing the computed cohort technicals above where relevant (do not "
        "invent numbers). Treat any implausibly large momentum figures as "
        "dataset quirks."
    )


def get_or_generate_cohort(
    label: str,
    key: str,
    rows: list[dict],
    as_of: str,
    store: BarStore,
    generate: bool = False,
    settings: Settings | None = None,
    now: str = "",
    force: bool = False,
    model: str | None = None,
    tech_text: str = "",
) -> dict:
    """Group-level briefing (winners/losers/sector), cached under `key`."""
    settings = settings or get_settings()

    if not force:
        cached = store.get_research(key, as_of)
        if cached:
            return _cached_result(cached)

    prompt = build_cohort_prompt(label, rows, as_of, tech_text)

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
