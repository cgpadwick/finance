import { useTechnicals } from "../lib/api";
import { num, pct, signColor } from "../lib/format";
import type { ScorecardItem } from "../types";

const SIGNAL_STYLE: Record<string, string> = {
  bullish: "bg-emerald-950/60 text-emerald-300 border-emerald-800",
  bearish: "bg-rose-950/60 text-rose-300 border-rose-800",
  neutral: "bg-slate-800/60 text-slate-400 border-slate-700",
};

// Plain-English explanations, matched by keyword in the label. Shown on hover.
const GLOSSARY: [string, string][] = [
  ["200-day MA", "The average price over the last ~200 trading days. Trading above it means the long-term trend is up."],
  ["50-day MA", "The average price over the last ~50 trading days. Above it = shorter-term uptrend."],
  ["50/200", "A 'golden cross' (50-day average rising above the 200-day) is a classic confirmed-uptrend signal; a 'death cross' is the opposite."],
  ["RSI", "Relative Strength Index, 0–100: how strongly the price has been rising. Above 70 = 'overbought' (possibly stretched); below 30 = 'oversold'."],
  ["MACD", "Compares short- vs long-term price trends. Above its signal line means upward momentum."],
  ["Bollinger", "Where today's price sits in its normal trading band. Near the top = strong move (or stretched); near the bottom = weak."],
  ["Drawdown", "How far the price is below its recent peak. Near 0% = at/near highs; a big negative = beaten down."],
  ["Volume", "How much the stock traded vs its recent average. A surge can confirm a price move."],
  ["Ann. Vol", "How much the price swings around, annualized. Higher = bumpier/riskier."],
  ["Max DD", "The worst peak-to-trough drop over the period."],
];

function explain(label: string): string | undefined {
  const hit = GLOSSARY.find(([k]) => label.includes(k));
  return hit?.[1];
}

function Chip({ item }: { item: ScorecardItem }) {
  return (
    <div
      title={explain(item.label)}
      className={`cursor-help rounded-lg border px-2.5 py-1.5 text-xs ${SIGNAL_STYLE[item.signal]}`}
    >
      <div className="font-medium">{item.label}</div>
      <div className="opacity-80">{item.detail}</div>
    </div>
  );
}

function Metric({ label, value, cls = "" }: { label: string; value: string; cls?: string }) {
  const tip = explain(label);
  return (
    <div title={tip} className={tip ? "cursor-help" : ""}>
      <div className="text-[10px] uppercase tracking-wide text-slate-500">{label}</div>
      <div className={`text-sm font-semibold tabular-nums ${cls}`}>{value}</div>
    </div>
  );
}

const VERDICT_PLAIN: Record<string, string> = {
  Bullish: "Most trend & momentum checks are positive — the stock is in an uptrend.",
  Bearish: "Most checks are negative — the stock is in a downtrend.",
  Mixed: "The signals are mixed — no clear trend right now.",
};

export function TechnicalsReport({ symbol }: { symbol: string }) {
  const { data, isLoading, error } = useTechnicals(symbol);

  if (isLoading) return <div className="text-xs text-slate-500">Computing technicals…</div>;
  if (error) return <div className="text-xs text-rose-400">{String(error)}</div>;
  if (!data) return null;

  const t = data.technicals;
  const rs = data.relative_strength;
  if (t.insufficient_data) {
    return (
      <div className="text-xs text-slate-500">
        Not enough price history for technical analysis ({t.bars} bars).
      </div>
    );
  }

  const vColor =
    t.verdict.label === "Bullish"
      ? "text-emerald-400"
      : t.verdict.label === "Bearish"
        ? "text-rose-400"
        : "text-slate-300";

  return (
    <div className="space-y-4">
      {/* verdict */}
      <div>
        <div className="flex items-center gap-3">
          <span className={`text-lg font-bold ${vColor}`}>{t.verdict.label}</span>
          <span className="text-xs text-slate-400">
            {t.verdict.bullish} of{" "}
            {t.verdict.bullish + t.verdict.bearish + t.verdict.neutral} checks positive
          </span>
        </div>
        <p className="mt-0.5 text-xs text-slate-400">{VERDICT_PLAIN[t.verdict.label]}</p>
        <p className="mt-0.5 text-[11px] text-slate-500">
          Each tile below is one technical check — green is positive, red negative.
          Hover any tile for what it means.
        </p>
      </div>

      {/* scorecard chips */}
      <div className="grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-4">
        {t.scorecard.map((c) => (
          <Chip key={c.label} item={c} />
        ))}
      </div>

      {/* performance & risk */}
      <div>
        <div className="mb-1 text-xs font-semibold text-slate-300">Performance & risk</div>
        <div className="grid grid-cols-3 gap-3 sm:grid-cols-4 lg:grid-cols-7">
          <Metric label="1M" value={pct(t.returns.r_1m, 0)} cls={signColor(t.returns.r_1m)} />
          <Metric label="3M" value={pct(t.returns.r_3m, 0)} cls={signColor(t.returns.r_3m)} />
          <Metric label="6M" value={pct(t.returns.r_6m, 0)} cls={signColor(t.returns.r_6m)} />
          <Metric label="12M" value={pct(t.returns.r_12m, 0)} cls={signColor(t.returns.r_12m)} />
          <Metric label="Ann. Vol" value={pct(t.volatility.ann_vol, 0)} />
          <Metric
            label="Max DD"
            value={pct(t.volatility.max_drawdown, 0)}
            cls="text-rose-400"
          />
          <Metric label="RSI(14)" value={num(t.momentum.rsi, 0)} />
        </div>
      </div>

      {/* relative strength */}
      {rs?.in_universe && (
        <div>
          <div className="mb-1 text-xs font-semibold text-slate-300">Relative strength</div>
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
            <Metric
              label="Market rank"
              value={`#${rs.rank} / ${rs.universe_size}`}
              cls="text-slate-100"
            />
            <Metric label="Percentile" value={pct(rs.percentile, 0)} cls="text-sky-300" />
            <Metric
              label={`vs ${rs.sector}`}
              value={`${rs.score_vs_sector! >= 0 ? "+" : ""}${num(rs.score_vs_sector, 2)}`}
              cls={signColor(rs.score_vs_sector)}
            />
            <Metric
              label="Decile"
              value={`${rs.decile} / 10`}
              cls={rs.decile! >= 8 ? "text-emerald-400" : rs.decile! <= 3 ? "text-rose-400" : ""}
            />
          </div>
        </div>
      )}
      {rs && !rs.in_universe && (
        <p className="text-[11px] text-slate-500">
          Not in the screened universe (filtered by price/liquidity), so no
          cross-sectional ranking — technicals above are still computed.
        </p>
      )}
    </div>
  );
}
