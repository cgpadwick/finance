import { useTechnicals } from "../lib/api";
import { num, pct, signColor } from "../lib/format";
import type { ScorecardItem } from "../types";

const SIGNAL_STYLE: Record<string, string> = {
  bullish: "bg-emerald-950/60 text-emerald-300 border-emerald-800",
  bearish: "bg-rose-950/60 text-rose-300 border-rose-800",
  neutral: "bg-slate-800/60 text-slate-400 border-slate-700",
};

function Chip({ item }: { item: ScorecardItem }) {
  return (
    <div className={`rounded-lg border px-2.5 py-1.5 text-xs ${SIGNAL_STYLE[item.signal]}`}>
      <div className="font-medium">{item.label}</div>
      <div className="opacity-80">{item.detail}</div>
    </div>
  );
}

function Metric({ label, value, cls = "" }: { label: string; value: string; cls?: string }) {
  return (
    <div>
      <div className="text-[10px] uppercase tracking-wide text-slate-500">{label}</div>
      <div className={`text-sm font-semibold tabular-nums ${cls}`}>{value}</div>
    </div>
  );
}

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
      <div className="flex items-center gap-3">
        <span className={`text-lg font-bold ${vColor}`}>{t.verdict.label}</span>
        <span className="text-xs text-slate-400">
          <span className="text-emerald-400">{t.verdict.bullish} bullish</span> ·{" "}
          <span className="text-rose-400">{t.verdict.bearish} bearish</span> ·{" "}
          {t.verdict.neutral} neutral
        </span>
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
