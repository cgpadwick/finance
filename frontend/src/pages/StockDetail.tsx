import { Link, useParams } from "react-router-dom";
import { useStock } from "../lib/api";
import { CandleChart } from "../components/CandleChart";
import { ResearchPanel } from "../components/ResearchPanel";
import { compactMoney, money, num, pct, signColor } from "../lib/format";

const LINKS = (s: string) => [
  { label: "Yahoo", url: `https://finance.yahoo.com/quote/${s}` },
  { label: "StockAnalysis", url: `https://stockanalysis.com/stocks/${s}/` },
  { label: "Finviz", url: `https://finviz.com/quote.ashx?t=${s}` },
  {
    label: "SEC filings",
    url: `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&ticker=${s}&type=10-K`,
  },
];

function Stat({
  label,
  value,
  hint,
  className = "",
}: {
  label: string;
  value: string;
  hint?: string;
  className?: string;
}) {
  return (
    <div className="rounded-lg border border-slate-800 bg-slate-900/40 p-3">
      <div className="text-[11px] uppercase tracking-wide text-slate-500">
        {label}
      </div>
      <div className={`text-lg font-semibold tabular-nums ${className}`}>
        {value}
      </div>
      {hint && <div className="text-[11px] text-slate-500">{hint}</div>}
    </div>
  );
}

export function StockDetail() {
  const { symbol } = useParams<{ symbol: string }>();
  const { data, isLoading, error } = useStock(symbol);

  return (
    <div>
      <div className="mb-1 flex items-center gap-3">
        <Link to="/" className="text-sm text-sky-400 hover:text-sky-300">
          ← Leaderboard
        </Link>
        <h2 className="text-xl font-bold text-slate-100">{symbol}</h2>
        {data?.company?.name && (
          <span className="text-sm text-slate-400">{data.company.name}</span>
        )}
        {data?.signals && (
          <span
            className={`rounded-full px-2 py-0.5 text-xs font-medium ${
              data.signals.eligible
                ? "bg-emerald-950 text-emerald-400"
                : "bg-slate-800 text-slate-400"
            }`}
          >
            {data.signals.eligible ? "In universe" : "Filtered out"}
          </span>
        )}
      </div>

      {/* About row: sector/industry + outbound links */}
      <div className="mb-5 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-slate-400">
        {data?.company?.sector && (
          <span className="rounded bg-slate-800 px-2 py-0.5 text-slate-300">
            {data.company.sector}
          </span>
        )}
        {data?.company?.industry && <span>{data.company.industry}</span>}
        <span className="flex items-center gap-2">
          {LINKS(symbol ?? "").map((l) => (
            <a
              key={l.label}
              href={l.url}
              target="_blank"
              rel="noreferrer"
              className="text-sky-400 hover:text-sky-300"
            >
              {l.label} ↗
            </a>
          ))}
        </span>
      </div>

      {error && (
        <div className="rounded-lg border border-rose-900 bg-rose-950/40 p-4 text-sm text-rose-300">
          {String(error)}
        </div>
      )}
      {isLoading && <div className="text-slate-500">Loading…</div>}

      {data && data.signals && (
        <div className="mb-5 grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-6">
          <Stat label="Price" value={money(data.signals.last_close)} />
          <Stat
            label="12-1 Momentum"
            value={pct(data.signals.mom_12_1, 0)}
            className={signColor(data.signals.mom_12_1)}
            hint="trailing 12mo, skip 1"
          />
          <Stat
            label="Risk-adj Mom"
            value={num(data.signals.risk_adjusted_mom, 2)}
            className={signColor(data.signals.risk_adjusted_mom)}
            hint="mom / volatility"
          />
          <Stat
            label="52w High"
            value={pct(data.signals.high_52w_proximity, 0)}
            hint="proximity to high"
          />
          <Stat label="Volatility" value={pct(data.signals.volatility, 0)} hint="annualized" />
          <Stat
            label="$ Volume"
            value={compactMoney(data.signals.avg_dollar_volume)}
            hint="20d avg"
          />
        </div>
      )}

      {data && data.bars.length > 0 && (
        <div className="rounded-xl border border-slate-800 bg-slate-900/30 p-4">
          <div className="mb-2 flex items-center gap-4 text-xs text-slate-400">
            <span>Adjusted price</span>
            <span className="flex items-center gap-1">
              <span className="inline-block h-0.5 w-4 bg-[#60a5fa]" /> MA20
            </span>
            <span className="flex items-center gap-1">
              <span className="inline-block h-0.5 w-4 bg-[#f59e0b]" /> MA50
            </span>
            <span className="flex items-center gap-1">
              <span className="inline-block h-0.5 w-4 bg-[#a78bfa]" /> MA200
            </span>
          </div>
          <CandleChart bars={data.bars} movingAverages={data.moving_averages} />
        </div>
      )}

      {symbol && (
        <div className="mt-5">
          <ResearchPanel symbol={symbol} />
        </div>
      )}
    </div>
  );
}
