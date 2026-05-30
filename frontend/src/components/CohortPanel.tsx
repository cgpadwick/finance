import ReactMarkdown from "react-markdown";
import { useCohort, useGenerateCohort } from "../lib/api";
import type { CohortAggregate, ResearchResult } from "../types";
import { ReportMeta } from "./ResearchPanel";
import { num, pct } from "../lib/format";

function AggregateStats({ a }: { a: CohortAggregate }) {
  const cells: [string, string][] = [
    ["Above 200d MA", pct(a.pct_above_200ma, 0)],
    ["Golden cross", pct(a.pct_golden_cross, 0)],
    ["Bullish verdict", pct(a.pct_bullish_verdict, 0)],
    ["Avg RSI", num(a.avg_rsi, 0)],
    ["Avg 6m", pct(a.avg_6m_return, 0)],
    ["Avg max DD", pct(a.avg_max_drawdown, 0)],
  ];
  return (
    <div className="mb-3 grid grid-cols-3 gap-2 sm:grid-cols-6">
      {cells.map(([label, value]) => (
        <div key={label} className="rounded-lg border border-slate-800 bg-slate-900/50 p-2">
          <div className="text-[10px] uppercase tracking-wide text-slate-500">{label}</div>
          <div className="text-sm font-semibold tabular-nums text-slate-200">{value}</div>
        </div>
      ))}
    </div>
  );
}

// Theme-level "why is this cohort moving" briefing for the current winners/losers.
export function CohortPanel({ view }: { view: "winners" | "losers" }) {
  const cached = useCohort(view);
  const gen = useGenerateCohort(view);

  const result: ResearchResult | undefined = gen.data ?? cached.data;
  const report = result?.report;
  const enabled = result?.research_enabled ?? true;

  return (
    <div className="rounded-xl border border-slate-800 bg-slate-900/30 p-4">
      <div className="mb-3 flex items-center justify-between">
        <h3 className="text-sm font-semibold text-slate-200">
          Cohort briefing — what's driving the {view}
        </h3>
        <button
          onClick={() => gen.mutate({ force: !!report })}
          disabled={gen.isPending}
          className="rounded-md bg-emerald-700/70 px-3 py-1 text-xs font-medium text-emerald-50 hover:bg-emerald-600/70 disabled:opacity-50"
        >
          {gen.isPending
            ? "Researching…"
            : report
              ? "Regenerate"
              : `Research top ${view}`}
        </button>
      </div>

      {gen.isError && (
        <div className="mb-2 text-xs text-rose-400">{String(gen.error)}</div>
      )}

      {result?.aggregate && result.aggregate.n > 0 && (
        <AggregateStats a={result.aggregate} />
      )}

      {report ? (
        <div className="prose prose-invert prose-sm max-w-none prose-headings:text-slate-200 prose-p:text-slate-300 prose-strong:text-slate-100 prose-li:text-slate-300">
          <ReactMarkdown>{report}</ReactMarkdown>
          {result && <ReportMeta r={result} />}
        </div>
      ) : (
        <p className="text-xs text-slate-500">
          {!enabled
            ? "Set OPENROUTER_API_KEY to enable in-app briefings."
            : `Generate a theme-level summary of why the current top ${view} are moving — grouped by sector, with the common root causes and shared risks.`}
        </p>
      )}
    </div>
  );
}
