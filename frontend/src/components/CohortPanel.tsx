import ReactMarkdown from "react-markdown";
import { useCohort, useGenerateCohort } from "../lib/api";
import type { ResearchResult } from "../types";
import { ReportMeta } from "./ResearchPanel";

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
