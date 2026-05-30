import { useEffect, useState } from "react";
import ReactMarkdown from "react-markdown";
import { useGeneratePanel, usePanel, useResearchConfig } from "../lib/api";
import type { PanelResult } from "../types";
import { ReportMeta } from "./ResearchPanel";

type Tab = "facts" | "bull" | "bear";

const TABS: { key: Tab; label: string; active: string }[] = [
  { key: "facts", label: "📋 Facts", active: "bg-slate-700 text-slate-100" },
  { key: "bull", label: "🟢 Bull case", active: "bg-emerald-700/70 text-emerald-50" },
  { key: "bear", label: "🔴 Bear case", active: "bg-rose-700/70 text-rose-50" },
];

export function DebatePanel({ symbol }: { symbol: string }) {
  const config = useResearchConfig();
  const cached = usePanel(symbol);
  const gen = useGeneratePanel(symbol);
  const [model, setModel] = useState("");
  const [tab, setTab] = useState<Tab>("facts");

  useEffect(() => {
    if (config.data?.model && !model) setModel(config.data.model);
  }, [config.data, model]);

  const result: PanelResult | undefined = gen.data ?? cached.data;
  const hasReport = !!(result?.facts || result?.bull || result?.bear);
  const enabled = config.data?.enabled;
  const handoff = result?.mode === "handoff" ? result.prompt : undefined;

  return (
    <div>
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <span className="text-sm font-semibold text-slate-200">
          Analyst panel — Facts · Bull · Bear
        </span>
        <div className="flex items-center gap-2">
          {enabled && config.data && (
            <select
              value={model}
              onChange={(e) => setModel(e.target.value)}
              className="rounded-md border border-slate-800 bg-slate-900/60 px-2 py-1 text-xs text-slate-200"
            >
              {config.data.models.map((m) => (
                <option key={m} value={m}>
                  {m}
                </option>
              ))}
            </select>
          )}
          <button
            onClick={() => gen.mutate({ model, force: hasReport })}
            disabled={gen.isPending}
            className="rounded-md bg-sky-700/70 px-3 py-1 text-xs font-medium text-sky-50 hover:bg-sky-600/70 disabled:opacity-50"
          >
            {gen.isPending
              ? "Convening analysts…"
              : hasReport
                ? "Regenerate"
                : "Generate analyst panel"}
          </button>
        </div>
      </div>

      {gen.isError && (
        <div className="mb-2 text-xs text-rose-400">{String(gen.error)}</div>
      )}

      {hasReport ? (
        <div>
          <div className="mb-3 flex gap-1 rounded-lg border border-slate-800 p-1">
            {TABS.map((t) => (
              <button
                key={t.key}
                onClick={() => setTab(t.key)}
                className={`rounded-md px-3 py-1 text-xs font-medium transition ${
                  tab === t.key ? t.active : "text-slate-400 hover:text-slate-200"
                }`}
              >
                {t.label}
              </button>
            ))}
          </div>
          <div className="prose prose-invert prose-sm max-w-none prose-headings:text-slate-200 prose-p:text-slate-300 prose-strong:text-slate-100 prose-li:text-slate-300">
            <ReactMarkdown>{result?.[tab] ?? ""}</ReactMarkdown>
          </div>
          {result && <ReportMeta r={result} />}
        </div>
      ) : handoff ? (
        <p className="text-xs text-slate-400">
          No <code>OPENROUTER_API_KEY</code> configured — set one in <code>.env</code>{" "}
          to generate the analyst panel in-app.
        </p>
      ) : (
        <p className="text-xs text-slate-500">
          {cached.isLoading
            ? "Checking for a saved panel…"
            : "Generate three independent takes — a neutral facts brief, a bull case, and a bear case — all grounded in the same computed numbers, so you can weigh both sides."}
        </p>
      )}
    </div>
  );
}
