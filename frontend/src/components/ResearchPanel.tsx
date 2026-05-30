import { useEffect, useState } from "react";
import ReactMarkdown from "react-markdown";
import {
  useGenerateResearch,
  useResearch,
  useResearchConfig,
} from "../lib/api";
import type { ResearchResult } from "../types";

export function ReportMeta({ r }: { r: ResearchResult }) {
  if (!r.model && r.cost == null) return null;
  return (
    <p className="mt-1 text-[11px] text-slate-500">
      {r.model && <>model {r.model} · </>}
      {r.cost != null && <>${r.cost.toFixed(4)} · </>}
      {r.tokens != null && <>{r.tokens} tok · </>}
      {r.generated_at}
      {r.mode === "cached" ? " (cached)" : ""}
    </p>
  );
}

export function ResearchPanel({ symbol }: { symbol: string }) {
  const config = useResearchConfig();
  const cached = useResearch(symbol);
  const gen = useGenerateResearch(symbol);
  const [model, setModel] = useState<string>("");
  const [copied, setCopied] = useState(false);

  // default the picker to the server's configured model once known
  useEffect(() => {
    if (config.data?.model && !model) setModel(config.data.model);
  }, [config.data, model]);

  const result: ResearchResult | undefined = gen.data ?? cached.data;
  const report = result?.report;
  const handoffPrompt = result?.mode === "handoff" ? result.prompt : undefined;
  const enabled = config.data?.enabled;

  const copy = async (text: string) => {
    await navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 1500);
  };

  return (
    <div className="rounded-xl border border-slate-800 bg-slate-900/30 p-4">
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <h3 className="text-sm font-semibold text-slate-200">AI Research</h3>
        <div className="flex items-center gap-2">
          {enabled && config.data && (
            <select
              value={model}
              onChange={(e) => setModel(e.target.value)}
              className="rounded-md border border-slate-800 bg-slate-900/60 px-2 py-1 text-xs text-slate-200"
              title="Model"
            >
              {config.data.models.map((m) => (
                <option key={m} value={m}>
                  {m}
                </option>
              ))}
            </select>
          )}
          <button
            onClick={() => gen.mutate({ model, force: !!report })}
            disabled={gen.isPending}
            className="rounded-md bg-sky-700/70 px-3 py-1 text-xs font-medium text-sky-50 hover:bg-sky-600/70 disabled:opacity-50"
          >
            {gen.isPending ? "Generating…" : report ? "Regenerate" : "Generate report"}
          </button>
        </div>
      </div>

      {gen.isError && (
        <div className="mb-2 text-xs text-rose-400">{String(gen.error)}</div>
      )}

      {report ? (
        <div className="prose prose-invert prose-sm max-w-none prose-headings:text-slate-200 prose-p:text-slate-300 prose-strong:text-slate-100 prose-li:text-slate-300">
          <ReactMarkdown>{report}</ReactMarkdown>
          {result && <ReportMeta r={result} />}
        </div>
      ) : handoffPrompt ? (
        <div>
          <p className="mb-2 text-xs text-slate-400">
            No <code>OPENROUTER_API_KEY</code> configured — copy this prompt into
            any LLM. Add a key to <code>.env</code> to generate in-app.
          </p>
          <textarea
            readOnly
            value={handoffPrompt}
            className="h-40 w-full resize-none rounded-md border border-slate-800 bg-slate-950/60 p-2 text-xs text-slate-300"
          />
          <button
            onClick={() => copy(handoffPrompt)}
            className="mt-2 rounded-md border border-slate-700 px-3 py-1 text-xs text-slate-200 hover:bg-slate-800"
          >
            {copied ? "Copied ✓" : "Copy prompt"}
          </button>
        </div>
      ) : (
        <p className="text-xs text-slate-500">
          {cached.isLoading
            ? "Checking for a saved report…"
            : "Click “Generate report” for a short, web-researched note on what this company does and why it's moving."}
        </p>
      )}
    </div>
  );
}
