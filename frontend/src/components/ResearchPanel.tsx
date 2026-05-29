import { useState } from "react";
import ReactMarkdown from "react-markdown";
import { useGenerateResearch, useResearch } from "../lib/api";
import type { ResearchResult } from "../types";

export function ResearchPanel({ symbol }: { symbol: string }) {
  const cached = useResearch(symbol);
  const gen = useGenerateResearch(symbol);
  const [copied, setCopied] = useState(false);

  // Prefer a freshly-generated result, else the cached one.
  const result: ResearchResult | undefined = gen.data ?? cached.data;
  const report = result?.report;
  const handoffPrompt =
    result?.mode === "handoff" ? result.prompt : undefined;

  const copy = async (text: string) => {
    await navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 1500);
  };

  return (
    <div className="rounded-xl border border-slate-800 bg-slate-900/30 p-4">
      <div className="mb-3 flex items-center justify-between">
        <h3 className="text-sm font-semibold text-slate-200">AI Research</h3>
        <button
          onClick={() => gen.mutate()}
          disabled={gen.isPending}
          className="rounded-md bg-sky-700/70 px-3 py-1 text-xs font-medium text-sky-50 hover:bg-sky-600/70 disabled:opacity-50"
        >
          {gen.isPending
            ? "Generating…"
            : report
              ? "Regenerate"
              : "Generate report"}
        </button>
      </div>

      {gen.isError && (
        <div className="mb-2 text-xs text-rose-400">
          {String(gen.error)}
        </div>
      )}

      {report ? (
        <div className="prose prose-invert prose-sm max-w-none prose-headings:text-slate-200 prose-p:text-slate-300 prose-strong:text-slate-100 prose-li:text-slate-300">
          <ReactMarkdown>{report}</ReactMarkdown>
          {result?.generated_at && (
            <p className="text-[11px] text-slate-500">
              Generated {result.generated_at}
              {result.mode === "cached" ? " (cached)" : ""}
            </p>
          )}
        </div>
      ) : handoffPrompt ? (
        <div>
          <p className="mb-2 text-xs text-slate-400">
            No <code>ANTHROPIC_API_KEY</code> configured — copy this prompt into
            any LLM (Claude, ChatGPT, etc.) to get the report. Add a key to{" "}
            <code>.env</code> to generate it in-app.
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
