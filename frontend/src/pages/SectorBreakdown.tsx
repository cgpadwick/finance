import { Fragment, useMemo, useState } from "react";
import { Link } from "react-router-dom";
import { useLeaderboard, useSectors } from "../lib/api";
import { pct, num, signColor } from "../lib/format";
import type { LeaderboardRow } from "../types";

// One stock as a diverging score bar (reused inside the sector drill-down).
function StockBar({ r, maxAbs }: { r: LeaderboardRow; maxAbs: number }) {
  const w = (Math.abs(r.score) / maxAbs) * 50;
  const pos = r.score >= 0;
  return (
    <div className="flex items-center gap-3 py-1 text-xs">
      <Link
        to={`/stock/${r.symbol}`}
        className="w-16 shrink-0 font-semibold text-sky-400 hover:text-sky-300"
      >
        {r.symbol}
      </Link>
      <span className={`w-12 shrink-0 text-right tabular-nums ${signColor(r.score)}`}>
        {num(r.score, 2)}
      </span>
      <div className="relative h-2.5 flex-1 rounded bg-slate-800">
        <div className="absolute left-1/2 top-0 h-full w-px bg-slate-600" />
        <div
          className={`absolute top-0 h-full rounded ${pos ? "bg-emerald-500" : "bg-rose-500"}`}
          style={{ width: `${w}%`, left: pos ? "50%" : `${50 - w}%` }}
        />
      </div>
      <span className={`w-12 shrink-0 text-right tabular-nums ${signColor(r.mom_12_1)}`}>
        {pct(r.mom_12_1, 0)}
      </span>
      <span className="w-10 shrink-0 text-right">
        {r.is_winner ? (
          <span className="text-emerald-400">▲</span>
        ) : r.is_loser ? (
          <span className="text-rose-400">▼</span>
        ) : null}
      </span>
    </div>
  );
}

export function SectorBreakdown() {
  const { data, isLoading, error } = useSectors();
  const lb = useLeaderboard({}); // all eligible rows, for the drill-down
  const [expanded, setExpanded] = useState<string | null>(null);

  const sectors = data?.sectors ?? [];
  const maxAbs = Math.max(0.01, ...sectors.map((s) => Math.abs(s.avg_score)));

  // group leaderboard rows by sector, sorted leaders -> laggards
  const bySector = useMemo(() => {
    const m: Record<string, LeaderboardRow[]> = {};
    for (const r of lb.data?.rows ?? []) {
      const k = r.sector || "Unknown";
      (m[k] ||= []).push(r);
    }
    for (const k of Object.keys(m)) m[k].sort((a, b) => b.score - a.score);
    return m;
  }, [lb.data]);

  return (
    <div>
      <div className="mb-4">
        <h2 className="text-lg font-semibold text-slate-100">Sector Breakdown</h2>
        <p className="text-xs text-slate-500">
          Average momentum score per sector — leading at top, lagging at bottom.
          Click a sector to drill into its stocks.
          {data?.as_of ? ` As of ${data.as_of}.` : ""}
        </p>
      </div>

      {error && (
        <div className="rounded-lg border border-rose-900 bg-rose-950/40 p-4 text-sm text-rose-300">
          {String(error)}
        </div>
      )}
      {isLoading && <div className="text-slate-500">Loading…</div>}

      {sectors.length > 0 && (
        <div className="overflow-x-auto rounded-xl border border-slate-800">
          <table className="w-full text-sm">
            <thead className="bg-slate-900/60 text-left text-xs uppercase tracking-wide text-slate-400">
              <tr>
                <th className="px-3 py-2.5 font-medium">Sector</th>
                <th className="px-3 py-2.5 font-medium">Avg score</th>
                <th className="w-1/3 px-3 py-2.5 font-medium"></th>
                <th className="px-3 py-2.5 text-right font-medium">Avg 12-1</th>
                <th className="px-3 py-2.5 text-right font-medium">Names</th>
                <th className="px-3 py-2.5 text-right font-medium text-emerald-400">
                  Winners
                </th>
                <th className="px-3 py-2.5 text-right font-medium text-rose-400">
                  Losers
                </th>
              </tr>
            </thead>
            <tbody>
              {sectors.map((s) => {
                const w = (Math.abs(s.avg_score) / maxAbs) * 50;
                const pos = s.avg_score >= 0;
                const open = expanded === s.sector;
                const stocks = bySector[s.sector] ?? [];
                const stockMax = Math.max(
                  0.01,
                  ...stocks.map((r) => Math.abs(r.score)),
                );
                return (
                  <Fragment key={s.sector}>
                    <tr
                      onClick={() => setExpanded(open ? null : s.sector)}
                      className={`cursor-pointer border-t border-slate-800/60 tabular-nums hover:bg-slate-800/40 ${open ? "bg-slate-800/30" : ""}`}
                    >
                      <td className="px-3 py-2 font-medium text-slate-200">
                        <span className="mr-1 inline-block w-3 text-slate-500">
                          {open ? "▾" : "▸"}
                        </span>
                        {s.sector}
                      </td>
                      <td className={`px-3 py-2 ${signColor(s.avg_score)}`}>
                        {num(s.avg_score, 3)}
                      </td>
                      <td className="px-3 py-2">
                        <div className="relative h-3 w-full rounded bg-slate-800">
                          <div className="absolute left-1/2 top-0 h-full w-px bg-slate-600" />
                          <div
                            className={`absolute top-0 h-full rounded ${pos ? "bg-emerald-500" : "bg-rose-500"}`}
                            style={{ width: `${w}%`, left: pos ? "50%" : `${50 - w}%` }}
                          />
                        </div>
                      </td>
                      <td className={`px-3 py-2 text-right ${signColor(s.avg_mom_12_1)}`}>
                        {pct(s.avg_mom_12_1, 0)}
                      </td>
                      <td className="px-3 py-2 text-right text-slate-300">{s.count}</td>
                      <td className="px-3 py-2 text-right font-medium text-emerald-400">
                        {s.winners}
                      </td>
                      <td className="px-3 py-2 text-right font-medium text-rose-400">
                        {s.losers}
                      </td>
                    </tr>
                    {open && (
                      <tr className="bg-slate-950/40">
                        <td colSpan={7} className="px-4 py-3">
                          {lb.isLoading ? (
                            <div className="text-xs text-slate-500">Loading stocks…</div>
                          ) : (
                            <div className="max-h-96 overflow-y-auto pr-2">
                              <div className="mb-1 flex items-center gap-3 text-[10px] uppercase tracking-wide text-slate-500">
                                <span className="w-16">Symbol</span>
                                <span className="w-12 text-right">Score</span>
                                <span className="flex-1 text-center">
                                  laggard ← → leader
                                </span>
                                <span className="w-12 text-right">12-1</span>
                                <span className="w-10" />
                              </div>
                              {stocks.map((r) => (
                                <StockBar key={r.symbol} r={r} maxAbs={stockMax} />
                              ))}
                            </div>
                          )}
                        </td>
                      </tr>
                    )}
                  </Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
      <p className="mt-2 text-[11px] leading-relaxed text-slate-500">
        Score is the cross-sectional composite (z-scored).{" "}
        <span className="text-emerald-400">Winners</span> /{" "}
        <span className="text-rose-400">Losers</span> = how many of the sector's
        stocks sit in the top / bottom decile of the overall ranking. Universe is
        currently NASDAQ + NYSE/AMEX.
      </p>
    </div>
  );
}
