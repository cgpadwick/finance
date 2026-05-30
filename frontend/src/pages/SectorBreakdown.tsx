import { useSectors } from "../lib/api";
import { pct, num, signColor } from "../lib/format";

export function SectorBreakdown() {
  const { data, isLoading, error } = useSectors();
  const sectors = data?.sectors ?? [];
  const maxAbs = Math.max(0.01, ...sectors.map((s) => Math.abs(s.avg_score)));

  return (
    <div>
      <div className="mb-4">
        <h2 className="text-lg font-semibold text-slate-100">Sector Breakdown</h2>
        <p className="text-xs text-slate-500">
          Average momentum score per sector — leading at top, lagging at bottom.
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
                <th
                  className="px-3 py-2.5 text-right font-medium text-emerald-400"
                  title="Stocks in the top decile of the overall momentum ranking"
                >
                  Winners
                </th>
                <th
                  className="px-3 py-2.5 text-right font-medium text-rose-400"
                  title="Stocks in the bottom decile of the overall momentum ranking"
                >
                  Losers
                </th>
              </tr>
            </thead>
            <tbody>
              {sectors.map((s) => {
                const w = (Math.abs(s.avg_score) / maxAbs) * 50; // % of half-width
                const pos = s.avg_score >= 0;
                return (
                  <tr
                    key={s.sector}
                    className="border-t border-slate-800/60 tabular-nums hover:bg-slate-800/30"
                  >
                    <td className="px-3 py-2 font-medium text-slate-200">
                      {s.sector}
                    </td>
                    <td className={`px-3 py-2 ${signColor(s.avg_score)}`}>
                      {num(s.avg_score, 3)}
                    </td>
                    <td className="px-3 py-2">
                      {/* diverging bar centered at 0 */}
                      <div className="relative h-3 w-full rounded bg-slate-800">
                        <div className="absolute left-1/2 top-0 h-full w-px bg-slate-600" />
                        <div
                          className={`absolute top-0 h-full rounded ${pos ? "bg-emerald-500" : "bg-rose-500"}`}
                          style={{
                            width: `${w}%`,
                            left: pos ? "50%" : `${50 - w}%`,
                          }}
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
                );
              })}
            </tbody>
          </table>
        </div>
      )}
      <p className="mt-2 text-[11px] leading-relaxed text-slate-500">
        Score is the cross-sectional composite (z-scored), so a positive sector
        average means its names skew toward the top of the overall ranking.{" "}
        <span className="text-emerald-400">Winners</span> /{" "}
        <span className="text-rose-400">Losers</span> = how many of the sector's
        stocks sit in the top / bottom decile of the overall momentum ranking.
        Universe is currently NASDAQ-listed only, so some sectors (e.g. Energy)
        are thin.
      </p>
    </div>
  );
}
