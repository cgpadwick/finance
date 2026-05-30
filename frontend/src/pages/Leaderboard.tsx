import { useMemo, useState } from "react";
import { Link } from "react-router-dom";
import {
  createColumnHelper,
  flexRender,
  getCoreRowModel,
  getPaginationRowModel,
  getSortedRowModel,
  useReactTable,
  type SortingState,
} from "@tanstack/react-table";
import { useHealth, useLeaderboard } from "../lib/api";
import { FilterPanel } from "../components/FilterPanel";
import { AnalysisControls } from "../components/AnalysisControls";
import { CohortPanel } from "../components/CohortPanel";
import type { FilterOverrides, LeaderboardRow } from "../types";
import { compactMoney, money, num, pct, signColor } from "../lib/format";

const col = createColumnHelper<LeaderboardRow>();

type View = "winners" | "losers" | "all";

export function Leaderboard() {
  const [overrides, setOverrides] = useState<FilterOverrides>({});
  const [view, setView] = useState<View>("winners");
  const [showInsights, setShowInsights] = useState(false);
  const [sorting, setSorting] = useState<SortingState>([
    { id: "score", desc: true },
  ]);

  const { data, isLoading, isFetching, error } = useLeaderboard(overrides);
  const health = useHealth();

  const dateRange = health.data?.date_range;
  const setLookback = (days: number) =>
    setOverrides((o) => ({ ...o, lookback_days: days }));
  const setAsOf = (date: string) =>
    setOverrides((o) => {
      const next = { ...o };
      if (date) next.as_of = date;
      else delete next.as_of;
      return next;
    });
  // Empty result with an explicit as_of usually means not enough history yet.
  const emptyHistory =
    !isLoading && !isFetching && data?.count === 0 && !!overrides.as_of;

  const [sector, setSector] = useState<string>("All");
  const sectors = useMemo(() => {
    const set = new Set<string>();
    (data?.rows ?? []).forEach((r) => r.sector && set.add(r.sector));
    return ["All", ...Array.from(set).sort()];
  }, [data]);

  const rows = useMemo(() => {
    let all = data?.rows ?? [];
    if (view === "winners") all = all.filter((r) => r.is_winner);
    else if (view === "losers") all = all.filter((r) => r.is_loser);
    if (sector !== "All") all = all.filter((r) => r.sector === sector);
    return all;
  }, [data, view, sector]);

  const columns = useMemo(
    () => [
      col.accessor("rank", { header: "#", cell: (c) => c.getValue() }),
      col.accessor("symbol", {
        header: "Symbol",
        cell: (c) => (
          <Link
            to={`/stock/${c.getValue()}`}
            className="font-semibold text-sky-400 hover:text-sky-300"
          >
            {c.getValue()}
          </Link>
        ),
      }),
      col.accessor("sector", {
        header: "Sector",
        cell: (c) => (
          <span className="text-slate-300" title={c.row.original.industry ?? ""}>
            {c.getValue() ?? "—"}
          </span>
        ),
      }),
      col.accessor("last_close", {
        header: "Price",
        cell: (c) => money(c.getValue()),
      }),
      col.accessor("score", {
        header: "Score",
        cell: (c) => (
          <span className={signColor(c.getValue())}>{num(c.getValue(), 2)}</span>
        ),
      }),
      col.accessor("mom_12_1", {
        header: "12-1 Mom",
        cell: (c) => (
          <span className={signColor(c.getValue())}>{pct(c.getValue(), 0)}</span>
        ),
      }),
      col.accessor("risk_adjusted_mom", {
        header: "Risk-adj",
        cell: (c) => num(c.getValue(), 2),
      }),
      col.accessor("high_52w_proximity", {
        header: "52w High",
        cell: (c) => pct(c.getValue(), 0),
      }),
      col.accessor("volatility", {
        header: "Vol",
        cell: (c) => pct(c.getValue(), 0),
      }),
      col.accessor("avg_dollar_volume", {
        header: "$ Vol",
        cell: (c) => compactMoney(c.getValue()),
      }),
    ],
    [],
  );

  const table = useReactTable({
    data: rows,
    columns,
    state: { sorting },
    onSortingChange: setSorting,
    getCoreRowModel: getCoreRowModel(),
    getSortedRowModel: getSortedRowModel(),
    getPaginationRowModel: getPaginationRowModel(),
    initialState: { pagination: { pageSize: 50 } },
  });

  return (
    <div className="grid grid-cols-1 gap-6 lg:grid-cols-[1fr_300px]">
      <div className="min-w-0">
        <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
          <div>
            <h2 className="text-lg font-semibold text-slate-100">
              Momentum Leaderboard
            </h2>
            <p className="text-xs text-slate-500">
              {data?.as_of ? `As of ${data.as_of} · ` : ""}
              showing{" "}
              <span className="text-slate-300">
                {rows.length.toLocaleString()} {view}
                {sector !== "All" ? ` in ${sector}` : ""}
              </span>{" "}
              of {(data?.count ?? 0).toLocaleString()} eligible
              {isFetching && (
                <span className="ml-2 text-sky-400">updating…</span>
              )}
            </p>
          </div>
          <div className="flex flex-wrap items-center gap-4">
            <AnalysisControls
              lookback={overrides.lookback_days ?? 252}
              asOf={overrides.as_of ?? ""}
              onLookback={setLookback}
              onAsOf={setAsOf}
              minDate={dateRange?.[0]?.slice(0, 10)}
              maxDate={dateRange?.[1]?.slice(0, 10)}
            />
            <select
              value={sector}
              onChange={(e) => setSector(e.target.value)}
              className="rounded-lg border border-slate-800 bg-slate-900/60 px-2 py-1.5 text-xs text-slate-200"
              title="Filter by sector"
            >
              {sectors.map((s) => (
                <option key={s} value={s}>
                  {s === "All" ? "All sectors" : s}
                </option>
              ))}
            </select>
            <div className="flex gap-1 rounded-lg border border-slate-800 p-1">
              {(["winners", "losers", "all"] as View[]).map((v) => (
                <button
                  key={v}
                  onClick={() => setView(v)}
                  className={`rounded-md px-3 py-1 text-xs font-medium capitalize transition ${
                    view === v
                      ? "bg-slate-700 text-slate-100"
                      : "text-slate-400 hover:text-slate-200"
                  }`}
                >
                  {v}
                </button>
              ))}
            </div>
            <button
              onClick={() => setShowInsights((s) => !s)}
              className="flex items-center gap-1.5 rounded-lg bg-gradient-to-r from-violet-600 to-fuchsia-600 px-3 py-1.5 text-xs font-semibold text-white shadow hover:from-violet-500 hover:to-fuchsia-500"
            >
              ✨ Deep Insights
              <span className="rounded bg-white/20 px-1 text-[9px] font-bold tracking-wide">
                PRO
              </span>
            </button>
          </div>
        </div>

        {error && (
          <div className="rounded-lg border border-rose-900 bg-rose-950/40 p-4 text-sm text-rose-300">
            Failed to load: {String(error)}
          </div>
        )}

        {emptyHistory && (
          <div className="rounded-lg border border-amber-900 bg-amber-950/30 p-4 text-sm text-amber-300">
            No ranking for {overrides.as_of} — there isn't enough price history
            before that date for the selected window. Pick a later date or a
            shorter window.
          </div>
        )}

        {showInsights && (
          <div className="mb-4">
            <CohortPanel view={view === "losers" ? "losers" : "winners"} />
          </div>
        )}

        <div className="overflow-x-auto rounded-xl border border-slate-800">
          <table className="w-full text-sm">
            <thead className="bg-slate-900/60 text-left text-xs uppercase tracking-wide text-slate-400">
              {table.getHeaderGroups().map((hg) => (
                <tr key={hg.id}>
                  {hg.headers.map((h) => (
                    <th
                      key={h.id}
                      onClick={h.column.getToggleSortingHandler()}
                      className="cursor-pointer select-none px-3 py-2.5 font-medium hover:text-slate-200"
                    >
                      {flexRender(h.column.columnDef.header, h.getContext())}
                      {{ asc: " ▲", desc: " ▼" }[
                        h.column.getIsSorted() as string
                      ] ?? ""}
                    </th>
                  ))}
                </tr>
              ))}
            </thead>
            <tbody>
              {isLoading ? (
                <tr>
                  <td colSpan={columns.length} className="px-3 py-10 text-center text-slate-500">
                    Loading…
                  </td>
                </tr>
              ) : (
                table.getRowModel().rows.map((r) => (
                  <tr
                    key={r.id}
                    className="border-t border-slate-800/60 tabular-nums hover:bg-slate-800/30"
                  >
                    {r.getVisibleCells().map((cell) => (
                      <td key={cell.id} className="px-3 py-2 text-slate-200">
                        {flexRender(cell.column.columnDef.cell, cell.getContext())}
                      </td>
                    ))}
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>

        {/* pagination controls */}
        <div className="mt-3 flex items-center justify-between text-xs text-slate-400">
          <span>
            {rows.length.toLocaleString()} {view} ·{" "}
            {table.getState().pagination.pageIndex * table.getState().pagination.pageSize + 1}
            –
            {Math.min(
              (table.getState().pagination.pageIndex + 1) *
                table.getState().pagination.pageSize,
              rows.length,
            )}{" "}
            shown
          </span>
          <div className="flex items-center gap-2">
            <select
              value={table.getState().pagination.pageSize}
              onChange={(e) => table.setPageSize(Number(e.target.value))}
              className="rounded-md border border-slate-800 bg-slate-900/60 px-2 py-1 text-slate-200"
            >
              {[25, 50, 100, 200].map((n) => (
                <option key={n} value={n}>
                  {n}/page
                </option>
              ))}
            </select>
            <button
              onClick={() => table.previousPage()}
              disabled={!table.getCanPreviousPage()}
              className="rounded-md border border-slate-700 px-2 py-1 hover:bg-slate-800 disabled:opacity-40"
            >
              ← Prev
            </button>
            <span>
              Page {table.getState().pagination.pageIndex + 1} of{" "}
              {table.getPageCount() || 1}
            </span>
            <button
              onClick={() => table.nextPage()}
              disabled={!table.getCanNextPage()}
              className="rounded-md border border-slate-700 px-2 py-1 hover:bg-slate-800 disabled:opacity-40"
            >
              Next →
            </button>
          </div>
        </div>
      </div>

      <aside className="space-y-4">
        <FilterPanel
          value={overrides}
          onChange={setOverrides}
          onReset={() => setOverrides({})}
        />
        {health.data && (
          <div className="rounded-xl border border-slate-800 bg-slate-900/40 p-4 text-xs text-slate-400">
            <div className="mb-1 font-semibold text-slate-300">Coverage</div>
            <div>{health.data.symbols} symbols</div>
            <div>{health.data.total_bars.toLocaleString()} daily bars</div>
            {health.data.date_range && (
              <div>
                {health.data.date_range[0].slice(0, 10)} →{" "}
                {health.data.date_range[1].slice(0, 10)}
              </div>
            )}
          </div>
        )}
      </aside>
    </div>
  );
}
