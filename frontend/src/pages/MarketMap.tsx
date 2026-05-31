import { useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import ReactECharts from "echarts-for-react";
import "echarts-gl"; // registers scatter3D / grid3D
import { useEmbedding, useSectors } from "../lib/api";
import type { EmbeddingPoint } from "../types";

type Method = "umap" | "tsne" | "pca";
type ColorBy = "score" | "sector";

const SECTOR_COLORS: Record<string, string> = {
  Technology: "#38bdf8",
  "Health Care": "#34d399",
  Financials: "#fbbf24",
  Industrials: "#f97316",
  "Consumer Discretionary": "#a78bfa",
  "Consumer Staples": "#4ade80",
  Energy: "#f87171",
  Materials: "#c084fc",
  Utilities: "#22d3ee",
  "Real Estate": "#fb7185",
  "Communication Services": "#818cf8",
  Unknown: "#64748b",
  Other: "#94a3b8",
};

const tooltipFormatter = (p: { data: { d: EmbeddingPoint } }) => {
  const d = p.data.d;
  const tag = d.is_winner ? "🟢 winner" : d.is_loser ? "🔴 loser" : "";
  return `<b>${d.symbol}</b> ${tag}<br/>${d.name ?? ""}<br/>${d.sector ?? "—"}<br/>score ${d.score?.toFixed(
    2,
  )} · $${d.last_close?.toFixed(0)}`;
};

export function MarketMap() {
  const [method, setMethod] = useState<Method>("umap");
  const [colorBy, setColorBy] = useState<ColorBy>("score");
  const [is3d, setIs3d] = useState(true);
  const [sector, setSector] = useState<string>(""); // "" = all
  const sectors = useSectors();
  const { data, isLoading, isFetching, error } = useEmbedding(
    method,
    is3d ? 3 : 2,
    sector || undefined,
  );
  const navigate = useNavigate();

  const points = data?.points ?? [];

  const option = useMemo(() => {
    const tooltip = {
      backgroundColor: "#0f1722",
      borderColor: "#1f2733",
      textStyle: { color: "#e6edf3", fontSize: 12 },
      formatter: tooltipFormatter,
    };
    const scores = points.map((p) => p.score);
    const seriesType = is3d ? "scatter3D" : "scatter";
    // value layout: [x, y] (2D) or [x, y, z] (3D); score appended for visualMap dim
    const valueOf = (p: EmbeddingPoint) =>
      is3d ? [p.x, p.y, p.z ?? 0, p.score] : [p.x, p.y, p.score];
    const scoreDim = is3d ? 3 : 2;

    const axisCommon = {
      axisLine: { lineStyle: { color: "#334155" } },
      splitLine: { lineStyle: { color: "#1a2230" } },
      axisLabel: { show: false },
      nameTextStyle: { color: "#64748b" },
    };

    const coordBlock = is3d
      ? {
          grid3D: {
            boxWidth: 100,
            boxDepth: 100,
            boxHeight: 100,
            viewControl: { distance: 200, autoRotate: false },
            axisPointer: { show: false },
            environment: "#0a0e14",
          },
          xAxis3D: { ...axisCommon, scale: true },
          yAxis3D: { ...axisCommon, scale: true },
          zAxis3D: { ...axisCommon, scale: true },
        }
      : {
          grid: { left: 8, right: 8, top: 8, bottom: 8 },
          xAxis: { show: false, scale: true },
          yAxis: { show: false, scale: true },
        };

    if (colorBy === "score") {
      return {
        tooltip,
        ...coordBlock,
        visualMap: {
          min: Math.min(...scores, -1),
          max: Math.max(...scores, 1),
          dimension: scoreDim,
          calculable: true,
          orient: "vertical",
          right: 4,
          bottom: 20,
          textStyle: { color: "#94a3b8" },
          inRange: { color: ["#3b82f6", "#64748b", "#ef4444"] }, // cold→hot
        },
        series: [
          {
            type: seriesType,
            symbolSize: is3d ? 8 : 11,
            data: points.map((p) => ({ value: valueOf(p), d: p })),
            itemStyle: { opacity: 0.85 },
          },
        ],
      };
    }

    // color by sector — one series per sector for a legend
    const sectors = Array.from(new Set(points.map((p) => p.sector ?? "Unknown")));
    return {
      tooltip,
      ...coordBlock,
      legend: { type: "scroll", textStyle: { color: "#94a3b8", fontSize: 10 }, top: 0 },
      series: sectors.map((sec) => ({
        name: sec,
        type: seriesType,
        symbolSize: is3d ? 8 : 11,
        itemStyle: { color: SECTOR_COLORS[sec] ?? "#94a3b8", opacity: 0.85 },
        data: points
          .filter((p) => (p.sector ?? "Unknown") === sec)
          .map((p) => ({ value: valueOf(p), d: p })),
      })),
    };
  }, [points, colorBy, is3d]);

  const onEvents = {
    click: (p: { data?: { d?: EmbeddingPoint } }) => {
      if (p.data?.d) navigate(`/stock/${p.data.d.symbol}`);
    },
  };

  return (
    <div>
      <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
        <div>
          <h2 className="text-lg font-semibold text-slate-100">Market Map</h2>
          <p className="text-xs text-slate-500">
            Each dot is a stock; nearby dots move together. {points.length} names
            {sector ? ` in ${sector}` : " (all sectors — slower)"}
            {isFetching && <span className="ml-2 text-sky-400">computing…</span>}
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-3">
          <select
            value={sector}
            onChange={(e) => setSector(e.target.value)}
            className="rounded-lg border border-slate-800 bg-slate-900/60 px-2 py-1.5 text-xs text-slate-200"
            title="Scope the map to one sector (much faster)"
          >
            <option value="">All sectors (slower)</option>
            {(sectors.data?.sectors ?? []).map((s) => (
              <option key={s.sector} value={s.sector}>
                {s.sector} ({s.count})
              </option>
            ))}
          </select>
          <div className="flex gap-1 rounded-lg border border-slate-800 p-1">
            {([
              { k: true, label: "3D" },
              { k: false, label: "2D" },
            ] as { k: boolean; label: string }[]).map(({ k, label }) => (
              <button
                key={label}
                onClick={() => setIs3d(k)}
                className={`rounded-md px-3 py-1 text-xs font-medium transition ${
                  is3d === k ? "bg-sky-700/70 text-sky-50" : "text-slate-400 hover:text-slate-200"
                }`}
              >
                {label}
              </button>
            ))}
          </div>
          <div className="flex gap-1 rounded-lg border border-slate-800 p-1">
            {(["score", "sector"] as ColorBy[]).map((c) => (
              <button
                key={c}
                onClick={() => setColorBy(c)}
                className={`rounded-md px-3 py-1 text-xs font-medium capitalize transition ${
                  colorBy === c ? "bg-slate-700 text-slate-100" : "text-slate-400 hover:text-slate-200"
                }`}
              >
                color: {c}
              </button>
            ))}
          </div>
          <div className="flex gap-1 rounded-lg border border-slate-800 p-1">
            {(["umap", "tsne", "pca"] as Method[]).map((m) => (
              <button
                key={m}
                onClick={() => setMethod(m)}
                className={`rounded-md px-2.5 py-1 text-xs font-medium uppercase transition ${
                  method === m ? "bg-emerald-700/70 text-emerald-50" : "text-slate-400 hover:text-slate-200"
                }`}
              >
                {m}
              </button>
            ))}
          </div>
        </div>
      </div>

      {error && (
        <div className="rounded-lg border border-rose-900 bg-rose-950/40 p-4 text-sm text-rose-300">
          {String(error)}
        </div>
      )}

      <div className="rounded-xl border border-slate-800 bg-slate-900/30 p-2">
        {isLoading ? (
          <div className="flex h-[560px] items-center justify-center text-slate-500">
            Computing embedding…
          </div>
        ) : (
          <ReactECharts
            key={is3d ? "3d" : "2d"}
            option={option}
            onEvents={onEvents}
            style={{ height: 560 }}
            notMerge
          />
        )}
      </div>
      <p className="mt-2 text-[11px] leading-relaxed text-slate-500">
        Built from 1-year daily-return correlations (UMAP/t-SNE/PCA). In 3D, drag
        to rotate and scroll to zoom. Clusters are meaningful — stocks that cluster
        move together — but absolute positions, axes, and distances between clusters
        are not. Click a dot to open the stock.
      </p>
    </div>
  );
}
