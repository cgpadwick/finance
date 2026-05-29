import type { FilterOverrides } from "../types";

interface Props {
  value: FilterOverrides;
  onChange: (next: FilterOverrides) => void;
  onReset: () => void;
}

const WEIGHTS: { key: keyof FilterOverrides; label: string }[] = [
  { key: "w_risk_adjusted_mom", label: "Risk-adjusted momentum" },
  { key: "w_high_52w_proximity", label: "52-week-high proximity" },
  { key: "w_path_quality", label: "Path quality (steadiness)" },
  { key: "w_mom_3_6", label: "3–6 month momentum" },
];

const DEFAULTS: Record<string, number> = {
  w_risk_adjusted_mom: 0.45,
  w_high_52w_proximity: 0.25,
  w_path_quality: 0.2,
  w_mom_3_6: 0.1,
  min_price: 5,
  min_dollar_volume: 1_000_000,
};

export function FilterPanel({ value, onChange, onReset }: Props) {
  const set = (key: keyof FilterOverrides, v: number) =>
    onChange({ ...value, [key]: v });

  const get = (key: keyof FilterOverrides): number =>
    Number(value[key] ?? DEFAULTS[key] ?? 0);

  return (
    <div className="rounded-xl border border-slate-800 bg-slate-900/40 p-4">
      <div className="mb-3 flex items-center justify-between">
        <h3 className="text-sm font-semibold text-slate-200">Tune the screen</h3>
        <button
          onClick={onReset}
          className="text-xs text-slate-400 hover:text-slate-200"
        >
          Reset
        </button>
      </div>

      <div className="space-y-3">
        <div className="text-xs font-medium uppercase tracking-wide text-slate-500">
          Universe filters
        </div>
        <label className="block text-xs text-slate-300">
          Min price: <span className="text-slate-100">${get("min_price")}</span>
          <input
            type="range"
            min={0}
            max={50}
            step={1}
            value={get("min_price")}
            onChange={(e) => set("min_price", Number(e.target.value))}
            className="mt-1 w-full accent-sky-500"
          />
        </label>
        <label className="block text-xs text-slate-300">
          Min $ volume:{" "}
          <span className="text-slate-100">
            ${(get("min_dollar_volume") / 1e6).toFixed(1)}M
          </span>
          <input
            type="range"
            min={0}
            max={50_000_000}
            step={500_000}
            value={get("min_dollar_volume")}
            onChange={(e) => set("min_dollar_volume", Number(e.target.value))}
            className="mt-1 w-full accent-sky-500"
          />
        </label>

        <div className="pt-2 text-xs font-medium uppercase tracking-wide text-slate-500">
          Signal weights
        </div>
        {WEIGHTS.map(({ key, label }) => (
          <label key={key} className="block text-xs text-slate-300">
            {label}:{" "}
            <span className="text-slate-100">{get(key).toFixed(2)}</span>
            <input
              type="range"
              min={0}
              max={1}
              step={0.05}
              value={get(key)}
              onChange={(e) => set(key, Number(e.target.value))}
              className="mt-1 w-full accent-emerald-500"
            />
          </label>
        ))}
        <p className="pt-1 text-[11px] leading-relaxed text-slate-500">
          Weights are renormalized server-side. Set one to 0 to drop that signal.
        </p>
      </div>
    </div>
  );
}
