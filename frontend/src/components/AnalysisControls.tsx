interface Props {
  lookback: number;
  asOf: string; // "" = latest
  onLookback: (days: number) => void;
  onAsOf: (date: string) => void;
  minDate?: string;
  maxDate?: string;
}

const LOOKBACKS: { label: string; days: number }[] = [
  { label: "3M", days: 63 },
  { label: "6M", days: 126 },
  { label: "12M", days: 252 },
];

export function AnalysisControls({
  lookback,
  asOf,
  onLookback,
  onAsOf,
  minDate,
  maxDate,
}: Props) {
  return (
    <div className="flex flex-wrap items-center gap-4">
      <div className="flex items-center gap-2">
        <span className="text-xs text-slate-500">Window</span>
        <div className="flex gap-1 rounded-lg border border-slate-800 p-1">
          {LOOKBACKS.map(({ label, days }) => (
            <button
              key={days}
              onClick={() => onLookback(days)}
              className={`rounded-md px-2.5 py-1 text-xs font-medium transition ${
                lookback === days
                  ? "bg-emerald-700/70 text-emerald-50"
                  : "text-slate-400 hover:text-slate-200"
              }`}
            >
              {label}
            </button>
          ))}
        </div>
      </div>

      <div className="flex items-center gap-2">
        <span className="text-xs text-slate-500">As of</span>
        <input
          type="date"
          value={asOf}
          min={minDate}
          max={maxDate}
          onChange={(e) => onAsOf(e.target.value)}
          className="rounded-md border border-slate-800 bg-slate-900/60 px-2 py-1 text-xs text-slate-200 [color-scheme:dark]"
        />
        {asOf && (
          <button
            onClick={() => onAsOf("")}
            className="text-xs text-slate-400 hover:text-slate-200"
            title="Back to latest"
          >
            latest
          </button>
        )}
      </div>
    </div>
  );
}
