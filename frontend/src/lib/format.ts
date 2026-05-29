export const pct = (x: number | null | undefined, digits = 1) =>
  x == null || Number.isNaN(x) ? "—" : `${(x * 100).toFixed(digits)}%`;

export const num = (x: number | null | undefined, digits = 2) =>
  x == null || Number.isNaN(x) ? "—" : x.toFixed(digits);

export const money = (x: number | null | undefined) =>
  x == null || Number.isNaN(x)
    ? "—"
    : `$${x.toLocaleString(undefined, { maximumFractionDigits: 2 })}`;

export const compactMoney = (x: number | null | undefined) => {
  if (x == null || Number.isNaN(x)) return "—";
  if (x >= 1e9) return `$${(x / 1e9).toFixed(1)}B`;
  if (x >= 1e6) return `$${(x / 1e6).toFixed(1)}M`;
  if (x >= 1e3) return `$${(x / 1e3).toFixed(1)}K`;
  return `$${x.toFixed(0)}`;
};

// Green for positive, red for negative — finance convention.
export const signColor = (x: number | null | undefined) =>
  x == null || Number.isNaN(x)
    ? "text-slate-400"
    : x > 0
      ? "text-emerald-400"
      : x < 0
        ? "text-rose-400"
        : "text-slate-300";
