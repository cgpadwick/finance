import { Suspense, lazy } from "react";
import { Link, NavLink, Route, Routes } from "react-router-dom";
import { Leaderboard } from "./pages/Leaderboard";
import { StockDetail } from "./pages/StockDetail";

// ECharts is heavy — only load the Market Map chunk when that route is visited.
const MarketMap = lazy(() =>
  import("./pages/MarketMap").then((m) => ({ default: m.MarketMap })),
);

function App() {
  return (
    <div className="min-h-full">
      <header className="border-b border-slate-800 bg-slate-950/60 backdrop-blur">
        <div className="mx-auto flex max-w-7xl items-center justify-between px-6 py-3">
          <div className="flex items-center gap-6">
            <Link to="/" className="flex items-center gap-2">
              <span className="text-lg font-bold tracking-tight text-slate-100">
                📈 Momentum<span className="text-sky-400">Screener</span>
              </span>
            </Link>
            <nav className="flex gap-4 text-sm">
              {[
                { to: "/", label: "Leaderboard" },
                { to: "/map", label: "Market Map" },
              ].map((l) => (
                <NavLink
                  key={l.to}
                  to={l.to}
                  end
                  className={({ isActive }) =>
                    isActive ? "text-slate-100" : "text-slate-400 hover:text-slate-200"
                  }
                >
                  {l.label}
                </NavLink>
              ))}
            </nav>
          </div>
          <span className="text-xs text-slate-500">
            Quality momentum · risk-adjusted · split-clean
          </span>
        </div>
      </header>
      <main className="mx-auto max-w-7xl px-6 py-6">
        <Suspense
          fallback={<div className="text-slate-500">Loading…</div>}
        >
          <Routes>
            <Route path="/" element={<Leaderboard />} />
            <Route path="/map" element={<MarketMap />} />
            <Route path="/stock/:symbol" element={<StockDetail />} />
          </Routes>
        </Suspense>
      </main>
    </div>
  );
}

export default App;
