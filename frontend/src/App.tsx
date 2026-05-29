import { Link, Route, Routes } from "react-router-dom";
import { Leaderboard } from "./pages/Leaderboard";
import { StockDetail } from "./pages/StockDetail";

function App() {
  return (
    <div className="min-h-full">
      <header className="border-b border-slate-800 bg-slate-950/60 backdrop-blur">
        <div className="mx-auto flex max-w-7xl items-center justify-between px-6 py-3">
          <Link to="/" className="flex items-center gap-2">
            <span className="text-lg font-bold tracking-tight text-slate-100">
              📈 Momentum<span className="text-sky-400">Screener</span>
            </span>
          </Link>
          <span className="text-xs text-slate-500">
            Quality momentum · risk-adjusted · split-clean
          </span>
        </div>
      </header>
      <main className="mx-auto max-w-7xl px-6 py-6">
        <Routes>
          <Route path="/" element={<Leaderboard />} />
          <Route path="/stock/:symbol" element={<StockDetail />} />
        </Routes>
      </main>
    </div>
  );
}

export default App;
