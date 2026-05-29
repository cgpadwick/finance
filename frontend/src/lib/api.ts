import { useQuery } from "@tanstack/react-query";
import type {
  EmbeddingResponse,
  FilterOverrides,
  LeaderboardResponse,
  StockDetail,
} from "../types";

async function fetchJson<T>(url: string): Promise<T> {
  const res = await fetch(url);
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(`${res.status} ${res.statusText} ${detail}`.trim());
  }
  return res.json() as Promise<T>;
}

function buildQuery(overrides: FilterOverrides): string {
  const params = new URLSearchParams();
  for (const [k, v] of Object.entries(overrides)) {
    if (v !== undefined && v !== null && !Number.isNaN(v)) {
      params.set(k, String(v));
    }
  }
  const q = params.toString();
  return q ? `?${q}` : "";
}

export function useLeaderboard(overrides: FilterOverrides) {
  const query = buildQuery(overrides);
  return useQuery({
    queryKey: ["leaderboard", overrides],
    queryFn: () => fetchJson<LeaderboardResponse>(`/api/leaderboard${query}`),
    placeholderData: (prev) => prev, // keep table visible while re-tuning
  });
}

export function useStock(symbol: string | undefined) {
  return useQuery({
    queryKey: ["stock", symbol],
    queryFn: () => fetchJson<StockDetail>(`/api/stock/${symbol}?ma=20,50,200`),
    enabled: !!symbol,
  });
}

export function useEmbedding(method: string) {
  return useQuery({
    queryKey: ["embedding", method],
    queryFn: () =>
      fetchJson<EmbeddingResponse>(`/api/embedding?method=${method}`),
  });
}

export interface Health {
  status: string;
  symbols: number;
  total_bars: number;
  date_range: [string, string] | null;
}

export function useHealth() {
  return useQuery({
    queryKey: ["health"],
    queryFn: () => fetchJson<Health>("/api/health"),
  });
}
