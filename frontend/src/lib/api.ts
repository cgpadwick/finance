import { useMutation, useQuery } from "@tanstack/react-query";
import type {
  EmbeddingResponse,
  FilterOverrides,
  LeaderboardResponse,
  ResearchConfig,
  ResearchResult,
  SectorsResponse,
  StockDetail,
  TechnicalsResponse,
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

export function useTechnicals(symbol: string | undefined) {
  return useQuery({
    queryKey: ["technicals", symbol],
    queryFn: () => fetchJson<TechnicalsResponse>(`/api/stock/${symbol}/technicals`),
    enabled: !!symbol,
  });
}

export function useResearchConfig() {
  return useQuery({
    queryKey: ["research-config"],
    queryFn: () => fetchJson<ResearchConfig>("/api/research/config"),
  });
}

// Cached research report (no generation / no cost).
export function useResearch(symbol: string | undefined) {
  return useQuery({
    queryKey: ["research", symbol],
    queryFn: () => fetchJson<ResearchResult>(`/api/stock/${symbol}/research`),
    enabled: !!symbol,
  });
}

// Generate (or hand-off) a report on demand, optionally with a model + force.
export function useGenerateResearch(symbol: string | undefined) {
  return useMutation({
    mutationFn: async (opts: { model?: string; force?: boolean } = {}): Promise<ResearchResult> => {
      const p = new URLSearchParams();
      if (opts.model) p.set("model", opts.model);
      if (opts.force) p.set("force", "true");
      const res = await fetch(`/api/stock/${symbol}/research?${p}`, { method: "POST" });
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
      return res.json();
    },
  });
}

// Cohort briefing (top winners / bottom losers).
export function useCohort(view: "winners" | "losers") {
  return useQuery({
    queryKey: ["cohort", view],
    queryFn: () => fetchJson<ResearchResult>(`/api/research/cohort?view=${view}`),
  });
}

export function useGenerateCohort(view: "winners" | "losers") {
  return useMutation({
    mutationFn: async (opts: { model?: string; force?: boolean } = {}): Promise<ResearchResult> => {
      const p = new URLSearchParams({ view });
      if (opts.model) p.set("model", opts.model);
      if (opts.force) p.set("force", "true");
      const res = await fetch(`/api/research/cohort?${p}`, { method: "POST" });
      if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
      return res.json();
    },
  });
}

export function useSectors() {
  return useQuery({
    queryKey: ["sectors"],
    queryFn: () => fetchJson<SectorsResponse>("/api/sectors"),
  });
}

export function useEmbedding(method: string, dims: number) {
  return useQuery({
    queryKey: ["embedding", method, dims],
    queryFn: () =>
      fetchJson<EmbeddingResponse>(`/api/embedding?method=${method}&dims=${dims}`),
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
