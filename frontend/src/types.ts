export interface LeaderboardRow {
  rank: number;
  symbol: string;
  last_close: number;
  mom_12_1: number;
  mom_3_6: number;
  risk_adjusted_mom: number;
  high_52w_proximity: number;
  path_quality: number;
  volatility: number;
  avg_dollar_volume: number;
  z_risk_adjusted_mom?: number;
  z_high_52w_proximity?: number;
  z_path_quality?: number;
  z_mom_3_6?: number;
  score: number;
  percentile: number;
  decile: number;
  is_winner: boolean;
  is_loser: boolean;
  as_of: string;
  sector?: string;
  industry?: string;
  name?: string;
}

export interface LeaderboardResponse {
  as_of: string | null;
  count: number;
  rows: LeaderboardRow[];
}

export interface Bar {
  date: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface StockSignals {
  last_close: number;
  mom_12_1: number;
  mom_3_6: number;
  risk_adjusted_mom: number;
  high_52w_proximity: number;
  path_quality: number;
  volatility: number;
  avg_dollar_volume: number;
  eligible: boolean;
}

export interface Company {
  symbol: string;
  name?: string;
  sic?: string;
  industry?: string;
  sector?: string;
}

export interface StockDetail {
  symbol: string;
  company: Company | null;
  bars: Bar[];
  moving_averages: Record<string, number[]>;
  signals: StockSignals | null;
}

export interface ResearchResult {
  mode: "cached" | "generated" | "handoff";
  report?: string | null;
  prompt?: string;
  research_enabled?: boolean;
  generated_at?: string;
}

export interface EmbeddingPoint {
  symbol: string;
  x: number;
  y: number;
  z?: number;
  score: number;
  sector?: string;
  industry?: string;
  name?: string;
  is_winner: boolean;
  is_loser: boolean;
  last_close: number;
}

export interface EmbeddingResponse {
  count: number;
  points: EmbeddingPoint[];
}

export interface FilterOverrides {
  min_price?: number;
  min_dollar_volume?: number;
  w_risk_adjusted_mom?: number;
  w_high_52w_proximity?: number;
  w_path_quality?: number;
  w_mom_3_6?: number;
  lookback_days?: number;
  as_of?: string;
}
