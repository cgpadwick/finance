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

export interface CohortAggregate {
  n: number;
  pct_above_200ma: number | null;
  pct_golden_cross: number | null;
  n_overbought: number;
  pct_bullish_verdict: number | null;
  avg_rsi: number | null;
  avg_6m_return: number | null;
  avg_12m_return: number | null;
  avg_max_drawdown: number | null;
  avg_ann_vol: number | null;
}

export interface ResearchResult {
  mode: "cached" | "generated" | "handoff";
  report?: string | null;
  prompt?: string;
  research_enabled?: boolean;
  generated_at?: string;
  model?: string | null;
  cost?: number | null;
  tokens?: number | null;
  aggregate?: CohortAggregate;
}

export interface PanelResult {
  mode: "cached" | "generated" | "handoff";
  facts?: string;
  bull?: string;
  bear?: string;
  prompt?: string;
  research_enabled?: boolean;
  generated_at?: string;
  model?: string | null;
  cost?: number | null;
  tokens?: number | null;
}

export interface ResearchConfig {
  enabled: boolean;
  model: string;
  models: string[];
  web_search: boolean;
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

export interface SectorRow {
  sector: string;
  count: number;
  avg_score: number;
  median_score: number;
  avg_mom_12_1: number;
  avg_high_52w: number;
  winners: number;
  losers: number;
}

export interface SectorsResponse {
  as_of: string | null;
  sectors: SectorRow[];
}

export interface ScorecardItem {
  label: string;
  signal: "bullish" | "bearish" | "neutral";
  detail: string;
}

export interface Technicals {
  insufficient_data?: boolean;
  bars: number;
  price: number;
  trend: {
    ma50: number | null;
    ma200: number | null;
    above_ma50: boolean;
    above_ma200: boolean;
    golden_cross: boolean;
  };
  momentum: {
    rsi: number | null;
    macd: number | null;
    macd_signal: number | null;
    macd_hist: number | null;
  };
  volatility: {
    ann_vol: number | null;
    atr_pct: number | null;
    bollinger_pctb: number | null;
    max_drawdown: number | null;
    current_drawdown: number | null;
  };
  returns: {
    r_1m: number | null;
    r_3m: number | null;
    r_6m: number | null;
    r_12m: number | null;
    up_day_pct: number | null;
  };
  range: {
    high_52w: number | null;
    low_52w: number | null;
    pct_from_high: number | null;
    pct_from_low: number | null;
  };
  volume_ratio: number | null;
  scorecard: ScorecardItem[];
  verdict: { label: string; bullish: number; bearish: number; neutral: number };
}

export interface RelativeStrength {
  in_universe: boolean;
  rank?: number;
  universe_size?: number;
  percentile?: number;
  decile?: number;
  score?: number;
  sector?: string;
  sector_size?: number;
  score_vs_sector?: number;
  mom_vs_sector?: number;
  z_breakdown?: Record<string, number | null>;
}

export interface TechnicalsResponse {
  symbol: string;
  technicals: Technicals;
  relative_strength: RelativeStrength | null;
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
