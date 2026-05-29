import { useEffect, useRef } from "react";
import {
  CandlestickSeries,
  HistogramSeries,
  LineSeries,
  createChart,
  type IChartApi,
  type Time,
} from "lightweight-charts";
import type { Bar } from "../types";

interface Props {
  bars: Bar[];
  movingAverages: Record<string, number[]>;
}

const MA_COLORS: Record<string, string> = {
  ma_20: "#60a5fa",
  ma_50: "#f59e0b",
  ma_200: "#a78bfa",
};

export function CandleChart({ bars, movingAverages }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);

  useEffect(() => {
    if (!containerRef.current || bars.length === 0) return;

    const chart = createChart(containerRef.current, {
      layout: {
        background: { color: "transparent" },
        textColor: "#94a3b8",
        attributionLogo: true, // required by Lightweight Charts license
      },
      grid: {
        vertLines: { color: "#1a2230" },
        horzLines: { color: "#1a2230" },
      },
      rightPriceScale: { borderColor: "#1f2733" },
      timeScale: { borderColor: "#1f2733", timeVisible: false },
      autoSize: true,
      crosshair: { mode: 0 },
    });
    chartRef.current = chart;

    const candles = chart.addSeries(CandlestickSeries, {
      upColor: "#10b981",
      downColor: "#ef4444",
      borderUpColor: "#10b981",
      borderDownColor: "#ef4444",
      wickUpColor: "#10b981",
      wickDownColor: "#ef4444",
    });
    candles.setData(
      bars.map((b) => ({
        time: b.date as Time,
        open: b.open,
        high: b.high,
        low: b.low,
        close: b.close,
      })),
    );

    // Volume as a histogram on its own bottom scale.
    const volume = chart.addSeries(HistogramSeries, {
      priceFormat: { type: "volume" },
      priceScaleId: "vol",
    });
    volume.priceScale().applyOptions({
      scaleMargins: { top: 0.82, bottom: 0 },
    });
    volume.setData(
      bars.map((b) => ({
        time: b.date as Time,
        value: b.volume,
        color: b.close >= b.open ? "#10b98155" : "#ef444455",
      })),
    );

    // Moving-average overlays.
    for (const [key, values] of Object.entries(movingAverages)) {
      const line = chart.addSeries(LineSeries, {
        color: MA_COLORS[key] ?? "#64748b",
        lineWidth: 1,
        priceLineVisible: false,
        lastValueVisible: false,
      });
      line.setData(
        bars.map((b, i) => ({ time: b.date as Time, value: values[i] })),
      );
    }

    chart.timeScale().fitContent();

    return () => {
      chart.remove();
      chartRef.current = null;
    };
  }, [bars, movingAverages]);

  return <div ref={containerRef} className="h-[460px] w-full" />;
}
