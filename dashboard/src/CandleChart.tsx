import {
  CandlestickSeries,
  ColorType,
  createChart,
  createSeriesMarkers,
  HistogramSeries,
  type IChartApi,
  type ISeriesApi,
  type ISeriesMarkersPluginApi,
  LineSeries,
  type SeriesMarker,
  type Time,
  type UTCTimestamp,
} from "lightweight-charts";
import { useEffect, useRef } from "react";
import type { Incident } from "./api";
import type { Candle } from "./coins";
import { TYPE_LABEL } from "./format";
import { chartPalette, useTheme } from "./theme";


interface Props {
  candles: Candle[];
  other: Candle[];
  otherLabel: string;
  incidents: Incident[];
  stablecoin: boolean;
}

const localTime = (t: Time) =>
  new Date((t as number) * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });

/** One-minute candles for one exchange, the other exchange as a line, volume underneath,
 *  and incident markers. Built on TradingView's lightweight-charts. */
export function CandleChart({ candles, other, otherLabel, incidents, stablecoin }: Props) {
  const box = useRef<HTMLDivElement>(null);
  const theme = useTheme();
  const chart = useRef<IChartApi | null>(null);
  const series = useRef<{
    candles: ISeriesApi<"Candlestick">;
    other: ISeriesApi<"Line">;
    volume: ISeriesApi<"Histogram">;
    markers: ISeriesMarkersPluginApi<Time>;
  } | null>(null);

  useEffect(() => {
    if (!box.current) return;
    const COLORS = chartPalette();
    const c = createChart(box.current, {
      autoSize: true,
      layout: { background: { type: ColorType.Solid, color: "transparent" }, textColor: COLORS.text, fontSize: 11 },
      grid: { vertLines: { color: COLORS.grid }, horzLines: { color: COLORS.grid } },
      rightPriceScale: { borderColor: COLORS.grid },
      timeScale: {
        borderColor: COLORS.grid, timeVisible: true, secondsVisible: false, rightOffset: 3,
        tickMarkFormatter: localTime,  // the same local time as the trades list
      },
      localization: { timeFormatter: localTime },
      crosshair: { mode: 0 },
    });
    const candleSeries = c.addSeries(CandlestickSeries, {
      upColor: COLORS.up, downColor: COLORS.down, borderVisible: false,
      wickUpColor: COLORS.up, wickDownColor: COLORS.down,
      priceFormat: { type: "price", precision: stablecoin ? 5 : 2, minMove: stablecoin ? 0.00001 : 0.01 },
    });
    const otherSeries = c.addSeries(LineSeries, {
      color: COLORS.info, lineWidth: 1, lineStyle: 2, priceLineVisible: false, lastValueVisible: true,
      crosshairMarkerVisible: false, title: otherLabel,
    });
    const volume = c.addSeries(HistogramSeries, {
      priceScaleId: "volume", priceFormat: { type: "volume" }, lastValueVisible: false, priceLineVisible: false,
    });
    c.priceScale("volume").applyOptions({ scaleMargins: { top: 0.82, bottom: 0 } });
    candleSeries.priceScale().applyOptions({ scaleMargins: { top: 0.08, bottom: 0.22 } });
    if (stablecoin) {
      for (const price of [0.995, 1.005]) {
        candleSeries.createPriceLine({ price, color: `${COLORS.warning}88`, lineWidth: 1, lineStyle: 1, axisLabelVisible: true, title: "±0.5 %" });
      }
    }
    chart.current = c;
    series.current = { candles: candleSeries, other: otherSeries, volume, markers: createSeriesMarkers(candleSeries, []) };
    return () => {
      c.remove();
      chart.current = null;
      series.current = null;
    };
  }, [stablecoin, otherLabel, theme]);  // a theme change rebuilds the chart in the new colours

  useEffect(() => {
    const s = series.current;
    if (!s) return;
    const P = chartPalette();
    const t = (time: number) => time as UTCTimestamp;
    s.candles.setData(candles.map((c) => ({ time: t(c.time), open: c.open, high: c.high, low: c.low, close: c.close })));
    s.volume.setData(candles.map((c) => ({
      time: t(c.time), value: c.volume, color: c.close >= c.open ? `${P.up}40` : `${P.down}40`,
    })));
    s.other.setData(other.map((c) => ({ time: t(c.time), value: c.close })));
    const first = candles[0]?.time ?? 0;
    const markers: SeriesMarker<Time>[] = incidents
      .map((inc) => ({ inc, time: Math.floor(Date.parse(inc.opened_at) / 60000) * 60 }))
      .filter(({ time }) => time >= first)
      .sort((a, b) => a.time - b.time)
      .map(({ inc, time }) => ({
        time: t(time), position: "aboveBar", shape: "arrowDown",
        color: P[inc.severity], text: TYPE_LABEL[inc.type] ?? inc.type,
      }));
    s.markers.setMarkers(markers);
  }, [candles, other, incidents, theme]);

  return <div className="candle-chart" ref={box} role="img" aria-label="One-minute candle chart" />;
}
