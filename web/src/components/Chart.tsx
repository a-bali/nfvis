import { useCallback, useEffect, useRef, useState } from 'react';
import type { MouseEvent as ReactMouseEvent } from 'react';
import * as echarts from 'echarts/core';
import { BarChart, LineChart } from 'echarts/charts';
import {
  GridComponent,
  LegendComponent,
  TitleComponent,
  TooltipComponent,
  DatasetComponent,
} from 'echarts/components';
import { CanvasRenderer } from 'echarts/renderers';
import type { EChartsOption } from 'echarts';

echarts.use([
  BarChart,
  LineChart,
  GridComponent,
  LegendComponent,
  TitleComponent,
  TooltipComponent,
  DatasetComponent,
  CanvasRenderer,
]);

type Props = {
  option: EChartsOption;
  height?: number;
  loading?: boolean;
  onItemClick?: (params: { name: string; dataIndex: number; seriesName?: string }) => void;
  onZoom?: (range: { from_ms: number; to_ms: number }) => void;
  resetKey?: string | number;
};

type Selection = { x0: number; x1: number };

const minute = 60_000;
const minDragPixels = 8;

function pixelToValue(instance: echarts.ECharts, px: number, py: number): number {
  const found = instance.convertFromPixel({ xAxisIndex: 0, yAxisIndex: 0 }, [px, py]);
  const value = Array.isArray(found) ? Number(found[0]) : Number(found);
  return value;
}

function seriesExtent(instance: echarts.ECharts, seriesIndex = 0): { from: number; to: number } | null {
  const current = instance.getOption() as { series?: { data?: unknown[] }[] };
  const data = current.series?.[seriesIndex]?.data;
  if (!Array.isArray(data) || data.length < 2) return null;
  const from = Number((data[0] as number[])[0]);
  const to = Number((data[data.length - 1] as number[])[0]);
  if (!Number.isFinite(from) || !Number.isFinite(to) || to <= from) return null;
  return { from, to };
}

export function Chart({ option, height = 280, loading = false, onItemClick, onZoom, resetKey }: Props) {
  const el = useRef<HTMLDivElement | null>(null);
  const wrap = useRef<HTMLDivElement | null>(null);
  const chart = useRef<echarts.ECharts | null>(null);
  const clickRef = useRef(onItemClick);
  const zoomRef = useRef(onZoom);
  const resetKeyRef = useRef(resetKey);
  // Every band is its own series, so the series count changes when a split is
  // chosen or cleared. ECharts merges options by series index, which would leave
  // the old bands painted when the new option has fewer series, so a count change
  // forces a full replace.
  const seriesCount = useRef(-1);
  const dragStart = useRef<{ x: number; y: number } | null>(null);
  const [selection, setSelection] = useState<Selection | null>(null);
  clickRef.current = onItemClick;
  zoomRef.current = onZoom;

  useEffect(() => {
    if (!el.current) return;
    const instance = echarts.init(el.current, undefined, { renderer: 'canvas' });
    chart.current = instance;
    const handler = (params: { name: string; dataIndex: number; seriesName?: string }) => {
      clickRef.current?.(params);
    };
    instance.on('click', handler);
    const observer = new ResizeObserver(() => instance.resize());
    observer.observe(el.current);
    return () => {
      observer.disconnect();
      instance.off('click', handler);
      instance.dispose();
      chart.current = null;
    };
  }, []);

  useEffect(() => {
    const instance = chart.current;
    if (!instance) return;
    const count = Array.isArray(option.series) ? option.series.length : 0;
    const shouldReset = resetKeyRef.current !== resetKey || seriesCount.current !== count;
    resetKeyRef.current = resetKey;
    seriesCount.current = count;
    instance.setOption(option, { notMerge: shouldReset, lazyUpdate: false });
  }, [option, resetKey]);

  const offsetX = (event: MouseEvent): number => {
    const rect = wrap.current?.getBoundingClientRect();
    if (!rect) return 0;
    return Math.min(Math.max(event.clientX - rect.left, 0), rect.width);
  };

  const offsetY = (event: MouseEvent): number => {
    const rect = wrap.current?.getBoundingClientRect();
    if (!rect) return 0;
    return Math.min(Math.max(event.clientY - rect.top, 0), rect.height);
  };

  const onMouseDown = (event: ReactMouseEvent) => {
    if (event.button !== 0 || !zoomRef.current) return;
    dragStart.current = { x: offsetX(event.nativeEvent), y: offsetY(event.nativeEvent) };
  };

  const onMouseMove = (event: ReactMouseEvent) => {
    const start = dragStart.current;
    if (!start) return;
    const x = offsetX(event.nativeEvent);
    if (Math.abs(x - start.x) < minDragPixels) {
      setSelection(null);
      return;
    }
    setSelection({ x0: start.x, x1: x });
  };

  const finishDrag = useCallback((event?: MouseEvent) => {
    const start = dragStart.current;
    dragStart.current = null;
    setSelection(null);
    const instance = chart.current;
    if (!start || !instance || !zoomRef.current) return;
    const endX = event ? offsetX(event) : start.x;
    if (Math.abs(endX - start.x) < minDragPixels) return;
    const from = pixelToValue(instance, Math.min(start.x, endX), start.y);
    const to = pixelToValue(instance, Math.max(start.x, endX), start.y);
    if (!Number.isFinite(from) || !Number.isFinite(to)) return;
    const lo = Math.min(from, to);
    const hi = Math.max(from, to);
    if (hi - lo < minute) return;
    const visible = seriesExtent(instance);
    // Select-drag across almost the whole plot means "no change"; keep the view.
    if (visible && hi - lo >= (visible.to - visible.from) * 0.98) return;
    zoomRef.current({
      from_ms: Math.floor(lo / minute) * minute,
      to_ms: Math.ceil(hi / minute) * minute,
    });
  }, []);

  useEffect(() => {
    const onUp = (event: MouseEvent) => finishDrag(event);
    window.addEventListener('mouseup', onUp);
    return () => window.removeEventListener('mouseup', onUp);
  }, [finishDrag]);

  const box = selection
    ? {
        left: Math.min(selection.x0, selection.x1),
        top: 0,
        width: Math.abs(selection.x1 - selection.x0),
        height: '100%',
      }
    : null;

  return (
    <div className="chart-wrap" ref={wrap}>
      <div
        className="chart"
        ref={el}
        style={{ height }}
        onMouseDown={onMouseDown}
        onMouseMove={onMouseMove}
      />
      {box && <div className="chart-selection" style={box} />}
      {loading && (
        <div className="chart-loading" role="status" aria-live="polite">
          <span className="spinner" aria-hidden="true" />
          <span>Loading…</span>
        </div>
      )}
    </div>
  );
}

export type { EChartsOption } from 'echarts';

export const axisColor = '#8b95a7';
export const gridColor = 'rgba(148, 163, 184, 0.12)';
