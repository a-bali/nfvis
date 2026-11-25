import { useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import {
  APIError,
  api,
  type ChartMetric,
  type Coverage,
  type Exporter,
  type Flow,
  type FlowPage,
  type Health,
  type InterfaceSeries,
  type Range,
  type SplitSeries,
  type SystemStats,
  type Timeseries,
  type Top,
  type TopRow,
} from './api';
import { Chart, axisColor, gridColor, type EChartsOption } from './components/Chart';
import './styles.css';

type View = 'overview' | 'interfaces' | 'hosts' | 'vlans' | 'protocols' | 'flows' | 'system';

const VIEWS: { id: View; label: string; dimension?: string; direction?: string }[] = [
  { id: 'overview', label: 'Overview' },
  { id: 'interfaces', label: 'Interfaces', dimension: 'interface', direction: 'ingress' },
  { id: 'hosts', label: 'Hosts', dimension: 'host', direction: 'source' },
  { id: 'vlans', label: 'VLANs', dimension: 'vlan', direction: 'source' },
  { id: 'protocols', label: 'Protocols', dimension: 'protocol' },
  { id: 'flows', label: 'Flows' },
];

const ALL_VIEWS: { id: View; label: string; dimension?: string; direction?: string }[] = [
  ...VIEWS,
  { id: 'system', label: 'System' },
];

const RANGES = [
  { label: '15 min', ms: 15 * 60_000 },
  { label: '1 hour', ms: 60 * 60_000 },
  { label: '6 hours', ms: 6 * 60 * 60_000 },
  { label: '24 hours', ms: 24 * 60 * 60_000 },
  { label: '2 days', ms: 2 * 24 * 60 * 60_000 },
  { label: '7 days', ms: 7 * 24 * 60 * 60_000 },
  { label: '30 days', ms: 30 * 24 * 60 * 60_000 },
];

const PROTOCOLS: Record<number, string> = {
  0: 'IPv6 Hop-by-Hop', 1: 'ICMP', 2: 'IGMP', 4: 'IPv4', 6: 'TCP',
  17: 'UDP', 41: 'IPv6', 47: 'GRE', 50: 'ESP', 51: 'AH',
  58: 'ICMPv6', 89: 'OSPF', 132: 'SCTP',
};

const METRICS: { id: ChartMetric; label: string }[] = [
  { id: 'bytes', label: 'Bytes' },
  { id: 'packets', label: 'Packets' },
  { id: 'flows', label: 'Flows' },
];

// Dimensions a Flows chart can be stacked by, in the same order as the Flows
// filters so the chart mirrors the filter row.
const SPLIT_OPTIONS: { id: string; label: string }[] = [
  { id: '', label: 'No split' },
  { id: 'input_if', label: 'Input interface' },
  { id: 'output_if', label: 'Output interface' },
  { id: 'input_vlan', label: 'Input VLAN' },
  { id: 'output_vlan', label: 'Output VLAN' },
  { id: 'src_host', label: 'Source host' },
  { id: 'dst_host', label: 'Destination host' },
  { id: 'protocol', label: 'Protocol' },
];

const SPLIT_KEYS = SPLIT_OPTIONS.map((o) => o.id);

// Stacked bands cycle through this palette; the fold-out band stays grey so it
// never looks like a first-class category.
const SERIES_COLORS = [
  '#5eead4', '#38bdf8', '#a78bfa', '#f472b6', '#fbbf24',
  '#34d399', '#fb923c', '#22d3ee', '#c084fc', '#f87171',
];
const OTHER_COLOR = '#64748b';
const MAX_LEGEND_ROWS = 10;
// The unstacked chart recolors per metric so the shape and the unit stay
// visually linked; stacked bands keep their own palette for identity.
const METRIC_COLORS: Record<ChartMetric, { line: string; area: string }> = {
  bytes: { line: '#5eead4', area: 'rgba(94, 234, 212, 0.22)' },
  packets: { line: '#60a5fa', area: 'rgba(96, 165, 250, 0.22)' },
  flows: { line: '#fbbf24', area: 'rgba(251, 191, 36, 0.22)' },
};

type HistoryMode = 'push' | 'replace';
type NavState = {
  view: View;
  rangeKey: string;
  customFrom: string;
  customTo: string;
  direction: string;
  hostQuery: string;
  topPage: number;
  flowPage: number;
  flowFilters: Record<string, string>;
  metric: ChartMetric;
  splitBy: string;
  overviewInterface: string;
};

const FILTER_KEYS = [
  'src_host', 'dst_host',
  'input_interface', 'output_interface',
  'input_vlan', 'output_vlan',
  'protocol', 'src_port', 'dst_port', 'exporter',
] as const;

function defaultDirection(view: View): string {
  if (view === 'interfaces') return 'ingress';
  return 'source';
}

function readNav(): NavState {
  const p = new URLSearchParams(window.location.search);
  const rawView = p.get('view') as View;
  const view = ALL_VIEWS.some((v) => v.id === rawView) ? rawView : 'overview';
  const rawRange = p.get('range') ?? '1 hour';
  const rangeKey = RANGES.some((r) => r.label === rawRange) || rawRange === 'Custom' ? rawRange : '1 hour';
  let direction = p.get('direction') || defaultDirection(view);
  const flowFilters: Record<string, string> = {};
  for (const key of FILTER_KEYS) {
    const value = p.get(`f_${key}`);
    if (value) flowFilters[key] = value;
  }
  // Accept URLs created before the input/output filters replaced the toggle.
  const legacyInterface = p.get('f_interface');
  if (legacyInterface) {
    const side = direction === 'egress' ? 'output_interface' : 'input_interface';
    if (!flowFilters[side]) flowFilters[side] = legacyInterface;
  }
  const legacyVLAN = p.get('f_vlan');
  if (legacyVLAN) {
    const side = direction === 'destination' ? 'output_vlan' : 'input_vlan';
    if (!flowFilters[side]) flowFilters[side] = legacyVLAN;
  }
  const page = (name: string) => {
    const n = Number(p.get(name));
    return Number.isInteger(n) && n >= 0 ? n : 0;
  };
  const rawMetric = p.get('metric') as ChartMetric;
  const metric: ChartMetric = METRICS.some((m) => m.id === rawMetric) ? rawMetric : 'bytes';
  const rawSplit = p.get('split') ?? '';
  const splitBy = SPLIT_KEYS.includes(rawSplit) ? rawSplit : '';
  if (view === 'flows') direction = defaultDirection(view);
  return {
    view,
    rangeKey,
    customFrom: p.get('from') ?? '',
    customTo: p.get('to') ?? '',
    direction,
    hostQuery: p.get('f_host_query') ?? '',
    topPage: page('tp'),
    flowPage: page('fp'),
    flowFilters,
    metric,
    splitBy,
    overviewInterface: p.get('interface') ?? '',
  };
}

function navSearch(s: NavState): string {
  const p = new URLSearchParams();
  p.set('view', s.view);
  if (s.rangeKey !== '1 hour') p.set('range', s.rangeKey);
  if (s.rangeKey === 'Custom') {
    if (s.customFrom) p.set('from', s.customFrom);
    if (s.customTo) p.set('to', s.customTo);
  }
  if (s.direction !== defaultDirection(s.view)) p.set('direction', s.direction);
  if (s.hostQuery) p.set('f_host_query', s.hostQuery);
  if (s.topPage) p.set('tp', String(s.topPage));
  if (s.flowPage) p.set('fp', String(s.flowPage));
  for (const key of FILTER_KEYS) {
    const value = s.flowFilters[key];
    if (value) p.set(`f_${key}`, value);
  }
  if (s.metric !== 'bytes') p.set('metric', s.metric);
  if (s.view === 'flows' && s.splitBy) p.set('split', s.splitBy);
  if (s.overviewInterface) p.set('interface', s.overviewInterface);
  const query = p.toString();
  return query ? `?${query}` : '';
}

function overviewInterfaces(exporters: Exporter[]) {
  return exporters.flatMap((exporter) => Object.entries(exporter.interfaces ?? {})
    .filter(([index]) => Number(index) > 0)
    .map(([index, label]) => ({
      key: `${exporter.address}@${index}`,
      address: exporter.address,
      index,
      name: label || `Interface ${index}`,
      exporter: exporter.label || exporter.address,
      wan: /^wan(?:[ _-]?\d+)?$/i.test(label.trim()),
      observed: exporter.observed,
    })))
    .sort((a, b) => a.name.localeCompare(b.name, undefined, { numeric: true }) || a.key.localeCompare(b.key));
}

function selectedOverviewInterface(options: ReturnType<typeof overviewInterfaces>, key: string) {
  if (key === 'all') return undefined;
  return options.find((option) => option.key === key) ?? options.find((option) => option.wan && option.observed) ?? options.find((option) => option.wan);
}

const METRIC_LABELS: Record<ChartMetric, string> = { bytes: 'Bytes', packets: 'Packets', flows: 'Flows' };

function metricValue(row: { bytes: number; packets: number; flows: number }, metric: ChartMetric): number {
  if (metric === 'packets') return row.packets;
  if (metric === 'flows') return row.flows;
  return row.bytes;
}

function fmtMetric(n: number, metric: ChartMetric): string {
  return metric === 'bytes' ? fmtBytes(n) : fmtNumber(n);
}

function bandLabel(band: SplitSeries): string {
  return band.label || band.key || 'unknown';
}

function bandKey(band: SplitSeries): string {
  return band.other ? 'other' : band.key;
}

function bandColor(band: SplitSeries, index: number): string {
  return band.other ? OTHER_COLOR : SERIES_COLORS[index % SERIES_COLORS.length];
}

function fmtBytes(n: number): string {
  if (!Number.isFinite(n)) return '—';
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB'];
  let i = 0;
  let v = n;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v >= 10 || i === 0 ? v.toFixed(0) : v.toFixed(1)} ${units[i]}`;
}

function fmtNumber(n: number): string {
  return new Intl.NumberFormat('en-US', { notation: n > 1_000_000 ? 'compact' : 'standard', maximumFractionDigits: 1 }).format(n ?? 0);
}

function fmtDateTimeLocal(ms: number): string {
  const d = new Date(ms);
  const pad = (v: number) => String(v).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

function sumPoints(series: Timeseries | null): { bytes: number; packets: number; flows: number } {
  const points = series?.points ?? [];
  return {
    bytes: points.reduce((sum, p) => sum + p.bytes, 0),
    packets: points.reduce((sum, p) => sum + p.packets, 0),
    flows: points.reduce((sum, p) => sum + p.flows, 0),
  };
}

function fmtAge(from: Date | null, now: number): string {
  if (!from) return '…';
  const seconds = Math.max(0, Math.floor((now - from.getTime()) / 1000));
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h`;
  return `${Math.floor(hours / 24)}d`;
}

function fmtVLAN(v: number): string {
  return v >= 0 ? String(v) : '-';
}

function fmtInterface(index: number | undefined, label?: string): string {
  if (label) return label;
  return index ? String(index) : '-';
}

function fmtTime(ms: number): string {
  if (!ms) return '—';
  return new Date(ms).toLocaleString([], { year: 'numeric', month: 'short', day: '2-digit', hour: '2-digit', minute: '2-digit' });
}

function fmtTableTime(ms: number): string {
  if (!ms) return '—';
  const d = new Date(ms);
  const pad = (v: number) => String(v).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

type SortDir = 'asc' | 'desc';
type SortState<K extends string = string> = { key: K; dir: SortDir };

function useTableSort<K extends string>(initialKey: K, initialDir: SortDir = 'desc') {
  const [sort, setSort] = useState<SortState<K>>({ key: initialKey, dir: initialDir });
  const toggle = (key: K, numeric = false) => {
    setSort((prev) => prev.key === key
      ? { key, dir: prev.dir === 'asc' ? 'desc' : 'asc' }
      : { key, dir: numeric || key === 'time' ? 'desc' : 'asc' });
  };
  return { sort, toggle };
}

function SortableTh<K extends string>({
  label, sortKey, sort, onSort, numeric, title, bare,
}: {
  label: string;
  sortKey: K;
  sort: SortState<K>;
  onSort: (key: K, numeric?: boolean) => void;
  numeric?: boolean;
  title?: string;
  bare?: boolean;
}) {
  const active = sort.key === sortKey;
  return (
    <th className={numeric ? 'num' : undefined} title={title}>
      <button
        type="button"
        className={`sort-header${active ? ' active' : ''}`}
        onClick={() => onSort(sortKey, numeric)}
        aria-label={`Sort by ${label}`}
      >
        {bare ? null : <span>{label}</span>}
        <span className="sort-arrow" aria-hidden="true">{active ? (sort.dir === 'asc' ? '▲' : '▼') : '↕'}</span>
      </button>
    </th>
  );
}

function fmtUTC(ms: number): string {
  return ms ? new Date(ms).toISOString().replace('.000Z', 'Z') : '—';
}

function fmtDays(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds <= 0) return '-';
  if (seconds < 60) return `${Math.round(seconds)}s`;
  if (seconds < 3600) return `${Math.round(seconds / 60)}m`;
  if (seconds < 86400) return `${Math.round(seconds / 3600)}h`;
  const days = seconds / 86400;
  // Whole days read as "1d", "30d"; fractional ones keep one decimal.
  if (Number.isInteger(days)) return `${days}d`;
  return days < 10 ? `${days.toFixed(1)}d` : `${Math.round(days)}d`;
}

function fmtUptime(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds <= 0) return '0s';
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  if (days) return `${days}d ${hours}h`;
  if (hours) return `${hours}h ${minutes}m`;
  return `${minutes}m`;
}

function fmtDuration(ms: number): string {
  if (!Number.isFinite(ms) || ms <= 0) return '0s';
  return `${Math.round(ms / 1000)}s`;
}

// fmtElapsed renders a short elapsed time with sub-second and millisecond
// precision, for a sweep that often finishes in well under a second.
function fmtElapsed(ms: number): string {
  if (!Number.isFinite(ms) || ms < 0) return '—';
  if (ms < 1) return '<1ms';
  if (ms < 1000) return `${Math.round(ms)}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(ms < 10_000 ? 1 : 0)}s`;
  const minutes = Math.floor(ms / 60_000);
  const seconds = Math.round((ms % 60_000) / 1000);
  return `${minutes}m ${seconds}s`;
}

// fmtAgo renders how long ago `ms` was, relative to now.
function fmtAgo(now: number, ms: number): string {
  const delta = now - ms;
  if (!Number.isFinite(delta)) return '—';
  if (delta < 0) return 'just now';
  if (delta < 2000) return 'just now';
  return `${fmtElapsed(delta)} ago`;
}

// fmtIn renders how long until `ms`, relative to now.
function fmtIn(now: number, ms: number): string {
  const delta = ms - now;
  if (!Number.isFinite(delta)) return '—';
  if (delta <= 0) return 'due now';
  return `in ${fmtElapsed(delta)}`;
}

function protocolName(v: number): string {
  return PROTOCOLS[v] ?? String(v || '—');
}

function DirectionPicker({ value, onChange, options, hint, label = 'Direction' }: { value: string; onChange: (v: string) => void; options: [string, string][]; hint?: string; label?: string }) {
  return (
    <div className="segmented" role="group" aria-label={label} title={hint}>
      {options.map(([key, label]) => (
        <button key={key} className={value === key ? 'active' : ''} onClick={() => onChange(key)} type="button">
          {label}
        </button>
      ))}
    </div>
  );
}

function Banner({ children, tone = 'info', onDismiss }: { children: ReactNode; tone?: 'info' | 'warn' | 'error'; onDismiss: () => void }) {
  return <div className={`banner ${tone}`}><span>{children}</span><button className="banner-close" type="button" aria-label="Dismiss notice" onClick={onDismiss}>×</button></div>;
}

function Empty({ title, detail }: { title: string; detail?: string }) {
  return (
    <div className="empty">
      <strong>{title}</strong>
      {detail && <span>{detail}</span>}
    </div>
  );
}

export default function App() {
  const [nav, setNav] = useState<NavState>(readNav);
  const historyMode = useRef<HistoryMode>('replace');
  const popNavigation = useRef(false);

  const [tick, setTick] = useState(0);
  const [settled, setSettled] = useState(0);
  const [paused, setPaused] = useState(false);
  const [error, setError] = useState('');
  const [dismissedNotices, setDismissedNotices] = useState<Set<string>>(() => new Set());
  const dismissNotice = (key: string) => setDismissedNotices((previous) => new Set(previous).add(key));
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [now, setNow] = useState(() => Date.now());
  const [health, setHealth] = useState<Health | null>(null);
  const [system, setSystem] = useState<SystemStats | null>(null);
  const [flushingDNS, setFlushingDNS] = useState(false);
  const [runningRetention, setRunningRetention] = useState(false);
  const [exporters, setExporters] = useState<Exporter[]>([]);
  const [timeseries, setTimeseries] = useState<Timeseries | null>(null);
  const [directional, setDirectional] = useState<{ key: string; series: InterfaceSeries } | null>(null);
  const overviewOptions = useMemo(() => overviewInterfaces(exporters), [exporters]);
  const overviewSelection = selectedOverviewInterface(overviewOptions, nav.overviewInterface);
  const visibleDirectional = directional?.key === overviewSelection?.key ? directional?.series : undefined;
  const [recentTraffic, setRecentTraffic] = useState<Timeseries[]>([]);
  const [flowSeries, setFlowSeries] = useState<Timeseries | null>(null);
  const [chartLoading, setChartLoading] = useState(false);
  // The flows table shares the chart's request, but not its spinner: a polling
  // refresh must not flash the table, while a deliberate change (a filter, the
  // page, the sort) should show that the rows on screen are stale. `chartLoading`
  // cannot serve this because it is cleared for polling by design.
  const [flowsLoading, setFlowsLoading] = useState(false);
  const [showFlowChart, setShowFlowChart] = useState(true);
  const [menuOpen, setMenuOpen] = useState(false);
  useEffect(() => {
    if (!menuOpen) return;
    const onKey = (event: KeyboardEvent) => { if (event.key === 'Escape') setMenuOpen(false); };
    const media = window.matchMedia('(min-width: 901px)');
    const onResize = () => { if (media.matches) setMenuOpen(false); };
    window.addEventListener('keydown', onKey);
    media.addEventListener('change', onResize);
    return () => { window.removeEventListener('keydown', onKey); media.removeEventListener('change', onResize); };
  }, [menuOpen]);
  const [top, setTop] = useState<Top | null>(null);
  const [topResultKey, setTopResultKey] = useState('');
  const [flows, setFlows] = useState<FlowPage | null>(null);
  const [vlanOptions, setVlanOptions] = useState<string[]>([]);
  const [selectedFlow, setSelectedFlow] = useState<Flow | null>(null);
  const [aggregateNotice, setAggregateNotice] = useState('');
  const [pendingRequests, setPendingRequests] = useState(0);
  // Bands the reader switched off in the stacked-chart legend. This is view
  // state, not URL state: it resets whenever the chart's data changes.
  const [hiddenBands, setHiddenBands] = useState<string[]>([]);

  const track = <T,>(promise: Promise<T>): Promise<T> => {
    setPendingRequests((v) => v + 1);
    return promise.finally(() => setPendingRequests((v) => Math.max(0, v - 1)));
  };

  const updateNav = (
    patch: Partial<NavState> | ((prev: NavState) => NavState),
    mode: HistoryMode = 'replace',
  ) => {
    historyMode.current = mode;
    setNav((prev) => (typeof patch === 'function' ? patch(prev) : { ...prev, ...patch }));
  };

  useEffect(() => {
    const target = `${window.location.pathname}${navSearch(nav)}${window.location.hash}`;
    const current = `${window.location.pathname}${window.location.search}${window.location.hash}`;
    if (popNavigation.current) {
      popNavigation.current = false;
      historyMode.current = 'replace';
      return;
    }
    if (target !== current) {
      try {
        const method = historyMode.current === 'push' ? 'pushState' : 'replaceState';
        window.history[method]({ nfvis: nav }, '', target);
      } catch {
        // URL updates are best-effort (e.g. sandboxed frames).
      }
    }
    historyMode.current = 'replace';
  }, [nav]);

  useEffect(() => {
    const onPop = () => {
      popNavigation.current = true;
      setNav(readNav());
    };
    window.addEventListener('popstate', onPop);
    return () => window.removeEventListener('popstate', onPop);
  }, []);

  // Poll five seconds after the previous load settled rather than on a fixed
  // interval: a range or filter change that takes longer than the interval
  // would otherwise be aborted by the next tick before it could finish.
  useEffect(() => {
    if (paused) return;
    const id = window.setTimeout(() => setTick((v) => v + 1), 5000);
    return () => window.clearTimeout(id);
  }, [paused, settled]);

  useEffect(() => {
    const id = window.setInterval(() => setNow(Date.now()), 1000);
    return () => window.clearInterval(id);
  }, []);

  const togglePaused = () => {
    const next = !paused;
    setPaused(next);
    if (!next) setTick((v) => v + 1);
  };

  const applyChartZoom = (zoomed: Range) => {
    const customFrom = fmtDateTimeLocal(zoomed.from_ms);
    const customTo = fmtDateTimeLocal(zoomed.to_ms);
    updateNav(
      (prev) => ({
        ...prev,
        rangeKey: 'Custom',
        customFrom,
        customTo,
        topPage: 0,
        flowPage: 0,
      }),
      'push',
    );
  };

  const range = useMemo<Range>(() => {
    const to = nav.rangeKey === 'Custom' && nav.customTo ? new Date(nav.customTo).getTime() : Date.now();
    if (nav.rangeKey === 'Custom') {
      const coverage = health?.coverage;
      const oldest = [coverage?.raw_available ? coverage.raw_oldest_ms : 0, coverage?.aggregate_available ? coverage.aggregate_oldest_ms : 0].filter((v): v is number => !!v && v > 0);
      // API ranges are bounded to 367 days; without coverage yet, request a valid window.
      const from = nav.customFrom ? new Date(nav.customFrom).getTime() : Math.max(to - 367 * 86400000, oldest.length ? Math.min(to - 1, ...oldest) : to - 60 * 60_000);
      if (Number.isFinite(from) && Number.isFinite(to) && from < to) return { from_ms: from, to_ms: to };
    }
    const preset = RANGES.find((r) => r.label === nav.rangeKey) ?? RANGES[1];
    return { from_ms: to - preset.ms, to_ms: to };
  }, [nav.rangeKey, nav.customFrom, nav.customTo, tick, health?.coverage?.raw_oldest_ms, health?.coverage?.aggregate_oldest_ms, health?.coverage?.raw_available, health?.coverage?.aggregate_available]);

  const active = ALL_VIEWS.find((v) => v.id === nav.view)!;
  const dimension = active.dimension ?? '';
  const effectiveDirection =
    dimension === 'interface' || dimension === 'host' || dimension === 'vlan' ? nav.direction : '';
  const hostFilterActive = nav.view === 'hosts' && nav.hostQuery.trim() !== '';
  const topSorting = useTableSort<TopSortKey>('bytes', 'desc');
  const flowSorting = useTableSort<FlowSortKey>('time', 'desc');
  // Keep old results during polling, but never display them for a different
  // direction, search, page, or sort while the new request is in flight.
  const topQueryKey = JSON.stringify([nav.view, nav.rangeKey, nav.customFrom, nav.customTo, effectiveDirection, nav.hostQuery, nav.topPage, topSorting.sort]);
  const chartView = nav.view === 'overview' || nav.view === 'flows';
  // Polling refreshes move `to` every few seconds. The spinner is reserved for
  // deliberate changes (window, view, filters), so it keys off those alone.
  const chartQueryKey = JSON.stringify([
    nav.view,
    nav.rangeKey,
    nav.customFrom,
    nav.customTo,
    nav.flowFilters,
    nav.metric,
    nav.splitBy,
    nav.overviewInterface,
  ]);
  const loadedQueryKey = useRef(chartQueryKey);
  const loadedFlowQueryKey = useRef<string | null>(null);
  const flowReqSeq = useRef(0);

  useEffect(() => {
    const controller = new AbortController();
    const signal = controller.signal;
    let cancelled = false;
    setError('');
    setAggregateNotice('');
    const showSpinner = chartView && loadedQueryKey.current !== chartQueryKey;
    if (showSpinner) setChartLoading(true);
    // Deliberate changes to what the flows table shows — filters, page, sort —
    // set a spinner on the rows. A polling tick also refetches, but it must not
    // dim a table whose contents are not changing, so this keys off the same
    // query key as the chart rather than off every fetch.
    const flowQueryKey = JSON.stringify([nav.view, nav.rangeKey, nav.customFrom, nav.customTo,
      nav.flowFilters, nav.flowPage, nav.direction, flowSorting.sort]);
    const showFlowSpinner = nav.view === 'flows' && loadedFlowQueryKey.current !== flowQueryKey;
    if (showFlowSpinner) setFlowsLoading(true);
    // Clearing on success alone would leave the spinner up forever when a
    // request is aborted (the effect re-runs on every poll), because the abort
    // path skips the `!cancelled` branch. The sequence number makes only the
    // newest request responsible for clearing it.
    const flowSeq = ++flowReqSeq.current;

    const load = async () => {
      try {
        const [h, e] = await Promise.all([track(api.health(signal)), track(api.exporters(signal))]);
        if (cancelled) return;
        setHealth(h);
        setExporters(e.exporters);

        if (nav.view === 'system') {
          const s = await track(api.system(signal));
          if (cancelled) return;
          setSystem(s);
          setTimeseries(null);
          setFlowSeries(null);
          setTop(null);
          setFlows(null);
        } else if (nav.view === 'overview') {
          const end = Date.now();
          const selection = selectedOverviewInterface(overviewInterfaces(e.exporters), nav.overviewInterface);
          const [interfaceSeries, ts, ...recent] = await Promise.all([
            selection ? track(api.interfaceTimeseries(range, selection.address, selection.index, signal)) : Promise.resolve(null),
            track(api.timeseries(range, {}, { metric: nav.metric }, signal)),
            ...[5 * 60_000, 60 * 60_000, 24 * 60 * 60_000].map((duration) => track(api.timeseries({ from_ms: end - duration, to_ms: end }, {}, { metric: 'bytes' }, signal))),
          ]);
          if (cancelled) return;
          setDirectional(selection && interfaceSeries ? { key: selection.key, series: interfaceSeries } : null);
          setRecentTraffic(recent);
          setTimeseries(ts);
          setFlowSeries(null);
          setTop(null);
        } else if (nav.view === 'flows') {
          const [pageResult, ts] = await Promise.all([
            track(api.flows(range, { direction: nav.direction, ...nav.flowFilters }, 100, nav.flowPage * 100, signal, flowSorting.sort.key, flowSorting.sort.dir))
              .then((page) => ({ page, notice: '' }))
              .catch((err: unknown) => {
                if (err instanceof APIError && err.code === 'aggregate_only') return { page: null, notice: err.message };
                throw err;
              }),
            track(api.timeseries(range, { ...nav.flowFilters }, { metric: nav.metric, splitBy: nav.splitBy }, signal)),
          ]);
          if (cancelled) return;
          setFlows(pageResult.page);
          setAggregateNotice(pageResult.notice);
          setFlowSeries(ts);
          setTimeseries(null);
        } else {
          const t = await track(api.top(
            range, dimension, effectiveDirection, '',
            50, nav.topPage * 50,
            signal, topSorting.sort.key, topSorting.sort.dir, hostFilterActive ? nav.hostQuery.trim() : '',
          ));
          if (cancelled) return;
          setTop(t);
          setTopResultKey(topQueryKey);
        }
        if (!cancelled) {
          setLastUpdated(new Date());
          setNow(Date.now());
        }
      } catch (err) {
        if (!cancelled && !controller.signal.aborted) {
          setError(err instanceof Error ? err.message : String(err));
        }
      } finally {
        if (!cancelled) {
          loadedQueryKey.current = chartQueryKey;
          if (nav.view === 'flows') loadedFlowQueryKey.current = flowQueryKey;
          setChartLoading(false);
          setSettled((v) => v + 1);
        }
        // Unconditional: whatever happened to this request, if it is still the
        // newest one it owns the spinner, and leaving it set would dim the table
        // until the next deliberate change.
        if (flowReqSeq.current === flowSeq) setFlowsLoading(false);
      }
    };
    void load();
    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [nav.view, range, chartQueryKey, chartView, effectiveDirection, nav.topPage, nav.flowPage, nav.flowFilters, nav.direction, nav.metric, nav.splitBy, dimension, nav.hostQuery, hostFilterActive, topSorting.sort, flowSorting.sort, topQueryKey]);

  const systemNum = (key: string) => Number(system?.ingest[key] ?? 0);

  const flushDNS = async () => {
    setFlushingDNS(true);
    setError('');
    try {
      await track(api.flushDNSCache());
      const s = await track(api.system());
      setSystem(s);
      setLastUpdated(new Date());
      setNow(Date.now());
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setFlushingDNS(false);
    }
  };

  const runRetentionNow = async () => {
    setRunningRetention(true);
    setError('');
    try {
      await track(api.runRetention());
      const s = await track(api.system());
      setSystem(s);
      setLastUpdated(new Date());
      setNow(Date.now());
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setRunningRetention(false);
    }
  };

  useEffect(() => {
    if (nav.view !== 'flows') {
      setVlanOptions([]);
      return;
    }
    const controller = new AbortController();
    const load = async () => {
      try {
        const [src, dst] = await Promise.all([
          track(api.top(range, 'vlan', 'source', '', 200, 0, controller.signal)),
          track(api.top(range, 'vlan', 'destination', '', 200, 0, controller.signal)),
        ]);
        const values = new Set<string>();
        for (const row of [...src.rows, ...dst.rows]) {
          if (row.key) values.add(row.key);
        }
        const sorted = [...values];
        sorted.sort((a, b) => {
          const na = Number(a);
          const nb = Number(b);
          if (Number.isFinite(na) && Number.isFinite(nb)) return na - nb;
          return a.localeCompare(b);
        });
        if (!controller.signal.aborted) setVlanOptions(sorted);
      } catch {
        // The main data effect reports request failures.
      }
    };
    void load();
    return () => controller.abort();
  }, [nav.view, range]);

  const changeView = (next: View) => {
    updateNav(
      (prev) => ({
        ...prev,
        view: next,
        direction: defaultDirection(next),
        topPage: 0,
        flowPage: 0,
        flowFilters: {},
      }),
      'push',
    );
    window.scrollTo({ top: 0 });
  };

  const drillDown = (row: TopRow) => {
    if (nav.view === 'overview') {
      updateNav((prev) => ({ ...prev, view: 'hosts', direction: 'source', topPage: 0, flowPage: 0, flowFilters: {} }), 'push');
      return;
    }
    const filters: Record<string, string> = {};
    if (dimension === 'host') {
      filters[nav.direction === 'destination' ? 'dst_host' : 'src_host'] = row.key;
    }
    if (dimension === 'vlan') filters[nav.direction === 'destination' ? 'output_vlan' : 'input_vlan'] = row.key;
    if (dimension === 'protocol') filters.protocol = row.key;
    if (dimension === 'interface') {
      const [idx, exporter] = row.key.split('@');
      filters[nav.direction === 'egress' ? 'output_interface' : 'input_interface'] = idx;
      if (exporter) filters.exporter = exporter;
    }
    updateNav((prev) => ({
      ...prev,
      flowFilters: filters,
      direction: defaultDirection('flows'),
      view: 'flows',
      flowPage: 0,
      topPage: 0,
    }), 'push');
    window.scrollTo({ top: 0 });
  };

  const setHostQuery = (value: string) => updateNav({ hostQuery: value, topPage: 0 });

  const setFlowFilter = (key: string, value: string) => {
    updateNav((prev) => {
      const next = { ...prev.flowFilters };
      if (value) next[key] = value;
      else delete next[key];
      return { ...prev, flowFilters: next, flowPage: 0 };
    });
  };

  const seriesOption = (series: Timeseries | null, hidden: string[] = [], directions?: {
    result?: InterfaceSeries; wan: boolean;
  }): EChartsOption => {
    const points = series?.points ?? [];
    const bands = series?.series ?? [];
    const metric = nav.metric;
    const metricColor = METRIC_COLORS[metric];
    const stacked = bands.length > 0;
    const fmtRate = (v: number) => {
      if (metric === 'bytes' && v >= 1) return `${fmtBytes(v)}/s`;
      const value = new Intl.NumberFormat('en-US', { notation: v >= 1_000_000 ? 'compact' : 'standard', maximumFractionDigits: 3 }).format(v);
      return `${value} ${metric === 'bytes' ? 'B' : metric}/s`;
    };
    const durationByBucket = new Map(points.map((p) => [p.bucket_ms, p.duration_ms]));
    const rate = (value: number, bucket: number) => value * 1000 / (durationByBucket.get(bucket) || series?.bucket_ms || 60000);
    // Every band keeps its series (with empty data when hidden) so ECharts never
    // merges a stale series back in and colors stay attached to their band.
    const chartSeries = directions
      ? [
          { name: directions.wan ? 'Download' : 'Ingress', points: directions.result?.ingress, sign: 1, color: '#5eead4' },
          { name: directions.wan ? 'Upload' : 'Egress', points: directions.result?.egress, sign: -1, color: '#a78bfa' },
        ].map(({ name, points, sign, color }) => ({
          name,
          type: 'line' as const,
          showSymbol: false,
          lineStyle: { width: 2, color },
          areaStyle: { opacity: 0.22, color, origin: 0 },
          itemStyle: { color },
          data: points?.map((point) => [point.bucket_ms, sign * metricValue(point, metric) * 1000 / point.duration_ms]) ?? [],
        }))
      : stacked
      ? bands.map((band, index) => {
          const color = bandColor(band, index);
          const visible = !hidden.includes(bandKey(band));
          return {
            name: bandLabel(band),
            type: 'line' as const,
            stack: 'traffic',
            smooth: false,
            showSymbol: false,
            lineStyle: { width: 0, color },
            areaStyle: { opacity: 0.85, color },
            itemStyle: { color },
            emphasis: { focus: 'series' as const },
            data: visible
              ? band.points.map((p) => [p[0], rate(metricValue({ bytes: p[1], packets: p[2], flows: p[3] }, metric), p[0])])
              : [],
          };
        })
      : [
          {
            name: METRIC_LABELS[metric],
            type: 'line' as const,
            smooth: true,
            showSymbol: false,
            areaStyle: { color: metricColor.area },
            lineStyle: { width: 2, color: metricColor.line },
            itemStyle: { color: metricColor.line },
            data: points.map((p) => [p.bucket_ms, rate(metricValue(p, metric), p.bucket_ms)]),
          },
        ];

    return {
      backgroundColor: 'transparent',
      grid: { left: 58, right: 24, top: 44, bottom: 56, containLabel: true },
      tooltip: {
        trigger: 'axis',
        backgroundColor: '#151b28',
        borderColor: '#2c364a',
        textStyle: { color: '#e6ecf5' },
        formatter: (params) => {
          const items = (Array.isArray(params) ? params : [params]) as unknown as {
            axisValue: number;
            seriesName: string;
            value: [number, number] | number;
          }[];
          const valueOf = (p: { value: [number, number] | number }) => (Array.isArray(p.value) ? p.value[1] : p.value);
          const lines = items.map((p) => `${p.seriesName}: ${fmtRate(Math.abs(valueOf(p)))}`);
          if (!directions && items.length > 1) {
            const total = items.reduce((sum, p) => sum + valueOf(p), 0);
            lines.push(`Total: ${fmtRate(total)}`);
          }
          return `${new Date(items[0]?.axisValue ?? 0).toLocaleString()}<br/>${lines.join('<br/>')}`;
        },
      },
      // A stacked chart has too many bands for an inline legend; the table under
      // the chart names them instead.
      legend: { show: !!directions, textStyle: { color: axisColor } },
      xAxis: {
        type: 'time' as const,
        axisLabel: {
          color: axisColor,
          hideOverlap: true,
          // Day boundaries are otherwise abbreviated and hard to read on a
          // narrow chart, so show dates only on midnight ticks.
          formatter: (value: number) => {
            const d = new Date(value);
            const pad = (n: number) => String(n).padStart(2, '0');
            if (d.getHours() === 0 && d.getMinutes() === 0) {
              return `${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
            }
            return `${pad(d.getHours())}:${pad(d.getMinutes())}`;
          },
        },
        axisLine: { onZero: false, lineStyle: { color: gridColor } },
        splitLine: { show: false },
      },
      yAxis: [
        {
          type: 'value' as const,
          axisLabel: {
            color: axisColor,
            formatter: (v: number) => `${v < 0 ? '−' : ''}${fmtRate(Math.abs(v))}`,
          },
          splitLine: { lineStyle: { color: gridColor } },
        },
      ],
      series: chartSeries,
    };
  };

  const tsOption = useMemo(() => overviewSelection
    ? seriesOption(null, [], { result: visibleDirectional, wan: overviewSelection.wan })
    : seriesOption(timeseries), [timeseries, nav.metric, overviewSelection, visibleDirectional]);
  const overviewHasData = overviewSelection
    ? !!visibleDirectional?.ingress.some((point, i) => point.flows || visibleDirectional.egress[i]?.flows)
    : !!timeseries?.points.length;
  const flowChartOption = useMemo(
    () => seriesOption(flowSeries, hiddenBands),
    [flowSeries, nav.metric, hiddenBands],
  );
  const selectedTotals = useMemo(
    () => sumPoints(timeseries ?? flowSeries),
    [timeseries, flowSeries],
  );
  const seriesIdentity = `${range.from_ms}|${range.to_ms}`;
  const overviewResetKey = `${seriesIdentity}|${overviewSelection?.key ?? 'all'}`;
  const flowResetKey = `${seriesIdentity}|${nav.splitBy}|${FILTER_KEYS.map((key) => `${key}=${nav.flowFilters[key] ?? ''}`).join('&')}`;
  // Reset hidden bands only when the chart's shape changes, not on every 5 s
  // polling tick (which shifts the window bounds).
  const bandResetKey = `${nav.rangeKey}|${nav.customFrom}|${nav.customTo}|${nav.splitBy}|${nav.metric}|${FILTER_KEYS.map((key) => `${key}=${nav.flowFilters[key] ?? ''}`).join('&')}`;
  useEffect(() => setHiddenBands([]), [bandResetKey]);
  const toggleBand = (key: string) => {
    setHiddenBands((prev) => (prev.includes(key) ? prev.filter((k) => k !== key) : [...prev, key]));
  };
  const metricToggle = (
    <DirectionPicker
      label="Chart metric"
      hint="Choose what the chart plots."
      value={nav.metric}
      onChange={(v) => updateNav({ metric: v as ChartMetric }, 'push')}
      options={METRICS.map((m) => [m.id, m.label])}
    />
  );
  const splitSelect = (
    <label className="chart-stack">
      <span>Stack</span>
      <select
        className="chart-split"
        aria-label="Stack chart by"
        title="Stack the chart by this dimension, using the applied filters."
        value={nav.splitBy}
        onChange={(e) => updateNav({ splitBy: e.target.value }, 'push')}
      >
        {SPLIT_OPTIONS.map((o) => <option key={o.id || 'none'} value={o.id}>{o.label}</option>)}
      </select>
    </label>
  );

  const counters = health?.counters ?? {};
  const coverage: Coverage | undefined = health?.coverage;
  const overviewLimitations = overviewSelection ? visibleDirectional?.limitations : timeseries?.limitations;
  const limitations = [...(nav.view === 'overview' ? overviewLimitations ?? [] : []), ...(flowSeries?.limitations ?? []), ...(top?.limitations ?? [])]
    .filter((v, i, a) => a.indexOf(v) === i);
  const visibleTop = topResultKey === topQueryKey ? top : null;
  const connected = !!health && health.status === 'ok' && !error;
  const interfaceLabels = useMemo(() => {
    const labels = new Set<string>();
    for (const e of exporters) {
      for (const label of Object.values(e.interfaces ?? {})) {
        if (label) labels.add(label);
      }
    }
    return [...labels].sort((a, b) => a.localeCompare(b));
  }, [exporters]);

  const interfaceSelect = (key: string, label: string) => {
    const value = nav.flowFilters[key] ?? '';
    return (
      <select value={value} onChange={(e) => setFlowFilter(key, e.target.value)} aria-label={label}>
        <option value="">{label}</option>
        {interfaceLabels.map((name) => <option key={name} value={name}>{name}</option>)}
        {value && !interfaceLabels.includes(value) && <option value={value}>{value}</option>}
      </select>
    );
  };

  const vlanSelect = (key: string, label: string, placeholder: string) => {
    const value = nav.flowFilters[key] ?? '';
    return (
      <select value={value} onChange={(e) => setFlowFilter(key, e.target.value)} aria-label={label} className="vlan-select">
        <option value="">{placeholder}</option>
        {vlanOptions.map((vlan) => <option key={vlan} value={vlan}>{vlan === '-1' ? '-' : vlan}</option>)}
        {value && !vlanOptions.includes(value) && <option value={value}>{value === '-1' ? '-' : value}</option>}
      </select>
    );
  };

  return (
    <div className="app">
      <div className="mobile-bar">
        <a className="brand" href="/"><span className="logo">◈</span><span>nfvis</span></a>
        <button type="button" className="menu-toggle" aria-label={menuOpen ? 'Close navigation' : 'Open navigation'} aria-expanded={menuOpen} aria-controls="sidebar-navigation" onClick={() => setMenuOpen((open) => !open)}>☰</button>
      </div>
      {menuOpen && <button className="sidebar-backdrop" aria-label="Close navigation" onClick={() => setMenuOpen(false)} />}
      {pendingRequests > 0 && <div className="request-indicator" role="status" aria-label="Loading" />}
      <aside id="sidebar-navigation" className={`sidebar${menuOpen ? ' open' : ''}`}>
        <a className="brand" href="/"><span className="logo">◈</span><span>nfvis</span></a>
        <nav>
          {VIEWS.map((v) => (
            <button key={v.id} className={nav.view === v.id ? 'nav active' : 'nav'} onClick={() => { changeView(v.id); setMenuOpen(false); }} type="button">
              {v.label}
            </button>
          ))}
          <div className="nav-spacer" />
          <button className={nav.view === 'system' ? 'nav active' : 'nav'} onClick={() => { changeView('system'); setMenuOpen(false); }} type="button">
            System
          </button>
        </nav>
        <div className="sidebar-foot">
          <span className={`status-dot ${paused ? 'paused' : connected ? 'connected' : 'disconnected'}`} />
          <span className="foot-updated">Updated {fmtAge(lastUpdated, now)} ago</span>
        </div>
        <div className="sidebar-version">{health?.version || '—'}</div>
      </aside>

      <main>
        <header>
          <h1>{active.label}</h1>
          <div className="header-actions">
            {nav.view !== 'system' && (
              <TimeWindowPicker
                range={range}
                rangeKey={nav.rangeKey}
                customFrom={nav.customFrom}
                customTo={nav.customTo}
                onPreset={(key) => updateNav({ rangeKey: key, topPage: 0, flowPage: 0 }, 'push')}
                onApply={(from, to) => updateNav({ rangeKey: 'Custom', customFrom: from, customTo: to, topPage: 0, flowPage: 0 }, 'push')}
              />
            )}
            <button
              className={`pause-button ${paused ? 'paused' : ''}`}
              onClick={togglePaused}
              type="button"
              aria-pressed={paused}
              title={paused ? 'Resume live updates' : 'Pause live updates'}
            >
              {paused ? '▶ Resume' : '❚❚ Pause'}
            </button>
          </div>
        </header>

        {error && !dismissedNotices.has(`error:${error}`) && <Banner tone="error" onDismiss={() => dismissNotice(`error:${error}`)}>API error: {error}. Retrying automatically.</Banner>}
        {health?.warnings?.length && !dismissedNotices.has(`health:${health.warnings.join(' · ')}`) ? <Banner tone="warn" onDismiss={() => dismissNotice(`health:${health.warnings.join(' · ')}`)}>{health.warnings.join(' · ')}</Banner> : null}
        {aggregateNotice && !dismissedNotices.has(`aggregate:${aggregateNotice}`) && <Banner tone="warn" onDismiss={() => dismissNotice(`aggregate:${aggregateNotice}`)}>{aggregateNotice} Choose a more recent time range for individual flows.</Banner>}
        {limitations.filter((l) => !dismissedNotices.has(`limitation:${l}`)).map((l) => <Banner key={l} onDismiss={() => dismissNotice(`limitation:${l}`)}>{l}</Banner>)}
        {coverage?.resolution === 'aggregate' && !dismissedNotices.has('aggregate-only') && <Banner tone="warn" onDismiss={() => dismissNotice('aggregate-only')}>This period has hourly aggregates only. Flows and host/port filters are unavailable.</Banner>}

        {nav.view === 'overview' && (
          <>
            <section className="cards">
              <div className="card"><span className="card-label">Last flow received</span><strong>{Number(counters.last_flow_received_ms) > 0 ? `${Math.max(0, Math.floor((now - Number(counters.last_flow_received_ms)) / 1000))} seconds ago` : '—'}</strong>{[['Last 5 minutes', 'flows_5min', 5], ['Last hour', 'flows_1hour', 60], ['Last 24 hours', 'flows_24hour', 1440]].map(([label, key, minutes]) => <Stat key={String(key)} label={String(label)} value={`${(Number(counters[String(key)] ?? 0) / Number(minutes)).toLocaleString(undefined, { maximumFractionDigits: 1 })} flows/min`} />)}<span className="card-hint">Since collector start</span></div>
              <div className="card"><span className="card-label">Flows received</span>{[['Last 5 minutes', 'flows_5min'], ['Last hour', 'flows_1hour'], ['Last 24 hours', 'flows_24hour']].map(([label, key]) => <Stat key={key} label={label} value={fmtNumber(Number(counters[key] ?? 0))} />)}<span className="card-hint">Since collector start</span></div>
              <div className="card"><span className="card-label">Collector</span><Stat label="Uptime" value={health ? fmtUptime(health.uptime_seconds) : '—'} /><Stat label="Dropped flows" value={fmtNumber(Number(counters.flows_dropped ?? 0))} /><Stat label="Avg/Med/P90 processing time" value={['processing_avg_ms', 'processing_median_ms', 'processing_p90_ms'].map((key) => typeof counters[key] === 'number' ? `${Number(counters[key]).toFixed(3)} ms` : '—').join(' / ')} /><span className="card-hint">Since collector start</span></div>
              <div className="card"><span className="card-label">Reported traffic</span>{['Last 5 minutes', 'Last hour', 'Last 24 hours'].map((label, i) => <Stat key={label} label={label} value={recentTraffic[i] ? fmtBytes(recentTraffic[i].points.reduce((sum, p) => sum + p.bytes, 0)) : '—'} />)}</div>
              <div className="card"><span className="card-label">All reported traffic · Selected window</span><Stat label="Bytes" value={fmtBytes(selectedTotals.bytes)} /><Stat label="Packets" value={fmtNumber(selectedTotals.packets)} /><Stat label="Flows" value={fmtNumber(selectedTotals.flows)} /></div>
            </section>
            <Panel title={overviewSelection ? 'Interface traffic' : 'All reported traffic'} action={<div className="overview-chart-controls">
              <label className="chart-stack">
                <span>Interface</span>
                <select className="chart-split" aria-label="Interface" value={overviewSelection?.key ?? 'all'}
                  onChange={(e) => updateNav({ overviewInterface: e.target.value }, 'push')}>
                  <option value="all">All reported traffic</option>
                  {overviewOptions.map((option) => <option key={option.key} value={option.key}>
                    {option.name}{exporters.length > 1 ? ` · ${option.exporter} (${option.address})` : ''}
                  </option>)}
                </select>
              </label>
              {metricToggle}
            </div>}>
              {overviewHasData ? (
                <Chart
                  option={tsOption}
                  height={400}
                  resetKey={overviewResetKey}
                  onZoom={applyChartZoom}
                  loading={chartLoading}
                />
              ) : chartLoading ? (
                <Chart option={tsOption} resetKey={overviewResetKey} loading height={400} />
              ) : (
                <Empty title="No traffic in this range" detail="Check exporter configuration or widen the time range." />
              )}
            </Panel>
          </>
        )}

        {nav.view !== 'overview' && nav.view !== 'flows' && nav.view !== 'system' && (
          <>
            {(dimension === 'interface' || dimension === 'host' || dimension === 'vlan') && (
              <div className="controls direction-row">
                <span>Direction</span>
                <DirectionPicker
                  value={nav.direction}
                  onChange={(v) => updateNav({ direction: v, topPage: 0 })}
                  options={dimension === 'interface' ? [['ingress', 'Ingress'], ['egress', 'Egress']] : [['source', 'Source'], ['destination', 'Destination']]}
                  hint={dimension === 'interface' ? 'Choose ingress or egress interfaces to rank.' : dimension === 'host' ? 'Choose source or destination hosts to rank.' : 'Choose source or destination VLANs to rank.'}
                />
                {nav.view === 'hosts' && (
                  <input
                    className="host-filter-input"
                    aria-label="filter hosts"
                    placeholder="Host name or IP"
                    value={nav.hostQuery}
                    onChange={(e) => setHostQuery(e.target.value)}
                  />
                )}
              </div>
            )}
            <Panel wide title={`Top ${active.label.toLowerCase()}`} subtitle={visibleTop ? `${fmtNumber(visibleTop.total)} values` : ''}>
              {visibleTop?.rows.length ? <Table top={visibleTop} onDrill={drillDown} sort={topSorting.sort} onSort={(key, numeric) => { topSorting.toggle(key, numeric); updateNav({ topPage: 0 }); }} /> : !visibleTop && !error ? <Empty title="Loading results" detail="Updating the ranked table." /> : <Empty title={hostFilterActive ? 'No matching hosts' : 'No data in this range'} detail={hostFilterActive ? 'Try a different DNS name or IP substring.' : 'Traffic may only exist in hourly aggregates; ranked aggregates still appear here.'} />}
              {visibleTop && visibleTop.total > 50 && (
                <div className="pager">
                  <button disabled={nav.topPage === 0} onClick={() => updateNav((prev) => ({ ...prev, topPage: prev.topPage - 1 }), 'push')}>Previous</button>
                  <span>Page {nav.topPage + 1} / {Math.ceil(visibleTop.total / 50)}</span>
                  <button disabled={(nav.topPage + 1) * 50 >= visibleTop.total} onClick={() => updateNav((prev) => ({ ...prev, topPage: prev.topPage + 1 }), 'push')}>Next</button>
                </div>
              )}
            </Panel>
          </>
        )}

        {nav.view === 'flows' && (
          <>
          <Panel
            title="Flow traffic"
            titleToggle={{ expanded: showFlowChart, onToggle: () => setShowFlowChart((v) => !v) }}
            compact={!showFlowChart}
            leading={<button
              className="chart-chevron"
              type="button"
              aria-label={showFlowChart ? 'Collapse chart' : 'Expand chart'}
              title={showFlowChart ? 'Collapse chart' : 'Expand chart'}
              aria-expanded={showFlowChart}
              onClick={() => setShowFlowChart((v) => !v)}
            >{showFlowChart ? '▾' : '▸'}</button>}
            action={showFlowChart ? <div className="chart-controls">{splitSelect}{metricToggle}</div> : undefined}
          >
            {!showFlowChart ? null : flowSeries && flowSeries.points.length ? (
              <>
                <Chart
                  option={flowChartOption}
                  height={400}
                  resetKey={flowResetKey}
                  onZoom={applyChartZoom}
                  loading={chartLoading}
                />
                <ChartLegend
                  series={flowSeries.series ?? []}
                  metric={nav.metric}
                  hidden={hiddenBands}
                  onToggle={toggleBand}
                />
              </>
            ) : chartLoading ? (
              <Chart option={flowChartOption} height={400} resetKey={flowResetKey} loading />
            ) : (
              <Empty title="No traffic in this range" detail="Widen the time range or clear filters." />
            )}
          </Panel>
          <Panel title="Individual flows" subtitle={flows ? `${fmtNumber(flows.total)} flows · ${fmtBytes(selectedTotals.bytes)} · ${fmtNumber(selectedTotals.packets)} pkts` : ''}>
            {nav.flowFilters.exporter && <div className="controls filter-row">Exporter: {nav.flowFilters.exporter} <button type="button" onClick={() => setFlowFilter('exporter', '')}>Clear exporter</button></div>}
            <div className="controls filter-row">
              {interfaceSelect('input_interface', 'Input interface')}
              {interfaceSelect('output_interface', 'Output interface')}
              {vlanSelect('input_vlan', 'Input VLAN', 'Input VLAN')}
              {vlanSelect('output_vlan', 'Output VLAN', 'Output VLAN')}
              <input className="host-input" aria-label="source host" placeholder="src host" value={nav.flowFilters.src_host ?? ''} onChange={(e) => setFlowFilter('src_host', e.target.value)} />
              <input className="host-input" aria-label="destination host" placeholder="dst host" value={nav.flowFilters.dst_host ?? ''} onChange={(e) => setFlowFilter('dst_host', e.target.value)} />
              <select value={nav.flowFilters.protocol ?? ''} onChange={(e) => setFlowFilter('protocol', e.target.value)}>
                <option value="">Protocol</option>
                {nav.flowFilters.protocol && ![1, 2, 6, 17, 47, 58].includes(Number(nav.flowFilters.protocol)) && <option value={nav.flowFilters.protocol}>{protocolName(Number(nav.flowFilters.protocol))}</option>}
                {[1, 2, 6, 17, 47, 58].map((p) => <option key={p} value={p}>{protocolName(p)}</option>)}
              </select>
              <input className="port-input" aria-label="source port" placeholder="src port" inputMode="numeric" value={nav.flowFilters.src_port ?? ''} onChange={(e) => setFlowFilter('src_port', e.target.value)} />
              <input className="port-input" aria-label="destination port" placeholder="dst port" inputMode="numeric" value={nav.flowFilters.dst_port ?? ''} onChange={(e) => setFlowFilter('dst_port', e.target.value)} />
            </div>
            <div className={`table-loading-wrap${flowsLoading ? ' busy' : ''}`} aria-busy={flowsLoading}>
              {flows?.rows.length ? <FlowTable flows={flows.rows} onSelect={setSelectedFlow} sort={flowSorting.sort} onSort={(key, numeric) => { flowSorting.toggle(key, numeric); updateNav({ flowPage: 0 }); }} /> : <Empty title="No individual flows" detail="Widen the range or clear filters. Older periods may only have hourly aggregates." />}
              {flowsLoading && (
                <div className="table-loading" role="status" aria-live="polite">
                  <span className="spinner" aria-hidden="true" />
                  <span>Loading flows</span>
                </div>
              )}
            </div>
            {flows && flows.total > 100 && (
              <div className="pager">
                <button disabled={nav.flowPage === 0} onClick={() => updateNav((prev) => ({ ...prev, flowPage: prev.flowPage - 1 }), 'push')}>Previous</button>
                <span>Page {nav.flowPage + 1} / {Math.ceil(flows.total / 100)} · showing {fmtNumber(flows.rows.length)} of {fmtNumber(flows.total)}</span>
                <button disabled={(nav.flowPage + 1) * 100 >= flows.total} onClick={() => updateNav((prev) => ({ ...prev, flowPage: prev.flowPage + 1 }), 'push')}>Next</button>
              </div>
            )}
          </Panel>
          </>
        )}

        {nav.view === 'system' && (
          system ? (
            <>
              <Panel title="Database" subtitle={system.database.path}>
                <div className="stat-grid">
                  <Stat label="Database size" value={fmtBytes(system.database.size_bytes)} hint="main SQLite file" />
                  <Stat label="WAL size" value={fmtBytes(system.database.wal_bytes)} hint="write-ahead log" />
                  <Stat label="Total on disk" value={fmtBytes(system.database.size_bytes + system.database.wal_bytes)} />
                  <Stat label="Flow rows" value={fmtNumber(system.database.flow_rows)} />
                  <Stat label="Hourly rows" value={fmtNumber(system.database.hourly_aggregate_rows)} />
                  <Stat
                    label="Retention"
                    value={`${fmtDays(system.retention.raw_seconds)} raw / ${fmtDays(system.retention.hourly_seconds)} hourly`}
                    hint={`cleanup every ${fmtDays(system.retention.cleanup_interval_seconds)}`}
                  />
                </div>
              </Panel>

              <Panel
                title="Retention cleanup"
                subtitle={system.sweep.in_progress ? 'sweep running…' : `every ${fmtDays(system.sweep.interval_ms / 1000)}`}
                action={(
                  <button type="button" onClick={runRetentionNow} disabled={runningRetention || system.sweep.in_progress}>
                    {runningRetention ? 'Running…' : system.sweep.in_progress ? 'Sweep in progress' : 'Run cleanup now'}
                  </button>
                )}
              >
                <div className="stat-grid">
                  <Stat
                    label="Last run"
                    value={system.sweep.last_finished_ms ? `${fmtAgo(now, system.sweep.last_finished_ms)}` : 'not since start'}
                    hint={system.sweep.last_finished_ms ? fmtTableTime(system.sweep.last_finished_ms) : 'a sweep runs at startup'}
                  />
                  <Stat
                    label="Duration"
                    value={system.sweep.last_finished_ms ? fmtElapsed(system.sweep.last_duration_ms) : '—'}
                    hint="last completed sweep"
                  />
                  <Stat
                    label="Rows deleted"
                    value={system.sweep.last_finished_ms ? fmtNumber(system.sweep.last_rows_deleted) : '—'}
                    hint={system.sweep.last_finished_ms
                      ? `${fmtNumber(system.sweep.last_raw_deleted)} flows · ${fmtNumber(system.sweep.last_aggregate_deleted)} hourly · ${fmtNumber(system.sweep.last_traffic_deleted)} traffic`
                      : 'flows · hourly · traffic tiers'}
                  />
                  <Stat
                    label="Next run"
                    value={system.sweep.next_run_ms ? fmtIn(now, system.sweep.next_run_ms) : '—'}
                    hint={system.sweep.next_run_ms ? fmtTableTime(system.sweep.next_run_ms) : 'scheduled sweep'}
                  />
                  <Stat
                    label="Runs / failures"
                    value={`${fmtNumber(system.sweep.runs)} / ${fmtNumber(system.sweep.failures)}`}
                    hint="since collector start"
                  />
                  <Stat
                    label="Deleted since start"
                    value={fmtNumber(system.sweep.raw_deleted_total + system.sweep.aggregate_deleted_total + system.sweep.traffic_deleted_total)}
                    hint={`${fmtNumber(system.sweep.raw_deleted_total)} flows · ${fmtNumber(system.sweep.aggregate_deleted_total)} hourly · ${fmtNumber(system.sweep.traffic_deleted_total)} traffic`}
                  />
                </div>
                {system.sweep.last_error && (
                  <p className="panel-note error-note">Last sweep failed: {system.sweep.last_error}</p>
                )}
                <p className="panel-note">
                  Rows are deleted for flows older than the raw retention, hourly aggregates and DNS history older than the hourly retention,
                  and chart tiers older than their own retention. Deleted pages are reused by new data, so the database file does not shrink.
                </p>
              </Panel>

              <Panel
                title="API requests"
                subtitle={`last ${fmtDays(system.api.window_seconds)} · ${fmtNumber(system.api.requests)} in window`}
              >
                <div className="stat-grid">
                  <Stat
                    label="Requests"
                    value={fmtNumber(system.api.requests)}
                    hint={`${fmtNumber(system.api.requests_total)} since start`}
                  />
                  <Stat label="Window" value={fmtDays(system.api.window_seconds)} hint="rolling, in memory" />
                  <Stat
                    label="Response time avg"
                    value={fmtElapsed(system.api.duration_ms.avg)}
                    hint="mean over the window"
                  />
                  <Stat
                    label="Response time median"
                    value={fmtElapsed(system.api.duration_ms.median)}
                    hint="50th percentile"
                  />
                  <Stat
                    label="Response time p90"
                    value={fmtElapsed(system.api.duration_ms.p90)}
                    hint="9 in 10 requests are faster"
                  />
                  <Stat
                    label="Response size avg"
                    value={fmtBytes(system.api.size_bytes.avg)}
                    hint="mean body size"
                  />
                  <Stat
                    label="Response size median"
                    value={fmtBytes(system.api.size_bytes.median)}
                    hint="50th percentile"
                  />
                  <Stat
                    label="Response size p90"
                    value={fmtBytes(system.api.size_bytes.p90)}
                    hint="9 in 10 responses are smaller"
                  />
                </div>
                <p className="panel-note">
                  Measured in this process for requests to <code>/api/</code> only — static assets are excluded, since they would
                  dominate the distribution. Samples are held in memory and cover the last 24 hours or the most recent
                  100,000 requests, whichever is smaller, so the figures restart with the collector.
                </p>
              </Panel>

              <Panel
                title="DNS cache"
                subtitle={system.dns.enabled ? `TTL ${fmtDays(system.dns.ttl_seconds)} · lookup ${system.dns.lookup_timeout_ms}ms` : 'reverse DNS disabled'}
              >
                <div className="stat-grid">
                  <Stat label="Cached entries" value={fmtNumber(system.dns.entries)} hint={`limit ${fmtNumber(system.dns.max_entries)}`} />
                  <Stat label="Cache TTL" value={fmtDays(system.dns.ttl_seconds)} />
                  <Stat label="Lookup timeout" value={`${system.dns.lookup_timeout_ms}ms`} />
                  <Stat label="Server" value={system.dns.server || 'system resolver'} hint={system.dns.server ? 'configured dns.server' : 'from /etc/resolv.conf'} />
                  <Stat label="State" value={system.dns.enabled ? 'enabled' : 'disabled'} />
                  <Stat label="Queue" value={`${fmtNumber(system.dns.queue_depth)} / ${fmtNumber(system.dns.queue_capacity)}`} hint="flow jobs" />
                  <Stat label="Queue drops" value={fmtNumber(system.dns.dropped)} hint="flow jobs not enriched" />
                  <Stat label="Cache hits / misses" value={`${fmtNumber(system.dns.cache_hits)} / ${fmtNumber(system.dns.cache_misses)}`} />
                  <Stat label="Named / unresolved" value={`${fmtNumber(system.dns.named)} / ${fmtNumber(system.dns.unresolved)}`} hint="endpoint lookups" />
                  <Stat label="Rows annotated / skipped" value={`${fmtNumber(system.dns.written)} / ${fmtNumber(system.dns.skipped)}`} hint="writes that landed vs unchanged repeats" />
                  <Stat label="DNS write errors" value={fmtNumber(system.dns.errors)} />
                  <Stat label="Avg / max lookup lag" value={`${system.dns.avg_lag_ms.toFixed(1)} / ${fmtNumber(system.dns.max_lag_ms)} ms`} hint="from flow receipt to lookup completion" />
                </div>
                <div className="system-actions">
                  <button type="button" onClick={flushDNS} disabled={!system.dns.enabled || flushingDNS}>
                    {flushingDNS ? 'Emptying…' : 'Empty DNS cache'}
                  </button>
                </div>
              </Panel>

              <Panel title="Ingest" subtitle={`started ${new Date(system.started_at).toLocaleString()}`}>
                <div className="stat-grid">
                  <Stat label="Status" value={system.ingest.ingest_healthy ? 'healthy' : 'degraded'} />
                  <Stat label="Uptime" value={fmtUptime(system.uptime_seconds)} />
                  <Stat label="Packets received" value={fmtNumber(systemNum('packets_received'))} />
                  <Stat label="Packets decoded" value={fmtNumber(systemNum('packets_decoded'))} />
                  <Stat label="Malformed packets" value={fmtNumber(systemNum('packets_malformed'))} />
                  <Stat label="Unsupported packets" value={fmtNumber(systemNum('packets_unsupported'))} />
                  <Stat label="Flows decoded" value={fmtNumber(systemNum('flows_decoded'))} />
                  <Stat label="Flows stored" value={fmtNumber(systemNum('flows_stored'))} />
                  <Stat label="Flows deduplicated" value={fmtNumber(systemNum('flows_duplicates'))} />
                  <Stat label="Flows dropped" value={fmtNumber(systemNum('flows_dropped'))} />
                  <Stat label="UDP queue drops" value={fmtNumber(systemNum('udp_queue_drops'))} />
                  <Stat label="DB errors" value={fmtNumber(systemNum('db_errors'))} />
                  <Stat label="Templates active" value={fmtNumber(systemNum('templates_active'))} />
                  <Stat label="Raw rows removed" value={fmtNumber(systemNum('retention_raw_deleted'))} />
                  <Stat label="Hourly rows removed" value={fmtNumber(systemNum('retention_aggregate_deleted'))} />
                </div>
              </Panel>
            </>
          ) : <Empty title="Loading system stats" detail="Reading database and collector counters." />
        )}

      </main>
      {selectedFlow && <FlowModal flow={selectedFlow} onClose={() => setSelectedFlow(null)} />}
    </div>
  );
}

function Stat({ label, value, hint }: { label: string; value: string; hint?: string }) {
  return (
    <div className="stat">
      <span>{label}</span>
      <strong>{value}</strong>
      {hint && <small>{hint}</small>}
    </div>
  );
}

function ChartLegend({ series, metric, hidden, onToggle }: {
  series: SplitSeries[];
  metric: ChartMetric;
  hidden: string[];
  onToggle: (key: string) => void;
}) {
  // The store already returns at most ten named bands plus the fold, so this is
  // a defensive cap rather than the real truncation.
  const bands = series.slice(0, MAX_LEGEND_ROWS + 1);
  if (!bands.length) return null;
  const max = Math.max(1, ...bands.map((band) => metricValue(band, metric)));
  return (
    <div className="chart-legend">
      <div className="chart-legend-row head">
        <span aria-hidden="true" />
        <span>Name</span>
        <span className="num">{METRIC_LABELS[metric]}</span>
        <span />
      </div>
      {bands.map((band, index) => {
        const value = metricValue(band, metric);
        const key = bandKey(band);
        const color = bandColor(band, index);
        const off = hidden.includes(key);
        return (
          <button
            key={key}
            type="button"
            className={`chart-legend-row toggle${band.other ? ' other' : ''}${off ? ' off' : ''}`}
            onClick={() => onToggle(key)}
            aria-pressed={!off}
            title={off ? 'Show this band' : 'Hide this band'}
          >
            <span className="swatch" style={{ background: color }} />
            <span className="legend-name" title={band.key || bandLabel(band)}>{bandLabel(band)}</span>
            <span className="num">{fmtMetric(value, metric)}</span>
            <span className="share-track">
              <span
                className="share-fill"
                style={{ width: `${Math.max(1, Math.round((value / max) * 100))}%`, background: color }}
              />
            </span>
          </button>
        );
      })}
    </div>
  );
}

function TimeWindowPicker({ range, rangeKey, customFrom, customTo, onPreset, onApply }: {
  range: Range;
  rangeKey: string;
  customFrom: string;
  customTo: string;
  onPreset: (key: string) => void;
  onApply: (from: string, to: string) => void;
}) {
  const [open, setOpen] = useState(false);
  const [side, setSide] = useState<'from' | 'to'>('from');
  const [from, setFrom] = useState('');
  const [to, setTo] = useState('');
  const [month, setMonth] = useState(() => new Date());
  const [presetDraft, setPresetDraft] = useState(false);
  const root = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!open) return;
    const outside = (e: MouseEvent) => { if (!root.current?.contains(e.target as Node)) setOpen(false); };
    const escape = (e: KeyboardEvent) => { if (e.key === 'Escape') setOpen(false); };
    document.addEventListener('mousedown', outside);
    document.addEventListener('keydown', escape);
    return () => { document.removeEventListener('mousedown', outside); document.removeEventListener('keydown', escape); };
  }, [open]);
  const openPicker = () => {
    if (!open) { setFrom(rangeKey === 'Custom' ? customFrom : fmtDateTimeLocal(range.from_ms)); setTo(rangeKey === 'Custom' ? customTo : ''); setPresetDraft(rangeKey !== 'Custom'); setMonth(new Date(side === 'from' ? range.from_ms : range.to_ms)); }
    setOpen(!open);
  };
  const selectPreset = (preset: typeof RANGES[number]) => {
    const end = Date.now();
    setFrom(fmtDateTimeLocal(end - preset.ms));
    setTo('');
    setPresetDraft(true);
    setMonth(new Date(side === 'from' ? end - preset.ms : end));
    onPreset(preset.label);
  };
  const current = (side === 'from' ? from : to) || fmtDateTimeLocal(Date.now());
  const setCurrent = (value: string) => { setPresetDraft(false); (side === 'from' ? setFrom : setTo)(value); };
  const toMS = to ? new Date(to).getTime() : Date.now();
  const fromMS = from ? new Date(from).getTime() : Math.min(range.from_ms, toMS - 1);
  const valid = Number.isFinite(fromMS) && Number.isFinite(toMS) && fromMS < toMS && toMS <= Date.now() && (!from || toMS - fromMS <= 367 * 86400000);
  const display = (ms: number) => fmtDateTimeLocal(ms).replace('T', ' ');
  const today = fmtDateTimeLocal(Date.now()).slice(0, 10);
  const year = month.getFullYear(), monthIndex = month.getMonth();
  const firstDay = (new Date(year, monthIndex, 1).getDay() + 6) % 7;
  const days = new Date(year, monthIndex + 1, 0).getDate();
  const pad = (value: number) => String(value).padStart(2, '0');
  const switchSide = (next: 'from' | 'to') => {
    setSide(next);
    const date = new Date((next === 'from' ? from : to) || Date.now());
    if (Number.isFinite(date.getTime())) setMonth(date);
  };
  const moveMonth = (delta: number) => setMonth(new Date(year, monthIndex + delta, 1));
  return <div className="time-picker" ref={root}>
    <button type="button" className="time-picker-trigger" aria-label="Time range" aria-expanded={open} onClick={openPicker}>
      {rangeKey !== 'Custom' ? <span className="time-trailing">Last {rangeKey}</span> : `${customFrom ? display(new Date(customFrom).getTime()) : ''} – ${customTo ? display(new Date(customTo).getTime()) : ''}`} <span aria-hidden="true">▾</span>
    </button>
    {open && <div className="time-picker-popover">
      <div className="time-presets"><strong>Presets</strong>{RANGES.map((preset) => <button type="button" key={preset.label} className={rangeKey === preset.label ? 'active' : ''} onClick={() => selectPreset(preset)}>{preset.label}</button>)}</div>
      <div className="time-editor">
        <div className="segmented time-side"><button type="button" className={side === 'from' ? 'active' : ''} onClick={() => switchSide('from')}>From</button><button type="button" className={side === 'to' ? 'active' : ''} onClick={() => switchSide('to')}>To</button></div>
        <div className="calendar-head"><button type="button" aria-label="Previous month" onClick={() => moveMonth(-1)}>‹</button><strong>{month.toLocaleString(undefined, { month: 'long', year: 'numeric' })}</strong><button type="button" aria-label="Next month" disabled={year * 12 + monthIndex >= new Date().getFullYear() * 12 + new Date().getMonth()} onClick={() => moveMonth(1)}>›</button></div>
        <div className="calendar-grid">
          {Array.from({ length: firstDay }, (_, i) => <span key={`empty-${i}`} />)}
          {Array.from({ length: days }, (_, i) => {
            const date = `${year}-${pad(monthIndex + 1)}-${pad(i + 1)}`;
            return <button type="button" key={date} disabled={date > today} aria-label={date} aria-pressed={current.slice(0, 10) === date} className={`${date === today ? 'today ' : ''}${current.slice(0, 10) === date ? 'selected' : ''}`} onClick={() => setCurrent(`${date}T${current.slice(11, 16) || '00:00'}`)}>{i + 1}</button>;
          })}
        </div>
        <div className="time-selects"><label>Hour <select aria-label="Hour" value={current.slice(11, 13)} onChange={(e) => setCurrent(`${current.slice(0, 11)}${e.target.value}:${current.slice(14, 16)}`)}>{Array.from({ length: 24 }, (_, i) => <option key={i}>{pad(i)}</option>)}</select></label><span>:</span><label>Minute <select aria-label="Minute" value={current.slice(14, 16)} onChange={(e) => setCurrent(`${current.slice(0, 14)}${e.target.value}`)}>{Array.from({ length: 60 }, (_, i) => <option key={i}>{pad(i)}</option>)}</select></label></div>
      </div>
      <div className="time-footer">
        <label className="time-bound">From <input type="datetime-local" max={fmtDateTimeLocal(Date.now())} value={from} onFocus={() => switchSide('from')} onChange={(e) => { setPresetDraft(false); setFrom(e.target.value); const date = new Date(e.target.value); if (Number.isFinite(date.getTime())) setMonth(date); }} /><button type="button" aria-label="Reset from" onClick={() => { setPresetDraft(false); setFrom(''); }}>Reset</button></label>
        <label className="time-bound">To <input type="datetime-local" max={fmtDateTimeLocal(Date.now())} value={to} onFocus={() => switchSide('to')} onChange={(e) => { setPresetDraft(false); setTo(e.target.value); const date = new Date(e.target.value); if (Number.isFinite(date.getTime())) setMonth(date); }} /><button type="button" aria-label="Reset to" onClick={() => { setPresetDraft(false); setTo(''); }}>Reset</button></label>
        {!valid && <span className="time-error">Choose a valid range ending no later than now, of at most 367 days.</span>}
        <button type="button" className="time-apply" disabled={!valid} onClick={() => { if (!presetDraft) onApply(from, to); setOpen(false); }}>Apply</button>
      </div>
    </div>}
  </div>;
}

function Panel({ title, subtitle, action, leading, children, wide, compact, titleToggle }: { title: string; subtitle?: string; action?: ReactNode; leading?: ReactNode; children: ReactNode; wide?: boolean; compact?: boolean; titleToggle?: { expanded: boolean; onToggle: () => void } }) {
  return (
    <section className={`panel${wide ? ' wide' : ''}${compact ? ' compact' : ''}`}>
      <div className="panel-head">
        <div className="panel-head-left">{leading}
          {titleToggle ? (
            // The heading toggles the same state as the chevron beside it, so the
            // obvious thing to click does what the reader expects. It is a real
            // button for keyboard and screen-reader use, and the chevron remains
            // as the visual affordance.
            <button
              className="panel-title-toggle"
              type="button"
              aria-expanded={titleToggle.expanded}
              aria-label={titleToggle.expanded ? `Collapse ${title}` : `Expand ${title}`}
              title={titleToggle.expanded ? 'Collapse' : 'Expand'}
              onClick={titleToggle.onToggle}
            ><h2>{title}</h2></button>
          ) : <h2>{title}</h2>}
        </div>
        <div className="panel-head-right">
          {subtitle && <span>{subtitle}</span>}
          {action}
        </div>
      </div>
      {children}
    </section>
  );
}

type TopSortKey = 'key' | 'label' | 'bytes' | 'packets' | 'flows' | 'share';

function Table({ top, onDrill, sort, onSort: toggle }: { top: Top; onDrill: (row: TopRow) => void; sort: SortState<TopSortKey>; onSort: (key: TopSortKey, numeric?: boolean) => void }) {
  const rows = top.rows;
  const maxBytes = Math.max(1, ...rows.map((r) => r.bytes));
  return (
    <div className="table-wrap">
      <table>
        <thead><tr>
          <SortableTh label="Key" sortKey="key" sort={sort} onSort={toggle} />
          <SortableTh label="Label" sortKey="label" sort={sort} onSort={toggle} />
          <SortableTh label="Bytes" sortKey="bytes" sort={sort} onSort={toggle} numeric />
          <SortableTh label="Packets" sortKey="packets" sort={sort} onSort={toggle} numeric />
          <SortableTh label="Flows" sortKey="flows" sort={sort} onSort={toggle} numeric />
          <SortableTh label="Share" sortKey="share" sort={sort} onSort={toggle} numeric bare />
        </tr></thead>
        <tbody>
          {rows.map((r) => {
            const pct = (r.bytes / maxBytes) * 100;
            return (
              <tr key={r.key} onClick={() => onDrill(r)} tabIndex={0} onKeyDown={(e) => { if (e.key === 'Enter') onDrill(r); }}>
                <td><code>{r.key || 'unknown'}</code>{r.unknown && <span className="badge">unknown</span>}</td>
                <td>{r.label || '-'}</td>
                <td className="num">{fmtBytes(r.bytes)}</td>
                <td className="num">{fmtNumber(r.packets)}</td>
                <td className="num">{fmtNumber(r.flows)}</td>
                <td>
                  <div className="share-cell">
                    <span className="share-track"><span className="share-fill" style={{ width: `${pct}%` }} /></span>
                    <span className="share-value">{Math.round(pct)}%</span>
                  </div>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function Detail({ label, value, mono }: { label: string; value: ReactNode; mono?: boolean }) {
  return (
    <div className="detail">
      <span>{label}</span>
      <strong className={mono ? 'mono' : ''}>{value}</strong>
    </div>
  );
}

function Endpoint({
  title, ip, name, port, vlan, vlanSource, asn, tos,
}: {
  title: string; ip: string; name?: string; port?: number; vlan: number; vlanSource: string; asn?: number; tos?: number;
}) {
  return (
    <div className="endpoint-card">
      <h4>{title}</h4>
      <Detail label="Resolved hostname" value={name || '-'} />
      <Detail label="Raw IP" value={ip || '-'} mono />
      <Detail label="Port" value={port ?? '-'} />
      <Detail label="VLAN" value={`${fmtVLAN(vlan)} · ${vlanSource || 'unknown'}`} />
      <Detail label="AS" value={asn ?? '-'} />
      <Detail label="TOS" value={`0x${(tos ?? 0).toString(16).padStart(2, '0')}`} mono />
    </div>
  );
}

function FlowModal({ flow, onClose }: { flow: Flow; onClose: () => void }) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  return (
    <div className="modal-backdrop" onMouseDown={(e) => { if (e.target === e.currentTarget) onClose(); }}>
      <div className="modal" role="dialog" aria-modal="true" aria-label="Flow details">
        <header className="modal-header">
          <div><h2>Flow details</h2><p>Record #{flow.id}</p></div>
          <button type="button" onClick={onClose} aria-label="Close flow details">×</button>
        </header>
        <div className="modal-body">
          <section className="modal-section">
            <h3>Timing</h3>
            <div className="detail-grid">
              <Detail label="End" value={`${fmtUTC(flow.end_ms)} (local ${fmtTime(flow.end_ms)})`} mono />
              <Detail label="Start" value={fmtUTC(flow.start_ms)} mono />
              <Detail label="Received" value={fmtUTC(flow.received_at_ms)} mono />
              <Detail label="Duration" value={fmtDuration(flow.end_ms - (flow.start_ms || flow.end_ms))} />
              <Detail label="Time source" value={flow.time_source || '-'} />
            </div>
          </section>

          <section className="modal-section">
            <h3>Exporter</h3>
            <div className="detail-grid">
              <Detail label="Name" value={flow.exporter_label || flow.exporter || '-'} />
              <Detail label="Address" value={flow.exporter_ip || flow.exporter || '-'} mono />
              <Detail label="Exporter ID" value={flow.exporter_id ?? '-'} />
              <Detail label="NetFlow version" value={flow.version || '-'} />
              <Detail label="Engine" value={flow.engine_type !== undefined || flow.engine_id !== undefined ? `${flow.engine_type ?? 0} / ${flow.engine_id ?? 0}` : '-'} />
              <Detail label="Sequence" value={flow.sequence ?? '-'} />
            </div>
          </section>

          <section className="modal-section">
            <h3>Path</h3>
            <div className="detail-grid">
              <Detail label="Input interface" value={`${fmtInterface(flow.input_if, flow.input_label)} (${flow.input_if || 0})`} />
              <Detail label="Output interface" value={`${fmtInterface(flow.output_if, flow.output_label)} (${flow.output_if || 0})`} />
              <Detail label="Direction" value={flow.direction === undefined ? '-' : `${flow.direction} (${flow.direction_known ? 'known' : 'unknown'})`} />
            </div>
          </section>

          <section className="modal-section">
            <h3>Endpoints</h3>
            <div className="endpoint-grid">
              <Endpoint title="Source" ip={flow.src_addr} name={flow.src_name} port={flow.src_port} vlan={flow.src_vlan} vlanSource={flow.src_vlan_source} asn={flow.src_as} tos={flow.src_tos} />
              <Endpoint title="Destination" ip={flow.dst_addr} name={flow.dst_name} port={flow.dst_port} vlan={flow.dst_vlan} vlanSource={flow.dst_vlan_source} asn={flow.dst_as} tos={flow.dst_tos} />
            </div>
          </section>

          <section className="modal-section">
            <h3>Traffic</h3>
            <div className="detail-grid">
              <Detail label="Protocol" value={`${protocolName(flow.protocol)} (${flow.protocol})`} />
              <Detail label="Bytes" value={`${fmtBytes(flow.bytes)} (${fmtNumber(flow.bytes)})`} />
              <Detail label="Packets" value={fmtNumber(flow.packets)} />
              <Detail label="TCP flags" value={`0x${(flow.tcp_flags ?? 0).toString(16).padStart(2, '0')}`} mono />
            </div>
          </section>
        </div>
      </div>
    </div>
  );
}

type FlowSortKey = 'time' | 'interface' | 'vlan' | 'source' | 'destination' | 'proto' | 'bytes' | 'packets';

function FlowTable({ flows, onSelect, sort, onSort: toggle }: { flows: Flow[]; onSelect: (flow: Flow) => void; sort: SortState<FlowSortKey>; onSort: (key: FlowSortKey, numeric?: boolean) => void }) {
  const rows = flows;
  return (
    <div className="table-wrap">
      <table>
        <thead><tr>
          <SortableTh label="Time" sortKey="time" sort={sort} onSort={toggle} />
          <SortableTh label="Interface" sortKey="interface" sort={sort} onSort={toggle} title="Sort by input / output interface numbers" />
          <SortableTh label="VLAN" sortKey="vlan" sort={sort} onSort={toggle} title="Sort by source / destination VLAN numbers" />
          <SortableTh label="Source" sortKey="source" sort={sort} onSort={toggle} title="Sort by source IP, not DNS name" />
          <SortableTh label="Destination" sortKey="destination" sort={sort} onSort={toggle} title="Sort by destination IP, not DNS name" />
          <SortableTh label="Proto" sortKey="proto" sort={sort} onSort={toggle} title="Sort by protocol number" />
          <SortableTh label="Bytes" sortKey="bytes" sort={sort} onSort={toggle} numeric />
          <SortableTh label="Pkts" sortKey="packets" sort={sort} onSort={toggle} numeric />
        </tr></thead>
        <tbody>
          {rows.map((f) => (
            <tr
              key={f.id}
              className="flow-row"
              tabIndex={0}
              title="Open full flow details"
              onClick={() => onSelect(f)}
              onKeyDown={(e) => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); onSelect(f); } }}
            >
              <td title={`${f.time_source || 'flow'} time · ${fmtDuration(f.end_ms - (f.start_ms || f.end_ms))} duration`}>{fmtTableTime(f.end_ms)}</td>
              <td title="input / output interface">{fmtInterface(f.input_if, f.input_label)} / {fmtInterface(f.output_if, f.output_label)}</td>
              <td title="source / destination VLAN">{fmtVLAN(f.src_vlan)} / {fmtVLAN(f.dst_vlan)}</td>
              <td title={f.src_addr || '-'}><span>{f.src_name || f.src_addr || '-'}</span>{f.src_port ? <span className="badge">:{f.src_port}</span> : null}</td>
              <td title={f.dst_addr || '-'}><span>{f.dst_name || f.dst_addr || '-'}</span>{f.dst_port ? <span className="badge">:{f.dst_port}</span> : null}</td>
              <td>{protocolName(f.protocol)}</td>
              <td className="num">{fmtBytes(f.bytes)}</td>
              <td className="num">{fmtNumber(f.packets)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
