export type Range = { from_ms: number; to_ms: number };

export type Health = {
  version: string;
  status: 'ok' | 'degraded';
  started_at: string;
  uptime_seconds: number;
  http_addr: string;
  udp_addr: string;
  counters: Record<string, number | string | boolean>;
  database: { flow_rows: number; hourly_aggregate_rows: number; size_bytes: number };
  coverage: Coverage;
  warnings: string[];
};

export type SystemStats = {
  started_at: string;
  uptime_seconds: number;
  database: {
    path: string;
    schema_version: number;
    flow_rows: number;
    hourly_aggregate_rows: number;
    size_bytes: number;
    wal_bytes: number;
  };
  dns: {
    enabled: boolean;
    entries: number;
    max_entries: number;
    ttl_seconds: number;
    lookup_timeout_ms: number;
    server: string;
    queue_depth: number; queue_capacity: number; dropped: number; completed: number;
    cache_hits: number; cache_misses: number; named: number; unresolved: number; errors: number;
    written: number; skipped: number;
    avg_lag_ms: number; max_lag_ms: number;
  };
  ingest: Record<string, number | string | boolean>;
  retention: {
    raw_seconds: number;
    hourly_seconds: number;
    cleanup_interval_seconds: number;
  };
  sweep: {
    in_progress: boolean;
    last_started_ms: number;
    last_finished_ms: number;
    last_duration_ms: number;
    last_rows_deleted: number;
    last_raw_deleted: number;
    last_aggregate_deleted: number;
    last_traffic_deleted: number;
    last_error?: string;
    runs: number;
    failures: number;
    raw_deleted_total: number;
    aggregate_deleted_total: number;
    traffic_deleted_total: number;
    interval_ms: number;
    next_run_ms: number;
  };
  api: {
    window_seconds: number;
    requests: number;
    requests_total: number;
    duration_ms: { avg: number; median: number; p90: number; max: number };
    size_bytes: { avg: number; median: number; p90: number; max: number };
  };
};

export type RetentionRunResult = { sweep: SystemStats['sweep'] };

export type DNSFlushResult = {
  flushed: number;
  dns: SystemStats['dns'];
};

export type Coverage = {
  raw_available: boolean;
  aggregate_available: boolean;
  raw_oldest_ms: number;
  raw_newest_ms: number;
  aggregate_oldest_ms: number;
  aggregate_newest_ms: number;
  resolution: 'empty' | 'raw' | 'aggregate' | 'mixed';
};

export type Point = {
  bucket_ms: number;
  duration_ms: number;
  bytes: number;
  packets: number;
  flows: number;
  unknown_flows: number;
};

export type Timeseries = Range & {
  resolution: string;
  raw_available_from_ms: number;
  bucket_ms: number;
  aggregate_bucket_ms?: number;
  limitations: string[];
  points: Point[];
  split_by?: string;
  metric?: ChartMetric;
  series?: SplitSeries[];
};

// ChartMetric selects which quantity the traffic chart draws.
export type ChartMetric = 'bytes' | 'packets' | 'flows';

// A stacked band. Points are [bucket_ms, bytes, packets, flows] tuples to keep
// wide stacked responses small.
export type SplitSeries = {
  key: string;
  label?: string;
  other?: boolean;
  bytes: number;
  packets: number;
  flows: number;
  points: [number, number, number, number][];
};

export type TopRow = {
  key: string;
  label?: string;
  unknown?: boolean;
  bytes: number;
  packets: number;
  flows: number;
};

export type Top = Range & {
  resolution: string;
  limitations: string[];
  dimension: string;
  direction: string;
  total: number;
  rows: TopRow[];
};

export type Flow = {
  id: number;
  received_at_ms: number;
  start_ms: number;
  end_ms: number;
  time_source: string;
  version: number;
  exporter: string;
  exporter_label?: string;
  exporter_ip: string;
  exporter_id?: number;
  sequence?: number;
  engine_type?: number;
  engine_id?: number;
  input_if?: number;
  input_label?: string;
  output_if?: number;
  output_label?: string;
  direction?: number;
  direction_known?: boolean;
  src_addr: string;
  src_name?: string;
  dst_addr: string;
  dst_name?: string;
  src_port?: number;
  dst_port?: number;
  protocol: number;
  tcp_flags?: number;
  src_tos?: number;
  dst_tos?: number;
  bytes: number;
  packets: number;
  src_as?: number;
  dst_as?: number;
  src_vlan: number;
  dst_vlan: number;
  src_vlan_source: string;
  dst_vlan_source: string;
};

export type FlowPage = Range & {
  detail_available: boolean;
  limitation?: string;
  total: number;
  limit: number;
  offset: number;
  rows: Flow[];
};

export type Exporter = {
  address: string;
  label?: string;
  interfaces: Record<string, string>;
  observed: boolean;
};

export class APIError extends Error {
  code: string;
  status: number;
  body: unknown;

  constructor(status: number, code: string, message: string, body: unknown) {
    super(message);
    this.status = status;
    this.code = code;
    this.body = body;
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(path, { headers: { Accept: 'application/json' }, ...init });
  let body: unknown;
  try {
    body = await response.json();
  } catch {
    body = {};
  }
  if (!response.ok) {
    const err = body as { error?: { code?: string; message?: string } };
    throw new APIError(
      response.status,
      err?.error?.code ?? 'request_failed',
      err?.error?.message ?? `Request failed (${response.status})`,
      body,
    );
  }
  return body as T;
}

function get<T>(path: string, signal?: AbortSignal): Promise<T> {
  return request<T>(path, { signal });
}

function post<T>(path: string): Promise<T> {
  return request<T>(path, { method: 'POST' });
}

function params(values: Record<string, string | number | undefined>): string {
  const q = new URLSearchParams();
  for (const [key, value] of Object.entries(values)) {
    if (value !== undefined && value !== '') q.set(key, String(value));
  }
  return q.size ? `?${q.toString()}` : '';
}

export const api = {
  health: (signal?: AbortSignal) => get<Health>('/api/v1/health', signal),
  system: (signal?: AbortSignal) => get<SystemStats>('/api/v1/system', signal),
  flushDNSCache: () => post<DNSFlushResult>('/api/v1/system/dns/cache'),
  runRetention: () => post<RetentionRunResult>('/api/v1/system/retention'),
  exporters: (signal?: AbortSignal) =>
    get<{ exporters: Exporter[] }>('/api/v1/exporters', signal),
  coverage: (signal?: AbortSignal) => get<Coverage>('/api/v1/coverage', signal),
  timeseries: (
    range: Range,
    filters: Record<string, string>,
    options: { bucketSeconds?: number; splitBy?: string; metric?: ChartMetric } = {},
    signal?: AbortSignal,
  ) =>
    get<Timeseries>(
      `/api/v1/timeseries${params({
        from: new Date(range.from_ms).toISOString(),
        to: new Date(range.to_ms).toISOString(),
        bucket_seconds: options.bucketSeconds || undefined,
        split_by: options.splitBy || undefined,
        metric: options.metric,
        ...filters,
      })}`,
      signal,
    ),
  top: (
    range: Range,
    dimension: string,
    direction: string,
    exporter: string,
    limit: number,
    offset: number,
    signal?: AbortSignal,
    sort?: string, order?: string, search?: string,
  ) =>
    get<Top>(
      `/api/v1/top${params({
        from: new Date(range.from_ms).toISOString(),
        to: new Date(range.to_ms).toISOString(),
        dimension,
        direction: direction || undefined,
        exporter: exporter || undefined,
        sort, order, search: search || undefined,
        limit,
        offset,
      })}`,
      signal,
    ),
  flows: (
    range: Range,
    filters: Record<string, string>,
    limit: number,
    offset: number,
    signal?: AbortSignal,
    sort?: string, order?: string,
  ) =>
    get<FlowPage>(
      `/api/v1/flows${params({
        from: new Date(range.from_ms).toISOString(),
        to: new Date(range.to_ms).toISOString(),
        ...filters,
        sort, order,
        limit,
        offset,
      })}`,
      signal,
    ),
};
