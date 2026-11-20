# Dashboard notices and warnings

The dashboard can display the following messages depending on collector health, selected time range and filters. A limitation describes the data returned; it does not necessarily mean the collector is currently unhealthy.

| Notice or state | Meaning and typical action |
| --- | --- |
| **API error: … Retrying automatically** | A request failed, including connection, database or validation errors. Check the detail and service logs. Automatic polling retries while updates are not paused; a deliberate change also triggers a request. |
| **Collector health warning / last error** | The health response may include the collector's last error, a database-statistics error, or **UDP packets were dropped because the decode queue was full**. Inspect System counters and logs; reduce load or adjust capacity/queue sizing. |
| **Individual flows … outside raw retention** / **hourly aggregates only** | Flow detail and raw-only host/port filtering cannot recover records outside raw retention. Select a more recent window; aggregated rankings and charts may still be available. A mixed range can retain detail only for its newer portion. |
| **Flow details are unavailable before … UTC** | The beginning of the selected window is older than the raw detail available; earlier results use available aggregates where supported. |
| **No hourly aggregates cover the requested period** | Neither raw flow records nor hourly aggregates are available to answer the ranking. |
| **No individual flows are retained; only hourly aggregates are available** / **No retained individual flows cover this period** | Raw records are absent for the relevant window. Raw-only filtered charts or flow detail cannot include that history. |
| **No data is available before … UTC** | Part of the selected chart window predates available chart data or has aged out. Narrow the window or adjust future retention. |
| **Stacked bands before … UTC are hourly** | Older stacked bands use hourly dimension aggregates; recent portions can use raw flow detail. This is a resolution notice, not necessarily missing traffic. |
| **Filtered chart / stacked chart stopped early … time budget** | The chart is incomplete because its bounded query timed out. Narrow the range or remove filters. |
| **No individual flows or hourly aggregates cover this period** / **No traffic was recorded in this period** | No applicable data was found; this can mean no matching traffic, a gap in collection, or missing retained data. Check coverage, filters, and exporter configuration. |
| **No matching hosts**, **No traffic in this range**, **No data in this range**, **No individual flows** | Empty result states. Try a different search, clear filters, widen or move the range, and check exporter and retention settings. |
| **Loading results / Loading system stats** and chart/request indicators | A request is in progress; these are not errors. |
| **Choose a valid range … at most 367 days** | The custom range is invalid, ends in the future, or exceeds the API range cap. Correct the From/To values. |

Notices from API responses are deduplicated before display. Use the close button to dismiss a notice until the page is reloaded; a different notice can still appear. Exact timestamps and error details vary. Reverse DNS is optional and can return no name for a host; an empty label alone is not a warning. Older flows and dropped DNS jobs have no captured name.
