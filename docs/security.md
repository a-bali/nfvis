# Security

## Threat model

nfvis is a homelab observability tool, not a hardened multi-tenant service.

- **UDP is unauthenticated.** Any host that can send packets to the collector port can inject flows, spoof exporter addresses, fill the disk, or distort dashboards.
- **HTTP has no built-in authentication, authorization, or TLS.** Anyone who can reach the HTTP port can read traffic metadata and configuration.
- Flow records contain IP addresses, ports, protocols, VLANs, timings, and exporter/interface labels. Treat the database and API as sensitive.

## Defaults

- UDP binds `0.0.0.0:2055` because NetFlow usually arrives from network devices on other hosts.
- HTTP binds `127.0.0.1:8080`, so only local users can query it by default.
- No credentials are created. There is no login endpoint.
- The dashboard sends `X-Content-Type-Options: nosniff` for JSON and uses `no-cache` for embedded assets.
- Backups are written with mode `0600`.

## Network guidance

1. Keep `listen.http` on loopback. If remote dashboard access is required, put an authenticated TLS reverse proxy (Caddy, nginx, Tailscale, etc.) in front and bind nfvis to the proxy's loopback/private address.
2. Restrict UDP access with a firewall or ACL to known exporter IPs. Example (Linux nftables):

   ```bash
   nft add table inet nfvis
   nft add chain inet nfvis input '{ type filter hook input priority 0; }'
   nft add rule inet nfvis input udp dport 2055 ip saddr 192.0.2.1 accept
   nft add rule inet nfvis input udp dport 2055 drop
   ```

3. Prefer a management VLAN or VPN for exporter traffic. Do not expose UDP 2055 or HTTP 8080 to the public internet.
4. If using a reverse proxy, terminate TLS there and enforce authentication there. Do not rely on obscurity of the port.
5. Run nfvis as a dedicated unprivileged user with write access only to its data directory. Ports above 1024 need no special privileges.
6. Monitor `udp_queue_drops`, `flows_dropped`, disk usage and unexpected exporter addresses in `/api/v1/exporters`.

## Data handling

- VLAN rules and exporter labels are configuration data and appear in API responses. They are not secrets, but they may reveal internal topology.
- `/api/v1/config` deliberately returns no credentials because there are none. Do not add secrets to the configuration file.
- Retention deletes raw flows and aggregates from SQLite; it does not securely erase pages. Use filesystem/disk encryption if deletion assurance matters.
- Backups contain traffic metadata and inherit the sensitivity of the database.

## Known limitations

- No rate limiting on API requests or UDP ingestion beyond queue bounds.
- No validation of NetFlow payload authenticity; malformed packets are counted and skipped, but valid-looking forged packets are accepted.
- No per-user access control; the dashboard and API share the same read privileges.
- NetFlow v9 options with field lengths not divisible by four may be skipped by the underlying decoder; sampling metadata will then be absent rather than fabricated.
