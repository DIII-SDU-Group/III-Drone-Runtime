# Runtime API Configuration

`iii-runtime-api` is configured with environment variables. Use
`config/iii-runtime-api.env.example` as the template and keep real secret files
out of git.

Required for real deployments:
- `III_RUNTIME_API_BROWSER_PASSWORD`
- `III_RUNTIME_API_CREDENTIALS_PATH`

Set `III_RUNTIME_API_REQUIRE_SECRETS=1` or `III_RUNTIME_API_PROFILE=real` to
fail startup when either input is missing. Sim/dev profiles may use explicit
local defaults, but production should never rely on them.

Network and discovery:
- `III_RUNTIME_API_HOST`
- `III_RUNTIME_API_PORT`
- `III_RUNTIME_API_MDNS_ENABLED`
- `III_RUNTIME_API_MDNS_INSTANCE`
- `III_RUNTIME_API_MDNS_HOST` or `III_RUNTIME_API_ADVERTISE_HOST`
- `III_RUNTIME_API_SYSTEM_ID`
- `III_RUNTIME_API_NAME`
- `III_RUNTIME_API_PROFILE`

When enabled, `iii-runtime-api` advertises `_iii-runtime-api._tcp.local` with
the runtime ID/name, API compatibility version, profile when set, advertised
host/port, and system/drone identifier. Keep operational state behind browser or
CLI authentication; `/identity` and `/health` are the unauthenticated discovery
surface.

Runtime behavior:
- `III_RUNTIME_API_HEARTBEAT_INTERVAL_SEC`
- `III_RUNTIME_API_SESSION_LEASE_TIMEOUT_SEC`
- `III_RUNTIME_API_PX4_MAVLINK_ENDPOINT`
- `III_RUNTIME_API_PX4_ENABLED`
- `III_RUNTIME_API_LOG_DIR`
- `III_RUNTIME_SESSION_LOG_ROOT`
- `III_RUNTIME_SESSION_DEBUG`
- `III_RECEIVER_CLOCK_STATE_PATH`
- `III_CLOCK_FLUSH_COMMIT_PATH` (production host unit only)

`III_RUNTIME_SESSION_LOG_ROOT` enables durable boot/session event logs. Real and
opti-track profiles default it to `/var/log/iii`; simulation leaves it disabled
unless explicitly configured. Before the receiver clock gate becomes
`OPERATIONAL`, events use only boot identity and monotonic ordering in a bounded
10,000-record/16-MiB memory ring. The first trusted clock mapping flushes that
ring once with reconstructed UTC bounds and explicit uncertainty. In production,
the Ansible-owned unit sets `III_CLOCK_FLUSH_COMMIT_PATH`; the API writes a
content-bound, durable `FLUSHING_CLOCK` commit before the receiver may enter
`OPERATIONAL` or boot the ROS graph. `CLOCK_FAULT_ACTIVE` starts a new in-memory
uncertain ring and blocks new mutations without interrupting existing monotonic-
time control. Debug logging
is disabled by default, must be enabled for a new session with
`III_RUNTIME_SESSION_DEBUG=1`, and is capped at 256 MiB for that session.

The root-owned `iii-log-maintenance.timer` applies the shared 14-day,
lesser-of-1-GiB-or-five-percent policy while preserving the deployment storage
reserve, current session, and four newest completed sessions. Rosbags, datasets,
tuning state, configuration checkpoints, and deployment evidence are governed by
their own retention domains.

## Real Profile

In the real profile, `iii-runtime-api` runs on the onboard runtime host beside
the III daemon, ROS graph, DDS participants, MAVSDK/PX4 transport, logs, and
configuration services. The ground-control computer reaches it over the
operator network through the GC proxy; the ground-control computer does not
need ROS, DDS, MAVSDK, or runtime package access.

Recommended real-profile environment:

- `III_SYSTEM_PROFILE=real`
- `III_RUNTIME_API_PROFILE=real`
- `III_RUNTIME_API_REQUIRE_SECRETS=1`
- `III_RUNTIME_API_HOST=0.0.0.0`
- `III_RUNTIME_API_PORT=8765`
- `III_RUNTIME_API_MDNS_ENABLED=1`
- `III_RUNTIME_API_MDNS_INSTANCE=<operator-visible runtime name>`
- `III_RUNTIME_API_MDNS_HOST=<runtime host address or DNS name>` when automatic
  address selection is not correct.
- `III_RUNTIME_API_ID=iii-aircraft-runtime`
- `III_RUNTIME_API_SYSTEM_ID=iii-aircraft`
- `III_RUNTIME_API_BROWSER_PASSWORD=<unique operator login password of at least 16 characters>`
- `III_RUNTIME_API_CREDENTIALS_PATH=/var/lib/iii/deployment/runtime-api-client-verifiers.json`
- `III_RUNTIME_API_HEARTBEAT_INTERVAL_SEC=2`
- `III_RUNTIME_API_SESSION_LEASE_TIMEOUT_SEC=8`
- `III_RUNTIME_API_PX4_MAVLINK_ENDPOINT=<MAVLink endpoint>`
- `III_RUNTIME_API_PX4_ENABLED=1`
- `III_RUNTIME_API_LOG_DIR=<runtime API log directory>`
- `III_RUNTIME_SESSION_LOG_ROOT=/var/log/iii`
- `III_RUNTIME_SESSION_DEBUG=0`
- `III_RECEIVER_CLOCK_STATE_PATH=/var/lib/iii/deployment/clock-state.json`
- `III_CLOCK_FLUSH_COMMIT_PATH=/run/iii/clock-flush/runtime-api.json`

Network ports on the runtime host:

- TCP `8765`: runtime API HTTP and WebSocket traffic from the GC proxy and
  remote CLI clients.
- UDP `5353`: mDNS/zeroconf discovery when enabled and supported by the
  operator network.
- TCP `22`: optional SSH administration/deploy/file-transfer. SSH is not the
  GUI v2 command transport.

ROS, DDS, MAVLink/MAVSDK, the III daemon Unix socket, and systemd control stay
local to the runtime host.

See `../../III-Drone-GC/docs/gui-v2-deployment.md` for the full two-host GUI v2
deployment model and ground-control environment variables.

## Transport Security Position

The first GUI v2 deployment uses a trusted isolated operator network. Runtime
API traffic is HTTP/WebSocket on TCP `8765`; TLS is deferred until certificate
provisioning and endpoint identity checks are implemented.

Real-profile requirements:

- Set `III_RUNTIME_API_REQUIRE_SECRETS=1`.
- Use a non-default `III_RUNTIME_API_BROWSER_PASSWORD` and receiver-derived,
  per-machine Runtime token verifiers. A shared onboard
  `III_RUNTIME_API_CLI_TOKEN` is rejected in real and opti-track profiles.
- Restrict TCP `8765` and UDP `5353` to the operator network.
- Do not expose `iii-runtime-api` to public or shared networks.
- Treat TLS deferral as an accepted deployment risk until HTTPS/WSS support is
  added.

For production, do not copy the example file into a workspace. Aircraft Ansible
owns `/etc/iii/runtime.env`, `/etc/iii/secrets/runtime-api.env`, the nftables
operator-LAN policy, and the fixed `iii-runtime-api.service`. The non-secret file
is root-owned and group-readable by `iii`; the external secret file is supplied
as an owner-controlled provisioning input and never enters a release bundle.
Application activation cannot replace or enable the host unit.

The `real` and `opti_track` profiles also fail startup when `III_RUNTIME_API_ID` or
`III_RUNTIME_API_SYSTEM_ID` still uses a generic development identity, or when
the browser credential uses a documented development/placeholder value. The
shared hardware-role identity is `iii-aircraft` / `iii-aircraft-runtime`; it is
stable across release switches, reboot, and replacement Raspberry Pis. Runtime
CLI authentication hashes the presented per-computer token and compares it to
the receiver-derived active verifier set and authoritative receiver access state
on every request. A stale projection therefore cannot preserve revoked authority,
and enrollment or revocation does not require an API restart.
