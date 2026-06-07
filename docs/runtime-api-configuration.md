# Runtime API Configuration

`iii-runtime-api` is configured with environment variables. Use
`config/iii-runtime-api.env.example` as the template and keep real secret files
out of git.

Required for real deployments:
- `III_RUNTIME_API_BROWSER_PASSWORD`
- `III_RUNTIME_API_CLI_TOKEN`

Set `III_RUNTIME_API_REQUIRE_SECRETS=1` or `III_RUNTIME_API_PROFILE=real` to
fail startup when either secret is missing. Sim/dev profiles may use explicit
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
- `III_RUNTIME_API_SYSTEM_ID=<drone or system id>`
- `III_RUNTIME_API_BROWSER_PASSWORD=<operator login password>`
- `III_RUNTIME_API_CLI_TOKEN=<remote CLI token>`
- `III_RUNTIME_API_HEARTBEAT_INTERVAL_SEC=2`
- `III_RUNTIME_API_SESSION_LEASE_TIMEOUT_SEC=8`
- `III_RUNTIME_API_PX4_MAVLINK_ENDPOINT=<MAVLink endpoint>`
- `III_RUNTIME_API_PX4_ENABLED=1`
- `III_RUNTIME_API_LOG_DIR=<runtime API log directory>`

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
- Use non-default `III_RUNTIME_API_BROWSER_PASSWORD` and
  `III_RUNTIME_API_CLI_TOKEN` values.
- Restrict TCP `8765` and UDP `5353` to the operator network.
- Do not expose `iii-runtime-api` to public or shared networks.
- Treat TLS deferral as an accepted deployment risk until HTTPS/WSS support is
  added.
